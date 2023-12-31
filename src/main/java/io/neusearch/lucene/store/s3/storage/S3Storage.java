package io.neusearch.lucene.store.s3.storage;

import io.neusearch.lucene.store.s3.S3Directory;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.DownloadFileRequest;
import software.amazon.awssdk.transfer.s3.model.FileDownload;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * A Storage implementation for AWS S3.
 */
public class S3Storage implements Storage {
    private static final Logger logger = LoggerFactory.getLogger(S3Storage.class);
    private final String bucket;

    private final String prefix;

    private final S3Client s3;

    S3TransferManager transferManager;

    /**
     * Creates and initializes a new S3 Storage object.
     *
     * @param params the parameters required to initialize S3 clients, not null
     */
    public S3Storage(HashMap<String, Object> params) {
        String bucket = params.get("bucket").toString();
        String prefix = params.get("prefix").toString();

        this.bucket = bucket.toLowerCase();
        while (prefix.endsWith("/")) {
            prefix = prefix.substring(0, prefix.length() - 1);
        }
        this.prefix = prefix.toLowerCase() + "/";
        this.s3 = S3Client.create();
        this.transferManager = S3TransferManager.create();
    }

    /**
     * Lists all the object names excluding the prefix part in the configured S3 bucket prefix
     *
     * @return the object names array
     */
    public String[] listAll() {
        logger.debug("listAll()");
        ArrayList<String> names = new ArrayList<>();

        ArrayList<String> rawNames = new ArrayList<>();
        ArrayList<S3Object> s3ObjectList = listAllObjects();
        for (S3Object object : s3ObjectList) {
            rawNames.add(object.key());
        }

        // Remove prefix from S3 keys
        for (String rawName : rawNames) {
            if (rawName.equals(prefix)) {
                continue;
            }
            names.add(rawName.substring(prefix.length()));
        }

        logger.debug("listAll {}", names);
        return names.toArray(new String[]{});
    }

    /**
     * Lists all the metadata of the objects in the configured S3 buket prefix
     *
     * @return the object metadata array
     */
    public ArrayList<S3Object> listAllObjects() {
        ListObjectsV2Request request = ListObjectsV2Request.builder()
                .bucket(bucket)
                .prefix(prefix)
                .build();
        ListObjectsV2Iterable responses = s3.listObjectsV2Paginator(request);
        ArrayList<S3Object> s3ObjectList = new ArrayList<>();
        for (ListObjectsV2Response response : responses) {
            s3ObjectList.addAll(response.contents().stream().toList());
        }
        return s3ObjectList;
    }

    /**
     * Gets object length matched with the provided name
     *
     * @param name the name of object in the configured bucket prefix
     * @return the object size in bytes
     */
    public long fileLength(final String name) {
        logger.debug("fileLength {}", name);

        return s3.headObject(b -> b.bucket(bucket).key(prefix + name)).contentLength();
    }

    /**
     * Deletes object matched with the provided name
     *
     * @param name the name of object in the configured bucket prefix
     */
    public void deleteFile(final String name) {
        logger.debug("deleteFile {}", name);

        s3.deleteObject(b -> b.bucket(bucket).key(prefix + name));
    }

    /**
     * Renames object based on the provided names
     *
     * @param from the object name to be renamed from
     * @param to the object name to be renamed to
     */
    public void rename(final String from, final String to) {
        logger.debug("rename {} -> {}", from, to);
        // Assume rename() is not called after commit due to the Lucene's immutable nature
        s3.copyObject(b -> b.sourceBucket(bucket).sourceKey(prefix + from).
                destinationBucket(bucket).destinationKey(prefix + to));
        s3.deleteObject(b -> b.bucket(bucket).key(prefix + from));
    }

    /**
     * Reads a range of bytes within an object and writes to a file
     *
     * @param name the object name
     * @param fileOffset the range start offset
     * @param len the length of the range
     * @param file the File object to be written
     * @throws IOException if writing to the file failed for reasons
     */
    public void readRangeToFile(final String name, final int fileOffset,
                    final int len, final File file) throws IOException {
        logger.debug("readToFile {} -> {}", file.getPath(), buildS3PathFromName(name));
        ResponseInputStream<GetObjectResponse> res = s3.
                getObject(b -> b.bucket(bucket).key(prefix + name)
                        .range(String.format("bytes=%d-%d", fileOffset, fileOffset + len - 1)));

        // Copy the object to a cache page file
        FileUtils.copyInputStreamToFile(res, file);
        res.close();
    }

    /**
     * Reads a whole object and writes to a file
     *
     * @param name the object name
     * @param file the File object to be written
     * @throws IOException if writing to the file failed for reasons
     */
    public void readToFile(final String name, final File file) throws IOException {
        logger.debug("readToFile {} -> {}", buildS3PathFromName(name), file.getPath());
        ResponseInputStream<GetObjectResponse> res = s3.
                getObject(b -> b.bucket(bucket).key(prefix + name));

        // Copy the object to a cache page file
        FileUtils.copyInputStreamToFile(res, file);
        res.close();
    }

    /**
     * Reads all the objects and writes to a directory
     *
     * @param dir the directory to write all the objects
     * @param s3Directory the caller's object to access/change the stats of the directory
     */
    public void readAllToDir(final String dir, final S3Directory s3Directory) {
        Long currentDirSize = s3Directory.getCurrentLocalCacheSize();
        Long maxDirSize = s3Directory.getMaxLocalCacheSize();
        ArrayList<S3Object> objectList = listAllObjects();
        ArrayList<FileDownload> fileDownloads = new ArrayList<>();
        for (S3Object object : objectList) {
            if (object.key().equals(prefix)) {
                // Skip prefix object
                continue;
            }
            if (currentDirSize + object.size() < maxDirSize) {
                currentDirSize += object.size();
                fileDownloads.add(
                        transferManager.downloadFile(
                                DownloadFileRequest.builder()
                                        .getObjectRequest(b -> b.bucket(bucket).key(object.key()))
                                        .destination(Paths.get(dir + "/" + object.key().substring(prefix.length())))
                                        .build()
                        )
                );
            } else {
                logger.info("max cahce size is reached current {} object {} max {}", currentDirSize, object.size(), maxDirSize);
                break;
            }
        }
        // Wait for all the transfer to complete
        for (FileDownload fileDownload : fileDownloads) {
            fileDownload.completionFuture().join();
        }
        s3Directory.setCurrentLocalCacheSize(currentDirSize);
    }

    /**
     * Reads a range of bytes from an object and writes to a specific offset of a given buffer
     *
     * @param name the object name
     * @param buffer the buffer to populate read data
     * @param bufOffset the start offset inside the buffer
     * @param fileOffset the start offset inside the object
     * @param len the length to read from the object
     * @return the read bytes
     * @throws IOException if copying into buffer failed for reasons
     */
    public int readBytes(final String name, final byte[] buffer, final int bufOffset, final int fileOffset, final int len) throws IOException {
        logger.debug("readBytes {} bufOffset {} fileOffset {} length {}", name, bufOffset, fileOffset, len);
        ResponseInputStream<GetObjectResponse> res = s3.
                getObject(b -> b.bucket(bucket).key(prefix + name)
                        .range(String.format("bytes=%d-%d", fileOffset, fileOffset + len - 1)));

        int bytesRead = res.readNBytes(buffer, bufOffset, len);
        res.close();
        return bytesRead;
    }

    /**
     * Writes an object using a given file
     *
     * @param filePath the absolute file path
     */
    public void writeFromFile(final Path filePath) {
        logger.debug("writeFromFile {} -> {}", filePath.toString(), buildS3PathFromName(filePath.getFileName().toString()));
        String name = filePath.getFileName().toString();
        s3.putObject(b -> b.bucket(bucket).key(prefix + name), filePath);
    }

    /**
     * Releases the created S3 clients
     */
    public void close() {
        logger.debug("close\n");
        s3.close();
        transferManager.close();
    }

    private String buildS3PathFromName(String name) {
        return bucket + "/" + prefix + "/" + name;
    }
}