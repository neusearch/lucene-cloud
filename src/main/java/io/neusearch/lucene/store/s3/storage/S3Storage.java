package io.neusearch.lucene.store.s3.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Publisher;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.DownloadFileRequest;
import software.amazon.awssdk.transfer.s3.model.FileDownload;
import software.amazon.awssdk.transfer.s3.model.FileUpload;
import software.amazon.awssdk.transfer.s3.model.UploadFileRequest;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
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

    private final S3AsyncClient s3Client;

    S3TransferManager transferManager;

    /**
     * Creates and initializes a new S3 Storage object.
     *
     * @config config the parameters required to initialize S3 clients, not null
     */
    public S3Storage(final Config config) {
        String bucket = config.getBucket();
        String prefix = config.getPrefix();

        this.bucket = bucket;
        while (prefix.endsWith("/")) {
            prefix = prefix.substring(0, prefix.length() - 1);
        }
        this.prefix = prefix + "/";
        this.s3Client = S3AsyncClient.crtBuilder().build();
        this.transferManager = S3TransferManager.builder().s3Client(this.s3Client).build();
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
        ListObjectsV2Publisher responses = s3Client.listObjectsV2Paginator(r -> r.bucket(bucket).prefix(prefix).build());
        ArrayList<S3Object> s3ObjectList = new ArrayList<>();
        responses.contents().subscribe(s3ObjectList::add).join();
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

        return s3Client.headObject(b -> b.bucket(bucket).key(prefix + name)).join().contentLength();
    }

    /**
     * Deletes object matched with the provided name
     *
     * @param name the name of object in the configured bucket prefix
     */
    public void deleteFile(final String name) {
        logger.debug("deleteFile {}", name);

        s3Client.deleteObject(b -> b.bucket(bucket).key(prefix + name)).join();
    }

    /**
     * Renames object based on the provided names
     *
     * @param from the object name to be renamed from
     * @param to the object name to be renamed to
     */
    public void rename(final String from, final String to) {
        logger.debug("rename {} -> {}", from, to);

        s3Client.copyObject(b -> b.sourceBucket(bucket).sourceKey(prefix + from).
                destinationBucket(bucket).destinationKey(prefix + to)).join();
        deleteFile(from);
    }

    /**
     * Reads a range of bytes within an object and writes to a file
     *
     * @param name the object name
     * @param fileOffset the range start offset
     * @param len the length of the range
     * @param file the File object to be written
     */
    public void readRangeToFile(final String name, final int fileOffset,
                    final int len, final File file) {
        logger.debug("readToFile {} -> {}", file.getPath(), buildS3PathFromName(name));

        s3Client.getObject(req -> req.bucket(bucket).key(prefix + name)
                        .range(String.format("bytes=%d-%d", fileOffset, fileOffset + len - 1)),
                AsyncResponseTransformer.toFile(file.toPath())).join();
    }

    /**
     * Reads a whole object and writes to a file
     *
     * @param name the object name
     * @param file the File object to be written
     */
    public void readToFile(final String name, final File file) {
        logger.debug("readToFile {} -> {}", buildS3PathFromName(name), file.getPath());

        s3Client.getObject(req -> req.bucket(bucket).key(prefix + name),
                AsyncResponseTransformer.toFile(file.toPath())).join();
    }

    /**
     * Reads all the objects and writes to a directory
     *
     * @param dir the directory to write all the objects
     */
    public void readAllToDir(final String dir) {
        ArrayList<S3Object> objectList = listAllObjects();
        ArrayList<FileDownload> fileDownloads = new ArrayList<>();
        for (S3Object object : objectList) {
            if (object.key().equals(prefix)) {
                // Skip prefix object
                continue;
            }

            fileDownloads.add(
                    transferManager.downloadFile(
                            DownloadFileRequest.builder()
                                    .getObjectRequest(b -> b.bucket(bucket).key(object.key()))
                                    .destination(Paths.get(dir + "/" + object.key().substring(prefix.length())))
                                    .build()
                    )
            );
        }

        // Wait for all the transfer to complete
        for (FileDownload fileDownload : fileDownloads) {
            fileDownload.completionFuture().join();
        }
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

        ResponseBytes<GetObjectResponse> b = s3Client.getObject(req -> req.bucket(bucket).key(prefix + name)
                        .range(String.format("bytes=%d-%d", fileOffset, fileOffset + len - 1)),
                AsyncResponseTransformer.toBytes()).join();
        int actualLen;
        try (InputStream is = b.asInputStream()) {
            actualLen = is.read(buffer, bufOffset, len);
        }
        return actualLen;
    }

    public byte[] readBytes(final String name, final int offset, final int len) {
        logger.debug("readBytes {} offset {} length {}", name, offset, len);

        ResponseBytes<GetObjectResponse> b = s3Client.getObject(req -> req.bucket(bucket).key(prefix + name)
                        .range(String.format("bytes=%d-%d", offset, offset + len - 1)),
                AsyncResponseTransformer.toBytes()).join();

        return b.asByteArray();
    }

    /**
     * Writes an object using a given file
     *
     * @param filePath the absolute file path
     */
    public void writeFromFile(final Path filePath) {
        logger.debug("writeFromFile {} -> {}", filePath.toString(), buildS3PathFromName(filePath.getFileName().toString()));

        String name = filePath.getFileName().toString();
        s3Client.putObject(b -> b.bucket(bucket).key(prefix + name), filePath).join();
    }

    /**
     * Writes a set of files to S3
     *
     * @param filePaths the list of absolute file paths
     */
    public void writeFromFiles(final List<Path> filePaths) {
        logger.debug("writeFromFiles");
        ArrayList<FileUpload> fileUploads = new ArrayList<>();
        for (Path filePath : filePaths) {
            String name = filePath.getFileName().toString();
            fileUploads.add(
                    transferManager.uploadFile(
                            UploadFileRequest.builder()
                                    .putObjectRequest(b -> b.bucket(bucket).key(prefix + name))
                                    .source(filePath)
                                    .build()
                    )
            );
        }
        // Wait for all the transfer to complete
        for (FileUpload fileUpload : fileUploads) {
            fileUpload.completionFuture().join();
        }
    }

    /**
     * Releases the created S3 clients
     */
    public void close() {
        logger.debug("close\n");

        s3Client.close();
        transferManager.close();
    }

    private String buildS3PathFromName(String name) {
        return bucket + "/" + prefix + "/" + name;
    }

    public static class Config {
        private String bucket;
        private String prefix;

        public Config(final String bucket, final String prefix) {
            this.bucket = bucket;
            this.prefix = prefix;
        }

        public String getBucket() {
            return bucket;
        }

        public void setBucket(String bucket) {
            this.bucket = bucket;
        }

        public String getPrefix() {
            return prefix;
        }

        public void setPrefix(String prefix) {
            this.prefix = prefix;
        }
    }
}