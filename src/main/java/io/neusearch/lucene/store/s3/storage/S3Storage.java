package io.neusearch.lucene.store.s3.storage;

import io.neusearch.lucene.store.s3.cache.FSCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.paginators.ListObjectVersionsPublisher;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Publisher;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.*;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.CompletableFuture;

import static io.neusearch.lucene.store.s3.index.S3IndexInput.BLOCK_SIZE;

/**
 * A Storage implementation for AWS S3.
 */
public class S3Storage implements Storage {
    private static final Logger logger = LoggerFactory.getLogger(S3Storage.class);
    private final String bucket;

    private final String prefix;

    private final S3AsyncClient s3Client;

    S3TransferManager transferManager;
    private final Map<String,S3Object> cachedObjectMap;

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
        this.cachedObjectMap = new HashMap<>();
    }

    /**
     * Lists all the object names excluding the prefix part in the configured S3 bucket prefix
     *
     * @return the object names array
     */
    public String[] listAll() {
        logger.debug("listAll()");
        List<String> names = new ArrayList<>();
        List<S3Object> s3ObjectList = listAllObjects();
        for (S3Object object : s3ObjectList) {
            if (object.key().equals(prefix)) {
                continue;
            }

            // Remove prefix from S3 keys
            String name = object.key().substring(prefix.length());
            names.add(name);

            // Update metadata cache map
            cachedObjectMap.put(name, object);
        }

        logger.debug("listAll {}", names);
        return names.toArray(new String[]{});
    }

    /**
     * Lists all the metadata of the objects in the configured S3 buket prefix
     *
     * @return the object metadata array
     */
    public List<S3Object> listAllObjects() {
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

        S3Object cachedObject = cachedObjectMap.get(name);
        if (cachedObject != null) {
            return cachedObject.size();
        } else {
            return s3Client.headObject(b -> b.bucket(bucket).key(prefix + name)).join().contentLength();
        }
    }

    /**
     * Deletes object matched with the provided name
     *
     * @param name the name of object in the configured bucket prefix
     */
    public void deleteFile(final String name) {
        logger.debug("deleteFile {}", name);

        s3Client.deleteObject(b -> b.bucket(bucket).key(prefix + name)).join();
        cachedObjectMap.remove(name);
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
        S3Object cachedObject = cachedObjectMap.get(from);
        if (cachedObject != null) {
            cachedObjectMap.put(to, cachedObject);
        }
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

        try {
            s3Client.getObject(req -> req.bucket(bucket).key(prefix + name)
                            .range(String.format("bytes=%d-%d", fileOffset, fileOffset + len - 1)),
                    AsyncResponseTransformer.toFile(file.toPath())).join();
        } catch (Exception e) {
            if (e.getCause() instanceof NoSuchKeyException) {
                // The object may be deleted concurrently, try again with the version ID
                s3Client.getObject(req -> req.bucket(bucket).key(prefix + name)
                                .versionId(getVersionId(name))
                                .range(String.format("bytes=%d-%d", fileOffset, fileOffset + len - 1)),
                        AsyncResponseTransformer.toFile(file.toPath())).join();
            } else {
                throw e;
            }
        }
    }

    /**
     * Reads a whole object and writes to a file
     *
     * @param name the object name
     * @param file the File object to be written
     */
    public void readToFile(final String name, final File file) {
        logger.debug("readToFile {} -> {}", buildS3PathFromName(name), file.getPath());

        try {
            s3Client.getObject(req -> req.bucket(bucket).key(prefix + name),
                    AsyncResponseTransformer.toFile(file.toPath())).join();
        } catch (Exception e) {
            if (e.getCause() instanceof NoSuchKeyException) {
                // The object may be deleted concurrently, try again with the version ID
                s3Client.getObject(req -> req.bucket(bucket).key(prefix + name)
                                .versionId(getVersionId(name)),
                        AsyncResponseTransformer.toFile(file.toPath())).join();
            } else {
                throw e;
            }
        }
    }

    /**
     * Reads all the objects and writes to a directory
     *
     * @param dir the directory to write all the objects
     */
    public void readAllToDir(final String dir) {
        List<S3Object> objectList = listAllObjects();
        List<FileDownload> fileDownloads = new ArrayList<>();
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

    public void readAllInitialBlocksToCache(FSCache fsCache,
                                            Map<String, Map<Long,Boolean>> cachedFileMap) throws IOException {
        List<S3Object> objectList = listAllObjects();

        List<IODescriptor> ioDescs = new ArrayList<>();

        // Submit all the requests asynchronously
        for (S3Object object : objectList) {
            if (object.key().equals(prefix)) {
                // Skip prefix object
                continue;
            }

            // Read the first block from S3
            final long firstLen = object.size() > BLOCK_SIZE ? BLOCK_SIZE : object.size();
            String name = object.key().substring(prefix.length());
            ioDescs.add(new IODescriptor(name, 0, firstLen,
                    s3Client.getObject(req -> req.bucket(bucket).key(object.key())
                                    .range(String.format("bytes=%d-%d", 0, firstLen - 1)),
                            AsyncResponseTransformer.toBytes())));

            if (object.size() > BLOCK_SIZE) {
                // This object is larger than the BLOCK_SIZE
                long lastOffset = (object.size() / BLOCK_SIZE) * BLOCK_SIZE;
                long lastLen = object.size() > lastOffset + BLOCK_SIZE ?
                        BLOCK_SIZE : object.size() - lastOffset;

                ioDescs.add(new IODescriptor(name, lastOffset, lastLen,
                        s3Client.getObject(req -> req.bucket(bucket).key(object.key())
                                        .range(String.format("bytes=%d-%d", lastOffset, lastOffset + lastLen - 1)),
                                AsyncResponseTransformer.toBytes())));
            }
        }

        // Write all the blocks to the corresponding positions of the files
        RandomAccessFile file;
        Map<Long, Boolean> cachedBlockMap;
        ResponseBytes<GetObjectResponse> respBytes;
        for (IODescriptor ioDesc : ioDescs) {
            file = new RandomAccessFile(fsCache.getDirectory()
                    .resolve(ioDesc.name).toFile(), "rw");

            try {
                 respBytes = ioDesc.resp.join();
            } catch (Exception e) {
                if (e.getCause() instanceof NoSuchKeyException) {
                    // The object may be deleted concurrently, try again with the version ID
                    respBytes = s3Client.getObject(req -> req.bucket(bucket).key(prefix + ioDesc.name)
                                    .versionId(getVersionId(ioDesc.name))
                                    .range(String.format("bytes=%d-%d", ioDesc.offset,
                                            ioDesc.offset + ioDesc.length - 1)),
                            AsyncResponseTransformer.toBytes()).join();
                } else {
                    throw e;
                }
            }

            file.seek(ioDesc.offset);
            file.write(respBytes.asByteArray());
            file.close();

            // Update cached block map
            cachedBlockMap =
                    cachedFileMap.computeIfAbsent(ioDesc.name, k -> new HashMap<>());

            long blockIdx = ioDesc.offset / BLOCK_SIZE;
            cachedBlockMap.put(blockIdx, true);
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

        ResponseBytes<GetObjectResponse> b;
        try {
            b = s3Client.getObject(req -> req.bucket(bucket).key(prefix + name)
                            .range(String.format("bytes=%d-%d", fileOffset, fileOffset + len - 1)),
                    AsyncResponseTransformer.toBytes()).join();
        } catch (Exception e) {
            if (e.getCause() instanceof NoSuchKeyException) {
                b = s3Client.getObject(req -> req.bucket(bucket).key(prefix + name)
                                .versionId(getVersionId(name))
                                .range(String.format("bytes=%d-%d", fileOffset, fileOffset + len - 1)),
                        AsyncResponseTransformer.toBytes()).join();
            } else {
                throw e;
            }
        }

        int actualLen;
        try (InputStream is = b.asInputStream()) {
            actualLen = is.read(buffer, bufOffset, len);
        }
        return actualLen;
    }

    public byte[] readBytes(final String name, final int offset, final int len) {
        logger.debug("readBytes {} offset {} length {}", name, offset, len);

        ResponseBytes<GetObjectResponse> b;
        try {
            b = s3Client.getObject(req -> req.bucket(bucket).key(prefix + name)
                            .range(String.format("bytes=%d-%d", offset, offset + len - 1)),
                    AsyncResponseTransformer.toBytes()).join();
        } catch (Exception e) {
            if (e.getCause() instanceof NoSuchKeyException) {
                b = s3Client.getObject(req -> req.bucket(bucket).key(prefix + name)
                                .versionId(getVersionId(name))
                                .range(String.format("bytes=%d-%d", offset, offset + len - 1)),
                        AsyncResponseTransformer.toBytes()).join();
            } else {
                throw e;
            }
        }

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

    private record IODescriptor(String name, long offset, long length,
                                CompletableFuture<ResponseBytes<GetObjectResponse>> resp) {
    }

    private String getVersionId(String name) {
        ListObjectVersionsPublisher responses =
                s3Client.listObjectVersionsPaginator(r -> r.bucket(bucket).prefix(prefix + name).build());
        ArrayList<ObjectVersion> objectList = new ArrayList<>();
        responses.versions().subscribe(objectList::add).join();
        return objectList.getFirst().versionId();
    }
}