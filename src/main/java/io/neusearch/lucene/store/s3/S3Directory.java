package io.neusearch.lucene.store.s3;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.util.*;

import io.neusearch.lucene.store.s3.buffer.Buffer;
import io.neusearch.lucene.store.s3.buffer.BufferFactory;
import io.neusearch.lucene.store.s3.cache.Cache;
import io.neusearch.lucene.store.s3.cache.CacheFactory;
import io.neusearch.lucene.store.s3.index.S3IndexOutput;
import io.neusearch.lucene.store.s3.storage.Storage;
import io.neusearch.lucene.store.s3.storage.StorageFactory;
import org.apache.lucene.store.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.neusearch.lucene.store.s3.index.S3IndexInput;

import java.util.concurrent.atomic.AtomicLong;

/**
 * A S3 based implementation of a Lucene <code>Directory</code> allowing the storage of a Lucene index within S3.
 * The directory works against a single object prefix, where the binary data is stored in <code>objects</code>.
 * Each "object" has an entry in the S3.
 *
 * @author swkim86
 */
public class S3Directory extends BaseDirectory {
    private static final Logger logger = LoggerFactory.getLogger(S3Directory.class);

    private final String storageType = "s3";
    private final String bufferType = "fs";
    private final String cacheType = "fs";

    private final Storage storage;

    private final Buffer buffer;

    private final Cache cache;

    private final AtomicLong nextTempFileCounter = new AtomicLong();

    /**
     * Creates a new S3 directory.
     *
     * @param bucket The bucket name
     */
    public S3Directory(final String bucket, String prefix, final String bufferPath, final String cachePath) throws IOException {
        super(FSLockFactory.getDefault());

        StorageFactory storageFactory = new StorageFactory();
        HashMap<String, Object> storageParams = new HashMap<>();
        storageParams.put("bucket", bucket);
        storageParams.put("prefix", prefix);
        this.storage = storageFactory.createStorage(storageType, storageParams);

        BufferFactory bufferFactory = new BufferFactory();
        HashMap<String, Object> bufferParams = new HashMap<>();
        bufferParams.put("bufferPath", bufferPath);
        bufferParams.put("storage", this.storage);
        this.buffer = bufferFactory.createBuffer(bufferType, bufferParams);

        CacheFactory cacheFactory = new CacheFactory();
        HashMap<String, Object> cacheParams = new HashMap<>();
        cacheParams.put("cachePath", cachePath);
        cacheParams.put("buffer", this.buffer);
        cacheParams.put("storage", this.storage);
        this.cache = cacheFactory.createCache(cacheType, cacheParams);

        logger.debug("S3Directory ({} {} {} {})", bucket, prefix, bufferPath, cachePath);
    }

    /**
     * ********************************************************************************************
     * DIRECTORY METHODS
     * ********************************************************************************************
     */
    @Override
    public String[] listAll() {
        logger.debug("listAll()");

        ArrayList<String> names = new ArrayList<>();
        try {
            // Get file list in storage
            String[] storagePaths = storage.listAll();
            names.addAll(Arrays.stream(storagePaths).toList());

            // Get file list in buffer
            String[] filePaths = buffer.listAll();

            // Add buffer paths to list
            if (filePaths.length > 0) {
                names.addAll(Arrays.stream(filePaths).toList());

                // Remove potential duplicates between S3 and local file system
                names = new ArrayList<>(new HashSet<>(names));
                // The output must be in sorted (UTF-16, java's {@link String#compareTo}) order.
                names.sort(String::compareTo);
            }
        } catch (Exception e) {
            logger.error("{}", e.toString());
        }
        logger.debug("listAll {}", names);
        return names.toArray(new String[]{});
    }

    @Override
    public void deleteFile(final String name) throws IOException {
        logger.debug("deleteFile {}", name);

        storage.deleteFile(name);
        buffer.deleteFile(name);
        cache.deleteFile(name);
    }

    @Override
    public long fileLength(final String name) throws IOException {
        logger.debug("fileLength {}", name);
        long length = buffer.fileLength(name);
        if (length == -1) {
            length = storage.fileLength(name);
        }
        return length;
    }

    @Override
    public IndexOutput createOutput(final String name, final IOContext context) throws IOException {
        logger.debug("createOutput {}", name);

        // Output always goes to local files first before sync to S3

        ensureOpen();
        return new S3IndexOutput(name, buffer);
    }

    @Override
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
        logger.debug("createTempOutput {} {}\n", prefix, suffix);

        // Temp output does not need to sync to S3
        ensureOpen();
        while (true) {
            try {
                String name = getTempFileName(prefix, suffix, nextTempFileCounter.getAndIncrement());
                return new S3IndexOutput(name, buffer);
            } catch (
                    @SuppressWarnings("unused")
                    FileAlreadyExistsException faee) {
                // Retry with next incremented name
            }
        }
    }

    @Override
    public void sync(final Collection<String> names) throws IOException {
        logger.debug("sync {}", names);
        ensureOpen();
        buffer.sync(names);
    }

    @Override
    public void syncMetaData() throws IOException {
        logger.debug("syncMetaData\n");

        buffer.syncMetaData();
    }

    @Override
    public void rename(final String from, final String to) throws IOException {
        logger.debug("rename {} -> {}", from, to);
        ensureOpen();
        storage.rename(from, to);
        buffer.rename(from, to);
        cache.rename(from, to);
    }

    @Override
    public synchronized void close() throws IOException {
        logger.debug("close\n");

        isOpen = false;
        cache.close();
        buffer.close();
        storage.close();
    }

    @Override
    public IndexInput openInput(final String name, final IOContext context) throws IOException {
        logger.debug("openInput {}", name);

        ensureOpen();
        return new S3IndexInput(name, cache);
    }

    /**
     * *********************************************************************************************
     * Setter/getter methods
     * *********************************************************************************************
     */

    @Override
    public Set<String> getPendingDeletions() {
        return Collections.emptySet();
    }
}
