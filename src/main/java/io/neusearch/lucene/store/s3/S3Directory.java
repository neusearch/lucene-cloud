package io.neusearch.lucene.store.s3;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;

import io.neusearch.lucene.store.s3.storage.Storage;
import io.neusearch.lucene.store.s3.storage.StorageFactory;
import org.apache.commons.io.FileUtils;
import org.apache.lucene.store.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A S3 based implementation of a Lucene <code>Directory</code> allowing the storage of a Lucene index within S3.
 * The directory works against a single object prefix, where the binary data is stored in <code>objects</code>.
 * Each "object" has an entry in the S3.
 *
 * @author swkim86
 */
public class S3Directory extends FSDirectory {
    private static final Logger logger = LoggerFactory.getLogger(S3Directory.class);

    private static final String storageType = "s3";

    private final Storage storage;

    private final FSDirectory localCache;

    private final Long maxLocalCacheSize; // Cache capacity in bytes

    private Long currentLocalCacheSize;

    /**
     * Creates a new S3 directory.
     *
     * @param bucket The bucket name
     */
    public S3Directory(final String bucket, String prefix, final String localCachePath, final Long localCacheSize) throws IOException {
        super(Paths.get("/tmp"), FSLockFactory.getDefault());

        StorageFactory storageFactory = new StorageFactory();
        HashMap<String, Object> storageParams = new HashMap<>();
        storageParams.put("bucket", bucket);
        storageParams.put("prefix", prefix);
        this.storage = storageFactory.createStorage(storageType, storageParams);

        Path localCacheDir = Paths.get(localCachePath);
        // Cleanup cache directory
        FileUtils.deleteDirectory(localCacheDir.toFile());
        this.localCache = FSDirectory.open(localCacheDir);
        this.maxLocalCacheSize = localCacheSize;
        this.currentLocalCacheSize = 0L;
        prePopulateCache(localCachePath);
        logger.debug("S3Directory ({} {} {})", bucket, prefix, localCachePath);
    }

    /**
     * ********************************************************************************************
     * DIRECTORY METHODS
     * ********************************************************************************************
     */
    @Override
    public String[] listAll() {
        logger.debug("listAll()");
        ensureOpen();
        ArrayList<String> names = new ArrayList<>();
        try {
            // Get file list in storage
            String[] storagePaths = storage.listAll();
            names.addAll(Arrays.stream(storagePaths).toList());

            // Get file list in buffer
            String[] filePaths = localCache.listAll();

            // Add buffer paths to list
            if (filePaths.length > 0) {
                names.addAll(Arrays.stream(filePaths).toList());

                // Remove potential duplicates between storage and buffer
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

        if (Files.exists(localCache.getDirectory().resolve(name))) {
            currentLocalCacheSize -= localCache.fileLength(name);
            localCache.deleteFile(name);
        }
        storage.deleteFile(name);
    }

    @Override
    public long fileLength(final String name) throws IOException {
        logger.debug("fileLength {}", name);
        ensureOpen();
        if (Files.exists(localCache.getDirectory().resolve(name))) {
            return localCache.fileLength(name);
        } else {
            // A file can be located either buffer or storage
            return storage.fileLength(name);
        }
    }

    @Override
    public IndexOutput createOutput(final String name, final IOContext context) throws IOException {
        logger.debug("createOutput {}", name);

        // Output always goes to local files first before sync to S3

        ensureOpen();
        return localCache.createOutput(name, context);
    }

    @Override
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
        logger.debug("createTempOutput {} {}\n", prefix, suffix);

        // Temp output does not need to sync to S3
        ensureOpen();
        return localCache.createTempOutput(prefix, suffix, context);
    }

    @Override
    public void sync(final Collection<String> names) throws IOException {
        logger.debug("sync {}", names);
        ensureOpen();
        // Sync all the buffered files that have not been written to storage yet
        for (String name : names) {
            Path filePath = localCache.getDirectory().resolve(name);
            if (Files.exists(filePath)) {
                currentLocalCacheSize += localCache.fileLength(name);
                storage.writeFromFile(filePath);
            }
        }
    }

    @Override
    public void syncMetaData() throws IOException {
        logger.debug("syncMetaData\n");
        ensureOpen();
    }

    @Override
    public void rename(final String from, final String to) throws IOException {
        logger.debug("rename {} -> {}", from, to);
        ensureOpen();
        if (Files.exists(localCache.getDirectory().resolve(from))) {
            localCache.rename(from, to);
        }
        storage.rename(from, to);
    }

    @Override
    public synchronized void close() throws IOException {
        logger.debug("close\n");

        isOpen = false;
        storage.close();
        localCache.close();
    }

    @Override
    public IndexInput openInput(final String name, final IOContext context) throws IOException {
        logger.debug("openInput {}", name);

        ensureOpen();
        Path filePath = localCache.getDirectory().resolve(name);
        if (Files.notExists(filePath)) {
            long fileLength = fileLength(name);
            long sizeAfter = currentLocalCacheSize + fileLength;
            if (sizeAfter >= maxLocalCacheSize) {
                // List access times of all the cached files
                List<String> cachedFileNameList = getCachedFilesSizeSortedList();
                // Reclaim cache space based on last access time
                for (String fileName : cachedFileNameList) {
                    currentLocalCacheSize -= localCache.fileLength(fileName);
                    localCache.deleteFile(fileName);
                    sizeAfter = currentLocalCacheSize + fileLength;
                    if (sizeAfter < maxLocalCacheSize) {
                        break;
                    }
                }
            }
            // Read file into local cache from storage
            storage.readToFile(name, filePath.toFile());
            currentLocalCacheSize += fileLength;
        }

        return localCache.openInput(name, context);
    }

    private List<String> getCachedFilesLruList() throws IOException {
        List<String> fileNames = Arrays.asList(localCache.listAll());
        fileNames.sort((o1, o2) -> {
            try {
                Path path1 = localCache.getDirectory().resolve(o1);
                Path path2 = localCache.getDirectory().resolve(o2);
                BasicFileAttributes attr1 = Files.readAttributes(path1, BasicFileAttributes.class);
                BasicFileAttributes attr2 = Files.readAttributes(path2, BasicFileAttributes.class);
                return attr1.lastAccessTime().compareTo(attr2.lastAccessTime());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        return fileNames;
    }

    private List<String> getCachedFilesSizeSortedList() throws IOException {
        List<String> fileNames = Arrays.asList(localCache.listAll());
        fileNames.sort((o1, o2) -> {
            try {
                return Long.compare(localCache.fileLength(o1), localCache.fileLength(o2));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        return fileNames.reversed();
    }

    private void prePopulateCache(final String path) {
        storage.readAllToDir(path);
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
