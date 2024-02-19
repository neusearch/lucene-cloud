package io.neusearch.lucene.store.s3;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import io.neusearch.lucene.store.s3.cache.FSCache;
import io.neusearch.lucene.store.s3.index.S3IndexInput;
import io.neusearch.lucene.store.s3.storage.S3Storage;
import io.neusearch.lucene.store.s3.storage.Storage;
import io.neusearch.lucene.store.s3.storage.StorageFactory;
import org.apache.lucene.store.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.lucene.index.IndexFileNames.PENDING_SEGMENTS;

/**
 * A S3 based implementation of a Lucene <code>Directory</code> allowing the storage of a Lucene index within S3.
 * The directory works against a single object prefix, where the binary data is stored in <code>objects</code>.
 * Each "object" has an entry in the S3.
 */
public class S3Directory extends Directory {
    private static final Logger logger = LoggerFactory.getLogger(S3Directory.class);
    protected volatile boolean isOpen = true;

    private static final String storageType = "s3";

    private final Storage storage;

    private final FSCache fsCache;

    private final Map<String,Boolean> bufferedFileMap;
    private final Map<String,Boolean> syncedFileMap;
    private final Map<String,Boolean> renamedFileMap;
    private final Map<String,Map<Long,Boolean>> cachedFileMap;

    private static final StorageFactory storageFactory = new StorageFactory();

    /**
     * Creates a new S3 directory with the provided block size.
     *
     * @param s3Config the S3 configurations
     * @param fsCachePath the FS path to be used as a buffer/cache for a backend storage
     * @param blockSize the block size of the FS cache
     * @throws IOException if initializing cache directory failed for reasons
     */
    public S3Directory(final S3Storage.Config s3Config,
                       final String fsCachePath, final long blockSize) throws IOException {
        super();

        S3IndexInput.BLOCK_SIZE = blockSize;
        this.storage = storageFactory.createStorage(storageType, s3Config);
        this.fsCache = new FSCache(Paths.get(fsCachePath));
        this.bufferedFileMap = new ConcurrentHashMap<>();
        this.syncedFileMap = new HashMap<>();
        this.renamedFileMap = new HashMap<>();
        this.cachedFileMap = new ConcurrentHashMap<>();
        prePopulateCache(fsCache);

        logger.debug("S3Directory ({} {})", s3Config, fsCachePath);
    }

    /**
     * Creates a new S3 directory.
     *
     * @param s3Config the S3 configurations
     * @param fsCachePath the FS path to be used as a buffer/cache for a backend storage
     * @throws IOException if initializing cache directory failed for reasons
     */
    public S3Directory(final S3Storage.Config s3Config,
                       final String fsCachePath) throws IOException {
        super();

        S3IndexInput.BLOCK_SIZE = S3IndexInput.DEFAULT_BLOCK_SIZE;
        this.storage = storageFactory.createStorage(storageType, s3Config);
        this.fsCache = new FSCache(Paths.get(fsCachePath));
        this.bufferedFileMap = new ConcurrentHashMap<>();
        this.syncedFileMap = new HashMap<>();
        this.renamedFileMap = new HashMap<>();
        this.cachedFileMap = new ConcurrentHashMap<>();
        prePopulateCache(fsCache);

        logger.debug("S3Directory ({} {})", s3Config, fsCachePath);
    }

    @Override
    public String[] listAll() {
        logger.debug("listAll()");
        ensureOpen();
        ArrayList<String> names = new ArrayList<>();
        try {
            // Get file list in storage
            String[] storagePaths = storage.listAll();
            names.addAll(Arrays.stream(storagePaths).toList());

            // Add buffered file names to list
            if (!bufferedFileMap.isEmpty()) {
                for (Map.Entry<String,Boolean> entry : bufferedFileMap.entrySet()) {
                    String name = entry.getKey();
                    names.add(name);
                }
            }

            // Remove potential duplicates between storage and buffer
            names = new ArrayList<>(new HashSet<>(names));
            // The output must be in sorted (UTF-16, java's {@link String#compareTo}) order.
            names.sort(String::compareTo);
        } catch (Exception e) {
            logger.error("{}", e.toString());
        }
        logger.debug("listAll {}", names);
        return names.toArray(new String[]{});
    }

    @Override
    public void deleteFile(final String name) throws IOException {
        logger.debug("deleteFile {}", name);

        if (bufferedFileMap.get(name) != null) {
            fsCache.deleteFile(name);
            bufferedFileMap.remove(name);
        } else if (syncedFileMap.get(name) != null) {
            fsCache.deleteFile(name);
            syncedFileMap.remove(name);
            storage.deleteFile(name);
        } else if (cachedFileMap.get(name) != null) {
            fsCache.deleteFile(name);
            Map<Long,Boolean> cachedBlockMap = cachedFileMap.get(name);
            if (cachedBlockMap != null) {
                cachedBlockMap.clear();
            }
            cachedFileMap.remove(name);
            storage.deleteFile(name);
        } else {
            storage.deleteFile(name);
        }
    }

    @Override
    public long fileLength(final String name) throws IOException {
        logger.debug("fileLength {}", name);
        ensureOpen();
        if (isCached(name)) {
            return fsCache.fileLength(name);
        } else {
            // A file can be located either buffer or storage
            return storage.fileLength(name);
        }
    }

    @Override
    public IndexOutput createOutput(final String name, final IOContext context) throws IOException {
        logger.debug("createOutput {}", name);

        // Output always goes to FS cache first before sync to S3
        ensureOpen();
        IndexOutput indexOutput = fsCache.createOutput(name, context);
        bufferedFileMap.put(indexOutput.getName(), true);

        return indexOutput;
    }

    @Override
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
        logger.debug("createTempOutput {} {}\n", prefix, suffix);

        // Temp output does not need to sync to S3
        ensureOpen();
        IndexOutput indexOutput = fsCache.createTempOutput(prefix, suffix, context);
        bufferedFileMap.put(indexOutput.getName(), true);

        return indexOutput;
    }

    @Override
    public void sync(final Collection<String> names) {
        logger.debug("sync {}", names);
        ensureOpen();
        // Sync all the requested buffered files that have not been written to storage yet
        List<Path> filePaths = new ArrayList<>();
        for (String name : names) {
            if (bufferedFileMap.get(name) != null && !isTempFile(name)) {
                // Do not sync temporary files
                filePaths.add(fsCache.buildFullPath(name));
                syncedFileMap.put(name, true);
                bufferedFileMap.remove(name);
            }
        }

        if (!filePaths.isEmpty()) {
            storage.writeFromFiles(filePaths);
        }
    }

    @Override
    public void syncMetaData() {
        logger.debug("syncMetaData\n");
        ensureOpen();

        // Sync all the buffered files to storage
        List<Path> filePaths = new ArrayList<>();
        for (Map.Entry<String,Boolean> entry : renamedFileMap.entrySet()) {
            String name = entry.getKey();
            filePaths.add(fsCache.buildFullPath(name));
            syncedFileMap.put(name, true);
            bufferedFileMap.remove(name);
        }

        if (!filePaths.isEmpty()) {
            storage.writeFromFiles(filePaths);
        }

        renamedFileMap.clear();
    }

    @Override
    public void rename(final String from, final String to) throws IOException {
        logger.debug("rename {} -> {}", from, to);
        ensureOpen();

        if (bufferedFileMap.get(from) != null) {
            fsCache.rename(from, to);
            bufferedFileMap.remove(from);
            bufferedFileMap.put(to, true);
        } else if (syncedFileMap.get(from) != null) {
            fsCache.rename(from, to);
            storage.rename(from, to);
            syncedFileMap.remove(from);
            syncedFileMap.put(to, true);
        } else if (cachedFileMap.get(from) != null) {
            fsCache.rename(from, to);
            storage.rename(from, to);
            Map<Long,Boolean> cachedBlockMap = cachedFileMap.get(from);
            cachedFileMap.remove(from);
            cachedFileMap.put(to, cachedBlockMap);
        } else {
            // The requested file is not in memory
            storage.rename(from, to);
        }

        renamedFileMap.put(to, true);
    }

    @Override
    public synchronized void close() throws IOException {
        logger.debug("close\n");

        isOpen = false;
        bufferedFileMap.clear();
        syncedFileMap.clear();
        for (Map.Entry<String,Map<Long,Boolean>> entry : cachedFileMap.entrySet()) {
            entry.getValue().clear();
        }
        cachedFileMap.clear();

        storage.close();
        fsCache.close();
    }

    @Override
    public IndexInput openInput(final String name, final IOContext context) throws IOException {
        logger.debug("openInput {}", name);

        ensureOpen();
        if (bufferedFileMap.get(name) != null
                || syncedFileMap.get(name) != null) {
            // The requested file is fully populated in the FS cache
            return fsCache.openInput(name, context);
        } else {
            Map<Long, Boolean> cachedBlockMap =
                    cachedFileMap.computeIfAbsent(name, k -> new HashMap<>());
            return new S3IndexInput(name, storage,
                    fsCache, cachedBlockMap, context);
        }
    }

    @Override
    public Set<String> getPendingDeletions() {
        return Collections.emptySet();
    }

    @Override
    public final Lock obtainLock(String name) throws IOException {
        return fsCache.obtainLock(name);
    }

    @Override
    protected final void ensureOpen() throws AlreadyClosedException {
        if (!isOpen) {
            throw new AlreadyClosedException("this Directory is closed");
        }
    }

    /**
     * Gets an LRU list of locally cached files.
     *
     * @return the list of file names
     * @throws IOException if there is an I/O error during file metadata read
     *
     */
    private List<String> getCachedFilesLruList() throws IOException {
        List<String> fileNames = Arrays.asList(fsCache.listAll());
        fileNames.sort((o1, o2) -> {
            try {
                Path path1 = fsCache.buildFullPath(o1);
                Path path2 = fsCache.buildFullPath(o2);
                BasicFileAttributes attr1 = Files.readAttributes(path1, BasicFileAttributes.class);
                BasicFileAttributes attr2 = Files.readAttributes(path2, BasicFileAttributes.class);
                return attr1.lastAccessTime().compareTo(attr2.lastAccessTime());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        return fileNames;
    }

    /**
     * Gets a sorted list by size of locally cached files.
     *
     * @return the list of file names
     * @throws IOException if there is an I/O error during file metadata read
     */
    private List<String> getCachedFilesSizeSortedList() throws IOException {
        List<String> fileNames = Arrays.asList(fsCache.listAll());
        fileNames.sort((o1, o2) -> {
            try {
                return Long.compare(fsCache.fileLength(o1), fsCache.fileLength(o2));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        return fileNames.reversed();
    }

    /**
     * Pre-populates local cache directory to avoid storage reads in performance-critical paths.
     *
     * @param fsCache the FS cache object
     */
    private void prePopulateCache(FSCache fsCache) throws IOException {
        // Populate all the first and last blocks to cache from storage
        storage.readAllInitialBlocksToCache(fsCache, cachedFileMap);
    }

    private boolean isTempFile(String name) {
        return name.endsWith("tmp") || name.startsWith(PENDING_SEGMENTS);
    }

    private boolean isCached(String name) {
        return bufferedFileMap.get(name) != null
                || syncedFileMap.get(name) != null
                || cachedFileMap.get(name) != null;
    }
}
