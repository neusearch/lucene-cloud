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

/**
 * A S3 based implementation of a Lucene <code>Directory</code> allowing the storage of a Lucene index within S3.
 * The directory works against a single object prefix, where the binary data is stored in <code>objects</code>.
 * Each "object" has an entry in the S3.
 */
public class S3Directory extends FSDirectory {
    private static final Logger logger = LoggerFactory.getLogger(S3Directory.class);

    private static final String storageType = "s3";

    private final Storage storage;

    private final FSCache fsCache;

    private static final Map<String,Boolean> bufferedFileMap = new HashMap<>();

    private static final ConcurrentHashMap<String,Map<Long,Boolean>> cachedFileMap = new ConcurrentHashMap<>();

    private static final StorageFactory storageFactory = new StorageFactory();

    /**
     * Creates a new S3 directory.
     *
     * @param s3Config the S3 configurations
     * @param fsCachePath the FS path to be used as a buffer/cache for a backend storage
     * @throws IOException if initializing cache directory failed for reasons
     */
    public S3Directory(final S3Storage.Config s3Config,
                       final String fsCachePath) throws IOException {
        super(Paths.get("/tmp"), FSLockFactory.getDefault());

        this.storage = storageFactory.createStorage(storageType, s3Config);
        this.fsCache = new FSCache(Paths.get(fsCachePath));
        prePopulateCache(fsCachePath);

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

            // Add buffer paths to list
            if (!bufferedFileMap.isEmpty()) {
                for (Map.Entry<String,Boolean> entry : bufferedFileMap.entrySet()) {
                    String name = entry.getKey();
                    names.add(name);
                }

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

        fsCache.deleteFile(name);
        storage.deleteFile(name);
    }

    @Override
    public long fileLength(final String name) throws IOException {
        logger.debug("fileLength {}", name);
        ensureOpen();
        if (bufferedFileMap.get(name) != null) {
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
        ArrayList<Path> filePaths = new ArrayList<>();
        for (String name : names) {
            if (bufferedFileMap.get(name) != null) {
                filePaths.add(fsCache.buildFullPath(name));
            }
        }
        storage.writeFromFiles(filePaths);
    }

    @Override
    public void syncMetaData() {
        logger.debug("syncMetaData\n");
        ensureOpen();
        // Do nothing
    }

    @Override
    public void rename(final String from, final String to) throws IOException {
        logger.debug("rename {} -> {}", from, to);
        ensureOpen();
        if (fsCache.exists(from)) {
            fsCache.rename(from, to);
        }
        storage.rename(from, to);
    }

    @Override
    public synchronized void close() throws IOException {
        logger.debug("close\n");

        isOpen = false;
        storage.close();
        fsCache.close();
    }

    @Override
    public IndexInput openInput(final String name, final IOContext context) throws IOException {
        logger.debug("openInput {}", name);

        ensureOpen();
        if (bufferedFileMap.get(name) != null) {
            // The requested file is fully populated in the FS cache
            return fsCache.openInput(name, context);
        } else {
            // The requested file is in S3
            Map<Long, Boolean> cachedBlockMap =
                    cachedFileMap.computeIfAbsent(name, k -> new HashMap<>());

            return new S3IndexInput(name, storage,
                    fsCache, cachedBlockMap, context);
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
     * @param fsCachePath the directory path of the local FS cache
     */
    private void prePopulateCache(final String fsCachePath) {
        //storage.readAllInitialBlocks(fsCachePath, this);

        // Populate the cached block maps

    }

    @Override
    public Set<String> getPendingDeletions() {
        return Collections.emptySet();
    }
}
