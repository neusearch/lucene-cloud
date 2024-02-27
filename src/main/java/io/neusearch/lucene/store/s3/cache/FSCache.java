package io.neusearch.lucene.store.s3.cache;

import io.neusearch.lucene.store.s3.index.S3IndexInput;
import org.apache.lucene.store.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;

/**
 * A file system cache implementation intended for using as storage buffer/cache.
 */
public class FSCache {

    private static final Logger logger = LoggerFactory.getLogger(S3IndexInput.class);
    private final FSDirectory cache;
    private final Path directory;

    /**
     * Creates a new FSCache object with the provided file system path.
     *
     * @param directory the cache directory
     * @throws IOException if any i/o error occurs while opening directory
     */
    public FSCache(Path directory) throws IOException {
        this.directory = directory;
        this.cache = FSDirectory.open(directory);
    }

    /**
     * Deletes a file matching with the provided name.
     *
     * @param name the file name to be deleted
     * @throws IOException if deleting file system file is failed for reasons
     */
    public void deleteFile(String name) throws IOException {
        try {
            cache.deleteFile(name);
        } catch (NoSuchFileException ignored) {}
    }

    /**
     * Gets the length of a file matching with the provided name.
     *
     * @param name the file name
     * @return the file length
     * @throws IOException if retrieving the file length is failed for reasons
     */
    public long fileLength(String name) throws IOException {
        return cache.fileLength(name);
    }

    /**
     * Creates a new, empty in the directory and
     * returns an IndexOutput instance for appending data to this file.
     *
     * @param name the file name
     * @param context the requesting i/o context
     * @return the allocated IndexOutput instance
     * @throws IOException if any i/o error occurs while opening file for writing
     */
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        return cache.createOutput(name, context);
    }

    /**
     * Creates a new, empty, temporary file in the directory and
     * returns an IndexOutput instance for appending data to this file.
     *
     * @param prefix the prefix of a file name
     * @param suffix the suffix of a file name
     * @param context the requesting i/o context
     * @return the allocated IndexOutput object
     * @throws IOException if any i/o error occurs while opening file for writing
     */
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext context)
            throws IOException {
        return cache.createTempOutput(prefix, suffix, context);
    }

    /**
     * Opens a stream for reading an existing file.
     *
     * @param name the name of an existing file
     * @param context the requesting i/o context
     * @return the allocated IndexInput ojbect
     * @throws IOException if any i/o error occurs
     */
    public IndexInput openInput(String name, IOContext context) throws IOException {
        return cache.openInput(name, context);
    }

    /**
     * Renames a file.
     *
     * @param src the source file name
     * @param dest the destination file name
     * @throws IOException if any i/o error occurs while renaming
     */
    public void rename(String src, String dest) throws IOException {
        cache.rename(src, dest);
    }

    /**
     * Lists all the file names in the cache directory.
     *
     * @return the file names array
     * @throws IOException if any i/o error occurs attempting to list the cache directory
     */
    public String[] listAll() throws IOException {
        return cache.listAll();
    }

    /**
     * Gets the Path object of the cache directory.
     *
     * @return the Path object
     */
    public Path getDirectory() {
        return directory;
    }

    /**
     * Releases the allocated objects.
     *
     * @throws IOException if closing FS directory is failed for reasons
     */
    public void close() throws IOException {
        cache.close();
    }

    /**
     * Gets existence of a specific file in the cache.
     *
     * @param name the file name
     * @return the boolean value according the file existence
     */
    public boolean exists(String name) {
        return Files.exists(buildFullPath(name));
    }

    /**
     * Gets the Path object corresponding to the provided file name.
     *
     * @param name the file name
     * @return the built Path object using the provided file name
     */
    public Path buildFullPath(String name) {
        return directory.resolve(name);
    }

    /**
     * Acquires and returns a Lock for a file with the given name.
     *
     * @param name the name of the lock file
     * @return the acquired file lock
     * @throws IOException if any i/o error occurs attempting to gain the lock
     */
    public final Lock obtainLock(String name) throws IOException {
        return cache.obtainLock(name);
    }
}
