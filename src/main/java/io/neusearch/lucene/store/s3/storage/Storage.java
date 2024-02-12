package io.neusearch.lucene.store.s3.storage;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

/**
 * Interface to storage that durably stores Lucene data
 */
public interface Storage {
    /**
     * Lists all the file names within storage
     *
     * @return the object names array
     */
    String[] listAll();

    /**
     * Gets file length matched with the provided name
     * @param name the name of file within the storage
     * @return the file size in bytes
     */
    long fileLength(final String name);

    /**
     * Renames file based on the provided names
     *
     * @param from the file name to be renamed from
     * @param to the file name to be renamed to
     */
    void rename(final String from, final String to);

    /**
     * Writes a file to storage using the given file
     *
     * @param filePath the absolute file path
     */
    void writeFromFile(final Path filePath);

    /**
     * Writes a set of files to storage
     *
     * @param filePaths the list of absolute file paths
     */
    void writeFromFiles(final List<Path> filePaths);

    /**
     * Deletes a file matched with the provided name
     *
     * @param name the name of file in storage
     */
    void deleteFile(final String name);

    /**
     * Reads a range of bytes within a file from storage and writes to a file
     *
     * @param name the file name
     * @param fileOffset the range start offset
     * @param len the length of the range
     * @param file the File object to be written
     * @throws IOException if writing to the file failed for reasons
     */
    void readRangeToFile(final String name, final int fileOffset,
                   final int len, final File file) throws IOException;

    /**
     * Reads a whole file in storage and writes to the given file
     *
     * @param name the file name to read
     * @param file the File object to be written
     * @throws IOException if writing to the file failed for reasons
     */
    void readToFile(final String name, final File file) throws IOException;

    /**
     * Reads a range of bytes from a file in storage and writes to a specific offset of the given buffer
     *
     * @param name the file name in storage
     * @param buffer the buffer to populate read data
     * @param bufOffset the start offset inside the buffer
     * @param fileOffset the start offset inside the file
     * @param len the length to read from the file
     * @return the read bytes
     * @throws IOException if copying into buffer failed for reasons
     */
    int readBytes(final String name, final byte[] buffer, final int bufOffset,
                  final int fileOffset, final int len) throws IOException;

    byte[] readBytes(final String name, final int offset, final int len) throws IOException;

    /**
     * Reads all the files in storage and writes to the given directory
     *
     * @param dir the directory to write all the files in storage
     */
    void readAllToDir(final String dir);

    /**
     * Releases the created storage
     */
    void close();
}
