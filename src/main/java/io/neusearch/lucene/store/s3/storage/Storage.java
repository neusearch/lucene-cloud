package io.neusearch.lucene.store.s3.storage;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

public interface Storage {
    String[] listAll() throws IOException;

    long fileLength(final String name);

    void rename(final String from, final String to);

    void writeFromFile(final Path filePath) throws IOException;

    void deleteFile(final String name);

    void readToFile(final String name, final File file,
                   final int fileOffset, final int len) throws IOException;

    int readBytes(final String name, final byte[] buffer, final int bufOffset,
                  final int fileOffset, final int len) throws IOException;

    void close() throws IOException;
}
