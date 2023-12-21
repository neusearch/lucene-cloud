package io.neusearch.lucene.store.s3.buffer;

import java.io.File;
import java.io.IOException;

public interface Buffer {

    String[] listAll() throws IOException;

    void deleteFile(final String name) throws IOException;

    long fileLength(final String name) throws IOException;

    void writeByte(String name, byte b) throws IOException;

    void writeBytes(String name, byte[] b, int offset, int length) throws IOException;

    void readToFile(final String name, final int fileOffset, final int len, final File file) throws IOException;

    boolean fileExists(final String name);

    void sync(final String name) throws IOException;

    void rename(final String from, final String to) throws IOException;

    void syncMetaData() throws IOException;

    void openFile(String name) throws IOException;

    void closeFile(String name) throws IOException;

    void close() throws IOException;
}
