package io.neusearch.lucene.store.s3.cache;

import java.io.IOException;

public interface Cache {

    void deleteFile(final String name) throws IOException;

    void rename(final String from, final String to) throws IOException;

    long fileLength(final String name) throws IOException;

    byte readByte(final String name, long fileOffset) throws IOException;

    void readBytes(final String name, final byte[] buffer, int bufOffset, long fileOffset, int len) throws IOException;

    void openFile(final String name) throws IOException;

    void closeFile(final String name) throws IOException;

    void close() throws IOException;
}
