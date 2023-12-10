package io.neusearch.lucene.store.s3.index;

import io.neusearch.lucene.store.s3.buffer.Buffer;
import org.apache.lucene.store.IndexOutput;

import java.io.IOException;
import java.util.zip.CRC32;

public class S3IndexOutput extends IndexOutput {

    private final Buffer buffer;

    private final CRC32 crc = new CRC32();

    private long bytesWritten = 0L;

    public S3IndexOutput(final String name, final Buffer buffer) throws IOException {
        super("S3IndexOutput(name=" + name + ")", name);
        this.buffer = buffer;
        this.buffer.openFile(name);
    }

    /**
     * Writes a single byte.
     *
     * <p>The most primitive data type is an eight-bit byte. Files are accessed as sequences of bytes.
     * All other data types are defined as sequences of bytes, so file formats are byte-order
     * independent.
     *
     */
    @Override
    public void writeByte(byte b) throws IOException {
        buffer.writeByte(getName(), b);
        crc.update(b);
        bytesWritten++;
    }

    /**
     * Writes an array of bytes.
     *
     * @param b the bytes to write
     * @param offset the offset in the byte array
     * @param length the number of bytes to write
     */
    @Override
    public void writeBytes(byte[] b, int offset, int length) throws IOException {
        buffer.writeBytes(getName(), b, offset, length);
        crc.update(b, offset, length);
        bytesWritten += length;
    }

    @Override
    public void close() throws IOException {
        buffer.closeFile(getName());
    }

    @Override
    public long getFilePointer() {
        return bytesWritten;
    }

    @Override
    public long getChecksum() {
        return crc.getValue();
    }
}
