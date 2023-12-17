package io.neusearch.lucene.store.s3.index;

import io.neusearch.lucene.store.s3.buffer.Buffer;
import org.apache.lucene.store.IndexOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.zip.CRC32;

public class S3IndexOutput extends IndexOutput {
    private static final Logger logger = LoggerFactory.getLogger(S3IndexOutput.class);

    private final Buffer buffer;

    private final CRC32 crc = new CRC32();

    private long bytesWritten = 0L;

    private volatile boolean isOpen = false;

    public S3IndexOutput(final String name, final Buffer buffer) throws IOException {
        super("S3IndexOutput(name=" + name + ")", name);
        this.buffer = buffer;
        this.buffer.openFile(name);
        this.isOpen = true;
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
        logger.debug("writeByte {}", getName());
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
        logger.debug("writeBytes {} offset {} length {}", getName(), offset, length);
        buffer.writeBytes(getName(), b, offset, length);
        crc.update(b, offset, length);
        bytesWritten += length;
    }

    @Override
    public void close() throws IOException {
        logger.debug("close {}", getName());
        if (isOpen) {
            buffer.closeFile(getName());
            isOpen = false;
        }
    }

    @Override
    public long getFilePointer() {
        logger.debug("getFilePointer {} {}", getName(), bytesWritten);
        return bytesWritten;
    }

    @Override
    public long getChecksum() {
        logger.debug("getChecksum {} {}", getName(), crc.getValue());
        return crc.getValue();
    }
}
