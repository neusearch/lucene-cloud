package io.neusearch.lucene.store.s3.index;

import io.neusearch.lucene.store.s3.cache.Cache;
import org.apache.lucene.store.IndexInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

/**
 * A simple base class that performs index input memory based buffering. Allows the buffer size to be configurable.
 *
 * @author swkim86
 */
public class S3IndexInput extends IndexInput {

    private static final Logger logger = LoggerFactory.getLogger(S3IndexInput.class);

    private final String name;

    private final String sliceDesc;

    private final boolean isSlice;
    // lazy initialize the length
    private long totalLength;

    private final long sliceLength;

    private long position = 0;

    private final long sliceOffset;

    private final Cache cache;

    public S3IndexInput(final String name, final Cache cache) throws IOException {
        super("S3IndexInput(path=\"" + name + "\")");
        this.totalLength = cache.fileLength(name);
        this.isSlice = false;
        this.sliceLength = -1;
        this.sliceOffset = -1;
        this.name = name;
        this.sliceDesc = "";
        this.cache = cache;
        this.cache.openFile(name);
        logger.debug("S3IndexInput {}", name);
    }

    public S3IndexInput(final String name, final String sliceDesc, final long offset,
                        final long length, final long totalLength, final Cache cache) {
        super("S3IndexInput(path=" + name + ",slice=" + sliceDesc + ")");
        this.name = name;
        this.sliceDesc = sliceDesc;
        this.isSlice = true;
        this.sliceLength = length;
        this.totalLength = totalLength;
        this.sliceOffset = offset;
        this.position = offset;
        this.cache = cache;
        logger.debug("S3IndexInput {} {}", name, sliceDesc);
    }

    @Override
    public byte readByte() throws IOException {
        //logger.debug("S3IndexInput.readByte ({} {} pos {} totalLength {})", name, sliceDesc, position, totalLength);
        if (position > totalLength) {
            logger.error("readByte() position {} > totalLength {}", position, totalLength);
        }
        return cache.readByte(name, position++);
    }

    @Override
    public void readBytes(final byte[] buffer, int offset, int len) throws IOException {
        //logger.debug("S3IndexInput.readBytes ({} {} pos {} len {} totalLength {})", name, sliceDesc, position, len, totalLength);

        cache.readBytes(name, buffer, offset, position, len);
        position += len;
    }

    @Override
    public void close() throws IOException {
        logger.debug("close {}", name);

        cache.closeFile(name);
    }

    @Override
    public synchronized long length() {
        logger.debug("S3IndexInput.length\n");
        if (isSlice) {
            return sliceLength;
        } else {
            if (totalLength == -1) {
                try {
                    totalLength = cache.fileLength(name);
                } catch (final IOException e) {
                    // do nothing here for now, much better for performance
                }
            }
            return totalLength;
        }
    }

    @Override
    public long getFilePointer() {
        logger.debug("S3IndexInput.getFilePointer\n");
        if (isSlice) {
            return position - sliceOffset;
        } else {
            return position;
        }
    }

    @Override
    public void seek(final long pos) {
        logger.debug("S3IndexInput.seek {}", pos);
        if (isSlice) {
            position = sliceOffset + pos;
        } else {
            position = pos;
        }
    }

    @Override
    public IndexInput slice(final String sliceDescription, final long offset, final long length) {
        logger.debug("S3IndexInput.slice({} {} offset {} length {})", name, sliceDescription, offset, length);

        return new S3IndexInput(name, sliceDescription, offset, length, totalLength, cache);
    }
}
