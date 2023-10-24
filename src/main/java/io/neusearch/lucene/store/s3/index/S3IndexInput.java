package io.neusearch.lucene.store.s3.index;

import org.apache.commons.io.FileUtils;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MMapDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.neusearch.lucene.store.s3.S3Directory;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;

/**
 * A simple base class that performs index input memory based buffering. Allows the buffer size to be configurable.
 *
 * @author swkim86
 */
public class S3IndexInput extends IndexInput {

    private static final Logger logger = LoggerFactory.getLogger(S3IndexInput.class);

    private static final long CACHE_PAGE_SIZE = 4 * 1024;

    private final String name;

    private final String sliceDesc;

    private final Path cacheFilePath;

    private final HashMap<Long,IndexInput> openCacheFileMap;

    private final MMapDirectory cacheDirectory;

    private final boolean isSlice;
    // lazy initialize the length
    private long totalLength;

    private final long sliceLength;

    private long position = 0;

    private final long sliceOffset;

    private final S3Directory s3Directory;

    private IndexInput cacheMissHandler(long pageIdx) throws IOException {
        //logger.debug("S3IndexInput.cacheMissHandler ({} pos {} totalLength {} pageIdx {} )", name, position, totalLength, pageIdx);

        int pageStartOffset = (int) (pageIdx * CACHE_PAGE_SIZE);
        String pageIdxStr = Long.toString(pageIdx);
        Path pageFilePath = cacheFilePath.resolve(pageIdxStr);

        if (Files.notExists(pageFilePath)) {
            // Calculate the remaining bytes of the object
            int readLen = (int) (totalLength > pageStartOffset + CACHE_PAGE_SIZE ?
                    CACHE_PAGE_SIZE : totalLength - pageStartOffset);

            // Read the page from the corresponding S3 object
            ResponseInputStream<GetObjectResponse> res = s3Directory.getS3().
                    getObject(b -> b.bucket(s3Directory.getBucket()).key(s3Directory.getPrefix() + name)
                            .range(String.format("bytes=%d-%d", pageStartOffset, pageStartOffset + readLen - 1)));

            // Copy the object to a cache page file
            FileUtils.copyInputStreamToFile(res, pageFilePath.toFile());
            res.close();
        }

        return cacheDirectory.openInput(pageIdxStr, IOContext.DEFAULT);
    }

    public S3IndexInput(final String name, final S3Directory s3Directory) throws IOException {
        super("S3IndexInput(path=\"" + name + "\")");
        this.totalLength = s3Directory.fileLength(name);
        this.s3Directory = s3Directory;
        this.isSlice = false;
        this.sliceLength = -1;
        this.sliceOffset = -1;
        this.name = name;
        this.sliceDesc = "";
        this.cacheFilePath = s3Directory.getCachePath().resolve(name);
        this.cacheDirectory = new MMapDirectory(cacheFilePath);
        this.openCacheFileMap = new HashMap<>();
        logger.debug("S3IndexInput {}", name);
    }

    public S3IndexInput(final String name, final String sliceDesc, final S3Directory s3Directory,
                        final long offset, final long length, final long totalLength) throws IOException {
        super("S3IndexInput(path=" + name + ",slice=" + sliceDesc + ")");
        this.s3Directory = s3Directory;
        this.name = name;
        this.sliceDesc = sliceDesc;
        this.isSlice = true;
        this.sliceLength = length;
        this.totalLength = totalLength;
        this.sliceOffset = offset;
        this.position = offset;
        this.cacheFilePath = s3Directory.getCachePath().resolve(name);
        this.cacheDirectory = new MMapDirectory(cacheFilePath);
        this.openCacheFileMap = new HashMap<>();
        logger.debug("S3IndexInput {} {}", name, sliceDesc);
    }

    @Override
    public byte readByte() throws IOException {
        //logger.debug("S3IndexInput.readByte ({} {} pos {} totalLength {})", name, sliceDesc, position, totalLength);

        // Calculate a page index for serving this one-byte-read request
        long pageIdx = position / CACHE_PAGE_SIZE;
        long pageOffset = position % CACHE_PAGE_SIZE;

        // Check whether the cached page file exists
        IndexInput cacheInput = openCacheFileMap.get(pageIdx);
        if (cacheInput == null) {
            cacheInput = cacheMissHandler(pageIdx);

            // Create new entry for future access
            openCacheFileMap.put(pageIdx, cacheInput);
        }

        // Read byte from the cached page file
        cacheInput.seek(pageOffset);
        byte buf = cacheInput.readByte();
        position++;
        return buf;
    }

    @Override
    public void readBytes(final byte[] buffer, int offset, int len) throws IOException {
        //logger.debug("S3IndexInput.readBytes ({} {} pos {} len {} totalLength {})", name, sliceDesc, position, len, totalLength);

        if (len <= 0) {
            return;
        }

        // Initialize position- and size-related variables
        int remainingBytes = len;
        int bufferPos = offset;
        int readLen;
        long pageIdx, pageOffset, pageLen;
        IndexInput cacheInput;

        while (remainingBytes > 0) {
            // Calculate the page index for serving this request
            pageIdx = position / CACHE_PAGE_SIZE;
            pageOffset = position % CACHE_PAGE_SIZE;
            pageLen = CACHE_PAGE_SIZE - pageOffset;

            // Check whether the cached page file exists
            cacheInput = openCacheFileMap.get(pageIdx);
            if (cacheInput == null) {
                cacheInput = cacheMissHandler(pageIdx);

                // Create new entry for future access
                openCacheFileMap.put(pageIdx, cacheInput);
            }

            // Read bytes from the cached page file
            readLen = (int) (remainingBytes > pageLen ?
                    pageLen : remainingBytes);
            cacheInput.seek(pageOffset);
            cacheInput.readBytes(buffer, bufferPos, readLen);
            position += readLen;
            remainingBytes -= readLen;
            bufferPos += readLen;
        }
    }

    @Override
    public void close() throws IOException {
        // Close all the opened cache page file
        for (Long key : openCacheFileMap.keySet()) {
            IndexInput cacheInput = openCacheFileMap.get(key);
            cacheInput.close();
        }
        openCacheFileMap.clear();
        logger.debug("close {}", name);
    }

    @Override
    public synchronized long length() {
        logger.debug("S3IndexInput.length\n");
        if (isSlice) {
            return sliceLength;
        } else {
            if (totalLength == -1) {
                try {
                    totalLength = s3Directory.fileLength(name);
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
    public void seek(final long pos) throws IOException {
        logger.debug("S3IndexInput.seek {}", pos);
        if (isSlice) {
            position = sliceOffset + pos;
        } else {
            position = pos;
        }
    }

    @Override
    public IndexInput slice(final String sliceDescription, final long offset, final long length) throws IOException {
        logger.debug("S3IndexInput.slice({} {} offset {} length {})", name, sliceDescription, offset, length);

        return new S3IndexInput(name, sliceDescription, s3Directory, offset, length, totalLength);
    }
}
