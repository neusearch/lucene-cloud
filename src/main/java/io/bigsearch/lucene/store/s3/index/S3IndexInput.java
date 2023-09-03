package io.bigsearch.lucene.store.s3.index;

import io.bigsearch.lucene.store.s3.S3Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MMapDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
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

    private final HashMap<String,IndexInput> openCacheFileMap;

    private final MMapDirectory cacheDirectory;

    private final boolean isSlice;
    // lazy initialize the length
    private long totalLength;

    private final long sliceLength;

    private long position = 0;

    private final long sliceOffset;

    private final S3Directory s3Directory;

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
        // Create a cache directory for this file if not exists
        if (Files.notExists(this.cacheFilePath)) {
            try {
                Files.createDirectory(this.cacheFilePath);
            } catch (IOException e) {
                logger.error("S3IndexInput() {}", e.getMessage());
            }
            String[] files = s3Directory.listAll();
            if (!Arrays.asList(files).contains(name)) {
                logger.error("S3IndexInput() file not found {}", name);
                throw new FileNotFoundException();
            }
        }
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
        // Create a cache directory for this file if not exists
        if (Files.notExists(this.cacheFilePath)) {
            try {
                Files.createDirectory(this.cacheFilePath);
            } catch (IOException e) {
                logger.error("S3IndexInput() {}", e.getMessage());
                throw e;
            }
        }
        this.openCacheFileMap = new HashMap<>();
    }

    @Override
    public byte readByte() throws IOException {
        logger.debug("S3IndexInput.readByte ({} {} pos {} totalLength {})", name, sliceDesc, position, totalLength);

        // Calculate a page index for serving this one-byte-read request
        long pageIdx = position / CACHE_PAGE_SIZE;
        String pageIdxStr = Long.toString(pageIdx);
        long pageOffset = position % CACHE_PAGE_SIZE;
        Path pageFilePath = cacheFilePath.resolve(pageIdxStr);

        // Check whether the cached page file exists
        if (Files.notExists(pageFilePath)) {
            logger.debug("start S3IndexInput.readByte from s3 ({} pos {} totalLength {} pageFilePath {} )", name, position, totalLength, pageFilePath);
            // Cache miss
            int pageStartOffset = (int) (pageIdx * CACHE_PAGE_SIZE);

            // Calculate the remaining bytes of the object
            int readLen = (int) (totalLength > pageStartOffset + CACHE_PAGE_SIZE ?
                    CACHE_PAGE_SIZE : totalLength - pageStartOffset);

            // Read the page from the corresponding S3 object
            ResponseInputStream<GetObjectResponse> res = s3Directory.getS3().
                    getObject(b -> b.bucket(s3Directory.getBucket()).key(s3Directory.getPrefix() + name)
                            .range(String.format("bytes=%d-%d", pageStartOffset, pageStartOffset + readLen - 1)));

            // Read a range of the object into a buffer
            byte[] readBytes = res.readNBytes(readLen);
            if (readBytes.length != readLen) {
                logger.error("# bytes read does not match the requested length");
            }
            res.close();

            // Copy the page to the local page file
            Files.copy(new ByteArrayInputStream(readBytes), pageFilePath);

            logger.debug("S3IndexInput.readByte from s3 ({} pos {} totalLength {} pageFilePath {} )", name, position, totalLength, pageFilePath);
        }

        // Read byte from the cached page file
        IndexInput cacheInput = openCacheFileMap.get(pageIdxStr);
        if (cacheInput == null) {
            logger.debug("S3IndexInput.readByte open file cache miss ({})", pageFilePath);
            cacheInput = cacheDirectory.openInput(pageIdxStr, IOContext.DEFAULT);
            openCacheFileMap.put(pageIdxStr, cacheInput);
        }

        cacheInput.seek(pageOffset);
        byte buf = cacheInput.readByte();
        position++;
        return buf;
    }

    @Override
    public void readBytes(final byte[] buffer, int offset, int len) throws IOException {
        logger.debug("S3IndexInput.readBytes ({} {} pos {} len {} totalLength {})", name, sliceDesc, position, len, totalLength);

        if (len <= 0) {
            return;
        }

        // Initialize position- and size-related variables
        int remainingBytes = len;
        int bufferPos = offset;
        long pageIdx, pageOffset, pageLen;
        String pageIdxStr;
        Path pageFilePath;

        while (remainingBytes > 0) {
            // Calculate the page index for serving this request
            pageIdx = position / CACHE_PAGE_SIZE;
            pageIdxStr = Long.toString(pageIdx);
            pageOffset = position % CACHE_PAGE_SIZE;
            pageLen = CACHE_PAGE_SIZE - pageOffset;
            pageFilePath = cacheFilePath.resolve(Long.toString(pageIdx));

            // Check whether the cached page file exists
            if (Files.notExists(pageFilePath)) {
                logger.debug("start S3IndexInput.readBytes from s3 ({} pos {} len {} totalLength {} pageFilePath {} )", name, position, len, totalLength, pageFilePath);
                // Cache miss
                int pageStartOffset = (int) (pageIdx * CACHE_PAGE_SIZE);

                // Calculate the remaining bytes of the object
                int readLen = (int) (totalLength > pageStartOffset + CACHE_PAGE_SIZE ?
                        CACHE_PAGE_SIZE : totalLength - pageStartOffset);

                // Read the page from the corresponding S3 object
                ResponseInputStream<GetObjectResponse> res = s3Directory.getS3().
                        getObject(b -> b.bucket(s3Directory.getBucket()).key(s3Directory.getPrefix() + name)
                                .range(String.format("bytes=%d-%d", pageStartOffset, pageStartOffset + readLen - 1)));

                // Read a range of the object into a buffer
                byte[] readBytes = res.readNBytes(readLen);
                if (readBytes.length != readLen) {
                    logger.error("# bytes read does not match the requested length");
                }
                res.close();

                // Populate the local page file
                Files.copy(new ByteArrayInputStream(readBytes), pageFilePath);

                logger.debug("S3IndexInput.readBytes from s3 ({} pos {} len {} totalLength {} pageFilePath {} )", name, position, len, totalLength, pageFilePath);
            }

            // Read bytes from the cached page file
            IndexInput cacheInput = openCacheFileMap.get(pageIdxStr);
            if (cacheInput == null) {
                logger.debug("S3IndexInput.readByte open file cache miss ({})", pageFilePath);
                cacheInput = cacheDirectory.openInput(pageIdxStr, IOContext.DEFAULT);
                openCacheFileMap.put(pageIdxStr, cacheInput);
            }

            int readLen = (int) (remainingBytes > pageLen ?
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
        for (String key : openCacheFileMap.keySet()) {
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
