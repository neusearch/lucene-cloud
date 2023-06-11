package io.bigsearch.lucene.store.s3.index;

import io.bigsearch.lucene.store.s3.S3Directory;
import org.apache.lucene.store.IndexInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

/**
 * A simple base class that performs index input memory based buffering. Allows the buffer size to be configurable.
 *
 * @author swkim86
 */
public class S3IndexInput extends IndexInput {

    private static final Logger logger = LoggerFactory.getLogger(S3IndexInput.class);

    private static final long CACHE_PAGE_SIZE = 128 * 1024;

    private final String name;

    private final Path cacheFilePath;

    private final boolean isSlice;
    // lazy initialize the length
    private long totalLength = -1;

    private final long sliceLength;

    private long position = 0;

    private final long sliceOffset;

    private final S3Directory s3Directory;

    public S3IndexInput(final String name, final S3Directory s3Directory) throws IOException {
        super("S3IndexInput(path=\"" + name + "\")");
        this.s3Directory = s3Directory;
        this.isSlice = false;
        this.sliceLength = -1;
        this.sliceOffset = -1;
        this.name = name;
        this.cacheFilePath = s3Directory.getCachePath().resolve(name);
        // Create a cache directory for this file if not exists
        if (Files.notExists(this.cacheFilePath)) {
            try {
                Files.createDirectory(this.cacheFilePath);
            } catch (IOException e) {
                System.out.printf("S3IndexInput() %s\n", e.getMessage());
            }
            String[] files = s3Directory.listAll();
            if (!Arrays.asList(files).contains(name)) {
                System.out.printf("S3IndexInput() file not found %s\n", name);
                throw new FileNotFoundException();
            }
        }
    }

    public S3IndexInput(final String name, final String sliceDesc, final S3Directory s3Directory,
                        final long offset, final long length) throws IOException {
        super("S3IndexInput(path=" + name + ",slice=" + sliceDesc + ")");
        this.s3Directory = s3Directory;
        this.name = name;
        this.isSlice = true;
        this.sliceLength = length;
        this.sliceOffset = offset;
        this.position = offset;
        this.cacheFilePath = s3Directory.getCachePath().resolve(name);
        // Create a cache directory for this file if not exists
        if (Files.notExists(this.cacheFilePath)) {
            try {
                Files.createDirectory(this.cacheFilePath);
            } catch (IOException e) {
                System.out.printf("S3IndexInput() %s\n", e.getMessage());
                throw e;
            }
        }
    }

    @Override
    public byte readByte() throws IOException {
        logger.debug("S3IndexInput.readByte ({} pos {})", name, position);
        System.out.printf("S3IndexInput.readByte pos " + position + " length " + totalLength + " bucket " + s3Directory.getBucket() + " prefix " + s3Directory.getPrefix() + " name " + name + "\n");

        // Calculate a page index for serving this one-byte-read request
        long pageIdx = position / CACHE_PAGE_SIZE;
        long pageOffset = position % CACHE_PAGE_SIZE;
        Path pageFilePath = cacheFilePath.resolve(Long.toString(pageIdx));

        // Check whether the cached page file exists
        if (Files.notExists(pageFilePath)) {
            // Cache miss
            int pageStartOffset = (int) (pageIdx * CACHE_PAGE_SIZE);

            // Read the page from the corresponding S3 object
            ResponseInputStream<GetObjectResponse> res = s3Directory.getS3().
                    getObject(b -> b.bucket(s3Directory.getBucket()).key(s3Directory.getPrefix() + name));

            // It is possible that the file length is accessed for the first time
            if (totalLength == -1) {
                totalLength = res.response().contentLength();
            }

            // Calculate the remaining bytes of the object
            int readLen = (int) (totalLength > pageStartOffset + CACHE_PAGE_SIZE ?
                    CACHE_PAGE_SIZE : totalLength - pageStartOffset);
            // Read a range of the object into a buffer
            res.skipNBytes(pageStartOffset);
            byte[] readBytes = res.readNBytes(readLen);
            if (readBytes.length != readLen) {
                logger.error("# bytes read does not match the requested length");
                System.out.print("# bytes read does not match the requested length\n");
            }

            // Copy the page to the local page file
            Files.copy(new ByteArrayInputStream(readBytes), pageFilePath);
        }

        // Read byte from the cached page file
        RandomAccessFile file = new RandomAccessFile(pageFilePath.toString(), "r");
        file.seek(pageOffset);
        byte buf = file.readByte();
        file.close();
        position++;
        return buf;
    }

    @Override
    public void readBytes(final byte[] buffer, int offset, int len) throws IOException {
        logger.debug("S3IndexInput.readBytes ({} pos {} len {})", name, position, len);
        System.out.printf("S3IndexInput.readBytes pos " + position + " length " + totalLength + " offset " + offset + " len " + len + " bucket " + s3Directory.getBucket() + " prefix " + s3Directory.getPrefix() + " name " + name + "\n");

        if (len <= 0) {
            return;
        }

        // Initialize position- and size-related variables
        int remainingBytes = len;
        int bufferPos = offset;
        long pageIdx, pageOffset, pageLen;
        Path pageFilePath;

        while (remainingBytes > 0) {
            // Calculate the page index for serving this request
            pageIdx = position / CACHE_PAGE_SIZE;
            pageOffset = position % CACHE_PAGE_SIZE;
            pageLen = CACHE_PAGE_SIZE - pageOffset;
            pageFilePath = cacheFilePath.resolve(Long.toString(pageIdx));

            // Check whether the cached page file exists
            if (Files.notExists(pageFilePath)) {
                // Cache miss
                int pageStartOffset = (int) (pageIdx * CACHE_PAGE_SIZE);

                // Read the page from the corresponding S3 object
                ResponseInputStream<GetObjectResponse> res = s3Directory.getS3().
                        getObject(b -> b.bucket(s3Directory.getBucket()).key(s3Directory.getPrefix() + name));

                // It is possible that the file length is accessed for the first time
                if (totalLength == -1) {
                    totalLength = res.response().contentLength();
                }

                // Calculate the remaining bytes of the object
                int readLen = (int) (totalLength > pageStartOffset + CACHE_PAGE_SIZE ?
                        CACHE_PAGE_SIZE : totalLength - pageStartOffset);

                // Read a range of the object into a buffer
                res.skipNBytes(pageStartOffset);
                byte[] readBytes = res.readNBytes(readLen);
                if (readBytes.length != readLen) {
                    logger.error("# bytes read does not match the requested length");
                    System.out.print("# bytes read does not match the requested length\n");
                }

                // Populate the local page file
                Files.copy(new ByteArrayInputStream(readBytes), pageFilePath);
            }

            // Copy bytes from the cached page file
            RandomAccessFile file = new RandomAccessFile(pageFilePath.toString(), "r");
            int readLen = (int) (remainingBytes > pageLen ?
                    pageLen : remainingBytes);
            file.seek(pageOffset);
            int readBytes = file.read(buffer, bufferPos, readLen);
            position += readBytes;
            remainingBytes -= readBytes;
            bufferPos += readBytes;
            if (len != readBytes) {
                logger.error("readBytes ({} len {} readBytes {})", name, len, readBytes);
            }

            file.close();
        }
    }

    @Override
    public void close() throws IOException {
        // Do nothing
        System.out.printf("close %s\n", name);
    }

    @Override
    public synchronized long length() {
        System.out.print("S3IndexInput.length\n");
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
        System.out.print("S3IndexInput.getFilePointer\n");
        if (isSlice) {
            return position - sliceOffset;
        } else {
            return position;
        }
    }

    @Override
    public void seek(final long pos) throws IOException {
        System.out.printf("S3IndexInput.seek %d\n", pos);
        if (isSlice) {
            position = sliceOffset + pos;
        } else {
            position = pos;
        }
    }

    @Override
    public IndexInput slice(final String sliceDescription, final long offset, final long length) throws IOException {
        logger.debug("S3IndexInput.slice({} offset {} length {})", sliceDescription, offset, length);
        System.out.printf("S3IndexInput.slice %s %d %d\n", sliceDescription, offset, length);

        return new S3IndexInput(name, sliceDescription, s3Directory, offset, length);
    }
}
