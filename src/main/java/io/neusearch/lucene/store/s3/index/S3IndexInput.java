package io.neusearch.lucene.store.s3.index;

import io.neusearch.lucene.store.s3.cache.FSCache;
import io.neusearch.lucene.store.s3.storage.Storage;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Map;

public class S3IndexInput extends IndexInput {

    private static final Logger logger = LoggerFactory.getLogger(S3IndexInput.class);
    public static final long BLOCK_SIZE = 256 * 1024;
    private final String name;
    private final Storage storage;
    private final Map<Long,Boolean> cachedBlockMap;
    private final RandomAccessFile file;
    private final IndexInput indexInput;
    private final long sliceOffset;
    private final long baseLength;
    private final boolean isSlice;

    public S3IndexInput(final String name, final Storage storage, final FSCache fsCache,
                        final Map<Long,Boolean> cachedBlockMap, final IOContext context)
            throws IOException {

        super("S3IndexInput(path=\"" + name + "\")");
        this.name = name;
        this.sliceOffset = 0L;
        this.baseLength = storage.fileLength(name);
        this.storage = storage;
        this.file = new RandomAccessFile(fsCache.getDirectory().resolve(name).toFile(), "rw");
        if (baseLength != file.length()) {
            // Populate the last block to build sparse file properly
            populateLastBlock();
        }
        this.indexInput = fsCache.openInput(name, context);
        this.cachedBlockMap = cachedBlockMap;
        this.isSlice = false;
        logger.debug("S3IndexInput {}", name);
    }

    public S3IndexInput(final String name, final String sliceDesc, final Storage storage,
                        final RandomAccessFile file, final Map<Long,Boolean> cachedBlockMap,
                        final long sliceOffset, final long baseLength,
                        final IndexInput sliceInput) {
        super("S3IndexInput(path=" + name + ",slice=" + sliceDesc + ")");
        this.name = name;
        this.storage = storage;
        this.file = file;
        this.cachedBlockMap = cachedBlockMap;
        this.sliceOffset = sliceOffset;
        this.indexInput = sliceInput;
        this.baseLength = baseLength;
        this.isSlice = true;
        logger.debug("S3IndexInput {} {}", name, sliceDesc);
    }

    @Override
    public byte readByte() throws IOException {
        //logger.debug("S3IndexInput.readByte ({} {} pos {} totalLength {})", name, sliceDesc, position, totalLength);

        // Calculate a block index for serving this one-byte-read request
        long blockIdx = indexInput.getFilePointer() / BLOCK_SIZE;

        // Check whether the block was cached
        if (cachedBlockMap.get(blockIdx) == null) {
            // Read the block from storage
            cacheMissHandler(blockIdx);

            // Create new entry for future access
            cachedBlockMap.put(blockIdx, true);
        }

        // Read byte from the cached block file
        return indexInput.readByte();
    }

    @Override
    public void readBytes(final byte[] buffer, int offset, int len) throws IOException {
        //logger.debug("S3IndexInput.readBytes ({} {} pos {} len {} totalLength {})", name, sliceDesc, position, len, totalLength);

        if (len <= 0) {
            return;
        }

        // Initialize position- and size-related variables
        int remainingBytes = len;
        int readLen;
        long blockIdx, blockOffset, blockLen;
        long fileOffset = sliceOffset + indexInput.getFilePointer();

        while (remainingBytes > 0) {
            // Calculate the block index for serving this request
            blockIdx = fileOffset / BLOCK_SIZE;

            // Check whether the cached page file exists
            if (cachedBlockMap.get(blockIdx) == null) {
                // Read the block from storage
                cacheMissHandler(blockIdx);

                // Create new entry for future access
                cachedBlockMap.put(blockIdx, true);
            }

            // Calculate actual read length
            blockOffset = fileOffset % BLOCK_SIZE;
            blockLen = BLOCK_SIZE - blockOffset;
            readLen = (int) (remainingBytes > blockLen ?
                    blockLen : remainingBytes);
            fileOffset += readLen;
            remainingBytes -= readLen;
        }

        indexInput.readBytes(buffer, offset, len);
    }

    @Override
    public synchronized long length() {
        logger.debug("S3IndexInput.length\n");
        return indexInput.length();
    }

    @Override
    public long getFilePointer() {
        logger.debug("S3IndexInput.getFilePointer\n");
        return indexInput.getFilePointer();
    }

    @Override
    public void seek(final long pos) throws IOException {
        logger.debug("S3IndexInput.seek {}", pos);
        indexInput.seek(pos);
    }

    @Override
    public IndexInput slice(final String sliceDescription, final long offset, final long length) throws IOException {
        logger.debug("S3IndexInput.slice({} {} offset {} length {})", name, sliceDescription, offset, length);

        long baseOffset = offset + this.sliceOffset;
        return new S3IndexInput(name, sliceDescription,
                storage, file, cachedBlockMap, baseOffset, baseLength,
                indexInput.slice(sliceDescription, offset, length));
    }

    @Override
    public void close() throws IOException {
        indexInput.close();
        if (!isSlice) {
            file.close();
        }
        logger.debug("close {}", name);
    }

    private void cacheMissHandler(long blockIdx) throws IOException {
        logger.debug("S3IndexInput.cacheMissHandler ({} sliceOffset {} baseLength {} blockIdx {} )",
                name, sliceOffset, baseLength, blockIdx);

        // Calculate the start offset within the file
        int offset = (int) (blockIdx * BLOCK_SIZE);

        // Calculate the remaining bytes of the file
        int len = (int) (baseLength > offset + BLOCK_SIZE ?
                BLOCK_SIZE : baseLength - offset);

        // Read the block from the corresponding S3 object into file
        byte[] buffer = storage.readBytes(name, offset, len);
        if (buffer.length != len) {
            throw new IOException("The read length is not matched with the requested length");
        }

        file.seek(offset);
        file.write(buffer);
    }

    private void populateLastBlock() throws IOException {
        long lastBlockIdx = baseLength / BLOCK_SIZE;

        cacheMissHandler(lastBlockIdx);
    }
}
