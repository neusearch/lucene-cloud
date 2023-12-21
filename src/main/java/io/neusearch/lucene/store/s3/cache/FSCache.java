package io.neusearch.lucene.store.s3.cache;

import io.neusearch.lucene.store.s3.buffer.Buffer;
import io.neusearch.lucene.store.s3.storage.Storage;
import org.apache.commons.io.FileUtils;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

public class FSCache implements Cache {
    private static final Logger logger = LoggerFactory.getLogger(FSCache.class);

    private static final long CACHE_PAGE_SIZE = 256 * 1024;

    private final ConcurrentHashMap<String,IndexInput> indexInputMap;

    private final ConcurrentHashMap<String,Long> fileLengthMap;

    private final FSDirectory fsDirectory;

    private final Storage storage;

    private final Buffer buffer;

    public FSCache(HashMap<String, Object> params) throws IOException {
        Path cachePath = Paths.get(params.get("cachePath").toString());
        // Create cache directory if not exists
        Files.createDirectories(cachePath);
        this.fsDirectory = FSDirectory.open(cachePath);
        this.buffer = (Buffer) params.get("buffer");
        this.storage = (Storage) params.get("storage");

        this.indexInputMap = new ConcurrentHashMap<>();
        this.fileLengthMap = new ConcurrentHashMap<>();
    }

    public void deleteFile(final String name) throws IOException {
        logger.debug("deleteFile {}", name);
        // Cached block files are managed within a directory
        File cacheFileDir = fsDirectory.getDirectory().resolve(name).toFile();
        FileUtils.deleteDirectory(cacheFileDir);
    }

    public void rename(final String from, final String to) throws IOException {
        logger.debug("rename {} -> {}", from, to);
        File cacheFileDir = fsDirectory.getDirectory().resolve(from).toFile();
        File destCacheFileDir = fsDirectory.getDirectory().resolve(to).toFile();
        if (!cacheFileDir.renameTo(destCacheFileDir)) {
            throw new FileSystemException("rename cache file dir failed from " + from + " to " + to);
        }
    }

    public long fileLength(final String name) throws IOException {
        logger.debug("fileLength {}", name);
        Long length = fileLengthMap.get(name);
        if (length == null) {
            length = buffer.fileLength(name);
            if (length == -1) {
                length = storage.fileLength(name);
            }
            fileLengthMap.put(name, length);
        }

        return length;
    }

    public byte readByte(final String name, long fileOffset) throws IOException {
        //logger.debug("readByte ({} {})", name, fileOffset);

        // Calculate a page index for serving this one-byte-read request
        long pageIdx = fileOffset / CACHE_PAGE_SIZE;
        long pageOffset = fileOffset % CACHE_PAGE_SIZE;
        String key = name + "/" + pageIdx;

        // Check whether the cached page file exists
        IndexInput cachedPageInput = indexInputMap.get(key);
        if (cachedPageInput == null) {
            cachedPageInput = cacheMissHandler(name, pageIdx);

            // Create new entry for future access
            indexInputMap.put(key, cachedPageInput);
        }

        // Read byte from the cached page file
        cachedPageInput.seek(pageOffset);
        return cachedPageInput.readByte();
    }

    public void readBytes(final String name, final byte[] buffer, int bufOffset, long fileOffset, int len) throws IOException {
        //logger.debug("readBytes ({} pos {} len {})", name, fileOffset, len);

        if (len <= 0) {
            return;
        }

        // Initialize position- and size-related variables
        int remainingBytes = len;
        int bufferPos = bufOffset;
        int readLen;
        long pageIdx, pageOffset, pageLen;
        IndexInput cachedPageInput;
        String key;

        while (remainingBytes > 0) {
            // Calculate the page index for serving this request
            pageIdx = fileOffset / CACHE_PAGE_SIZE;
            pageOffset = fileOffset % CACHE_PAGE_SIZE;
            pageLen = CACHE_PAGE_SIZE - pageOffset;
            key = name + "/" + pageIdx;

            // Check whether the cached page file exists
            cachedPageInput = indexInputMap.get(key);
            if (cachedPageInput == null) {
                cachedPageInput = cacheMissHandler(name, pageIdx);

                // Create new entry for future access
                indexInputMap.put(key, cachedPageInput);
            }

            // Read bytes from the cached page file
            readLen = (int) (remainingBytes > pageLen ?
                    pageLen : remainingBytes);
            cachedPageInput.seek(pageOffset);
            cachedPageInput.readBytes(buffer, bufferPos, readLen);
            remainingBytes -= readLen;
            bufferPos += readLen;
        }
    }

    public void openFile(String name) throws IOException {
        logger.debug("openFile {}", name);
        // Create cache file directory
        Files.createDirectories(fsDirectory.getDirectory().resolve(name));
    }

    public void closeFile(String name) {
        logger.debug("closeFile {}", name);
        // FIXME: memory usage optimization
    }

    public void close() throws IOException {
        logger.debug("close");
        // Close all the opened index inputs
        for (String key : indexInputMap.keySet()) {
            IndexInput indexInput = indexInputMap.get(key);
            indexInput.close();
        }
        indexInputMap.clear();
        fsDirectory.close();
    }

    private IndexInput cacheMissHandler(String name, long pageIdx) throws IOException {
        //logger.debug("S3IndexInput.cacheMissHandler ({} pos {} totalLength {} pageIdx {} )", name, position, totalLength, pageIdx);

        int pageStartOffset = (int) (pageIdx * CACHE_PAGE_SIZE);
        String key = name + "/" + pageIdx;
        Path pageFilePath = fsDirectory.getDirectory().resolve(key);
        long fileLength = fileLength(name);

        if (Files.notExists(pageFilePath)) {
            // Calculate the remaining bytes of the object
            int readLen = (int) (fileLength > pageStartOffset + CACHE_PAGE_SIZE ?
                    CACHE_PAGE_SIZE : fileLength - pageStartOffset);

            // Read the page from the corresponding file in buffer or storage
            if (buffer.fileExists(name)) {
                buffer.readToFile(name, pageStartOffset, readLen, pageFilePath.toFile());
            } else {
                storage.readToFile(name, pageStartOffset, readLen, pageFilePath.toFile());
            }
        }

        return fsDirectory.openInput(key, IOContext.DEFAULT);
    }

    public boolean fileExists(final String name) {
        return Files.exists(fsDirectory.getDirectory().resolve(name));
    }
}
