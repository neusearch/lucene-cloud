package io.neusearch.lucene.store.s3.buffer;

import io.neusearch.lucene.store.s3.storage.Storage;
import org.apache.commons.io.FileUtils;
import org.apache.lucene.store.FSDirectory;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;

import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FSBuffer implements Buffer {
    private static final Logger logger = LoggerFactory.getLogger(FSBuffer.class);

    private final FSDirectory fsDirectory;

    private final Storage storage;

    private final HashMap<String, IndexOutput> indexOutputMap;

    public FSBuffer(HashMap<String, Object> params) throws IOException {
        Path bufferPath = Paths.get(params.get("bufferPath").toString());
        // Delete all the local orphan files not synced to S3 in the fsPath
        File bufferDir = bufferPath.toFile();
        FileUtils.deleteDirectory(bufferDir);
        Files.createDirectories(bufferPath);

        this.storage = (Storage) params.get("storage");
        this.fsDirectory = FSDirectory.open(bufferPath);
        this.indexOutputMap = new HashMap<>();
    }

    @Override
    public String[] listAll() throws IOException {
        logger.debug("listAll()");

        return fsDirectory.listAll();
    }

    public void deleteFile(final String name) throws IOException {
        logger.info("deleteFile {}", name);

        fsDirectory.deleteFile(name);
    }

    public long fileLength(final String name) throws IOException {
        logger.debug("fileLength {}", name);

        if (Files.exists(fsDirectory.getDirectory().resolve(name))) {
            return fsDirectory.fileLength(name);
        } else {
            return -1;
        }
    }

    public void writeByte(String name, byte b) throws IOException {
        IndexOutput indexOutput = indexOutputMap.get(name);
        if (indexOutput == null) {
            throw new FileSystemException(name + "is not opened before writing");
        }
        indexOutput.writeByte(b);
    }

    public void writeBytes(String name, byte[] b, int offset, int length) throws IOException {
        IndexOutput indexOutput = indexOutputMap.get(name);
        if (indexOutput == null) {
            throw new FileSystemException(name + "is not opened before writing");
        }
        indexOutput.writeBytes(b, offset, length);
    }

    public void rename(final String from, final String to) throws IOException {
        if (indexOutputMap.get(from) != null) {
            throw new RuntimeException(from + " exists in buffer during rename");
        }
        fsDirectory.rename(from, to);
    }

    public void sync(final Collection<String> names) throws IOException {
        // Sync all the local files that have not been written to S3 yet
        for (String name : names) {
            Path filePath = fsDirectory.getDirectory().resolve(name);
            if (Files.exists(filePath)) {
                if (indexOutputMap.get(name) != null) {
                    throw new RuntimeException(name + " exists in buffer during sync");
                }
                storage.writeFromFile(filePath);
                fsDirectory.deleteFile(name);
            } else {
                throw new NoSuchFileException(filePath.toString());
            }
        }
    }

    public void syncMetaData() {
        // Do nothing
    }

    public void openFile(String name) throws IOException {
        IndexOutput indexOutput = fsDirectory.createOutput(name, IOContext.DEFAULT);
        indexOutputMap.put(name, indexOutput);
    }

    public void closeFile(String name) throws IOException {
        IndexOutput indexOutput = indexOutputMap.remove(name);
        if (indexOutput == null) {
            throw new NoSuchFileException(fsDirectory.getDirectory().resolve(name).toString());
        }
        indexOutput.close();
    }

    public void close() throws IOException {
        // Close all the opened index outputs
        for (String key : indexOutputMap.keySet()) {
            IndexOutput indexOutput = indexOutputMap.get(key);
            indexOutput.close();
        }
        indexOutputMap.clear();
        fsDirectory.close();
    }
}
