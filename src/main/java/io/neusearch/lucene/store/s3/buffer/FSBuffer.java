package io.neusearch.lucene.store.s3.buffer;

import io.neusearch.lucene.store.s3.storage.Storage;
import org.apache.commons.io.FileUtils;
import org.apache.lucene.store.FSDirectory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
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
        logger.debug("deleteFile {}", name);

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
        logger.debug("writeByte {}", name);
        IndexOutput indexOutput = indexOutputMap.get(name);
        if (indexOutput == null) {
            throw new FileSystemException(name + "is not opened before writing");
        }
        indexOutput.writeByte(b);
    }

    public void writeBytes(String name, byte[] b, int offset, int length) throws IOException {
        logger.debug("writeBytes {} offset {} length {}", name, offset, length);
        IndexOutput indexOutput = indexOutputMap.get(name);
        if (indexOutput == null) {
            throw new FileSystemException(name + "is not opened before writing");
        }
        indexOutput.writeBytes(b, offset, length);
    }

    public void readToFile(final String name, final int fileOffset, final int len, final File file) throws IOException {
        FileChannel src = new FileInputStream(fsDirectory.getDirectory().resolve(name).toFile()).getChannel();
        FileChannel dest = new FileOutputStream(file).getChannel();
        src.transferTo(fileOffset, len, dest);
        src.close();
        dest.close();
    }

    public boolean fileExists(final String name) {
        return Files.exists(fsDirectory.getDirectory().resolve(name));
    }

    public void rename(final String from, final String to) throws IOException {
        logger.debug("rename {} -> {}", from, to);
        if (indexOutputMap.get(from) != null) {
            throw new RuntimeException(from + " exists in buffer during rename");
        }
        fsDirectory.rename(from, to);
    }

    public void sync(final String name) throws IOException {
        logger.debug("sync {}",name);
        Path filePath = fsDirectory.getDirectory().resolve(name);
        if (Files.exists(filePath)) {
            if (indexOutputMap.get(name) != null) {
                throw new RuntimeException(name + " opened in buffer during sync");
            }
            storage.writeFromFile(filePath);
            fsDirectory.deleteFile(name);
        } else {
            throw new NoSuchFileException(filePath.toString());
        }
    }

    public void syncMetaData() {
        logger.debug("syncMetadata");
        // Do nothing
    }

    public void openFile(String name) throws IOException {
        logger.debug("openFile {}", name);
        IndexOutput indexOutput = fsDirectory.createOutput(name, IOContext.DEFAULT);
        indexOutputMap.put(name, indexOutput);
    }

    public void closeFile(String name) throws IOException {
        logger.debug("closeFile {}", name);
        IndexOutput indexOutput = indexOutputMap.remove(name);
        if (indexOutput == null) {
            throw new NoSuchFileException(fsDirectory.getDirectory().resolve(name).toString());
        }
        indexOutput.close();
    }

    public void close() throws IOException {
        logger.debug("close");
        // Close all the opened index outputs
        for (String key : indexOutputMap.keySet()) {
            IndexOutput indexOutput = indexOutputMap.get(key);
            indexOutput.close();
        }
        indexOutputMap.clear();
        fsDirectory.close();
    }
}
