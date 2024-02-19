package io.neusearch.lucene.store.s3.cache;

import io.neusearch.lucene.store.s3.index.S3IndexInput;
import org.apache.lucene.store.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;

public class FSCache {

    private static final Logger logger = LoggerFactory.getLogger(S3IndexInput.class);
    private final FSDirectory cache;
    private final Path directory;

    public FSCache(Path directory) throws IOException {
        this.directory = directory;
        this.cache = FSDirectory.open(directory);
    }

    public void deleteFile(String name) throws IOException {
        try {
            cache.deleteFile(name);
        } catch (NoSuchFileException ignored) {}
    }

    public long fileLength(String name) throws IOException {
        return cache.fileLength(name);
    }

    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        return cache.createOutput(name, context);
    }

    public IndexOutput createTempOutput(String prefix, String suffix, IOContext context)
            throws IOException {
        return cache.createTempOutput(prefix, suffix, context);
    }

    public IndexInput openInput(String name, IOContext context) throws IOException {
        return cache.openInput(name, context);
    }

    public void rename(String src, String dest) throws IOException {
        cache.rename(src, dest);
    }

    public String[] listAll() throws IOException {
        return cache.listAll();
    }

    public Path getDirectory() {
        return directory;
    }

    public void close() throws IOException {
        cache.close();
    }

    public boolean exists(String name) {
        return Files.exists(buildFullPath(name));
    }

    public Path buildFullPath(String name) {
        return directory.resolve(name);
    }

    public final Lock obtainLock(String name) throws IOException {
        return cache.obtainLock(name);
    }

}
