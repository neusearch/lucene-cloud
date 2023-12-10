package io.neusearch.lucene.store.s3.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;

public class StorageFactory {
    private static final Logger logger = LoggerFactory.getLogger(StorageFactory.class);
    public Storage createStorage(String storageType, HashMap<String, Object> params) throws IOException
    {
        try {
            if (storageType == null || storageType.isEmpty())
                return null;
            return switch (storageType.toLowerCase()) {
                case "s3" -> new S3Storage(params);
                default -> throw new IllegalArgumentException("Unknown storage " + storageType);
            };
        } catch (IOException ioe) {
            logger.error("{}", ioe.toString());
            throw ioe;
        }
    }
}
