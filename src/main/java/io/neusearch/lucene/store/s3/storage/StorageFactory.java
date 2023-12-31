package io.neusearch.lucene.store.s3.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;

/**
 * Cloud storage factory class designed to handle various storage types.
 */
public class StorageFactory {
    private static final Logger logger = LoggerFactory.getLogger(StorageFactory.class);

    /**
     * Creates a new StorageFactory
     */
    public StorageFactory() {}

    /**
     * Creates a specific Storage object based on the passed parameters.
     *
     * @param storageType the type of storage to be created, currently only AWS S3 is supported
     * @param params the parameters map that is passed to a specific Storage implementation
     * @return the storage object
     * @throws IllegalArgumentException if the passed storage type is not supported
     */
    public Storage createStorage(String storageType, HashMap<String, Object> params) throws RuntimeException
    {
        if (storageType == null || storageType.isEmpty())
            return null;
        return switch (storageType.toLowerCase()) {
            case "s3" -> new S3Storage(params);
            default -> throw new IllegalArgumentException("Unknown storage " + storageType);
        };
    }
}
