package io.neusearch.lucene.store.s3.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
     * @param config the configuration object that is passed to a specific Storage implementation
     * @return the storage object
     * @throws IllegalArgumentException if the passed storage type is not supported
     */
    public Storage createStorage(String storageType, Object config) throws RuntimeException
    {
        if (storageType == null || storageType.isEmpty())
            return null;
        return switch (storageType.toLowerCase()) {
            case "s3" -> new S3Storage((S3StorageConfig) config);
            default -> throw new IllegalArgumentException("Unknown storage " + storageType);
        };
    }
}
