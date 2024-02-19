package io.neusearch.lucene.store.s3.storage;

/**
 * A S3 storage configuration.
 */
public class S3StorageConfig {
    private String bucket;
    private String prefix;

    /**
     * Creates a new configuration for S3 storage.
     *
     * @param bucket the bucket name
     * @param prefix the prefix name
     */
    public S3StorageConfig(final String bucket, final String prefix) {
        this.bucket = bucket;
        this.prefix = prefix;
    }

    /**
     * Gets the bucket name.
     *
     * @return the bucket name
     */
    public String getBucket() {
        return bucket;
    }

    /**
     * Sets the bucket name.
     *
     * @param bucket the bucket name to set
     */
    public void setBucket(String bucket) {
        this.bucket = bucket;
    }

    /**
     * Gets the prefix name.
     *
     * @return the prefix name
     */
    public String getPrefix() {
        return prefix;
    }

    /**
     * Sets the prefix name.
     *
     * @param prefix the prefix name to set
     */
    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }
}
