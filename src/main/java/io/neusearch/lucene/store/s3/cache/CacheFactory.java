package io.neusearch.lucene.store.s3.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;

public class CacheFactory {
    private static final Logger logger = LoggerFactory.getLogger(CacheFactory.class);
    public Cache createCache(String cacheType, HashMap<String, Object> params) throws IOException
    {
        try {
            if (cacheType == null || cacheType.isEmpty())
                return null;
            return switch (cacheType.toLowerCase()) {
                case "fs" -> new FSCache(params);
                default -> throw new IllegalArgumentException("Unknown cache type " + cacheType);
            };
        } catch (IOException ioe) {
            logger.error("{}", ioe.toString());
            throw ioe;
        }
    }
}
