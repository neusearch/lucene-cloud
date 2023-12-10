package io.neusearch.lucene.store.s3.buffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;

public class BufferFactory {
    private static final Logger logger = LoggerFactory.getLogger(BufferFactory.class);
    public Buffer createBuffer(String bufferType, HashMap<String, Object> params) throws IOException
    {
        try {
            if (bufferType == null || bufferType.isEmpty())
                return null;
            return switch (bufferType.toLowerCase()) {
                case "fs" -> new FSBuffer(params);
                default -> throw new IllegalArgumentException("Unknown buffer type " + bufferType);
            };
        } catch (IOException ioe) {
            logger.error("{}", ioe.toString());
            throw ioe;
        }
    }
}
