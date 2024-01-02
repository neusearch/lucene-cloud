package io.neusearch.lucene.store.s3.codec;

import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

import java.io.IOException;

/**
 * A KnnVectorsFormat implementation for extended dimension support.
 */
public class KnnVectorsFormatMaxDimExt extends KnnVectorsFormat {

    private static final short MAX_DIMS_COUNT = 4096;

    private final KnnVectorsFormat delegate;

    /**
     * Creates a new KnnVectorsFormatMaxDimExt.
     *
     * @param delegate the original implementation
     */
    public KnnVectorsFormatMaxDimExt(KnnVectorsFormat delegate) {
        super(delegate.getName());
        this.delegate = delegate;
    }

    @Override
    public KnnVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
        return delegate.fieldsWriter(state);
    }

    @Override
    public KnnVectorsReader fieldsReader(SegmentReadState state) throws IOException {
        return delegate.fieldsReader(state);
    }

    @Override
    public int getMaxDimensions(String fieldName) {
        return MAX_DIMS_COUNT;
    }
}