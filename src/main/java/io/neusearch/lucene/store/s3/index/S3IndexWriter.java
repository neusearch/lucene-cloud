package io.neusearch.lucene.store.s3.index;

import io.neusearch.lucene.store.s3.codec.KnnVectorsFormatMaxDimExt;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99Codec;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;

import java.io.IOException;

/**
 * A IndexWriter implementation for S3.
 */
public class S3IndexWriter extends IndexWriter {

    /**
     * Initializes this instance.
     *
     * @param dir the directory for writing
     * @param config the IndexWriter configurations
     * @throws IOException if any i/o error occurs
     */
    protected S3IndexWriter(Directory dir, IndexWriterConfig config) throws IOException {
        super(dir, config);
    }

    /**
     * Creates a new S3IndexWriter instance with some default settings.
     *
     * @param dir the directory for writing
     * @param config the IndexWriter configurations
     * @return the S3IndexWriter instance
     * @throws IOException if any i/o error occurs
     */
    public static S3IndexWriter create(Directory dir, IndexWriterConfig config) throws IOException {
        config.setCodec(
                new Lucene99Codec() {
                    @Override
                    public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
                        return new KnnVectorsFormatMaxDimExt(new Lucene99HnswVectorsFormat());
                    }
                });
        config.getMergePolicy().setNoCFSRatio(0.0);

        return new S3IndexWriter(dir, config);
    }
}
