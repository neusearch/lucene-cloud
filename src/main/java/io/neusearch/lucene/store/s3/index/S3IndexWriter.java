package io.neusearch.lucene.store.s3.index;

import io.neusearch.lucene.store.s3.codec.KnnVectorsFormatMaxDimExt;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99Codec;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;

import java.io.IOException;

public class S3IndexWriter extends IndexWriter {

    protected S3IndexWriter(Directory d, IndexWriterConfig conf) throws IOException {
        super(d, conf);
    }

    public static S3IndexWriter create(Directory d, IndexWriterConfig conf) throws IOException {
        conf.setCodec(
                new Lucene99Codec() {
                    @Override
                    public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
                        return new KnnVectorsFormatMaxDimExt(new Lucene99HnswVectorsFormat());
                    }
                });
        conf.getMergePolicy().setNoCFSRatio(0.0);

        return new S3IndexWriter(d, conf);
    }
}
