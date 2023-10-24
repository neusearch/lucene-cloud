package io.neusearch.lucene.store.s3;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Date;
import java.util.Random;

/**
 * Index all text files under a directory.
 *
 * <p>This is a command-line application demonstrating simple Lucene indexing. Run it with no
 * command-line arguments for usage information.
 */
public class SearchDemo {

    /** Index all text files under a directory. */
    public static void main(String[] args) throws Exception {
        String usage =
                "java org.apache.lucene.demo.IndexFiles"
                        + " [-index INDEX_PATH] [-docs DOCS_PATH] [-update] [-knn_dict DICT_PATH]\n\n"
                        + "This indexes the documents in DOCS_PATH, creating a Lucene index"
                        + "in INDEX_PATH that can be searched with SearchFiles\n"
                        + "IF DICT_PATH contains a KnnVector dictionary, the index will also support KnnVector search";
        String indexBucket = "index";
        String indexPrefix = "";
        String bufferPath = "";
        String cachePath = "";
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-indexBucket" -> indexBucket = args[++i];
                case "-indexPrefix" -> indexPrefix = args[++i];
                case "-bufferPath" -> bufferPath = args[++i];
                case "-cachePath" -> cachePath = args[++i];
                default -> throw new IllegalArgumentException("unknown parameter " + args[i]);
            }
        }

        System.out.println("Searching to bucket '" + indexBucket + " prefix '" + indexPrefix + "'...");

        Directory dir = new S3Directory(indexBucket, indexPrefix, Paths.get(bufferPath), Paths.get(cachePath));
        //Directory dir = FSDirectory.open(Paths.get("/Users/steven/lucene-test/local"));

        IndexReader reader = DirectoryReader.open(dir);
        /*
        System.out.println("Start to get num docs");
        Date start = new Date();
        int numDocs = reader.numDocs();
        Date end = new Date();
        System.out.println(
                "Get number of docs in index "
                        + numDocs
                        + " in "
                        + (end.getTime() - start.getTime())
                        + " ms");
         */
        float[] vector = new float[128];
        Random rand = new Random();
        for (int i = 0; i < 128; i++) {
            vector[i] = rand.nextFloat();
        }
        KnnFloatVectorQuery knnQuery =
                new KnnFloatVectorQuery(
                        "vector",
                        vector,
                        10);

        IndexSearcher searcher = new IndexSearcher(reader);
        for (int i = 1; i <= 3; i++) {
            Date start = new Date();
            TopDocs topDocs = searcher.search(knnQuery, 10);
            Date end = new Date();

            System.out.println(i + "th search time " + (end.getTime() - start.getTime()) + " ms " + topDocs.scoreDocs.toString());
        }

        reader.close();
    }
}
