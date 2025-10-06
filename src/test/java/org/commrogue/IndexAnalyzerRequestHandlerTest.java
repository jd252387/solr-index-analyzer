package org.commrogue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.commrogue.IndexAnalyzerRequestHandler.AnalysisParams;
import org.commrogue.IndexAnalyzerRequestHandler.AnalysisType;
import org.commrogue.IndexAnalyzerRequestHandler.SegmentFilter;
import org.commrogue.analysis.docvalues.DocValuesAnalysisMode;
import org.commrogue.analysis.iindex.TermStructureAnalysisMode;
import org.commrogue.analysis.knn.KnnVectorsAnalysisMode;
import org.commrogue.analysis.points.PointValuesAnalysisMode;
import org.commrogue.analysis.storedfields.StoredFieldsAnalysisMode;
import org.commrogue.analysis.termvectors.TermVectorsAnalysisMode;
import org.commrogue.lucene.Utils;
import org.commrogue.results.IndexAnalysisResult;
import org.commrogue.tracking.DelegatingDirectoryIndexCommit;
import org.commrogue.tracking.TrackingReadBytesDirectory;
import org.junit.jupiter.api.Test;

class IndexAnalyzerRequestHandlerTest {

    private static final String SINGLE_SEGMENT_FIELD = "singleSegmentField";
    private static final String FIRST_SEGMENT_FIELD = "firstSegmentField";
    private static final String SECOND_SEGMENT_FIELD = "secondSegmentField";

    private static final Analyzer NO_OP_ANALYZER = new Analyzer() {
        @Override
        protected TokenStreamComponents createComponents(String fieldName) {
            Tokenizer tokenizer = new Tokenizer() {
                @Override
                public boolean incrementToken() throws IOException {
                    return false;
                }
            };
            return new TokenStreamComponents(tokenizer);
        }
    };

    private static final AnalysisParams STORED_FIELDS_PARAMS = new AnalysisParams(
            TermStructureAnalysisMode.BLOCK_SKIPPING,
            DocValuesAnalysisMode.STRUCTURAL_WITH_FALLBACK,
            PointValuesAnalysisMode.STRUCTURAL_WITH_FALLBACK,
            TermVectorsAnalysisMode.STRUCTURAL_WITH_FALLBACK,
            KnnVectorsAnalysisMode.STRUCTURAL,
            StoredFieldsAnalysisMode.INSTRUMENTED);

    @Test
    void retainsMatchingSegmentInSingleSegmentIndex() throws Exception {
        try (Directory directory = createSingleSegmentIndex()) {
            List<String> segmentNames = listSegmentNames(directory);
            assertEquals(1, segmentNames.size());

            SegmentFilter filter = new SegmentFilter(Set.of(segmentNames.get(0)), true);
            IndexAnalysisResult result = analyze(directory, filter);

            assertEquals(
                    Set.of(SINGLE_SEGMENT_FIELD), result.getFieldAnalysisMap().keySet());
        }
    }

    @Test
    void skipsNonMatchingSegmentInSingleSegmentIndex() throws Exception {
        try (Directory directory = createSingleSegmentIndex()) {
            SegmentFilter filter = new SegmentFilter(Set.of("bogus"), true);
            IndexAnalysisResult result = analyze(directory, filter);

            assertTrue(result.getFieldAnalysisMap().isEmpty());
        }
    }

    @Test
    void includesOnlyRequestedSegmentInMultiSegmentIndex() throws Exception {
        try (Directory directory = createTwoSegmentIndex()) {
            Map<String, String> segmentFields = mapSegmentToField(directory);
            assertEquals(2, segmentFields.size());

            String targetSegment = segmentFields.entrySet().stream()
                    .filter(entry -> SECOND_SEGMENT_FIELD.equals(entry.getValue()))
                    .map(Map.Entry::getKey)
                    .findFirst()
                    .orElseThrow();

            SegmentFilter filter = new SegmentFilter(Set.of(targetSegment), true);
            IndexAnalysisResult result = analyze(directory, filter);

            assertEquals(
                    Set.of(SECOND_SEGMENT_FIELD), result.getFieldAnalysisMap().keySet());
        }
    }

    private IndexAnalysisResult analyze(Directory directory, SegmentFilter filter) throws Exception {
        try (DirectoryReader indexReader = DirectoryReader.open(directory)) {
            IndexCommit commit = indexReader.getIndexCommit();
            TrackingReadBytesDirectory trackingDirectory = new TrackingReadBytesDirectory(commit.getDirectory());
            try {
                IndexCommit trackingCommit = new DelegatingDirectoryIndexCommit(commit) {
                    @Override
                    public Directory getDirectory() {
                        return trackingDirectory;
                    }
                };

                try (DirectoryReader reader = DirectoryReader.open(trackingCommit)) {
                    trackingDirectory.resetBytesRead();
                    return IndexAnalyzerRequestHandler.analyzeSegments(
                            reader,
                            trackingDirectory,
                            List.of(AnalysisType.STORED_FIELDS),
                            STORED_FIELDS_PARAMS,
                            filter);
                }
            } finally {
                trackingDirectory.close();
            }
        }
    }

    private static Directory createSingleSegmentIndex() throws IOException {
        Directory directory = new ByteBuffersDirectory();
        try (IndexWriter writer = new IndexWriter(directory, writerConfig())) {
            writer.addDocument(documentWithStoredField(SINGLE_SEGMENT_FIELD, "value"));
            writer.commit();
        }
        return directory;
    }

    private static Directory createTwoSegmentIndex() throws IOException {
        Directory directory = new ByteBuffersDirectory();
        try (IndexWriter writer = new IndexWriter(directory, writerConfig())) {
            writer.addDocument(documentWithStoredField(FIRST_SEGMENT_FIELD, "first"));
            writer.commit();
            writer.addDocument(documentWithStoredField(SECOND_SEGMENT_FIELD, "second"));
            writer.commit();
        }
        return directory;
    }

    private static IndexWriterConfig writerConfig() {
        IndexWriterConfig config = new IndexWriterConfig(NO_OP_ANALYZER);
        config.setMergePolicy(NoMergePolicy.INSTANCE);
        config.setUseCompoundFile(true);
        return config;
    }

    private static Document documentWithStoredField(String fieldName, String value) {
        Document document = new Document();
        document.add(new StoredField(fieldName, value));
        return document;
    }

    private static List<String> listSegmentNames(Directory directory) throws IOException {
        try (DirectoryReader reader = DirectoryReader.open(directory)) {
            List<String> names = new ArrayList<>();
            for (LeafReaderContext context : reader.leaves()) {
                SegmentReader segmentReader = Utils.segmentReader(context.reader());
                names.add(segmentReader.getSegmentName());
            }
            return names;
        }
    }

    private static Map<String, String> mapSegmentToField(Directory directory) throws IOException {
        try (DirectoryReader reader = DirectoryReader.open(directory)) {
            Map<String, String> mapping = new java.util.LinkedHashMap<>();
            for (LeafReaderContext context : reader.leaves()) {
                SegmentReader segmentReader = Utils.segmentReader(context.reader());
                Document document = segmentReader.document(0);
                String fieldName = document.getFields().get(0).name();
                mapping.put(segmentReader.getSegmentName(), fieldName);
            }
            return mapping;
        }
    }
}
