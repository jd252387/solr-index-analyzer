package org.commrogue.analysis.termvectors;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.commrogue.LuceneFileExtension;
import org.commrogue.analysis.Analysis;
import org.commrogue.results.FieldAnalysis;
import org.commrogue.results.IndexAnalysisResult;
import org.commrogue.results.TermVectorsFieldAnalysis;
import org.commrogue.tracking.BytesReadTracker;
import org.commrogue.tracking.TrackingReadBytesDirectory;

public class TermVectorsAnalysis implements Analysis {
    private final TrackingReadBytesDirectory directory;
    private final SegmentReader segmentReader;
    private final IndexAnalysisResult indexAnalysisResult;
    private final TermVectorsAnalysisMode analysisMode;

    public TermVectorsAnalysis(
            TrackingReadBytesDirectory directory,
            SegmentReader segmentReader,
            IndexAnalysisResult indexAnalysisResult,
            TermVectorsAnalysisMode analysisMode) {
        this.directory = directory;
        this.segmentReader = segmentReader;
        this.indexAnalysisResult = indexAnalysisResult;
        this.analysisMode = analysisMode;
    }

    @Override
    public void analyze() throws Exception {
        if (!segmentReader.getFieldInfos().hasVectors()) {
            return;
        }

        TermVectorSizeEstimator estimator = new TermVectorSizeEstimator(directory, segmentReader);
        Map<String, FieldContribution> contributions = estimator.estimate();
        TermVectorsAnalysisMode resultMode = analysisMode == TermVectorsAnalysisMode.INSTRUMENTED
                ? TermVectorsAnalysisMode.INSTRUMENTED
                : TermVectorsAnalysisMode.STRUCTURAL;
        for (Map.Entry<String, FieldContribution> entry : contributions.entrySet()) {
            FieldAnalysis fieldAnalysis = indexAnalysisResult.getFieldAnalysis(entry.getKey());
            TermVectorsFieldAnalysis termVectors = new TermVectorsFieldAnalysis(resultMode);
            entry.getValue().bytesByExtension.forEach(termVectors::addTrackingByExtension);
            fieldAnalysis.termVectors = termVectors;
        }
    }

    private static final class TermVectorSizeEstimator {
        private final TrackingReadBytesDirectory directory;
        private final SegmentReader segmentReader;

        private TermVectorSizeEstimator(TrackingReadBytesDirectory directory, SegmentReader segmentReader) {
            this.directory = directory;
            this.segmentReader = segmentReader;
        }

        private Map<String, FieldContribution> estimate() throws IOException {
            Map<String, FieldContribution> contributions = new HashMap<>();
            int maxDoc = segmentReader.maxDoc();
            for (int docId = 0; docId < maxDoc; docId++) {
                directory.resetBytesRead();
                Fields termVectors = segmentReader.getTermVectors(docId);
                if (termVectors == null) {
                    continue;
                }

                Map<String, DocFieldStats> docStats = collectDocFieldStats(termVectors);
                Map<LuceneFileExtension, Long> docBytes = collectDocBytes();
                distributeDocBytes(docStats, docBytes, contributions);
            }
            return contributions;
        }

        private Map<String, DocFieldStats> collectDocFieldStats(Fields termVectors) throws IOException {
            Map<String, DocFieldStats> statsByField = new HashMap<>();
            Iterator<String> iterator = termVectors.iterator();
            PostingsEnum postings = null;
            while (iterator.hasNext()) {
                String field = iterator.next();
                Terms terms = termVectors.terms(field);
                if (terms == null) {
                    continue;
                }
                DocFieldStats stats = new DocFieldStats();
                stats.hasPositions = terms.hasPositions();
                stats.hasOffsets = terms.hasOffsets();
                stats.hasPayloads = terms.hasPayloads();
                TermsEnum termsEnum = terms.iterator();
                while (termsEnum.next() != null) {
                    BytesRef term = termsEnum.term();
                    stats.termCount++;
                    stats.termBytes += term.length;
                    postings = termsEnum.postings(postings, PostingsEnum.ALL);
                    while (postings.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                        int freq = postings.freq();
                        stats.totalPositions += freq;
                        for (int i = 0; i < freq; i++) {
                            postings.nextPosition();
                            if (stats.hasOffsets) {
                                postings.startOffset();
                                postings.endOffset();
                            }
                            if (stats.hasPayloads) {
                                BytesRef payload = postings.getPayload();
                                if (payload != null) {
                                    stats.payloadBytes += payload.length;
                                }
                            }
                        }
                    }
                }
                statsByField.put(field, stats);
            }
            return statsByField;
        }

        private Map<LuceneFileExtension, Long> collectDocBytes() {
            Map<LuceneFileExtension, Long> bytesByExtension = new EnumMap<>(LuceneFileExtension.class);
            for (BytesReadTracker.TrackerSummary summary : directory.summarize()) {
                for (BytesReadTracker.TrackedSlice slice : summary.slices()) {
                    LuceneFileExtension extension = LuceneFileExtension.fromFile(slice.sliceDescription());
                    if (extension != LuceneFileExtension.TVD
                            && extension != LuceneFileExtension.TVX
                            && extension != LuceneFileExtension.TVM) {
                        continue;
                    }
                    bytesByExtension.merge(extension, slice.bytesRead(), Long::sum);
                }
            }
            return bytesByExtension;
        }

        private void distributeDocBytes(
                Map<String, DocFieldStats> docStats,
                Map<LuceneFileExtension, Long> docBytes,
                Map<String, FieldContribution> contributions) {
            if (docStats.isEmpty() || docBytes.isEmpty()) {
                return;
            }
            Map<String, Double> weightByField = new HashMap<>();
            for (Map.Entry<String, DocFieldStats> entry : docStats.entrySet()) {
                DocFieldStats stats = entry.getValue();
                double weight = stats.termBytes + stats.termCount;
                if (stats.hasPositions) {
                    weight += stats.totalPositions;
                }
                if (stats.hasOffsets) {
                    weight += stats.totalPositions;
                }
                weight += stats.payloadBytes;
                if (weight <= 0) {
                    weight = 1.0d;
                }
                weightByField.put(entry.getKey(), weight);
            }

            for (String field : docStats.keySet()) {
                FieldContribution contribution = contributions.computeIfAbsent(field, k -> new FieldContribution());
                contribution.docCount++;
            }

            for (Map.Entry<LuceneFileExtension, Long> entry : docBytes.entrySet()) {
                LuceneFileExtension extension = entry.getKey();
                long totalBytes = entry.getValue();
                Map<String, Double> weightsForExtension = weightByField;
                if (extension == LuceneFileExtension.TVM || extension == LuceneFileExtension.TVX) {
                    weightsForExtension = new HashMap<>();
                    for (String field : docStats.keySet()) {
                        weightsForExtension.put(field, 1.0d);
                    }
                }
                Map<String, Long> assigned = distribute(totalBytes, weightsForExtension);
                for (Map.Entry<String, Long> assignment : assigned.entrySet()) {
                    FieldContribution contribution =
                            contributions.computeIfAbsent(assignment.getKey(), k -> new FieldContribution());
                    contribution.bytesByExtension.merge(extension, assignment.getValue(), Long::sum);
                }
            }
        }

        private Map<String, Long> distribute(long totalBytes, Map<String, Double> weights) {
            Map<String, Long> result = new HashMap<>();
            if (totalBytes <= 0 || weights.isEmpty()) {
                return result;
            }
            double totalWeight =
                    weights.values().stream().mapToDouble(Double::doubleValue).sum();
            if (totalWeight <= 0) {
                long perField = totalBytes / weights.size();
                long remainder = totalBytes - perField * weights.size();
                for (String field : weights.keySet()) {
                    long value = perField + (remainder > 0 ? 1 : 0);
                    if (remainder > 0) {
                        remainder--;
                    }
                    result.put(field, value);
                }
                return result;
            }
            List<FieldWeight> ordered = new ArrayList<>();
            long assigned = 0L;
            for (Map.Entry<String, Double> entry : weights.entrySet()) {
                double portion = (entry.getValue() / totalWeight) * totalBytes;
                long value = (long) Math.floor(portion);
                double fractional = portion - value;
                ordered.add(new FieldWeight(entry.getKey(), value, fractional));
                assigned += value;
            }
            long remainder = totalBytes - assigned;
            ordered.sort((a, b) -> Double.compare(b.fractional, a.fractional));
            for (int i = 0; i < remainder && i < ordered.size(); i++) {
                ordered.get(i).increment();
            }
            for (FieldWeight weight : ordered) {
                if (weight.value > 0) {
                    result.put(weight.field, weight.value);
                }
            }
            return result;
        }

        private static final class DocFieldStats {
            private boolean hasPositions;
            private boolean hasOffsets;
            private boolean hasPayloads;
            private long termCount;
            private long termBytes;
            private long totalPositions;
            private long payloadBytes;
        }

        private static final class FieldWeight {
            private final String field;
            private long value;
            private final double fractional;

            private FieldWeight(String field, long value, double fractional) {
                this.field = field;
                this.value = value;
                this.fractional = fractional;
            }

            private void increment() {
                this.value++;
            }
        }
    }

    private static final class FieldContribution {
        private final Map<LuceneFileExtension, Long> bytesByExtension = new EnumMap<>(LuceneFileExtension.class);
        private long docCount;
    }
}
