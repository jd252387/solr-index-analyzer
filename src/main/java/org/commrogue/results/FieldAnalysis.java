package org.commrogue.results;

import java.util.List;
import java.util.Objects;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.commrogue.analysis.knn.KnnVectorsFieldAnalysis;

@AllArgsConstructor
@NoArgsConstructor
@Getter
public class FieldAnalysis {
    public InvertedIndexFieldAnalysis invertedIndex;
    public KnnVectorsFieldAnalysis knnVectors;
    //    public final AggregateSegmentReference storedField = new AggregateSegmentReference();
    //    public final AggregateSegmentReference docValues = new AggregateSegmentReference();
    //    public final AggregateSegmentReference points = new AggregateSegmentReference();
    //    public final AggregateSegmentReference norms = new AggregateSegmentReference();
    //    public final AggregateSegmentReference termVectors = new AggregateSegmentReference();
    //    public final AggregateSegmentReference knnVectors = new AggregateSegmentReference();

    public long getTotalSize() {
        long total = 0L;
        if (invertedIndex != null) {
            total += invertedIndex.getTotalSize();
        }
        if (knnVectors != null) {
            total += knnVectors.getTotalSize();
        }
        return total;
    }

    public SimpleOrderedMap<Object> toSimpleOrderedMap() {
        SimpleOrderedMap<Object> map = new SimpleOrderedMap<>();

        if (invertedIndex != null) {
            map.add("inverted_index", invertedIndex.toSimpleOrderedMap());
        }
        if (knnVectors != null) {
            map.add("knn_vectors", knnVectors.toSimpleOrderedMap());
        }

        return map;
    }

    public static FieldAnalysis byMerging(List<FieldAnalysis> fieldAnalysisList) {
        InvertedIndexFieldAnalysis mergedInvertedIndex = null;
        List<InvertedIndexFieldAnalysis> invertedIndexAnalyses = fieldAnalysisList.stream()
                .map(FieldAnalysis::getInvertedIndex)
                .filter(Objects::nonNull)
                .toList();
        if (!invertedIndexAnalyses.isEmpty()) {
            mergedInvertedIndex = InvertedIndexFieldAnalysis.byMerging(invertedIndexAnalyses);
        }

        KnnVectorsFieldAnalysis mergedKnnVectors = null;
        List<KnnVectorsFieldAnalysis> knnAnalyses = fieldAnalysisList.stream()
                .map(FieldAnalysis::getKnnVectors)
                .filter(Objects::nonNull)
                .toList();
        if (!knnAnalyses.isEmpty()) {
            mergedKnnVectors = KnnVectorsFieldAnalysis.byMerging(knnAnalyses);
        }

        return new FieldAnalysis(mergedInvertedIndex, mergedKnnVectors);
    }
}
