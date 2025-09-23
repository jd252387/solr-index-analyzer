package org.commrogue.results;

import java.util.List;
import java.util.Objects;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.commons.io.FileUtils;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.commrogue.analysis.knn.KnnVectorsFieldAnalysis;

@AllArgsConstructor
@NoArgsConstructor
@Getter
public class FieldAnalysis {
    public InvertedIndexFieldAnalysis invertedIndex;
    public KnnVectorsFieldAnalysis knnVectors;
    public DocValuesFieldAnalysis docValues;
    public TermVectorsFieldAnalysis termVectors;
    public PointValuesFieldAnalysis pointValues;
    public StoredFieldsFieldAnalysis storedFields;
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
        if (docValues != null) {
            total += docValues.getTotalSize();
        }
        if (termVectors != null) {
            total += termVectors.getTotalSize();
        }
        if (pointValues != null) {
            total += pointValues.getTotalSize();
        }
        if (storedFields != null) {
            total += storedFields.getTotalSize();
        }
        return total;
    }

    public SimpleOrderedMap<Object> toSimpleOrderedMap() {
        SimpleOrderedMap<Object> map = new SimpleOrderedMap<>();

        long totalSize = getTotalSize();
        SimpleOrderedMap<Object> fieldTotal = new SimpleOrderedMap<>();
        fieldTotal.add("size", FileUtils.byteCountToDisplaySize(totalSize));
        fieldTotal.add("size_bytes", totalSize);
        map.add("field_total_size", fieldTotal);

        SimpleOrderedMap<Object> analysisTotals = new SimpleOrderedMap<>();
        SimpleOrderedMap<Object> typeDetails = new SimpleOrderedMap<>();

        if (invertedIndex != null) {
            analysisTotals.add("inverted_index", buildSizeEntry(invertedIndex.getTotalSize()));
            typeDetails.add("inverted_index", invertedIndex.toSimpleOrderedMap());
        }
        if (knnVectors != null) {
            analysisTotals.add("knn_vectors", buildSizeEntry(knnVectors.getTotalSize()));
            typeDetails.add("knn_vectors", knnVectors.toSimpleOrderedMap());
        }
        if (docValues != null) {
            analysisTotals.add("doc_values", buildSizeEntry(docValues.getTotalSize()));
            typeDetails.add("doc_values", docValues.toSimpleOrderedMap());
        }
        if (termVectors != null) {
            analysisTotals.add("term_vectors", buildSizeEntry(termVectors.getTotalSize()));
            typeDetails.add("term_vectors", termVectors.toSimpleOrderedMap());
        }
        if (pointValues != null) {
            analysisTotals.add("point_values", buildSizeEntry(pointValues.getTotalSize()));
            typeDetails.add("point_values", pointValues.toSimpleOrderedMap());
        }
        if (storedFields != null) {
            analysisTotals.add("stored_fields", buildSizeEntry(storedFields.getTotalSize()));
            typeDetails.add("stored_fields", storedFields.toSimpleOrderedMap());
        }

        if (analysisTotals.size() > 0) {
            map.add("analysis_totals", analysisTotals);
        }

        for (int i = 0; i < typeDetails.size(); i++) {
            map.add(typeDetails.getName(i), typeDetails.getVal(i));
        }

        return map;
    }

    private SimpleOrderedMap<Object> buildSizeEntry(long bytes) {
        SimpleOrderedMap<Object> sizeEntry = new SimpleOrderedMap<>();
        sizeEntry.add("size", FileUtils.byteCountToDisplaySize(bytes));
        sizeEntry.add("size_bytes", bytes);
        return sizeEntry;
    }

    public static FieldAnalysis byMerging(List<FieldAnalysis> fieldAnalysisList) {
        InvertedIndexFieldAnalysis mergedInvertedIndex = null;
        List<InvertedIndexFieldAnalysis> invertedIndexAnalyses = fieldAnalysisList.stream()
                .map(fieldAnalysis -> fieldAnalysis.invertedIndex)
                .filter(Objects::nonNull)
                .toList();
        if (!invertedIndexAnalyses.isEmpty()) {
            mergedInvertedIndex = InvertedIndexFieldAnalysis.byMerging(invertedIndexAnalyses);
        }

        KnnVectorsFieldAnalysis mergedKnnVectors = null;
        List<KnnVectorsFieldAnalysis> knnAnalyses = fieldAnalysisList.stream()
                .map(fieldAnalysis -> fieldAnalysis.knnVectors)
                .filter(Objects::nonNull)
                .toList();
        if (!knnAnalyses.isEmpty()) {
            mergedKnnVectors = KnnVectorsFieldAnalysis.byMerging(knnAnalyses);
        }

        DocValuesFieldAnalysis mergedDocValues = null;
        List<DocValuesFieldAnalysis> docValuesAnalyses = fieldAnalysisList.stream()
                .map(fieldAnalysis -> fieldAnalysis.docValues)
                .filter(Objects::nonNull)
                .toList();
        if (!docValuesAnalyses.isEmpty()) {
            mergedDocValues = DocValuesFieldAnalysis.byMerging(docValuesAnalyses);
        }

        TermVectorsFieldAnalysis mergedTermVectors = null;
        List<TermVectorsFieldAnalysis> termVectorAnalyses = fieldAnalysisList.stream()
                .map(fieldAnalysis -> fieldAnalysis.termVectors)
                .filter(Objects::nonNull)
                .toList();
        if (!termVectorAnalyses.isEmpty()) {
            mergedTermVectors = TermVectorsFieldAnalysis.byMerging(termVectorAnalyses);
        }

        PointValuesFieldAnalysis mergedPointValues = null;
        List<PointValuesFieldAnalysis> pointAnalyses = fieldAnalysisList.stream()
                .map(fieldAnalysis -> fieldAnalysis.pointValues)
                .filter(Objects::nonNull)
                .toList();
        if (!pointAnalyses.isEmpty()) {
            mergedPointValues = PointValuesFieldAnalysis.byMerging(pointAnalyses);
        }

        StoredFieldsFieldAnalysis mergedStoredFields = null;
        List<StoredFieldsFieldAnalysis> storedAnalyses = fieldAnalysisList.stream()
                .map(fieldAnalysis -> fieldAnalysis.storedFields)
                .filter(Objects::nonNull)
                .toList();
        if (!storedAnalyses.isEmpty()) {
            mergedStoredFields = StoredFieldsFieldAnalysis.byMerging(storedAnalyses);
        }

        return new FieldAnalysis(
                mergedInvertedIndex,
                mergedKnnVectors,
                mergedDocValues,
                mergedTermVectors,
                mergedPointValues,
                mergedStoredFields);
    }
}
