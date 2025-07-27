package org.commrogue.results;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.solr.common.util.SimpleOrderedMap;

@AllArgsConstructor
@NoArgsConstructor
@Getter
public class FieldAnalysis {
    public InvertedIndexFieldAnalysis invertedIndex;
    //    public final AggregateSegmentReference storedField = new AggregateSegmentReference();
    //    public final AggregateSegmentReference docValues = new AggregateSegmentReference();
    //    public final AggregateSegmentReference points = new AggregateSegmentReference();
    //    public final AggregateSegmentReference norms = new AggregateSegmentReference();
    //    public final AggregateSegmentReference termVectors = new AggregateSegmentReference();
    //    public final AggregateSegmentReference knnVectors = new AggregateSegmentReference();

    public long getTotalSize() {
        return invertedIndex.getTotalSize();
    }

    public SimpleOrderedMap<Object> toSimpleOrderedMap() {
        SimpleOrderedMap<Object> map = new SimpleOrderedMap<>();

        map.add("inverted_index", invertedIndex.toSimpleOrderedMap());

        return map;
    }

    public static FieldAnalysis byMerging(List<FieldAnalysis> fieldAnalysisList) {
        return new FieldAnalysis(InvertedIndexFieldAnalysis.byMerging(
                fieldAnalysisList.stream().map(FieldAnalysis::getInvertedIndex).toList()));
    }
}
