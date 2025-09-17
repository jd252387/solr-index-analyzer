package org.commrogue.analysis.knn;

import java.util.List;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.commrogue.LuceneFileExtension;
import org.commrogue.results.AggregateSegmentReference;

public class KnnVectorsFieldAnalysis extends AggregateSegmentReference {
    public final KnnVectorsAnalysisMode analysisMode;

    private KnnVectorsFieldAnalysis(
            java.util.Map<LuceneFileExtension, Long> fileEntries, KnnVectorsAnalysisMode analysisMode) {
        super(fileEntries);
        this.analysisMode = analysisMode;
    }

    public KnnVectorsFieldAnalysis(KnnVectorsAnalysisMode analysisMode) {
        super();
        this.analysisMode = analysisMode;
    }

    @Override
    public SimpleOrderedMap<Object> toSimpleOrderedMap() {
        SimpleOrderedMap<Object> map = super.toSimpleOrderedMap();
        map.add("analysis_mode", analysisMode.name());
        return map;
    }

    public static KnnVectorsFieldAnalysis byMerging(List<KnnVectorsFieldAnalysis> analyses) {
        AggregateSegmentReference merged = AggregateSegmentReference.byMergingReferences(analyses);
        return new KnnVectorsFieldAnalysis(merged.getFileEntries(), analyses.get(0).analysisMode);
    }
}
