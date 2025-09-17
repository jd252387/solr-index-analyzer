package org.commrogue.results;

import java.util.Comparator;
import java.util.List;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.commrogue.LuceneFileExtension;
import org.commrogue.analysis.points.PointValuesAnalysisMode;

public class PointValuesFieldAnalysis extends AggregateSegmentReference {
    public final PointValuesAnalysisMode analysisMode;

    private PointValuesFieldAnalysis(
            java.util.Map<LuceneFileExtension, Long> fileEntries, PointValuesAnalysisMode analysisMode) {
        super(fileEntries);
        this.analysisMode = analysisMode;
    }

    public PointValuesFieldAnalysis(PointValuesAnalysisMode analysisMode) {
        super();
        this.analysisMode = analysisMode;
    }

    @Override
    public SimpleOrderedMap<Object> toSimpleOrderedMap() {
        SimpleOrderedMap<Object> map = super.toSimpleOrderedMap();
        map.add("analysis_mode", analysisMode.name());
        return map;
    }

    public static PointValuesFieldAnalysis byMerging(List<PointValuesFieldAnalysis> analyses) {
        AggregateSegmentReference merged = AggregateSegmentReference.byMergingReferences(analyses);
        PointValuesAnalysisMode mode = analyses.stream()
                .map(analysis -> analysis.analysisMode)
                .max(Comparator.comparingInt(PointValuesAnalysisMode::priority))
                .orElse(PointValuesAnalysisMode.STRUCTURAL);
        return new PointValuesFieldAnalysis(merged.getFileEntries(), mode);
    }
}
