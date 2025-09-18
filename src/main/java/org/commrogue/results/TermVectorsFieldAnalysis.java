package org.commrogue.results;

import java.util.Comparator;
import java.util.List;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.commrogue.LuceneFileExtension;
import org.commrogue.analysis.termvectors.TermVectorsAnalysisMode;

public class TermVectorsFieldAnalysis extends AggregateSegmentReference {
    public final TermVectorsAnalysisMode analysisMode;

    public TermVectorsFieldAnalysis(TermVectorsAnalysisMode analysisMode) {
        super();
        this.analysisMode = analysisMode;
    }

    private TermVectorsFieldAnalysis(
            java.util.Map<LuceneFileExtension, Long> fileEntries, TermVectorsAnalysisMode analysisMode) {
        super(fileEntries);
        this.analysisMode = analysisMode;
    }

    @Override
    public SimpleOrderedMap<Object> toSimpleOrderedMap() {
        SimpleOrderedMap<Object> map = super.toSimpleOrderedMap();
        map.add("analysis_mode", analysisMode.name());
        return map;
    }

    public static TermVectorsFieldAnalysis byMerging(List<TermVectorsFieldAnalysis> analyses) {
        AggregateSegmentReference merged = AggregateSegmentReference.byMergingReferences(analyses);
        TermVectorsAnalysisMode mode = analyses.stream()
                .map(analysis -> analysis.analysisMode)
                .max(Comparator.comparingInt(TermVectorsAnalysisMode::getPriority))
                .orElse(TermVectorsAnalysisMode.STRUCTURAL);
        return new TermVectorsFieldAnalysis(merged.getFileEntries(), mode);
    }
}
