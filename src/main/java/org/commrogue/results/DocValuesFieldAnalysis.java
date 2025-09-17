package org.commrogue.results;

import java.util.Comparator;
import java.util.List;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.commrogue.LuceneFileExtension;
import org.commrogue.analysis.docvalues.DocValuesAnalysisMode;

public class DocValuesFieldAnalysis extends AggregateSegmentReference {
    public final DocValuesAnalysisMode analysisMode;

    private DocValuesFieldAnalysis(
            java.util.Map<LuceneFileExtension, Long> fileEntries, DocValuesAnalysisMode analysisMode) {
        super(fileEntries);
        this.analysisMode = analysisMode;
    }

    public DocValuesFieldAnalysis(DocValuesAnalysisMode analysisMode) {
        super();
        this.analysisMode = analysisMode;
    }

    @Override
    public SimpleOrderedMap<Object> toSimpleOrderedMap() {
        SimpleOrderedMap<Object> map = super.toSimpleOrderedMap();
        map.add("analysis_mode", analysisMode.name());
        return map;
    }

    public static DocValuesFieldAnalysis byMerging(List<DocValuesFieldAnalysis> analyses) {
        AggregateSegmentReference merged = AggregateSegmentReference.byMergingReferences(analyses);
        DocValuesAnalysisMode mode = analyses.stream()
                .map(analysis -> analysis.analysisMode)
                .max(Comparator.comparingInt(DocValuesAnalysisMode::priority))
                .orElse(DocValuesAnalysisMode.STRUCTURAL);
        return new DocValuesFieldAnalysis(merged.getFileEntries(), mode);
    }
}
