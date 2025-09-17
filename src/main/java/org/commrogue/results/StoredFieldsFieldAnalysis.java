package org.commrogue.results;

import java.util.Comparator;
import java.util.List;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.commrogue.LuceneFileExtension;
import org.commrogue.analysis.storedfields.StoredFieldsAnalysisMode;

public class StoredFieldsFieldAnalysis extends AggregateSegmentReference {
    public final StoredFieldsAnalysisMode analysisMode;

    private StoredFieldsFieldAnalysis(
            java.util.Map<LuceneFileExtension, Long> fileEntries, StoredFieldsAnalysisMode analysisMode) {
        super(fileEntries);
        this.analysisMode = analysisMode;
    }

    public StoredFieldsFieldAnalysis(StoredFieldsAnalysisMode analysisMode) {
        super();
        this.analysisMode = analysisMode;
    }

    @Override
    public SimpleOrderedMap<Object> toSimpleOrderedMap() {
        SimpleOrderedMap<Object> map = super.toSimpleOrderedMap();
        map.add("analysis_mode", analysisMode.name());
        return map;
    }

    public static StoredFieldsFieldAnalysis byMerging(List<StoredFieldsFieldAnalysis> analyses) {
        AggregateSegmentReference merged = AggregateSegmentReference.byMergingReferences(analyses);
        StoredFieldsAnalysisMode mode = analyses.stream()
                .map(analysis -> analysis.analysisMode)
                .max(Comparator.comparingInt(StoredFieldsAnalysisMode::priority))
                .orElse(StoredFieldsAnalysisMode.STRUCTURAL);
        return new StoredFieldsFieldAnalysis(merged.getFileEntries(), mode);
    }
}
