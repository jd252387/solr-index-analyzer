package org.commrogue.results;

import java.util.List;
import java.util.Map;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.commrogue.LuceneFileExtension;
import org.commrogue.analysis.iindex.TermStructureAnalysisMode;

public class InvertedIndexFieldAnalysis extends AggregateSegmentReference {
    public final TermStructureAnalysisMode termStructureAnalysisMode;

    private InvertedIndexFieldAnalysis(
            Map<LuceneFileExtension, Long> fileEntries, TermStructureAnalysisMode termStructureAnalysisMode) {
        super(fileEntries);
        this.termStructureAnalysisMode = termStructureAnalysisMode;
    }

    public InvertedIndexFieldAnalysis(TermStructureAnalysisMode termStructureAnalysisMode) {
        this.termStructureAnalysisMode = termStructureAnalysisMode;
    }

    @Override
    public SimpleOrderedMap<Object> toSimpleOrderedMap() {
        SimpleOrderedMap<Object> segmentReferenceMap = super.toSimpleOrderedMap();

        segmentReferenceMap.add("term_structure_analysis_mode", termStructureAnalysisMode.name());

        return segmentReferenceMap;
    }

    public static InvertedIndexFieldAnalysis byMerging(List<InvertedIndexFieldAnalysis> invertedIndexAnalyses) {
        AggregateSegmentReference mergedAggregateReference =
                AggregateSegmentReference.byMergingReferences(invertedIndexAnalyses);

        return new InvertedIndexFieldAnalysis(
                mergedAggregateReference.fileEntries, invertedIndexAnalyses.get(0).termStructureAnalysisMode);
    }
}
