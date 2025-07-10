package org.commrogue.results;

import lombok.RequiredArgsConstructor;
import org.commrogue.analysis.iindex.TermStructureAnalysisMode;

@RequiredArgsConstructor
public class InvertedIndexFieldAnalysis extends AggregateSegmentReference {
    public final TermStructureAnalysisMode termStructureAnalysisMode;
}
