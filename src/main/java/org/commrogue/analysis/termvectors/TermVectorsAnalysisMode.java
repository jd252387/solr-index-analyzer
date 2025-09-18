package org.commrogue.analysis.termvectors;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public enum TermVectorsAnalysisMode {
    STRUCTURAL("structural", 2),
    STRUCTURAL_WITH_FALLBACK("structural-with-fallback", 1),
    INSTRUMENTED("instrumented", 0);

    public final String param;

    @Getter
    private final int priority;

    public static TermVectorsAnalysisMode fromParam(String param) {
        for (TermVectorsAnalysisMode mode : values()) {
            if (mode.param.equalsIgnoreCase(param)) {
                return mode;
            }
        }
        throw new IllegalArgumentException("Unknown term vectors analysis mode: " + param);
    }
}
