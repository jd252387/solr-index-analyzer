package org.commrogue.analysis.knn;

import java.util.Arrays;

public enum KnnVectorsAnalysisMode {
    STRUCTURAL("structural"),
    INSTRUMENTED("instrumented"),
    STRUCTURAL_WITH_FALLBACK("structuralWithFallback");

    public final String param;

    KnnVectorsAnalysisMode(String param) {
        this.param = param;
    }

    public static KnnVectorsAnalysisMode fromParam(String param) {
        if (param == null) {
            return STRUCTURAL;
        }
        return Arrays.stream(values())
                .filter(mode -> mode.param.equalsIgnoreCase(param))
                .findFirst()
                .orElse(STRUCTURAL);
    }
}
