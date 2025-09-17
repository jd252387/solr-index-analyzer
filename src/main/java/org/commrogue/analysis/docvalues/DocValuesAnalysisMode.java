package org.commrogue.analysis.docvalues;

import java.util.Arrays;

public enum DocValuesAnalysisMode {
    STRUCTURAL("structural"),
    STRUCTURAL_WITH_FALLBACK("structural_with_fallback"),
    INSTRUMENTED("instrumented");

    private final String param;

    DocValuesAnalysisMode(String param) {
        this.param = param;
    }

    public static DocValuesAnalysisMode fromParam(String param) {
        return Arrays.stream(values())
                .filter(mode -> mode.param.equalsIgnoreCase(param))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unknown docValuesAnalysisMode: " + param));
    }

    public int priority() {
        return switch (this) {
            case STRUCTURAL -> 0;
            case STRUCTURAL_WITH_FALLBACK -> 1;
            case INSTRUMENTED -> 2;
        };
    }
}
