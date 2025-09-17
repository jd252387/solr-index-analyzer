package org.commrogue.analysis.points;

import java.util.Arrays;

public enum PointValuesAnalysisMode {
    STRUCTURAL("structural"),
    STRUCTURAL_WITH_FALLBACK("structural_with_fallback"),
    INSTRUMENTED("instrumented");

    private final String param;

    PointValuesAnalysisMode(String param) {
        this.param = param;
    }

    public static PointValuesAnalysisMode fromParam(String param) {
        return Arrays.stream(values())
                .filter(mode -> mode.param.equalsIgnoreCase(param))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unknown pointValuesAnalysisMode: " + param));
    }

    public int priority() {
        return switch (this) {
            case STRUCTURAL -> 0;
            case STRUCTURAL_WITH_FALLBACK -> 1;
            case INSTRUMENTED -> 2;
        };
    }
}
