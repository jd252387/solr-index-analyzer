package org.commrogue.analysis.storedfields;

import java.util.Arrays;

public enum StoredFieldsAnalysisMode {
    STRUCTURAL("structural"),
    STRUCTURAL_WITH_FALLBACK("structural_with_fallback"),
    INSTRUMENTED("instrumented");

    private final String param;

    StoredFieldsAnalysisMode(String param) {
        this.param = param;
    }

    public static StoredFieldsAnalysisMode fromParam(String param) {
        return Arrays.stream(values())
                .filter(mode -> mode.param.equalsIgnoreCase(param))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unknown storedFieldsAnalysisMode: " + param));
    }

    public int priority() {
        return switch (this) {
            case STRUCTURAL -> 0;
            case STRUCTURAL_WITH_FALLBACK -> 1;
            case INSTRUMENTED -> 2;
        };
    }
}
