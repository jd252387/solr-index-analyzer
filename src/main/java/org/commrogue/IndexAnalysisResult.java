package org.commrogue;

import lombok.Builder;
import lombok.Getter;
import lombok.Singular;

import java.util.HashMap;
import java.util.Map;

@Getter
@Builder
public class IndexAnalysisResult {
    @Singular("fieldAnalysisMap")
    private final Map<String, FieldAnalysis> fieldAnalysisMap = new HashMap<>();

    public FieldAnalysis getFieldAnalysis(String fieldName) {
        return fieldAnalysisMap.computeIfAbsent(fieldName, o -> new FieldAnalysis());
    }
}
