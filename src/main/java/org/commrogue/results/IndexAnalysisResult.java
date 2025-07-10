package org.commrogue.results;

import java.util.HashMap;
import java.util.Map;
import lombok.Getter;

@Getter
public class IndexAnalysisResult {
    private final Map<String, FieldAnalysis> fieldAnalysisMap = new HashMap<>();

    public FieldAnalysis getFieldAnalysis(String fieldName) {
        return fieldAnalysisMap.computeIfAbsent(fieldName, o -> new FieldAnalysis());
    }
}
