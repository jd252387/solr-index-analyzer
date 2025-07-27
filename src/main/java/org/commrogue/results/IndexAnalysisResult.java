package org.commrogue.results;

import java.util.*;
import java.util.stream.Collectors;
import lombok.Getter;
import org.apache.solr.common.util.SimpleOrderedMap;

@Getter
public class IndexAnalysisResult {
    private final Map<String, FieldAnalysis> fieldAnalysisMap;

    private IndexAnalysisResult(Map<String, FieldAnalysis> fieldAnalysisMap) {
        this.fieldAnalysisMap = fieldAnalysisMap;
    }

    public IndexAnalysisResult() {
        this.fieldAnalysisMap = new HashMap<>();
    }

    public FieldAnalysis getFieldAnalysis(String fieldName) {
        return fieldAnalysisMap.computeIfAbsent(fieldName, o -> new FieldAnalysis());
    }

    public SimpleOrderedMap<Object> toSimpleOrderedMap() {
        SimpleOrderedMap<Object> map = new SimpleOrderedMap<>();

        fieldAnalysisMap.entrySet().stream()
                .sorted(Comparator.<Map.Entry<String, FieldAnalysis>>comparingLong(
                                entry -> entry.getValue().getTotalSize())
                        .reversed())
                .forEach((entry) -> map.add(entry.getKey(), entry.getValue().toSimpleOrderedMap()));

        return map;
    }

    public static IndexAnalysisResult byMerging(List<IndexAnalysisResult> fieldAnalysisList) {
        return new IndexAnalysisResult(fieldAnalysisList.stream()
                .map(IndexAnalysisResult::getFieldAnalysisMap)
                .map(Map::entrySet)
                .flatMap(Collection::stream)
                .collect(Collectors.groupingBy(
                        Map.Entry::getKey, Collectors.mapping(Map.Entry::getValue, Collectors.toList())))
                .entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> FieldAnalysis.byMerging(entry.getValue()))));
    }
}
