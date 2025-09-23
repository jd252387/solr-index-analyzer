package org.commrogue.analysis.knn;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.commons.io.FileUtils;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.commrogue.LuceneFileExtension;
import org.commrogue.results.AggregateSegmentReference;

public class KnnVectorsFieldAnalysis extends AggregateSegmentReference {
    public record HnswLevelMetadata(int level, long nodeCount, long sizeBytes) {}

    public final KnnVectorsAnalysisMode analysisMode;
    private final Map<Integer, HnswLevelMetadata> hnswLevels;

    private KnnVectorsFieldAnalysis(
            java.util.Map<LuceneFileExtension, Long> fileEntries, KnnVectorsAnalysisMode analysisMode) {
        super(fileEntries);
        this.analysisMode = analysisMode;
        this.hnswLevels = new TreeMap<>();
    }

    public KnnVectorsFieldAnalysis(KnnVectorsAnalysisMode analysisMode) {
        super();
        this.analysisMode = analysisMode;
        this.hnswLevels = new TreeMap<>();
    }

    public void addHnswMetadata(List<HnswLevelMetadata> levels) {
        if (levels == null || levels.isEmpty()) {
            return;
        }

        for (HnswLevelMetadata level : levels) {
            hnswLevels.merge(
                    level.level(),
                    level,
                    (existing, incoming) ->
                            new HnswLevelMetadata(
                                    level.level(),
                                    existing.nodeCount() + incoming.nodeCount(),
                                    existing.sizeBytes() + incoming.sizeBytes()));
        }
    }

    public int getHnswLevelCount() {
        return hnswLevels.size();
    }

    public List<HnswLevelMetadata> getHnswLevels() {
        return new ArrayList<>(hnswLevels.values());
    }

    @Override
    public SimpleOrderedMap<Object> toSimpleOrderedMap() {
        SimpleOrderedMap<Object> map = super.toSimpleOrderedMap();
        map.add("analysis_mode", analysisMode.name());

        if (!hnswLevels.isEmpty()) {
            map.add("hnsw_level_count", getHnswLevelCount());

            SimpleOrderedMap<Object> levels = new SimpleOrderedMap<>();
            for (HnswLevelMetadata level : hnswLevels.values()) {
                SimpleOrderedMap<Object> levelMap = new SimpleOrderedMap<>();
                levelMap.add("nodes", level.nodeCount());
                levelMap.add("size", FileUtils.byteCountToDisplaySize(level.sizeBytes()));
                levelMap.add("size_bytes", level.sizeBytes());
                levels.add(Integer.toString(level.level()), levelMap);
            }
            map.add("hnsw_levels", levels);
        }

        return map;
    }

    public static KnnVectorsFieldAnalysis byMerging(List<KnnVectorsFieldAnalysis> analyses) {
        AggregateSegmentReference merged = AggregateSegmentReference.byMergingReferences(analyses);
        KnnVectorsFieldAnalysis mergedAnalysis =
                new KnnVectorsFieldAnalysis(merged.getFileEntries(), analyses.get(0).analysisMode);
        for (KnnVectorsFieldAnalysis analysis : analyses) {
            mergedAnalysis.addHnswMetadata(analysis.getHnswLevels());
        }
        return mergedAnalysis;
    }
}
