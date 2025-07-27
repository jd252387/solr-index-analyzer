package org.commrogue.results;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import org.apache.commons.io.FileUtils;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.commrogue.LuceneFileExtension;
import org.commrogue.tracking.TrackingReadBytesDirectory;

public class AggregateSegmentReference {
    @Getter
    public final Map<LuceneFileExtension, Long> fileEntries;

    protected AggregateSegmentReference(Map<LuceneFileExtension, Long> fileEntries) {
        this.fileEntries = fileEntries;
    }

    public AggregateSegmentReference() {
        this.fileEntries = new HashMap<>();
    }

    public long getTotalSize() {
        return fileEntries.values().stream().mapToLong(l -> l).sum();
    }

    public void addTrackingByDirectory(TrackingReadBytesDirectory directory) {
        directory.summarize().forEach(summary -> summary.slices()
                .forEach(slice -> addTrackingByExtension(
                        LuceneFileExtension.fromFile(slice.sliceDescription()), slice.bytesRead())));
    }

    public void addTrackingByExtension(LuceneFileExtension extension, long bytesRead) {
        if (bytesRead > 0) fileEntries.compute(extension, (k, v) -> ((v == null) ? 0L : v) + bytesRead);
    }

    public static AggregateSegmentReference byMergingReferences(List<? extends AggregateSegmentReference> aggregateSegmentReferences) {
        Map<LuceneFileExtension, Long> mergedFileEntries = new HashMap<>();
        aggregateSegmentReferences.stream()
                .map(AggregateSegmentReference::getFileEntries)
                .forEach(fileEntries -> {
                    fileEntries.forEach((extension, size) -> mergedFileEntries.merge(extension, size, Long::sum));
                });

        return new AggregateSegmentReference(mergedFileEntries);
    }

    public SimpleOrderedMap<Object> toSimpleOrderedMap() {
        SimpleOrderedMap<Object> map = new SimpleOrderedMap<>();

        fileEntries.entrySet().stream().sorted(Map.Entry.<LuceneFileExtension, Long>comparingByValue().reversed()).forEach((fileEntry) -> {
            SimpleOrderedMap<String> orderedMapEntry = new SimpleOrderedMap<>();
            orderedMapEntry.add("description", fileEntry.getKey().getDescription());
            orderedMapEntry.add("size", FileUtils.byteCountToDisplaySize(fileEntry.getValue()));

            map.add(fileEntry.getKey().getExtension(), orderedMapEntry);
        });

        return map;
    }
}
