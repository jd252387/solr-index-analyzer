package org.commrogue;

import org.commrogue.tracking.TrackingReadBytesDirectory;

import java.util.HashMap;
import java.util.Map;

public class FieldAnalysis {
    public static class AggregateSegmentReference {
        public final Map<LuceneFileExtension, Long> fileEntries = new HashMap<>();

        public long getTotalSize() {
            return fileEntries.values().stream().mapToLong(l -> l).sum();
        }

        public void addTrackingByDirectory(TrackingReadBytesDirectory directory) {
            directory.summarize().forEach(summary -> summary.slices().forEach(slice ->
                    addTrackingByExtension(LuceneFileExtension.fromFile(slice.sliceDescription()), slice.bytesRead())));
        }

        public void addTrackingByExtension(LuceneFileExtension extension, long bytesRead) {
            if (bytesRead > 0) fileEntries.compute(extension, (k, v) -> ((v == null) ? 0L : v) + bytesRead);
        }
    }

    public final AggregateSegmentReference invertedIndex = new AggregateSegmentReference();
    public final AggregateSegmentReference storedField = new AggregateSegmentReference();
    public final AggregateSegmentReference docValues = new AggregateSegmentReference();
    public final AggregateSegmentReference points = new AggregateSegmentReference();
    public final AggregateSegmentReference norms = new AggregateSegmentReference();
    public final AggregateSegmentReference termVectors = new AggregateSegmentReference();
    public final AggregateSegmentReference knnVectors = new AggregateSegmentReference();
}
