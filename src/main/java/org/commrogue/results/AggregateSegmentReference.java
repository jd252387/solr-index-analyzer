package org.commrogue.results;

import java.util.HashMap;
import java.util.Map;
import org.commrogue.LuceneFileExtension;
import org.commrogue.tracking.TrackingReadBytesDirectory;

public class AggregateSegmentReference {
    public final Map<LuceneFileExtension, Long> fileEntries = new HashMap<>();

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
}
