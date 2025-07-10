package org.commrogue.tracking;

import java.util.HashMap;
import java.util.Map;

public class CompoundFileBytesReadTracker extends BytesReadTracker {
    private final Map<Long, BytesReadTracker> offsetToSlicedTracker = new HashMap<>();

    // in a compound file, the sliceDescription refers to the compound file itself.
    // each sliced tracker refers to the files contained within the compound file,
    // e.g. .doc, .pos, .tvm
    public CompoundFileBytesReadTracker(String sliceDescription) {
        super(sliceDescription);
    }

    @Override
    public BytesReadTracker createSliceTracker(String sliceDescription, long offset) {
        return offsetToSlicedTracker.computeIfAbsent(offset, o -> new BytesReadTracker(sliceDescription));
    }

    @Override
    public void trackPositions(long position, int length) {
        // already tracked by a child tracker except for the header and footer, but we can ignore them.
    }

    @Override
    public void resetBytesRead() {
        offsetToSlicedTracker.values().forEach(BytesReadTracker::resetBytesRead);
    }

    @Override
    public TrackerSummary summarize() {
        return new TrackerSummary(offsetToSlicedTracker.values().stream()
                .map(tracker -> new TrackedSlice(tracker.sliceDescription, tracker.getBytesRead()))
                .toList());
    }

    @Override
    public long getBytesRead() {
        return offsetToSlicedTracker.values().stream()
                .mapToLong(BytesReadTracker::getBytesRead)
                .sum();
    }
}
