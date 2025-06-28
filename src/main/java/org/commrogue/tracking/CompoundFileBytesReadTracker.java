package org.commrogue.tracking;

import java.util.HashMap;
import java.util.Map;

public class CompoundFileBytesReadTracker extends BytesReadTracker {
    private final Map<Long, BytesReadTracker> offsetToSlicedTracker = new HashMap<>();

    @Override
    public BytesReadTracker createSliceTracker(long offset) {
        return offsetToSlicedTracker.computeIfAbsent(offset, o -> new BytesReadTracker());
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
    public long getBytesRead() {
        return offsetToSlicedTracker.values().stream().mapToLong(BytesReadTracker::getBytesRead).sum();
    }
}