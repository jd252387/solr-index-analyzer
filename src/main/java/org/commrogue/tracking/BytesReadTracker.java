package org.commrogue.tracking;

import java.util.List;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class BytesReadTracker {
    public record TrackedSlice(String sliceDescription, long bytesRead) {}

    public record TrackerSummary(List<TrackedSlice> slices) {
        public long getTotalBytesRead() {
            return slices.stream().mapToLong(TrackedSlice::bytesRead).sum();
        }
    }

    // in non-compound mode, refers to an actual on-disk file
    // in compound mode, refers to a slice within the compound file
    protected final String sliceDescription;
    protected long minPosition = Long.MAX_VALUE;
    protected long maxPosition = Long.MIN_VALUE;

    // in a non-compound file, a slice is considered to be of the same file as its parent IndexInput.
    // therefore, we return the same tracker for the slice as the tracker for the parent IndexInput.
    public BytesReadTracker createSliceTracker(String sliceDescription, long offset) {
        return this;
    }

    public TrackerSummary summarize() {
        return new TrackerSummary(List.of(new TrackedSlice(this.sliceDescription, this.getBytesRead())));
    }

    public void trackPositions(long position, int length) {
        minPosition = Math.min(minPosition, position);
        maxPosition = Math.max(maxPosition, position + length - 1);
    }

    public void resetBytesRead() {
        minPosition = Long.MAX_VALUE;
        maxPosition = Long.MIN_VALUE;
    }

    public long getBytesRead() {
        if (minPosition <= maxPosition) {
            return maxPosition - minPosition + 1;
        } else {
            return 0L;
        }
    }
}
