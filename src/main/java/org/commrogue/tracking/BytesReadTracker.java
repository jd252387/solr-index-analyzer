package org.commrogue.tracking;

public class BytesReadTracker {
    private long minPosition = Long.MAX_VALUE;
    private long maxPosition = Long.MIN_VALUE;

    // In a non-compound file, a slice is considered to be of the same file as its parent IndexInput.
    // Therefore, we return the same tracker for the slice as the tracker for the parent IndexInput.
    public BytesReadTracker createSliceTracker(long offset) {
        return this;
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