package org.commrogue.tracking;

import lombok.experimental.Delegate;
import org.apache.lucene.store.IndexInput;

import java.io.IOException;

public class TrackingReadBytesIndexInput extends IndexInput {
    private interface DelegateExcludes {
        byte readByte() throws IOException;
        void readBytes(byte[] b, int offset, int len) throws IOException;
        IndexInput clone();
        IndexInput slice(String sliceDescription, long offset, long length) throws IOException;
        void readGroupVInts(long[] dst, int limit) throws IOException;
    }

    @Delegate(excludes = DelegateExcludes.class)
    private final IndexInput in;
    private final BytesReadTracker bytesReadTracker;
    private final long fileOffset;

    public TrackingReadBytesIndexInput(IndexInput in, long fileOffset, BytesReadTracker bytesReadTracker) {
        super(in.toString());
        this.in = in;
        this.fileOffset = fileOffset;
        this.bytesReadTracker = bytesReadTracker;
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
        final IndexInput slice = in.slice(sliceDescription, offset, length);
        return new TrackingReadBytesIndexInput(slice, fileOffset + offset, bytesReadTracker.createSliceTracker(offset));
    }

    @Override
    public IndexInput clone() {
        return new TrackingReadBytesIndexInput(in.clone(), fileOffset, bytesReadTracker);
    }

    @Override
    public byte readByte() throws IOException {
        bytesReadTracker.trackPositions(fileOffset + getFilePointer(), 1);
        return in.readByte();
    }

    @Override
    public void readBytes(byte[] b, int offset, int len) throws IOException {
        bytesReadTracker.trackPositions(fileOffset + getFilePointer(), len);
        in.readBytes(b, offset, len);
    }
}