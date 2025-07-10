package org.commrogue.tracking;

import java.io.IOException;
import org.apache.lucene.store.IndexInput;

public class TrackingReadBytesIndexInput extends IndexInput {
    // cannot use @Delegate since it will forward calls to the proxied IndexInput directly,
    // so any subsequent calls to readByte or readBytes will not go through this tracker
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
        return new TrackingReadBytesIndexInput(
                slice, fileOffset + offset, bytesReadTracker.createSliceTracker(sliceDescription, offset));
    }

    @Override
    public void close() throws IOException {
        in.close();
    }

    @Override
    public long getFilePointer() {
        return in.getFilePointer();
    }

    @Override
    public void seek(long pos) throws IOException {
        in.seek(pos);
    }

    @Override
    public long length() {
        return in.length();
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
