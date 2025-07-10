package org.commrogue.tracking;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.IOUtils;

public class TrackingReadBytesDirectory extends FilterDirectory {
    private final Map<String, BytesReadTracker> trackers = new HashMap<>();
    private static final String CFS_EXTENSION = "cfs";

    public TrackingReadBytesDirectory(Directory in) {
        super(in);
    }

    public long getBytesRead() {
        return trackers.values().stream()
                .mapToLong(BytesReadTracker::getBytesRead)
                .sum();
    }

    public List<BytesReadTracker.TrackerSummary> summarize() {
        return trackers.values().stream().map(BytesReadTracker::summarize).toList();
    }

    public void resetBytesRead() {
        trackers.values().forEach(BytesReadTracker::resetBytesRead);
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        IndexInput in = super.openInput(name, context);
        try {
            final BytesReadTracker tracker = trackers.computeIfAbsent(name, o -> {
                String ext = IndexFileNames.getExtension(name);
                if (ext != null && ext.equals(CFS_EXTENSION)) {
                    return new CompoundFileBytesReadTracker(name);
                } else {
                    return new BytesReadTracker(name);
                }
            });
            final TrackingReadBytesIndexInput delegate = new TrackingReadBytesIndexInput(in, 0L, tracker);
            in = null;
            return delegate;
        } finally {
            // TODO - use Closeable.close()?
            IOUtils.close(in);
        }
    }
}
