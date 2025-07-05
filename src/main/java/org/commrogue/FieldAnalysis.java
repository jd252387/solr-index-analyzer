package org.commrogue;

import java.util.ArrayList;
import java.util.List;

public class FieldAnalysis {
    public static class AggregateSegmentReference {
        private final List<AggregateFileEntry> fileEntries = new ArrayList<>();
    }
    public long invertedIndexBytes;
    public long storedFieldBytes;
    public long docValuesBytes;
    public long pointsBytes;
    public long normsBytes;
    public long termVectorsBytes;
    public long knnVectorsBytes;
}
