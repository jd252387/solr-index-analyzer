package org.commrogue.analysis.knn;

import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader.readSimilarityFunction;
import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader.readVectorEncoding;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.lucene95.OrdToDocDISIReaderConfiguration;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.packed.DirectMonotonicReader;
import org.commrogue.LuceneFileExtension;
import org.commrogue.analysis.Analysis;
import org.commrogue.results.FieldAnalysis;
import org.commrogue.results.IndexAnalysisResult;
import org.commrogue.tracking.BytesReadTracker;
import org.commrogue.tracking.TrackingReadBytesDirectory;

public class KnnVectorsAnalysis implements Analysis {
    private final TrackingReadBytesDirectory directory;
    private final SegmentReader segmentReader;
    private final IndexAnalysisResult indexAnalysisResult;
    private final KnnVectorsAnalysisMode analysisMode;

    private record FormatContext(List<FieldInfo> fields) {}

    public KnnVectorsAnalysis(
            TrackingReadBytesDirectory directory,
            SegmentReader segmentReader,
            IndexAnalysisResult indexAnalysisResult,
            KnnVectorsAnalysisMode analysisMode) {
        this.directory = directory;
        this.segmentReader = segmentReader;
        this.indexAnalysisResult = indexAnalysisResult;
        this.analysisMode = analysisMode;
    }

    @Override
    public void analyze() throws Exception {
        FieldInfos fieldInfos = segmentReader.getFieldInfos();
        if (!fieldInfos.hasVectorValues()) {
            return;
        }

        Map<Integer, FieldInfo> vectorFields = new LinkedHashMap<>();
        for (FieldInfo fieldInfo : fieldInfos) {
            if (fieldInfo.hasVectorValues()) {
                vectorFields.put(fieldInfo.number, fieldInfo);
            }
        }

        if (vectorFields.isEmpty()) {
            return;
        }

        Map<String, FormatContext> contexts = new LinkedHashMap<>();
        for (FieldInfo fieldInfo : vectorFields.values()) {
            String formatName = fieldInfo.getAttribute(PerFieldKnnVectorsFormat.PER_FIELD_FORMAT_KEY);
            String suffix = fieldInfo.getAttribute(PerFieldKnnVectorsFormat.PER_FIELD_SUFFIX_KEY);
            if (formatName == null || suffix == null) {
                continue;
            }
            String fullSuffix = buildFullSuffix(formatName, suffix);
            contexts.computeIfAbsent(fullSuffix, key -> new FormatContext(new ArrayList<>()))
                    .fields()
                    .add(fieldInfo);
        }

        boolean structuralData = false;
        if (analysisMode != KnnVectorsAnalysisMode.INSTRUMENTED) {
            try {
                structuralData = analyzeStructurally(contexts);
            } catch (Exception e) {
                if (analysisMode == KnnVectorsAnalysisMode.STRUCTURAL_WITH_FALLBACK) {
                    structuralData = false;
                } else {
                    throw e;
                }
            }
        }

        if (!structuralData && analysisMode != KnnVectorsAnalysisMode.STRUCTURAL) {
            analyzeByInstrumentation(vectorFields.values());
        }
    }

    private boolean analyzeStructurally(Map<String, FormatContext> contexts) throws IOException {
        boolean any = false;
        for (Map.Entry<String, FormatContext> entry : contexts.entrySet()) {
            Map<Integer, FieldInfo> fieldInfoMap = new LinkedHashMap<>();
            for (FieldInfo field : entry.getValue().fields()) {
                fieldInfoMap.put(field.number, field);
            }

            boolean parsed = false;
            parsed |= parseHnswMetadata(entry.getKey(), fieldInfoMap);
            parsed |= parseFlatMetadata(entry.getKey(), fieldInfoMap);
            parsed |= parseQuantizedMetadata(entry.getKey(), fieldInfoMap);
            any |= parsed;
        }
        return any;
    }

    private boolean parseHnswMetadata(String fullSuffix, Map<Integer, FieldInfo> fieldInfoMap) throws IOException {
        Optional<ChecksumIndexInput> optionalInput = openOptionalChecksumInput(fullSuffix, LuceneFileExtension.VEM);
        if (optionalInput.isEmpty()) {
            return false;
        }

        try (ChecksumIndexInput input = optionalInput.get()) {
            CodecUtil.checkIndexHeader(
                    input,
                    "Lucene99HnswVectorsFormatMeta",
                    0,
                    0,
                    segmentReader.getSegmentInfo().info.getId(),
                    fullSuffix);

            Map<String, Long> metadataBytesByField = new LinkedHashMap<>();
            Map<String, Long> vectorIndexLengths = new LinkedHashMap<>();
            FieldInfo firstField = null;

            while (true) {
                int fieldNumber = input.readInt();
                if (fieldNumber == -1) {
                    break;
                }

                FieldInfo fieldInfo = fieldInfoMap.get(fieldNumber);
                if (fieldInfo == null) {
                    throw new IOException(
                            "Unexpected field number " + fieldNumber + " in .vem metadata for suffix " + fullSuffix);
                }

                if (firstField == null) {
                    firstField = fieldInfo;
                }

                long startPointer = input.getFilePointer();
                long vectorIndexLength = readHnswFieldEntry(input);
                long entryBytes = input.getFilePointer() - startPointer;

                metadataBytesByField.merge(fieldInfo.name, entryBytes, Long::sum);
                vectorIndexLengths.merge(fieldInfo.name, vectorIndexLength, Long::sum);
            }

            CodecUtil.checkFooter(input);
            long totalBytes = input.length();
            long consumed = metadataBytesByField.values().stream()
                    .mapToLong(Long::longValue)
                    .sum();
            long overhead = totalBytes - consumed;
            if (overhead > 0 && firstField != null) {
                metadataBytesByField.merge(firstField.name, overhead, Long::sum);
            }

            metadataBytesByField.forEach((fieldName, bytes) -> {
                FieldAnalysis fieldAnalysis = indexAnalysisResult.getFieldAnalysis(fieldName);
                KnnVectorsFieldAnalysis knn = ensureFieldAnalysis(fieldAnalysis, KnnVectorsAnalysisMode.STRUCTURAL);
                knn.addTrackingByExtension(LuceneFileExtension.VEM, bytes);
            });

            vectorIndexLengths.forEach((fieldName, bytes) -> {
                FieldAnalysis fieldAnalysis = indexAnalysisResult.getFieldAnalysis(fieldName);
                KnnVectorsFieldAnalysis knn = ensureFieldAnalysis(fieldAnalysis, KnnVectorsAnalysisMode.STRUCTURAL);
                knn.addTrackingByExtension(LuceneFileExtension.VEX, bytes);
            });

            return !metadataBytesByField.isEmpty() || !vectorIndexLengths.isEmpty();
        }
    }

    private boolean parseFlatMetadata(String fullSuffix, Map<Integer, FieldInfo> fieldInfoMap) throws IOException {
        Optional<ChecksumIndexInput> optionalInput = openOptionalChecksumInput(fullSuffix, LuceneFileExtension.VEMF);
        if (optionalInput.isEmpty()) {
            return false;
        }

        try (ChecksumIndexInput input = optionalInput.get()) {
            CodecUtil.checkIndexHeader(
                    input,
                    "Lucene99FlatVectorsFormatMeta",
                    0,
                    0,
                    segmentReader.getSegmentInfo().info.getId(),
                    fullSuffix);

            Map<String, Long> metadataBytesByField = new LinkedHashMap<>();
            Map<String, Long> vectorDataLengths = new LinkedHashMap<>();
            FieldInfo firstField = null;

            while (true) {
                int fieldNumber = input.readInt();
                if (fieldNumber == -1) {
                    break;
                }

                FieldInfo fieldInfo = fieldInfoMap.get(fieldNumber);
                if (fieldInfo == null) {
                    throw new IOException(
                            "Unexpected field number " + fieldNumber + " in .vemf metadata for suffix " + fullSuffix);
                }

                if (firstField == null) {
                    firstField = fieldInfo;
                }

                long startPointer = input.getFilePointer();
                long vectorDataLength = readFlatFieldEntry(input, fieldInfo);
                long entryBytes = input.getFilePointer() - startPointer;

                metadataBytesByField.merge(fieldInfo.name, entryBytes, Long::sum);
                vectorDataLengths.merge(fieldInfo.name, vectorDataLength, Long::sum);
            }

            CodecUtil.checkFooter(input);
            long totalBytes = input.length();
            long consumed = metadataBytesByField.values().stream()
                    .mapToLong(Long::longValue)
                    .sum();
            long overhead = totalBytes - consumed;
            if (overhead > 0 && firstField != null) {
                metadataBytesByField.merge(firstField.name, overhead, Long::sum);
            }

            metadataBytesByField.forEach((fieldName, bytes) -> {
                FieldAnalysis fieldAnalysis = indexAnalysisResult.getFieldAnalysis(fieldName);
                KnnVectorsFieldAnalysis knn = ensureFieldAnalysis(fieldAnalysis, KnnVectorsAnalysisMode.STRUCTURAL);
                knn.addTrackingByExtension(LuceneFileExtension.VEMF, bytes);
            });

            vectorDataLengths.forEach((fieldName, bytes) -> {
                FieldAnalysis fieldAnalysis = indexAnalysisResult.getFieldAnalysis(fieldName);
                KnnVectorsFieldAnalysis knn = ensureFieldAnalysis(fieldAnalysis, KnnVectorsAnalysisMode.STRUCTURAL);
                knn.addTrackingByExtension(LuceneFileExtension.VEC, bytes);
            });

            return !metadataBytesByField.isEmpty() || !vectorDataLengths.isEmpty();
        }
    }

    private boolean parseQuantizedMetadata(String fullSuffix, Map<Integer, FieldInfo> fieldInfoMap) throws IOException {
        Optional<ChecksumIndexInput> optionalInput = openOptionalChecksumInput(fullSuffix, LuceneFileExtension.VEMQ);
        if (optionalInput.isEmpty()) {
            return false;
        }

        try (ChecksumIndexInput input = optionalInput.get()) {
            int version = CodecUtil.checkIndexHeader(
                    input,
                    "Lucene99ScalarQuantizedVectorsFormatMeta",
                    0,
                    1,
                    segmentReader.getSegmentInfo().info.getId(),
                    fullSuffix);

            Map<String, Long> metadataBytesByField = new LinkedHashMap<>();
            Map<String, Long> vectorDataLengths = new LinkedHashMap<>();
            FieldInfo firstField = null;

            while (true) {
                int fieldNumber = input.readInt();
                if (fieldNumber == -1) {
                    break;
                }

                FieldInfo fieldInfo = fieldInfoMap.get(fieldNumber);
                if (fieldInfo == null) {
                    throw new IOException(
                            "Unexpected field number " + fieldNumber + " in .vemq metadata for suffix " + fullSuffix);
                }

                if (firstField == null) {
                    firstField = fieldInfo;
                }

                long startPointer = input.getFilePointer();
                long vectorDataLength = readQuantizedFieldEntry(input, version);
                long entryBytes = input.getFilePointer() - startPointer;

                metadataBytesByField.merge(fieldInfo.name, entryBytes, Long::sum);
                vectorDataLengths.merge(fieldInfo.name, vectorDataLength, Long::sum);
            }

            CodecUtil.checkFooter(input);
            long totalBytes = input.length();
            long consumed = metadataBytesByField.values().stream()
                    .mapToLong(Long::longValue)
                    .sum();
            long overhead = totalBytes - consumed;
            if (overhead > 0 && firstField != null) {
                metadataBytesByField.merge(firstField.name, overhead, Long::sum);
            }

            metadataBytesByField.forEach((fieldName, bytes) -> {
                FieldAnalysis fieldAnalysis = indexAnalysisResult.getFieldAnalysis(fieldName);
                KnnVectorsFieldAnalysis knn = ensureFieldAnalysis(fieldAnalysis, KnnVectorsAnalysisMode.STRUCTURAL);
                knn.addTrackingByExtension(LuceneFileExtension.VEMQ, bytes);
            });

            vectorDataLengths.forEach((fieldName, bytes) -> {
                FieldAnalysis fieldAnalysis = indexAnalysisResult.getFieldAnalysis(fieldName);
                KnnVectorsFieldAnalysis knn = ensureFieldAnalysis(fieldAnalysis, KnnVectorsAnalysisMode.STRUCTURAL);
                knn.addTrackingByExtension(LuceneFileExtension.VEQ, bytes);
            });

            return !metadataBytesByField.isEmpty() || !vectorDataLengths.isEmpty();
        }
    }

    private long readHnswFieldEntry(ChecksumIndexInput input) throws IOException {
        readVectorEncoding(input);
        readSimilarityFunction(input);
        input.readVLong();
        long vectorIndexLength = input.readVLong();
        input.readVInt(); // dimension
        int size = input.readInt();
        input.readVInt(); // M parameter
        int numLevels = input.readVInt();

        long totalNodes = 0;
        for (int level = 0; level < numLevels; level++) {
            if (level == 0) {
                totalNodes += size;
            } else {
                int nodesOnLevel = input.readVInt();
                totalNodes += nodesOnLevel;
                if (nodesOnLevel > 0) {
                    input.readVInt(); // first node id
                    for (int i = 1; i < nodesOnLevel; i++) {
                        input.readVInt();
                    }
                }
            }
        }

        if (totalNodes > 0) {
            input.readLong(); // offsets offset
            int blockShift = input.readVInt();
            DirectMonotonicReader.loadMeta(input, totalNodes, blockShift);
            input.readLong(); // offsets length
        }

        return vectorIndexLength;
    }

    private long readFlatFieldEntry(ChecksumIndexInput input, FieldInfo fieldInfo) throws IOException {
        readVectorEncoding(input);
        readSimilarityFunction(input);
        input.readVLong(); // vector data offset, not needed for size estimation
        long vectorDataLength = input.readVLong();
        input.readVInt(); // dimension
        int size = input.readInt();
        OrdToDocDISIReaderConfiguration.fromStoredMeta(input, size);
        return vectorDataLength;
    }

    private long readQuantizedFieldEntry(ChecksumIndexInput input, int version) throws IOException {
        readVectorEncoding(input);
        readSimilarityFunction(input);
        input.readVLong(); // vector data offset
        long vectorDataLength = input.readVLong();
        input.readVInt(); // dimension
        int size = input.readInt();

        if (size > 0) {
            if (version < 1) {
                input.readInt();
                input.readInt();
                input.readInt();
            } else {
                input.readInt();
                input.readByte();
                input.readByte();
                input.readInt();
                input.readInt();
            }
        }

        OrdToDocDISIReaderConfiguration.fromStoredMeta(input, size);
        return vectorDataLength;
    }

    private Optional<ChecksumIndexInput> openOptionalChecksumInput(String fullSuffix, LuceneFileExtension extension)
            throws IOException {
        String candidate =
                IndexFileNames.segmentFileName(segmentReader.getSegmentName(), fullSuffix, extension.getExtension());
        try {
            return Optional.of(directory.openChecksumInput(candidate, IOContext.READONCE));
        } catch (FileNotFoundException | NoSuchFileException e) {
            for (String fileName : directory.listAll()) {
                if (IndexFileNames.matchesExtension(fileName, extension.getExtension())
                        && IndexFileNames.parseSegmentName(fileName).equals(segmentReader.getSegmentName())
                        && fileName.contains("_" + fullSuffix + ".")) {
                    return Optional.of(directory.openChecksumInput(fileName, IOContext.READONCE));
                }
            }
            return Optional.empty();
        }
    }

    private void analyzeByInstrumentation(Collection<FieldInfo> fields) throws IOException {
        KnnVectorsReader vectorsReader = segmentReader.getVectorReader();
        if (vectorsReader == null) {
            return;
        }
        vectorsReader = vectorsReader.getMergeInstance();

        for (FieldInfo field : fields) {
            FieldAnalysis fieldAnalysis = indexAnalysisResult.getFieldAnalysis(field.name);
            KnnVectorsFieldAnalysis knn = ensureFieldAnalysis(fieldAnalysis, KnnVectorsAnalysisMode.INSTRUMENTED);

            directory.resetBytesRead();
            if (field.getVectorEncoding() == VectorEncoding.FLOAT32) {
                FloatVectorValues values = vectorsReader.getFloatVectorValues(field.name);
                if (values != null) {
                    while (values.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                        values.vectorValue();
                    }
                }
            } else {
                ByteVectorValues values = vectorsReader.getByteVectorValues(field.name);
                if (values != null) {
                    while (values.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                        values.vectorValue();
                    }
                }
            }

            for (BytesReadTracker.TrackerSummary summary : directory.summarize()) {
                for (BytesReadTracker.TrackedSlice slice : summary.slices()) {
                    knn.addTrackingByExtension(
                            LuceneFileExtension.fromFile(slice.sliceDescription()), slice.bytesRead());
                }
            }
            directory.resetBytesRead();
        }
    }

    private KnnVectorsFieldAnalysis ensureFieldAnalysis(FieldAnalysis fieldAnalysis, KnnVectorsAnalysisMode mode) {
        if (fieldAnalysis.knnVectors == null || fieldAnalysis.knnVectors.analysisMode != mode) {
            fieldAnalysis.knnVectors = new KnnVectorsFieldAnalysis(mode);
        }
        return fieldAnalysis.knnVectors;
    }

    private String buildFullSuffix(String formatName, String suffix) {
        return formatName + "_" + suffix;
    }
}
