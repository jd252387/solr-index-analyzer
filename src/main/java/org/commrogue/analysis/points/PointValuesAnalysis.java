package org.commrogue.analysis.points;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.lucene90.Lucene90PointsFormat;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.PointValues.IntersectVisitor;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.bkd.BKDWriter;
import org.commrogue.LuceneFileExtension;
import org.commrogue.analysis.Analysis;
import org.commrogue.results.FieldAnalysis;
import org.commrogue.results.IndexAnalysisResult;
import org.commrogue.results.PointValuesFieldAnalysis;
import org.commrogue.tracking.TrackingReadBytesDirectory;

public class PointValuesAnalysis implements Analysis {
    private final TrackingReadBytesDirectory directory;
    private final SegmentReader segmentReader;
    private final IndexAnalysisResult indexAnalysisResult;
    private final PointValuesAnalysisMode analysisMode;

    private record FieldPointValuesSizes(long metaBytes, long indexBytes, long dataBytes) {}

    public PointValuesAnalysis(
            TrackingReadBytesDirectory directory,
            SegmentReader segmentReader,
            IndexAnalysisResult indexAnalysisResult,
            PointValuesAnalysisMode analysisMode) {
        this.directory = directory;
        this.segmentReader = segmentReader;
        this.indexAnalysisResult = indexAnalysisResult;
        this.analysisMode = analysisMode;
    }

    @Override
    public void analyze() throws Exception {
        FieldInfos fieldInfos = segmentReader.getFieldInfos();
        List<FieldInfo> pointFields = new ArrayList<>();
        for (FieldInfo fieldInfo : fieldInfos) {
            if (fieldInfo.getPointDimensionCount() > 0) {
                pointFields.add(fieldInfo);
            }
        }

        if (pointFields.isEmpty()) {
            return;
        }

        Set<String> processedFields = new HashSet<>();
        boolean structuralData = false;

        if (analysisMode != PointValuesAnalysisMode.INSTRUMENTED) {
            try {
                structuralData = analyzeStructurally(pointFields, processedFields);
            } catch (Exception e) {
                if (analysisMode == PointValuesAnalysisMode.STRUCTURAL) {
                    throw e;
                }
            }
        }

        if ((analysisMode == PointValuesAnalysisMode.INSTRUMENTED)
                || (!structuralData && analysisMode != PointValuesAnalysisMode.STRUCTURAL)) {
            analyzeByInstrumentation(pointFields, processedFields);
        }
    }

    private boolean analyzeStructurally(List<FieldInfo> pointFields, Set<String> processedFields) throws IOException {
        Lucene90PointsInspector inspector = new Lucene90PointsInspector(directory, segmentReader);
        Map<String, FieldPointValuesSizes> sizes = inspector.inspect(pointFields);
        if (sizes.isEmpty()) {
            return false;
        }

        for (Map.Entry<String, FieldPointValuesSizes> entry : sizes.entrySet()) {
            FieldAnalysis fieldAnalysis = indexAnalysisResult.getFieldAnalysis(entry.getKey());
            PointValuesFieldAnalysis pointValues =
                    ensureFieldAnalysis(fieldAnalysis, PointValuesAnalysisMode.STRUCTURAL);
            FieldPointValuesSizes value = entry.getValue();
            if (value.metaBytes() > 0) {
                pointValues.addTrackingByExtension(LuceneFileExtension.KDM, value.metaBytes());
            }
            if (value.indexBytes() > 0) {
                pointValues.addTrackingByExtension(LuceneFileExtension.KDI, value.indexBytes());
            }
            if (value.dataBytes() > 0) {
                pointValues.addTrackingByExtension(LuceneFileExtension.KDD, value.dataBytes());
            }
            processedFields.add(entry.getKey());
        }

        return true;
    }

    private void analyzeByInstrumentation(List<FieldInfo> fields, Set<String> processedFields) throws IOException {
        for (FieldInfo field : fields) {
            if (!processedFields.add(field.name)) {
                continue;
            }

            PointValues values = segmentReader.getPointValues(field.name);
            if (values == null) {
                continue;
            }

            directory.resetBytesRead();
            values.intersect(new ExhaustiveIntersectVisitor());

            FieldAnalysis fieldAnalysis = indexAnalysisResult.getFieldAnalysis(field.name);
            PointValuesFieldAnalysis pointValues =
                    ensureFieldAnalysis(fieldAnalysis, PointValuesAnalysisMode.INSTRUMENTED);
            pointValues.addTrackingByDirectory(directory);
            directory.resetBytesRead();
        }
    }

    private PointValuesFieldAnalysis ensureFieldAnalysis(FieldAnalysis fieldAnalysis, PointValuesAnalysisMode mode) {
        if (fieldAnalysis.pointValues == null || fieldAnalysis.pointValues.analysisMode.priority() < mode.priority()) {
            fieldAnalysis.pointValues = new PointValuesFieldAnalysis(mode);
        }
        return fieldAnalysis.pointValues;
    }

    private static final class ExhaustiveIntersectVisitor implements IntersectVisitor {
        @Override
        public void visit(int docID) {
            // Exhaustive traversal does not need to record anything.
        }

        @Override
        public void visit(int docID, byte[] packedValue) {
            // Accessing the packed value forces the BKD reader to touch the leaf data.
        }

        @Override
        public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
            return Relation.CELL_CROSSES_QUERY;
        }
    }

    private static final class Lucene90PointsInspector {
        private static final String META_CODEC_NAME = "Lucene90PointsFormatMeta";
        private static final String INDEX_CODEC_NAME = "Lucene90PointsFormatIndex";
        private static final String DATA_CODEC_NAME = "Lucene90PointsFormatData";
        private static final int LUCENE90_VERSION_START = 0;
        private static final int LUCENE90_VERSION_CURRENT = 0;

        private final TrackingReadBytesDirectory directory;
        private final SegmentReader segmentReader;

        private Lucene90PointsInspector(TrackingReadBytesDirectory directory, SegmentReader segmentReader) {
            this.directory = directory;
            this.segmentReader = segmentReader;
        }

        Map<String, FieldPointValuesSizes> inspect(List<FieldInfo> fields) throws IOException {
            if (fields.isEmpty()) {
                return Map.of();
            }

            Map<Integer, FieldInfo> fieldInfosByNumber = new HashMap<>();
            for (FieldInfo field : fields) {
                fieldInfosByNumber.put(field.number, field);
            }

            Map<Integer, FieldPointValuesSizesMutable> sizesByNumber = new LinkedHashMap<>();
            for (FieldInfo field : fields) {
                sizesByNumber.put(field.number, new FieldPointValuesSizesMutable());
            }

            SegmentCommitInfo segmentCommitInfo = segmentReader.getSegmentInfo();
            String segmentName = segmentReader.getSegmentName();
            byte[] segmentId = segmentCommitInfo.info.getId();

            String metaName = IndexFileNames.segmentFileName(segmentName, "", Lucene90PointsFormat.META_EXTENSION);

            List<FieldEntry> entries = new ArrayList<>();
            long headerBytes = 0L;
            long sentinelBytes = 0L;
            long footerBytes = 0L;
            long indexLength = 0L;
            long dataLength = 0L;

            try (ChecksumIndexInput metaInput = directory.openChecksumInput(metaName, IOContext.READONCE)) {
                CodecUtil.checkIndexHeader(
                        metaInput, META_CODEC_NAME, LUCENE90_VERSION_START, LUCENE90_VERSION_CURRENT, segmentId, "");
                headerBytes = metaInput.getFilePointer();

                while (true) {
                    long entryStart = metaInput.getFilePointer();
                    int fieldNumber = metaInput.readInt();
                    if (fieldNumber == -1) {
                        sentinelBytes = metaInput.getFilePointer() - entryStart;
                        break;
                    }

                    Lucene90FieldMetadata metadata = readFieldMetadata(metaInput);
                    long entryEnd = metaInput.getFilePointer();

                    FieldPointValuesSizesMutable sizes =
                            sizesByNumber.computeIfAbsent(fieldNumber, key -> new FieldPointValuesSizesMutable());
                    sizes.addMetaBytes(entryEnd - entryStart);

                    FieldInfo fieldInfo = fieldInfosByNumber.get(fieldNumber);
                    if (fieldInfo == null) {
                        fieldInfo = segmentReader.getFieldInfos().fieldInfo(fieldNumber);
                    }
                    entries.add(new FieldEntry(fieldInfo, metadata));
                }

                indexLength = metaInput.readLong();
                dataLength = metaInput.readLong();
                long footerStart = metaInput.getFilePointer();
                CodecUtil.checkFooter(metaInput);
                footerBytes = metaInput.length() - footerStart;
            } catch (FileNotFoundException | NoSuchFileException e) {
                return Map.of();
            }

            if (entries.isEmpty()) {
                return Map.of();
            }

            long metaOverhead = headerBytes + sentinelBytes + footerBytes + Long.BYTES * 2L;
            FieldPointValuesSizesMutable firstEntry = firstMutable(entries, sizesByNumber);
            if (metaOverhead > 0 && firstEntry != null) {
                firstEntry.addMetaBytes(metaOverhead);
            }

            assignIndexSizes(segmentName, segmentId, entries, sizesByNumber, indexLength);
            assignDataSizes(segmentName, segmentId, entries, sizesByNumber, dataLength);

            Map<String, FieldPointValuesSizes> result = new LinkedHashMap<>();
            for (FieldEntry entry : entries) {
                FieldInfo fieldInfo = entry.fieldInfo();
                if (fieldInfo == null) {
                    continue;
                }
                FieldPointValuesSizesMutable mutable = sizesByNumber.get(fieldInfo.number);
                if (mutable == null || !mutable.hasAnyContribution()) {
                    continue;
                }
                result.put(
                        fieldInfo.name,
                        new FieldPointValuesSizes(mutable.metaBytes, mutable.indexBytes, mutable.dataBytes));
            }

            return result;
        }

        private void assignIndexSizes(
                String segmentName,
                byte[] segmentId,
                List<FieldEntry> entries,
                Map<Integer, FieldPointValuesSizesMutable> sizesByNumber,
                long indexLength)
                throws IOException {
            String indexName = IndexFileNames.segmentFileName(segmentName, "", Lucene90PointsFormat.INDEX_EXTENSION);
            long headerLength = readHeaderLength(indexName, INDEX_CODEC_NAME, segmentId);
            long footerLength = CodecUtil.footerLength();

            long payloadBytes = 0L;
            for (FieldEntry entry : entries) {
                FieldInfo fieldInfo = entry.fieldInfo();
                if (fieldInfo == null) {
                    continue;
                }
                FieldPointValuesSizesMutable mutable = sizesByNumber.get(fieldInfo.number);
                if (mutable == null) {
                    continue;
                }
                mutable.addIndexBytes(entry.metadata().packedIndexLength());
                payloadBytes += entry.metadata().packedIndexLength();
            }

            FieldPointValuesSizesMutable first = firstMutable(entries, sizesByNumber);
            FieldPointValuesSizesMutable last = lastMutable(entries, sizesByNumber);

            if (headerLength > 0 && first != null) {
                first.addIndexBytes(headerLength);
            }
            if (footerLength > 0 && last != null) {
                last.addIndexBytes(footerLength);
            }

            long remainder = indexLength - headerLength - footerLength - payloadBytes;
            if (remainder > 0 && first != null) {
                first.addIndexBytes(remainder);
            }
        }

        private void assignDataSizes(
                String segmentName,
                byte[] segmentId,
                List<FieldEntry> entries,
                Map<Integer, FieldPointValuesSizesMutable> sizesByNumber,
                long dataLength)
                throws IOException {
            String dataName = IndexFileNames.segmentFileName(segmentName, "", Lucene90PointsFormat.DATA_EXTENSION);
            long headerLength = readHeaderLength(dataName, DATA_CODEC_NAME, segmentId);
            long footerLength = CodecUtil.footerLength();

            long payloadBytes = 0L;
            for (int i = 0; i < entries.size(); i++) {
                FieldEntry entry = entries.get(i);
                FieldInfo fieldInfo = entry.fieldInfo();
                if (fieldInfo == null) {
                    continue;
                }
                FieldPointValuesSizesMutable mutable = sizesByNumber.get(fieldInfo.number);
                if (mutable == null) {
                    continue;
                }

                long start = entry.metadata().dataStartFP();
                long end;
                if (i + 1 < entries.size()) {
                    end = entries.get(i + 1).metadata().dataStartFP();
                } else {
                    end = dataLength - footerLength;
                }
                long delta = Math.max(0L, end - start);
                mutable.addDataBytes(delta);
                payloadBytes += delta;
            }

            FieldPointValuesSizesMutable first = firstMutable(entries, sizesByNumber);
            FieldPointValuesSizesMutable last = lastMutable(entries, sizesByNumber);
            if (headerLength > 0 && first != null) {
                first.addDataBytes(headerLength);
            }
            if (footerLength > 0 && last != null) {
                last.addDataBytes(footerLength);
            }

            long remainder = dataLength - headerLength - footerLength - payloadBytes;
            if (remainder > 0 && first != null) {
                first.addDataBytes(remainder);
            }
        }

        private long readHeaderLength(String fileName, String codecName, byte[] segmentId) throws IOException {
            try (IndexInput input = directory.openInput(fileName, IOContext.READONCE)) {
                CodecUtil.checkIndexHeader(
                        input, codecName, LUCENE90_VERSION_START, LUCENE90_VERSION_CURRENT, segmentId, "");
                return input.getFilePointer();
            } catch (FileNotFoundException | NoSuchFileException e) {
                return 0L;
            }
        }

        private Lucene90FieldMetadata readFieldMetadata(ChecksumIndexInput metaInput) throws IOException {
            CodecUtil.checkHeader(
                    metaInput, BKDWriter.CODEC_NAME, BKDWriter.VERSION_META_FILE, BKDWriter.VERSION_META_FILE);
            metaInput.readVInt();
            int numIndexDims = metaInput.readVInt();
            metaInput.readVInt();
            int bytesPerDim = metaInput.readVInt();
            metaInput.readVInt();

            int packedIndexBytesLength = Math.multiplyExact(numIndexDims, bytesPerDim);
            byte[] scratch = new byte[packedIndexBytesLength];
            metaInput.readBytes(scratch, 0, scratch.length);
            metaInput.readBytes(scratch, 0, scratch.length);

            metaInput.readVLong();
            metaInput.readVInt();
            int packedIndexLength = metaInput.readVInt();
            long dataStartFP = metaInput.readLong();
            metaInput.readLong();

            return new Lucene90FieldMetadata(dataStartFP, packedIndexLength);
        }

        private FieldPointValuesSizesMutable firstMutable(
                List<FieldEntry> entries, Map<Integer, FieldPointValuesSizesMutable> sizesByNumber) {
            for (FieldEntry entry : entries) {
                FieldInfo info = entry.fieldInfo();
                if (info == null) {
                    continue;
                }
                FieldPointValuesSizesMutable mutable = sizesByNumber.get(info.number);
                if (mutable != null) {
                    return mutable;
                }
            }
            return null;
        }

        private FieldPointValuesSizesMutable lastMutable(
                List<FieldEntry> entries, Map<Integer, FieldPointValuesSizesMutable> sizesByNumber) {
            for (int i = entries.size() - 1; i >= 0; i--) {
                FieldEntry entry = entries.get(i);
                FieldInfo info = entry.fieldInfo();
                if (info == null) {
                    continue;
                }
                FieldPointValuesSizesMutable mutable = sizesByNumber.get(info.number);
                if (mutable != null) {
                    return mutable;
                }
            }
            return null;
        }
    }

    private record FieldEntry(FieldInfo fieldInfo, Lucene90FieldMetadata metadata) {}

    private record Lucene90FieldMetadata(long dataStartFP, long packedIndexLength) {}

    private static final class FieldPointValuesSizesMutable {
        private long metaBytes;
        private long indexBytes;
        private long dataBytes;

        private void addMetaBytes(long bytes) {
            metaBytes += Math.max(0L, bytes);
        }

        private void addIndexBytes(long bytes) {
            indexBytes += Math.max(0L, bytes);
        }

        private void addDataBytes(long bytes) {
            dataBytes += Math.max(0L, bytes);
        }

        private boolean hasAnyContribution() {
            return metaBytes > 0 || indexBytes > 0 || dataBytes > 0;
        }
    }
}
