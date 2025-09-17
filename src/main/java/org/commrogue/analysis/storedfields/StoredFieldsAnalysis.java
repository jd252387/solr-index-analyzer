package org.commrogue.analysis.storedfields;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.compressing.CompressionMode;
import org.apache.lucene.codecs.compressing.Decompressor;
import org.apache.lucene.codecs.lucene90.Lucene90StoredFieldsFormat;
import org.apache.lucene.codecs.lucene90.compressing.Lucene90CompressingStoredFieldsWriter;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.BytesRef;
import org.commrogue.LuceneFileExtension;
import org.commrogue.analysis.Analysis;
import org.commrogue.results.FieldAnalysis;
import org.commrogue.results.IndexAnalysisResult;
import org.commrogue.results.StoredFieldsFieldAnalysis;
import org.commrogue.tracking.TrackingReadBytesDirectory;

public class StoredFieldsAnalysis implements Analysis {
    private final TrackingReadBytesDirectory directory;
    private final SegmentReader segmentReader;
    private final IndexAnalysisResult indexAnalysisResult;
    private final StoredFieldsAnalysisMode analysisMode;

    private static final String FAST_FORMAT_NAME = "Lucene90StoredFieldsFastData";
    private static final String HIGH_FORMAT_NAME = "Lucene90StoredFieldsHighData";
    private static final int VERSION_START = 1;
    private static final int VERSION_CURRENT = 1;
    private static final int META_VERSION_START = 0;

    private record FieldStoredSizes(long fdtBytes, long fdxBytes, long fdmBytes) {}

    public StoredFieldsAnalysis(
            TrackingReadBytesDirectory directory,
            SegmentReader segmentReader,
            IndexAnalysisResult indexAnalysisResult,
            StoredFieldsAnalysisMode analysisMode) {
        this.directory = directory;
        this.segmentReader = segmentReader;
        this.indexAnalysisResult = indexAnalysisResult;
        this.analysisMode = analysisMode;
    }

    @Override
    public void analyze() throws Exception {
        boolean processed = false;
        if (analysisMode != StoredFieldsAnalysisMode.INSTRUMENTED) {
            try {
                processed = analyzeStructurally();
            } catch (Exception e) {
                if (analysisMode == StoredFieldsAnalysisMode.STRUCTURAL) {
                    throw e;
                }
            }
        }

        if (!processed && analysisMode != StoredFieldsAnalysisMode.STRUCTURAL) {
            analyzeByInstrumentation();
        }
    }

    private boolean analyzeStructurally() throws IOException {
        SegmentInfo segmentInfo = segmentReader.getSegmentInfo().info;
        StoredFieldsFormat storedFieldsFormat = segmentInfo.getCodec().storedFieldsFormat();
        if (!(storedFieldsFormat instanceof Lucene90StoredFieldsFormat)) {
            return false;
        }

        String modeAttr = segmentInfo.getAttribute(Lucene90StoredFieldsFormat.MODE_KEY);
        if (modeAttr == null) {
            return false;
        }

        Lucene90StoredFieldsFormat.Mode mode = Lucene90StoredFieldsFormat.Mode.valueOf(modeAttr);
        String formatName =
                switch (mode) {
                    case BEST_SPEED -> FAST_FORMAT_NAME;
                    case BEST_COMPRESSION -> HIGH_FORMAT_NAME;
                };
        CompressionMode compressionMode =
                switch (mode) {
                    case BEST_SPEED -> Lucene90StoredFieldsFormat.BEST_SPEED_MODE;
                    case BEST_COMPRESSION -> Lucene90StoredFieldsFormat.BEST_COMPRESSION_MODE;
                };

        Lucene90StoredFieldsInspector inspector =
                new Lucene90StoredFieldsInspector(directory, segmentReader, formatName, compressionMode);
        Map<String, FieldStoredSizes> sizes = inspector.inspect();
        if (sizes.isEmpty()) {
            return false;
        }

        for (Map.Entry<String, FieldStoredSizes> entry : sizes.entrySet()) {
            FieldAnalysis fieldAnalysis = indexAnalysisResult.getFieldAnalysis(entry.getKey());
            StoredFieldsFieldAnalysis storedFields =
                    ensureFieldAnalysis(fieldAnalysis, StoredFieldsAnalysisMode.STRUCTURAL);
            FieldStoredSizes value = entry.getValue();
            if (value.fdtBytes() > 0) {
                storedFields.addTrackingByExtension(LuceneFileExtension.FDT, value.fdtBytes());
            }
            if (value.fdxBytes() > 0) {
                storedFields.addTrackingByExtension(LuceneFileExtension.FDX, value.fdxBytes());
            }
            if (value.fdmBytes() > 0) {
                storedFields.addTrackingByExtension(LuceneFileExtension.FDM, value.fdmBytes());
            }
        }

        return true;
    }

    private void analyzeByInstrumentation() throws IOException {
        Set<String> storedFieldNames = collectStoredFieldNames();
        if (storedFieldNames.isEmpty()) {
            return;
        }

        List<FieldInfo> storedFields = new ArrayList<>();
        FieldInfos fieldInfos = segmentReader.getFieldInfos();
        for (String fieldName : storedFieldNames) {
            FieldInfo info = fieldInfos.fieldInfo(fieldName);
            if (info != null) {
                storedFields.add(info);
            }
        }

        for (FieldInfo field : storedFields) {
            directory.resetBytesRead();
            SingleFieldVisitor visitor = new SingleFieldVisitor(field.name);
            for (int docId = 0; docId < segmentReader.maxDoc(); docId++) {
                visitor.reset();
                segmentReader.document(docId, visitor);
            }
            FieldAnalysis fieldAnalysis = indexAnalysisResult.getFieldAnalysis(field.name);
            StoredFieldsFieldAnalysis storedFieldsAnalysis =
                    ensureFieldAnalysis(fieldAnalysis, StoredFieldsAnalysisMode.INSTRUMENTED);
            storedFieldsAnalysis.addTrackingByDirectory(directory);
            directory.resetBytesRead();
        }
    }

    private Set<String> collectStoredFieldNames() throws IOException {
        Set<String> fieldNames = new LinkedHashSet<>();
        StoredFieldVisitor visitor = new StoredFieldVisitor() {
            @Override
            public Status needsField(FieldInfo fieldInfo) {
                fieldNames.add(fieldInfo.name);
                return Status.NO;
            }
        };

        for (int docId = 0; docId < segmentReader.maxDoc(); docId++) {
            segmentReader.document(docId, visitor);
        }
        return fieldNames;
    }

    private StoredFieldsFieldAnalysis ensureFieldAnalysis(FieldAnalysis fieldAnalysis, StoredFieldsAnalysisMode mode) {
        if (fieldAnalysis.storedFields == null || fieldAnalysis.storedFields.analysisMode != mode) {
            fieldAnalysis.storedFields = new StoredFieldsFieldAnalysis(mode);
        }
        return fieldAnalysis.storedFields;
    }

    private static final class SingleFieldVisitor extends StoredFieldVisitor {
        private final String targetField;

        private SingleFieldVisitor(String targetField) {
            this.targetField = targetField;
        }

        void reset() {
            // no-op, retained for symmetry with analysis loop
        }

        @Override
        public Status needsField(FieldInfo fieldInfo) {
            if (fieldInfo.name.equals(targetField)) {
                return Status.YES;
            }
            return Status.NO;
        }

        @Override
        public void binaryField(FieldInfo fieldInfo, DataInput value, int length) throws IOException {
            value.skipBytes(length);
        }

        @Override
        public void stringField(FieldInfo fieldInfo, String value) {}

        @Override
        public void intField(FieldInfo fieldInfo, int value) {}

        @Override
        public void longField(FieldInfo fieldInfo, long value) {}

        @Override
        public void floatField(FieldInfo fieldInfo, float value) {}

        @Override
        public void doubleField(FieldInfo fieldInfo, double value) {}
    }

    private static final class Lucene90StoredFieldsInspector {
        private static final int STRING = 0x00;
        private static final int BYTE_ARR = 0x01;
        private static final int NUMERIC_INT = 0x02;
        private static final int NUMERIC_FLOAT = 0x03;
        private static final int NUMERIC_LONG = 0x04;
        private static final int NUMERIC_DOUBLE = 0x05;
        private static final int TYPE_BITS = 3;
        private static final int TYPE_MASK = 0x07;

        private static final long SECOND = 1000L;
        private static final long HOUR = 60L * 60L * SECOND;
        private static final long DAY = 24L * HOUR;
        private static final int SECOND_ENCODING = 0x40;
        private static final int HOUR_ENCODING = 0x80;
        private static final int DAY_ENCODING = 0xC0;

        private final TrackingReadBytesDirectory directory;
        private final SegmentReader segmentReader;
        private final String formatName;
        private final CompressionMode compressionMode;

        private Lucene90StoredFieldsInspector(
                TrackingReadBytesDirectory directory,
                SegmentReader segmentReader,
                String formatName,
                CompressionMode compressionMode) {
            this.directory = directory;
            this.segmentReader = segmentReader;
            this.formatName = formatName;
            this.compressionMode = compressionMode;
        }

        Map<String, FieldStoredSizes> inspect() throws IOException {
            SegmentInfo segmentInfo = segmentReader.getSegmentInfo().info;
            String segmentName = segmentInfo.name;
            byte[] segmentId = segmentInfo.getId();
            String suffix = "";

            String fdtName = IndexFileNames.segmentFileName(
                    segmentName, suffix, Lucene90CompressingStoredFieldsWriter.FIELDS_EXTENSION);
            String fdxName = IndexFileNames.segmentFileName(
                    segmentName, suffix, Lucene90CompressingStoredFieldsWriter.INDEX_EXTENSION);
            String fdmName = IndexFileNames.segmentFileName(
                    segmentName, suffix, Lucene90CompressingStoredFieldsWriter.META_EXTENSION);

            long fdxLength = fileLengthSafe(fdxName);
            long fdmLength = fileLengthSafe(fdmName);

            int chunkSize = readChunkSize(fdmName, segmentId, suffix);
            if (chunkSize == -1) {
                return Map.of();
            }

            Map<String, FieldStatsMutable> statsByName =
                    parseDataFile(fdtName, segmentId, suffix, chunkSize, fdxLength, fdmLength);

            Map<String, FieldStoredSizes> result = new LinkedHashMap<>();
            for (FieldStatsMutable stats : statsByName.values()) {
                result.put(
                        stats.fieldInfo.name,
                        new FieldStoredSizes(stats.fdtAssigned, stats.fdxAssigned, stats.fdmAssigned));
            }
            return result;
        }

        private long fileLengthSafe(String fileName) throws IOException {
            try {
                return directory.fileLength(fileName);
            } catch (FileNotFoundException | NoSuchFileException e) {
                return 0L;
            }
        }

        private int readChunkSize(String metaName, byte[] segmentId, String suffix) throws IOException {
            try (ChecksumIndexInput metaInput = directory.openChecksumInput(metaName, IOContext.READONCE)) {
                CodecUtil.checkIndexHeader(
                        metaInput,
                        Lucene90CompressingStoredFieldsWriter.INDEX_CODEC_NAME + "Meta",
                        META_VERSION_START,
                        VERSION_CURRENT,
                        segmentId,
                        suffix);
                return metaInput.readVInt();
            } catch (FileNotFoundException | NoSuchFileException e) {
                return -1;
            }
        }

        private Map<String, FieldStatsMutable> parseDataFile(
                String dataName, byte[] segmentId, String suffix, int chunkSize, long fdxLength, long fdmLength)
                throws IOException {
            Map<Integer, FieldStatsMutable> statsByNumber = new LinkedHashMap<>();
            long headerBytes;
            long footerBytes;

            long dataLength;
            try (ChecksumIndexInput dataInput = directory.openChecksumInput(dataName, IOContext.READONCE)) {
                CodecUtil.checkIndexHeader(dataInput, formatName, VERSION_START, VERSION_CURRENT, segmentId, suffix);
                headerBytes = dataInput.getFilePointer();
                long footerStart = dataInput.length() - CodecUtil.footerLength();
                Decompressor decompressor = compressionMode.newDecompressor();
                BytesRef spare = new BytesRef();
                FieldInfos fieldInfos = segmentReader.getFieldInfos();
                long orphanBytes = 0L;
                Integer firstFieldNumber = null;

                while (dataInput.getFilePointer() < footerStart) {
                    long chunkStart = dataInput.getFilePointer();
                    int docBase = dataInput.readVInt();
                    int token = dataInput.readVInt();
                    int chunkDocs = token >>> 2;
                    boolean sliced = (token & 1) != 0;

                    long[] numStoredFields = new long[chunkDocs];
                    long[] offsets = new long[chunkDocs + 1];
                    offsets[0] = 0L;

                    if (chunkDocs == 1) {
                        numStoredFields[0] = dataInput.readVInt();
                        offsets[1] = dataInput.readVInt();
                    } else if (chunkDocs > 1) {
                        StoredFieldsIntDecoder.readInts(dataInput, chunkDocs, numStoredFields, 0);
                        StoredFieldsIntDecoder.readInts(dataInput, chunkDocs, offsets, 1);
                        for (int i = 0; i < chunkDocs; i++) {
                            offsets[i + 1] += offsets[i];
                        }
                    }

                    int totalLength = Math.toIntExact(offsets[chunkDocs]);
                    byte[] chunkData = new byte[totalLength];
                    if (totalLength > 0) {
                        if (sliced) {
                            BytesRef buffer = new BytesRef();
                            buffer.offset = 0;
                            buffer.length = 0;
                            buffer.bytes = ArrayUtil.grow(buffer.bytes, totalLength);
                            int decompressed = 0;
                            while (decompressed < totalLength) {
                                int toDecompress = Math.min(totalLength - decompressed, chunkSize);
                                decompressor.decompress(dataInput, toDecompress, 0, toDecompress, spare);
                                buffer.bytes = ArrayUtil.grow(buffer.bytes, buffer.length + spare.length);
                                System.arraycopy(spare.bytes, spare.offset, buffer.bytes, buffer.length, spare.length);
                                buffer.length += spare.length;
                                decompressed += toDecompress;
                            }
                            if (buffer.length != totalLength) {
                                throw new IOException("Corrupted stored fields chunk: expected "
                                        + totalLength
                                        + " bytes but decompressed "
                                        + buffer.length);
                            }
                            System.arraycopy(buffer.bytes, buffer.offset, chunkData, 0, totalLength);
                        } else {
                            BytesRef buffer = new BytesRef();
                            decompressor.decompress(dataInput, totalLength, 0, totalLength, buffer);
                            if (buffer.length != totalLength) {
                                throw new IOException("Corrupted stored fields chunk: expected "
                                        + totalLength
                                        + " bytes but decompressed "
                                        + buffer.length);
                            }
                            System.arraycopy(buffer.bytes, buffer.offset, chunkData, 0, totalLength);
                        }
                    }

                    long chunkEnd = dataInput.getFilePointer();
                    long chunkBytes = chunkEnd - chunkStart;
                    Map<Integer, Long> chunkFieldLengths = new LinkedHashMap<>();
                    ByteArrayDataInput docInput = new ByteArrayDataInput();
                    for (int i = 0; i < chunkDocs; i++) {
                        int docLength = (int) (offsets[i + 1] - offsets[i]);
                        int numFields = (int) numStoredFields[i];
                        if (docLength == 0) {
                            continue;
                        }
                        docInput.reset(chunkData, (int) offsets[i], docLength);
                        Set<Integer> docFields = new HashSet<>();
                        int consumedStart = docInput.getPosition();
                        for (int fieldIndex = 0; fieldIndex < numFields; fieldIndex++) {
                            int fieldStart = docInput.getPosition();
                            long infoAndBits = docInput.readVLong();
                            int fieldNumber = (int) (infoAndBits >>> TYPE_BITS);
                            int bits = (int) (infoAndBits & TYPE_MASK);
                            skipField(docInput, bits);
                            int fieldEnd = docInput.getPosition();
                            long fieldLength = fieldEnd - fieldStart;
                            chunkFieldLengths.merge(fieldNumber, fieldLength, Long::sum);
                            docFields.add(fieldNumber);
                            FieldStatsMutable stats = statsByNumber.computeIfAbsent(fieldNumber, key -> {
                                FieldInfo info = Objects.requireNonNull(
                                        fieldInfos.fieldInfo(key), () -> "Unknown field number " + key);
                                return new FieldStatsMutable(info);
                            });
                            stats.valueCount++;
                            if (firstFieldNumber == null) {
                                firstFieldNumber = fieldNumber;
                            }
                        }
                        for (int fieldNumber : docFields) {
                            statsByNumber.get(fieldNumber).docCount++;
                        }
                        int consumedEnd = docInput.getPosition();
                        if (consumedEnd - consumedStart != docLength) {
                            throw new IOException("Stored fields decoding error for docBase " + docBase);
                        }
                    }

                    long totalChunkLength = chunkFieldLengths.values().stream()
                            .mapToLong(Long::longValue)
                            .sum();
                    if (totalChunkLength > 0) {
                        for (Map.Entry<Integer, Long> entry : chunkFieldLengths.entrySet()) {
                            FieldStatsMutable stats = statsByNumber.get(entry.getKey());
                            double share = (double) entry.getValue() / (double) totalChunkLength;
                            stats.fdtBytes += share * chunkBytes;
                        }
                    } else {
                        orphanBytes += chunkBytes;
                    }
                }

                dataInput.seek(dataInput.length() - CodecUtil.footerLength());
                CodecUtil.checkFooter(dataInput);
                dataLength = dataInput.length();
                footerBytes = CodecUtil.footerLength();
                if (!statsByNumber.isEmpty()) {
                    FieldStatsMutable first = statsByNumber.get(firstFieldNumber);
                    if (first != null) {
                        first.fdtBytes += headerBytes + footerBytes + orphanBytes;
                    }
                }
            } catch (FileNotFoundException | NoSuchFileException e) {
                return Map.of();
            }

            long expectedFdtTotal = dataLength;
            distribute(
                    statsByNumber.values(),
                    stat -> stat.fdtBytes,
                    (stat, value) -> stat.fdtAssigned = value,
                    expectedFdtTotal);

            long totalDocCounts = statsByNumber.values().stream()
                    .mapToLong(stat -> stat.docCount)
                    .sum();
            if (totalDocCounts > 0) {
                for (FieldStatsMutable stats : statsByNumber.values()) {
                    double docShare = (double) stats.docCount / (double) totalDocCounts;
                    stats.fdxBytes = docShare * fdxLength;
                    stats.fdmBytes = docShare * fdmLength;
                }
            } else if (!statsByNumber.isEmpty()) {
                FieldStatsMutable first = statsByNumber.values().iterator().next();
                first.fdxBytes += fdxLength;
                first.fdmBytes += fdmLength;
            }

            distribute(
                    statsByNumber.values(),
                    stat -> stat.fdxBytes,
                    (stat, value) -> stat.fdxAssigned = value,
                    fdxLength);
            distribute(
                    statsByNumber.values(),
                    stat -> stat.fdmBytes,
                    (stat, value) -> stat.fdmAssigned = value,
                    fdmLength);

            Map<String, FieldStatsMutable> result = new LinkedHashMap<>();
            statsByNumber.values().stream()
                    .sorted(Comparator.comparing(stat -> stat.fieldInfo.name))
                    .forEach(stat -> result.put(stat.fieldInfo.name, stat));
            return result;
        }

        private void distribute(
                Collection<FieldStatsMutable> stats,
                java.util.function.ToDoubleFunction<FieldStatsMutable> getter,
                java.util.function.ObjLongConsumer<FieldStatsMutable> setter,
                long expectedTotal) {
            if (stats.isEmpty()) {
                return;
            }
            List<FieldContribution> contributions = new ArrayList<>();
            long assigned = 0L;
            for (FieldStatsMutable stat : stats) {
                double raw = getter.applyAsDouble(stat);
                long base = (long) Math.floor(raw);
                double fractional = raw - base;
                contributions.add(new FieldContribution(stat, base, fractional));
                setter.accept(stat, base);
                assigned += base;
            }
            long remainder = expectedTotal - assigned;
            contributions.sort((a, b) -> Double.compare(b.fractional, a.fractional));
            for (int i = 0; i < remainder && i < contributions.size(); i++) {
                FieldContribution contribution = contributions.get(i);
                contribution.increment(setter);
            }
        }

        private void skipField(DataInput input, int bits) throws IOException {
            switch (bits & TYPE_MASK) {
                case BYTE_ARR, STRING -> {
                    int length = input.readVInt();
                    input.skipBytes(length);
                }
                case NUMERIC_INT -> input.readZInt();
                case NUMERIC_FLOAT -> readZFloat(input);
                case NUMERIC_LONG -> readTLong(input);
                case NUMERIC_DOUBLE -> readZDouble(input);
                default -> throw new IOException("Unknown stored field type: " + Integer.toHexString(bits));
            }
        }

        private float readZFloat(DataInput input) throws IOException {
            int b = input.readByte() & 0xFF;
            if (b == 0xFF) {
                return Float.intBitsToFloat(input.readInt());
            } else if ((b & 0x80) != 0) {
                return (b & 0x7F) - 1;
            } else {
                int bits = (b << 24) | ((input.readShort() & 0xFFFF) << 8) | (input.readByte() & 0xFF);
                return Float.intBitsToFloat(bits);
            }
        }

        private double readZDouble(DataInput input) throws IOException {
            int b = input.readByte() & 0xFF;
            if (b == 0xFF) {
                return Double.longBitsToDouble(input.readLong());
            } else if (b == 0xFE) {
                return Float.intBitsToFloat(input.readInt());
            } else if ((b & 0x80) != 0) {
                return (b & 0x7F) - 1;
            } else {
                long bits = ((long) b << 56)
                        | ((input.readInt() & 0xFFFFFFFFL) << 24)
                        | ((input.readShort() & 0xFFFFL) << 8)
                        | (input.readByte() & 0xFFL);
                return Double.longBitsToDouble(bits);
            }
        }

        private long readTLong(DataInput input) throws IOException {
            int header = input.readByte() & 0xFF;
            long bits = header & 0x1F;
            if ((header & 0x20) != 0) {
                bits |= input.readVLong() << 5;
            }
            long value = BitUtil.zigZagDecode(bits);
            switch (header & DAY_ENCODING) {
                case SECOND_ENCODING -> value *= SECOND;
                case HOUR_ENCODING -> value *= HOUR;
                case DAY_ENCODING -> value *= DAY;
                case 0 -> {
                    // uncompressed
                }
                default -> throw new IOException("Unknown time encoding: " + header);
            }
            return value;
        }

        private static final class FieldStatsMutable {
            private final FieldInfo fieldInfo;
            private double fdtBytes;
            private double fdxBytes;
            private double fdmBytes;
            private long fdtAssigned;
            private long fdxAssigned;
            private long fdmAssigned;
            private long docCount;
            private long valueCount;

            private FieldStatsMutable(FieldInfo fieldInfo) {
                this.fieldInfo = fieldInfo;
            }
        }

        private record FieldContribution(FieldStatsMutable stats, long base, double fractional) {
            private void increment(java.util.function.ObjLongConsumer<FieldStatsMutable> setter) {
                setter.accept(stats, base + 1);
            }
        }
    }

    private static final class StoredFieldsIntDecoder {
        private StoredFieldsIntDecoder() {}

        static void readInts(IndexInput input, int count, long[] values, int offset) throws IOException {
            int bitsPerValue = input.readByte() & 0xFF;
            switch (bitsPerValue) {
                case 0 -> Arrays.fill(values, offset, offset + count, input.readVInt());
                case 8 -> readPacked8(input, count, values, offset);
                case 16 -> readPacked16(input, count, values, offset);
                case 32 -> readPacked32(input, count, values, offset);
                default -> throw new IOException("Unsupported bits per value: " + bitsPerValue);
            }
        }

        private static void readPacked8(IndexInput input, int count, long[] values, int offset) throws IOException {
            int processed = 0;
            while (processed <= count - 128) {
                input.readLongs(values, offset + processed, 16);
                for (int i = 0; i < 16; i++) {
                    long packed = values[offset + processed + i];
                    values[offset + processed + i] = (packed >>> 56) & 0xFFL;
                    values[offset + processed + i + 16] = (packed >>> 48) & 0xFFL;
                    values[offset + processed + i + 32] = (packed >>> 40) & 0xFFL;
                    values[offset + processed + i + 48] = (packed >>> 32) & 0xFFL;
                    values[offset + processed + i + 64] = (packed >>> 24) & 0xFFL;
                    values[offset + processed + i + 80] = (packed >>> 16) & 0xFFL;
                    values[offset + processed + i + 96] = (packed >>> 8) & 0xFFL;
                    values[offset + processed + i + 112] = packed & 0xFFL;
                }
                processed += 128;
            }
            for (; processed < count; processed++) {
                values[offset + processed] = Byte.toUnsignedInt(input.readByte());
            }
        }

        private static void readPacked16(IndexInput input, int count, long[] values, int offset) throws IOException {
            int processed = 0;
            while (processed <= count - 128) {
                input.readLongs(values, offset + processed, 32);
                for (int i = 0; i < 32; i++) {
                    long packed = values[offset + processed + i];
                    values[offset + processed + i] = (packed >>> 48) & 0xFFFFL;
                    values[offset + processed + i + 32] = (packed >>> 32) & 0xFFFFL;
                    values[offset + processed + i + 64] = (packed >>> 16) & 0xFFFFL;
                    values[offset + processed + i + 96] = packed & 0xFFFFL;
                }
                processed += 128;
            }
            for (; processed < count; processed++) {
                values[offset + processed] = Short.toUnsignedInt(input.readShort());
            }
        }

        private static void readPacked32(IndexInput input, int count, long[] values, int offset) throws IOException {
            int processed = 0;
            while (processed <= count - 128) {
                input.readLongs(values, offset + processed, 64);
                for (int i = 0; i < 64; i++) {
                    long packed = values[offset + processed + i];
                    values[offset + processed + i] = packed >>> 32;
                    values[offset + processed + i + 64] = packed & 0xFFFFFFFFL;
                }
                processed += 128;
            }
            for (; processed < count; processed++) {
                values[offset + processed] = input.readInt() & 0xFFFFFFFFL;
            }
        }
    }
}
