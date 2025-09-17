package org.commrogue.analysis.docvalues;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.perfield.PerFieldDocValuesFormat;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.packed.DirectMonotonicReader;
import org.commrogue.LuceneFileExtension;
import org.commrogue.analysis.Analysis;
import org.commrogue.results.DocValuesFieldAnalysis;
import org.commrogue.results.FieldAnalysis;
import org.commrogue.results.IndexAnalysisResult;
import org.commrogue.tracking.TrackingReadBytesDirectory;

public class DocValuesAnalysis implements Analysis {
    private final TrackingReadBytesDirectory directory;
    private final SegmentReader segmentReader;
    private final IndexAnalysisResult indexAnalysisResult;
    private final DocValuesAnalysisMode analysisMode;

    private static final String LUCENE90_DATA_CODEC = "Lucene90DocValuesData";
    private static final String LUCENE90_DATA_EXTENSION = "dvd";
    private static final String LUCENE90_META_CODEC = "Lucene90DocValuesMetadata";
    private static final String LUCENE90_META_EXTENSION = "dvm";
    private static final byte LUCENE90_NUMERIC = 0;
    private static final byte LUCENE90_BINARY = 1;
    private static final byte LUCENE90_SORTED = 2;
    private static final byte LUCENE90_SORTED_SET = 3;
    private static final byte LUCENE90_SORTED_NUMERIC = 4;
    private static final int LUCENE90_TERMS_DICT_BLOCK_SHIFT = 6;
    private static final int LUCENE90_VERSION_START = 0;
    private static final int LUCENE90_VERSION_CURRENT = 0;

    private record FormatContext(String formatName, String suffix, List<FieldInfo> fields) {}

    private record FieldDocValuesSizes(long metaBytes, long dataBytes) {}

    public DocValuesAnalysis(
            TrackingReadBytesDirectory directory,
            SegmentReader segmentReader,
            IndexAnalysisResult indexAnalysisResult,
            DocValuesAnalysisMode analysisMode) {
        this.directory = directory;
        this.segmentReader = segmentReader;
        this.indexAnalysisResult = indexAnalysisResult;
        this.analysisMode = analysisMode;
    }

    @Override
    public void analyze() throws Exception {
        FieldInfos fieldInfos = segmentReader.getFieldInfos();
        List<FieldInfo> docValuesFields = new ArrayList<>();
        for (FieldInfo fieldInfo : fieldInfos) {
            if (fieldInfo.getDocValuesType() != DocValuesType.NONE) {
                docValuesFields.add(fieldInfo);
            }
        }

        if (docValuesFields.isEmpty()) {
            return;
        }

        Map<String, FormatContext> contexts = new LinkedHashMap<>();
        List<FieldInfo> missingFormatFields = new ArrayList<>();
        for (FieldInfo fieldInfo : docValuesFields) {
            String formatName = fieldInfo.getAttribute(PerFieldDocValuesFormat.PER_FIELD_FORMAT_KEY);
            String suffix = fieldInfo.getAttribute(PerFieldDocValuesFormat.PER_FIELD_SUFFIX_KEY);
            if (formatName == null || suffix == null) {
                missingFormatFields.add(fieldInfo);
                continue;
            }
            String fullSuffix = buildFullSuffix(formatName, suffix);
            contexts.computeIfAbsent(fullSuffix, key -> new FormatContext(formatName, suffix, new ArrayList<>()))
                    .fields()
                    .add(fieldInfo);
        }

        Set<String> processedFields = new HashSet<>();
        Set<FieldInfo> instrumentationTargets = new LinkedHashSet<>(missingFormatFields);

        if (analysisMode != DocValuesAnalysisMode.INSTRUMENTED) {
            for (Map.Entry<String, FormatContext> entry : contexts.entrySet()) {
                FormatContext context = entry.getValue();
                boolean parsed = false;
                if ("Lucene90".equals(context.formatName())) {
                    try {
                        parsed = parseLucene90(entry.getKey(), context, processedFields);
                    } catch (Exception e) {
                        if (analysisMode == DocValuesAnalysisMode.STRUCTURAL) {
                            throw e;
                        }
                    }
                }
                if (!parsed) {
                    instrumentationTargets.addAll(context.fields());
                }
            }
        } else {
            instrumentationTargets.addAll(docValuesFields);
        }

        if (analysisMode == DocValuesAnalysisMode.STRUCTURAL && !instrumentationTargets.isEmpty()) {
            String unsupportedFields =
                    instrumentationTargets.stream().map(field -> field.name).collect(Collectors.joining(", "));
            throw new IllegalStateException(
                    "Structural DocValues analysis is not supported for fields: " + unsupportedFields);
        }

        if (!instrumentationTargets.isEmpty() && analysisMode != DocValuesAnalysisMode.STRUCTURAL) {
            analyzeByInstrumentation(instrumentationTargets, processedFields);
        }
    }

    private boolean parseLucene90(String fullSuffix, FormatContext context, Set<String> processedFields)
            throws IOException {
        Lucene90DocValuesInspector inspector = new Lucene90DocValuesInspector(directory, segmentReader);
        Map<String, FieldDocValuesSizes> sizes = inspector.inspect(fullSuffix, context.fields());
        if (sizes.isEmpty()) {
            return false;
        }

        for (Map.Entry<String, FieldDocValuesSizes> entry : sizes.entrySet()) {
            FieldAnalysis fieldAnalysis = indexAnalysisResult.getFieldAnalysis(entry.getKey());
            DocValuesFieldAnalysis docValues = ensureFieldAnalysis(fieldAnalysis, DocValuesAnalysisMode.STRUCTURAL);
            FieldDocValuesSizes value = entry.getValue();
            if (value.metaBytes() > 0) {
                docValues.addTrackingByExtension(LuceneFileExtension.DVM, value.metaBytes());
            }
            if (value.dataBytes() > 0) {
                docValues.addTrackingByExtension(LuceneFileExtension.DVD, value.dataBytes());
            }
            processedFields.add(entry.getKey());
        }

        return true;
    }

    private void analyzeByInstrumentation(Collection<FieldInfo> fields, Set<String> processedFields)
            throws IOException {
        for (FieldInfo field : fields) {
            if (!processedFields.add(field.name)) {
                continue;
            }

            directory.resetBytesRead();
            DocValuesType type = field.getDocValuesType();
            switch (type) {
                case NUMERIC -> consumeNumeric(field.name);
                case BINARY -> consumeBinary(field.name);
                case SORTED -> consumeSorted(field.name);
                case SORTED_NUMERIC -> consumeSortedNumeric(field.name);
                case SORTED_SET -> consumeSortedSet(field.name);
                default -> {
                    // unsupported type
                }
            }

            FieldAnalysis fieldAnalysis = indexAnalysisResult.getFieldAnalysis(field.name);
            DocValuesFieldAnalysis docValues = ensureFieldAnalysis(fieldAnalysis, DocValuesAnalysisMode.INSTRUMENTED);
            docValues.addTrackingByDirectory(directory);
            directory.resetBytesRead();
        }
    }

    private void consumeNumeric(String field) throws IOException {
        NumericDocValues values = DocValues.getNumeric(segmentReader, field);
        while (values.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
            values.longValue();
        }
    }

    private void consumeBinary(String field) throws IOException {
        BinaryDocValues values = DocValues.getBinary(segmentReader, field);
        while (values.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
            values.binaryValue();
        }
    }

    private void consumeSorted(String field) throws IOException {
        SortedDocValues values = DocValues.getSorted(segmentReader, field);
        while (values.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
            values.ordValue();
        }
        TermsEnum termsEnum = values.termsEnum();
        while (termsEnum.next() != null) {
            // exhaust dictionary
        }
    }

    private void consumeSortedNumeric(String field) throws IOException {
        SortedNumericDocValues values = DocValues.getSortedNumeric(segmentReader, field);
        while (values.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
            for (int i = 0; i < values.docValueCount(); i++) {
                values.nextValue();
            }
        }
    }

    private void consumeSortedSet(String field) throws IOException {
        SortedSetDocValues values = DocValues.getSortedSet(segmentReader, field);
        while (values.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
            for (int i = 0; i < values.docValueCount(); i++) {
                values.nextOrd();
            }
        }
        TermsEnum termsEnum = values.termsEnum();
        while (termsEnum.next() != null) {
            // exhaust dictionary
        }
    }

    private DocValuesFieldAnalysis ensureFieldAnalysis(FieldAnalysis fieldAnalysis, DocValuesAnalysisMode mode) {
        if (fieldAnalysis.docValues == null || fieldAnalysis.docValues.analysisMode != mode) {
            fieldAnalysis.docValues = new DocValuesFieldAnalysis(mode);
        }
        return fieldAnalysis.docValues;
    }

    private String buildFullSuffix(String formatName, String suffix) {
        return formatName + "_" + suffix;
    }

    private static final class Lucene90DocValuesInspector {
        private final TrackingReadBytesDirectory directory;
        private final SegmentReader segmentReader;

        private Lucene90DocValuesInspector(TrackingReadBytesDirectory directory, SegmentReader segmentReader) {
            this.directory = directory;
            this.segmentReader = segmentReader;
        }

        Map<String, FieldDocValuesSizes> inspect(String fullSuffix, List<FieldInfo> fields) throws IOException {
            if (fields.isEmpty()) {
                return Map.of();
            }

            Map<Integer, FieldDocValuesSizesMutable> sizesByNumber = new LinkedHashMap<>();
            for (FieldInfo field : fields) {
                sizesByNumber.put(field.number, new FieldDocValuesSizesMutable());
            }

            String segmentName = segmentReader.getSegmentInfo().info.name;
            byte[] segmentId = segmentReader.getSegmentInfo().info.getId();

            String metaName = IndexFileNames.segmentFileName(segmentName, fullSuffix, LUCENE90_META_EXTENSION);
            List<Integer> order = new ArrayList<>();

            long headerBytes = 0L;
            long sentinelBytes = 0L;
            long footerBytes = 0L;

            try (ChecksumIndexInput metaInput = directory.openChecksumInput(metaName, IOContext.READONCE)) {
                CodecUtil.checkIndexHeader(
                        metaInput,
                        LUCENE90_META_CODEC,
                        LUCENE90_VERSION_START,
                        LUCENE90_VERSION_CURRENT,
                        segmentId,
                        fullSuffix);
                headerBytes = metaInput.getFilePointer();

                while (true) {
                    long entryStart = metaInput.getFilePointer();
                    int fieldNumber = metaInput.readInt();
                    if (fieldNumber == -1) {
                        sentinelBytes = metaInput.getFilePointer() - entryStart;
                        break;
                    }
                    FieldInfo info = segmentReader.getFieldInfos().fieldInfo(fieldNumber);
                    if (info == null) {
                        throw new IOException("Invalid field number " + fieldNumber + " in DocValues metadata");
                    }
                    byte type = metaInput.readByte();
                    FieldDocValuesSizesMutable sizes =
                            sizesByNumber.computeIfAbsent(fieldNumber, num -> new FieldDocValuesSizesMutable());
                    switch (type) {
                        case LUCENE90_NUMERIC -> applyNumeric(readNumeric(metaInput), sizes);
                        case LUCENE90_BINARY -> applyBinary(readBinary(metaInput), sizes);
                        case LUCENE90_SORTED -> applySorted(readSorted(metaInput), sizes);
                        case LUCENE90_SORTED_SET -> applySortedSet(readSortedSet(metaInput), sizes);
                        case LUCENE90_SORTED_NUMERIC -> applySortedNumeric(readSortedNumeric(metaInput), sizes);
                        default -> throw new IOException("Unsupported DocValues type id: " + type);
                    }
                    long entryEnd = metaInput.getFilePointer();
                    sizes.addMeta(entryEnd - entryStart);
                    order.add(fieldNumber);
                }
                long footerStart = metaInput.getFilePointer();
                CodecUtil.checkFooter(metaInput);
                footerBytes = metaInput.length() - footerStart;
            } catch (FileNotFoundException | NoSuchFileException e) {
                return Map.of();
            }

            if (!order.isEmpty()) {
                long overhead = headerBytes + sentinelBytes + footerBytes;
                if (overhead > 0) {
                    FieldDocValuesSizesMutable sizes = null;
                    for (FieldInfo field : fields) {
                        sizes = sizesByNumber.get(field.number);
                        if (sizes != null) {
                            break;
                        }
                    }
                    if (sizes != null) {
                        sizes.addMeta(overhead);
                    }
                }
            }

            String dataName = IndexFileNames.segmentFileName(segmentName, fullSuffix, LUCENE90_DATA_EXTENSION);
            long payloadLength = 0L;
            try (IndexInput dataInput = directory.openInput(dataName, IOContext.READONCE)) {
                CodecUtil.checkIndexHeader(
                        dataInput,
                        LUCENE90_DATA_CODEC,
                        LUCENE90_VERSION_START,
                        LUCENE90_VERSION_CURRENT,
                        segmentId,
                        fullSuffix);
                long header = dataInput.getFilePointer();
                long footer = CodecUtil.footerLength();
                payloadLength = dataInput.length() - header - footer;
            } catch (FileNotFoundException | NoSuchFileException e) {
                payloadLength = 0L;
            }

            long assignedData = sizesByNumber.values().stream()
                    .mapToLong(FieldDocValuesSizesMutable::dataBytes)
                    .sum();
            long remainder = payloadLength - assignedData;
            if (remainder > 0 && !order.isEmpty()) {
                FieldDocValuesSizesMutable sizes = null;
                for (FieldInfo field : fields) {
                    sizes = sizesByNumber.get(field.number);
                    if (sizes != null) {
                        break;
                    }
                }
                if (sizes != null) {
                    sizes.addData(remainder);
                }
            }

            Map<String, FieldDocValuesSizes> result = new LinkedHashMap<>();
            for (FieldInfo field : fields) {
                FieldDocValuesSizesMutable mutable = sizesByNumber.get(field.number);
                if (mutable != null) {
                    result.put(field.name, new FieldDocValuesSizes(mutable.metaBytes(), mutable.dataBytes()));
                }
            }
            return result;
        }

        private NumericEntry readNumeric(IndexInput meta) throws IOException {
            NumericEntry entry = new NumericEntry();
            entry.docsWithFieldOffset = meta.readLong();
            entry.docsWithFieldLength = meta.readLong();
            meta.readShort();
            meta.readByte();
            entry.numValues = meta.readLong();
            int tableSize = meta.readInt();
            if (tableSize > 256) {
                throw new IOException("Invalid numeric table size: " + tableSize);
            }
            if (tableSize >= 0) {
                for (int i = 0; i < tableSize; i++) {
                    meta.readLong();
                }
            }
            if (tableSize < -1) {
                // block shift indicator, no additional data
            }
            meta.readByte();
            meta.readLong();
            meta.readLong();
            meta.readLong();
            entry.valuesLength = meta.readLong();
            meta.readLong();
            return entry;
        }

        private BinaryEntry readBinary(IndexInput meta) throws IOException {
            BinaryEntry entry = new BinaryEntry();
            meta.readLong();
            entry.dataLength = meta.readLong();
            entry.docsWithFieldOffset = meta.readLong();
            entry.docsWithFieldLength = meta.readLong();
            meta.readShort();
            meta.readByte();
            int numDocsWithField = meta.readInt();
            int minLength = meta.readInt();
            int maxLength = meta.readInt();
            if (minLength < maxLength) {
                meta.readLong();
                int blockShift = meta.readVInt();
                DirectMonotonicReader.loadMeta(meta, numDocsWithField + 1L, blockShift);
                entry.addressesLength = meta.readLong();
            }
            return entry;
        }

        private SortedEntry readSorted(IndexInput meta) throws IOException {
            SortedEntry entry = new SortedEntry();
            entry.ordsEntry = readNumeric(meta);
            entry.termsDictEntry = readTermsDict(meta);
            return entry;
        }

        private SortedNumericEntry readSortedNumeric(IndexInput meta) throws IOException {
            SortedNumericEntry entry = new SortedNumericEntry();
            readNumeric(meta, entry);
            entry.numDocsWithField = meta.readInt();
            if (entry.numDocsWithField != entry.numValues) {
                meta.readLong();
                int blockShift = meta.readVInt();
                DirectMonotonicReader.loadMeta(meta, entry.numDocsWithField + 1L, blockShift);
                entry.addressesLength = meta.readLong();
            }
            return entry;
        }

        private SortedSetEntry readSortedSet(IndexInput meta) throws IOException {
            SortedSetEntry entry = new SortedSetEntry();
            byte multiValued = meta.readByte();
            if (multiValued == 0) {
                entry.singleValueEntry = readSorted(meta);
            } else if (multiValued == 1) {
                entry.ordsEntry = readSortedNumeric(meta);
                entry.termsDictEntry = readTermsDict(meta);
            } else {
                throw new IOException("Invalid SortedSet multiValued flag: " + multiValued);
            }
            return entry;
        }

        private TermsDictEntry readTermsDict(IndexInput meta) throws IOException {
            TermsDictEntry entry = new TermsDictEntry();
            long termsDictSize = meta.readVLong();
            int blockShift = meta.readInt();
            long addressesSize =
                    (termsDictSize + (1L << LUCENE90_TERMS_DICT_BLOCK_SHIFT) - 1) >>> LUCENE90_TERMS_DICT_BLOCK_SHIFT;
            DirectMonotonicReader.loadMeta(meta, addressesSize, blockShift);
            meta.readInt();
            meta.readInt();
            meta.readLong();
            entry.termsDataLength = meta.readLong();
            meta.readLong();
            entry.termsAddressesLength = meta.readLong();
            int termsDictIndexShift = meta.readInt();
            long indexSize = (termsDictSize + (1L << termsDictIndexShift) - 1) >>> termsDictIndexShift;
            DirectMonotonicReader.loadMeta(meta, 1 + indexSize, blockShift);
            meta.readLong();
            entry.termsIndexLength = meta.readLong();
            meta.readLong();
            entry.termsIndexAddressesLength = meta.readLong();
            return entry;
        }

        private void applyNumeric(NumericEntry entry, FieldDocValuesSizesMutable sizes) {
            if (entry.docsWithFieldOffset >= 0) {
                sizes.addData(entry.docsWithFieldLength);
            }
            sizes.addData(entry.valuesLength);
        }

        private void applyBinary(BinaryEntry entry, FieldDocValuesSizesMutable sizes) {
            if (entry.docsWithFieldOffset >= 0) {
                sizes.addData(entry.docsWithFieldLength);
            }
            sizes.addData(entry.dataLength);
            if (entry.addressesLength > 0) {
                sizes.addData(entry.addressesLength);
            }
        }

        private void applySorted(SortedEntry entry, FieldDocValuesSizesMutable sizes) {
            applyNumeric(entry.ordsEntry, sizes);
            applyTermsDict(entry.termsDictEntry, sizes);
        }

        private void applySortedNumeric(SortedNumericEntry entry, FieldDocValuesSizesMutable sizes) {
            applyNumeric(entry, sizes);
            if (entry.addressesLength > 0) {
                sizes.addData(entry.addressesLength);
            }
        }

        private void applySortedSet(SortedSetEntry entry, FieldDocValuesSizesMutable sizes) {
            if (entry.singleValueEntry != null) {
                applySorted(entry.singleValueEntry, sizes);
            } else {
                applySortedNumeric(entry.ordsEntry, sizes);
                applyTermsDict(entry.termsDictEntry, sizes);
            }
        }

        private void applyTermsDict(TermsDictEntry entry, FieldDocValuesSizesMutable sizes) {
            sizes.addData(entry.termsDataLength);
            sizes.addData(entry.termsAddressesLength);
            sizes.addData(entry.termsIndexLength);
            sizes.addData(entry.termsIndexAddressesLength);
        }

        private void readNumeric(IndexInput meta, NumericEntry entry) throws IOException {
            NumericEntry temp = readNumeric(meta);
            entry.docsWithFieldOffset = temp.docsWithFieldOffset;
            entry.docsWithFieldLength = temp.docsWithFieldLength;
            entry.numValues = temp.numValues;
            entry.valuesLength = temp.valuesLength;
        }

        private static final class FieldDocValuesSizesMutable {
            private long metaBytes;
            private long dataBytes;

            void addMeta(long delta) {
                metaBytes += Math.max(0L, delta);
            }

            void addData(long delta) {
                dataBytes += Math.max(0L, delta);
            }

            long metaBytes() {
                return metaBytes;
            }

            long dataBytes() {
                return dataBytes;
            }
        }

        private static class NumericEntry {
            long docsWithFieldOffset;
            long docsWithFieldLength;
            long numValues;
            long valuesLength;
        }

        private static class SortedNumericEntry extends NumericEntry {
            int numDocsWithField;
            long addressesLength;
        }

        private static class BinaryEntry {
            long dataLength;
            long docsWithFieldOffset;
            long docsWithFieldLength;
            long addressesLength;
        }

        private static class SortedEntry {
            NumericEntry ordsEntry;
            TermsDictEntry termsDictEntry;
        }

        private static class SortedSetEntry {
            SortedEntry singleValueEntry;
            SortedNumericEntry ordsEntry;
            TermsDictEntry termsDictEntry;
        }

        private static class TermsDictEntry {
            long termsDataLength;
            long termsAddressesLength;
            long termsIndexLength;
            long termsIndexAddressesLength;
        }
    }
}
