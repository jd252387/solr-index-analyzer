package org.commrogue.analysis.iindex;

import java.io.IOException;
import java.util.*;

import com.sun.source.tree.Tree;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.lucene90.blocktree.FieldReader;
import org.apache.lucene.codecs.lucene90.blocktree.Lucene90BlockTreeTermsReader;
import org.apache.lucene.index.*;
import org.apache.lucene.store.*;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.fst.ByteSequenceOutputs;
import org.apache.lucene.util.fst.FST;
import org.commrogue.LuceneFileExtension;
import org.commrogue.lucene.Utils;

import static org.apache.lucene.codecs.lucene99.Lucene99PostingsFormat.*;

public class BlockSkippingTermsAnalyzer {
    private static final String TERMS_META_CODEC_NAME = "BlockTreeTermsMeta";
    private static final String TERMS_CODEC_NAME = "Lucene90PostingsWriterTerms";

    private static final int OUTPUT_FLAGS_NUM_BITS = 2;

    public record TermsAnalysis(long metadataSize, long indexSize, long dictionarySize) {}

    private record TermsFPs(int fieldNum, long metadataFP, long indexFP, long dictionaryFP) {}

    /**
     * Copied from {@link FieldReader}.readVLongOutput()
     */
    private static long readVLongOutput(int codecVersion, DataInput in) throws IOException {
        if (codecVersion >= Lucene90BlockTreeTermsReader.VERSION_MSB_VLONG_OUTPUT) {
            return readMSBVLong(in);
        } else {
            return in.readVLong();
        }
    }

    /**
     * Copied from {@link FieldReader}.readMSBVLong()
     */
    private static long readMSBVLong(DataInput in) throws IOException {
        long l = 0L;
        while (true) {
            byte b = in.readByte();
            l = (l << 7) | (b & 0x7FL);
            if ((b & 0x80) == 0) {
                break;
            }
        }
        return l;
    }

    private static long getRootBlockFPFromCode(int codecVersion, BytesRef rootBlockCode) throws IOException {
        return readVLongOutput(
                        codecVersion,
                        new ByteArrayDataInput(rootBlockCode.bytes, rootBlockCode.offset, rootBlockCode.length))
                >>> OUTPUT_FLAGS_NUM_BITS;
    }

    private static TermsFPs[] readTermFPs(SegmentReadState state) throws IOException {
        String metaName = IndexFileNames.segmentFileName(
                state.segmentInfo.name, state.segmentSuffix, LuceneFileExtension.TMD.getExtension());

        try (IndexInput metaIn = state.directory.openInput(metaName, state.context)) {
            // TODO - documentation states CodecHeader but we are reading IndexHeader?
            // Header -> CodecHeader
            final int codecVersion = CodecUtil.checkIndexHeader(
                    metaIn,
                    TERMS_META_CODEC_NAME,
                    Lucene90BlockTreeTermsReader.VERSION_START,
                    Lucene90BlockTreeTermsReader.VERSION_CURRENT,
                    state.segmentInfo.getId(),
                    state.segmentSuffix);

            // TODO - why are we parsing this? This, along with the IndexBlockSize VInt looks like .tim's PostingsHeader.
            //  Why is it appended to the .tmd as well?
            // PostingsHeader -> Header, PackedBlockSize
            // Header -> IndexHeader
            CodecUtil.checkIndexHeader(
                    metaIn,
                    TERMS_CODEC_NAME,
                    Lucene90BlockTreeTermsReader.VERSION_START,
                    Lucene90BlockTreeTermsReader.VERSION_CURRENT,
                    state.segmentInfo.getId(),
                    state.segmentSuffix);

            // TODO - again, no mention in the documentation. Seems like the IndexBlockSize from .tim's PostingsHeader.
            // PackedBlockSize -> VInt
            metaIn.readVInt();

            // NumFields -> VInt
            final int numFields = metaIn.readVInt();

            if (numFields < 0) {
                throw new CorruptIndexException("invalid numFields: " + numFields, metaIn);
            }

            TermsFPs[] termsFPs = new TermsFPs[numFields];

            for (int fieldCounter = 0; fieldCounter < numFields; fieldCounter++) {
                // FieldNumber -> VInt
                final int fieldNum = metaIn.readVInt();
                final FieldInfo fieldInfo = state.fieldInfos.fieldInfo(fieldNum);
                if (fieldInfo == null) {
                    throw new CorruptIndexException("invalid field number: " + fieldNum, metaIn);
                }

                // NumTerms -> VLong
                long numTerms = metaIn.readVLong();

                // RootCode -> VInt followed by byte[]
                final BytesRef rootDictionaryBlock = Utils.readBytesRef(metaIn);

                // SumTotalTermFreq? -> VLong
                // when frequencies are omitted, sumTotalTermFreq is not written, so sumDocFreq = sumTotalTermFreq
                if (fieldInfo.getIndexOptions() != IndexOptions.DOCS) metaIn.readVLong();

                // SumDocFreq -> VLong
                long sumDocFreq = metaIn.readVLong();

                // DocCount -> VInt
                metaIn.readVInt();

                // MinTerm -> VInt length followed byte[]
                BytesRef min = Utils.readBytesRef(metaIn);

                // MaxTerm -> VInt length followed byte[]
                Utils.readBytesRef(metaIn);

                // IndexStartFP -> VLong
                final long indexStartFP = metaIn.readVLong();

                termsFPs[fieldCounter] = new TermsFPs(fieldNum,
                        metaIn.getFilePointer(),
                        indexStartFP,
                        getRootBlockFPFromCode(codecVersion, rootDictionaryBlock));

                FST.readMetadata(metaIn, ByteSequenceOutputs.getSingleton());
            }

            return termsFPs;
        }
    }

    /**
     * Attempts to analyze terms-related files (.tip, .tim, and .tmd) by reading the .tmd (separate file from .tip since 8.6 - see LUCENE-9353),
     * which includes file offsets to the relevant files for each field.
     *
     * @param state {@link SegmentReadState} for the relevant segment.
     * @return array of {@link TermsAnalysis}. This array is always of size numFields, and is sorted by field numbers.
     */
    public static Map<String, TermsAnalysis> analyze(SegmentReadState state) throws IOException {
        TermsFPs[] termsFPs = readTermFPs(state);

        HashMap<String, TermsAnalysis> analysisResult = new HashMap<>();

        analysisResult.put(state.fieldInfos.fieldInfo(termsFPs[0].fieldNum).getName(), new TermsAnalysis(termsFPs[0].metadataFP, termsFPs[0].indexFP, termsFPs[0].dictionaryFP));

        for (int i = 1; i < termsFPs.length; i++) {
            analysisResult.put(state.fieldInfos.fieldInfo(termsFPs[i].fieldNum).getName(), new TermsAnalysis(
                    termsFPs[i].metadataFP - termsFPs[i - 1].metadataFP,
                    termsFPs[i].indexFP - termsFPs[i - 1].indexFP,
                    termsFPs[i].dictionaryFP - termsFPs[i - 1].dictionaryFP));
        }

        return analysisResult;
    }
}
