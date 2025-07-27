package org.commrogue.lucene;

import static org.apache.lucene.codecs.perfield.PerFieldPostingsFormat.PER_FIELD_FORMAT_KEY;
import static org.apache.lucene.codecs.perfield.PerFieldPostingsFormat.PER_FIELD_SUFFIX_KEY;

import java.io.IOException;
import org.apache.lucene.backward_codecs.lucene50.Lucene50PostingsFormat;
import org.apache.lucene.backward_codecs.lucene84.Lucene84PostingsFormat;
import org.apache.lucene.backward_codecs.lucene90.Lucene90PostingsFormat;
import org.apache.lucene.backward_codecs.lucene99.Lucene99PostingsFormat;
import org.apache.lucene.codecs.lucene912.Lucene912PostingsFormat;
import org.apache.lucene.index.*;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;

public class Utils {
    public static SegmentReader segmentReader(LeafReader reader) {
        if (reader instanceof SegmentReader) {
            return (SegmentReader) reader;
        } else if (reader instanceof final FilterLeafReader fReader) {
            return segmentReader(FilterLeafReader.unwrap(fReader));
        } else if (reader instanceof final FilterCodecReader fReader) {
            return segmentReader(FilterCodecReader.unwrap(fReader));
        }
        throw new IllegalStateException("Can not extract segment reader from given index reader [" + reader + "]");
    }

    /**
     * Container over doc, pos, and payload File Pointers from any historic Lucene Codec.
     * Necessary since abstract codec class does not expose FPs, due to historic
     * codecs not having them.
     * <p>
     * Note - In compound mode, FPs are offsets from the base pointer to the relevant
     * internal file within the compound file.
     *
     * @param docStartFP file pointer to the start of the doc ids enumeration, in .doc file
     * @param posStartFP file pointer to the start of the positions enumeration, in .pos file
     * @param payloadFP  file pointer to the start of the payloads enumeration, in .pay file
     */
    public record BlockTermState(long docStartFP, long posStartFP, long payloadFP) {
        public long distance(BlockTermState other) {
            return this.docStartFP
                    - other.docStartFP
                    + this.posStartFP
                    - other.posStartFP
                    + this.payloadFP
                    - other.payloadFP;
        }
    }

    /**
     * Seeks the given term, and resolves the Lucene version dependent BlockTermState for the given term,
     * to a version independent one, exposing necessary FPs.
     *
     * @param termsEnum TermsEnum positioned below the given term.
     * @param term      reference to term for which to return the BlockTermState.
     * @return BlockTermState for the given term
     * @throws IOException
     */
    public static BlockTermState getBlockTermState(TermsEnum termsEnum, BytesRef term) throws IOException {
        if (term != null && termsEnum.seekExact(term)) {
            final TermState termState = termsEnum.termState();
            if (termState instanceof final Lucene912PostingsFormat.IntBlockTermState blockTermState) {
                return new BlockTermState(
                        blockTermState.docStartFP, blockTermState.posStartFP, blockTermState.payStartFP);
            }
            if (termState instanceof final Lucene99PostingsFormat.IntBlockTermState blockTermState) {
                return new BlockTermState(
                        blockTermState.docStartFP, blockTermState.posStartFP, blockTermState.payStartFP);
            }
            if (termState instanceof final Lucene90PostingsFormat.IntBlockTermState blockTermState) {
                return new BlockTermState(
                        blockTermState.docStartFP, blockTermState.posStartFP, blockTermState.payStartFP);
            }
            if (termState instanceof final Lucene84PostingsFormat.IntBlockTermState blockTermState) {
                return new BlockTermState(
                        blockTermState.docStartFP, blockTermState.posStartFP, blockTermState.payStartFP);
            }
            if (termState instanceof final Lucene50PostingsFormat.IntBlockTermState blockTermState) {
                return new BlockTermState(
                        blockTermState.docStartFP, blockTermState.posStartFP, blockTermState.payStartFP);
            }
            assert false : "unsupported postings format: " + termState;
        }
        return null;
    }

    private static String getSuffix(String formatName, String suffix) {
        return formatName + "_" + suffix;
    }

    public static String getSegmentSuffix(FieldInfo fieldInfo) {
        final String formatName = fieldInfo.getAttribute(PER_FIELD_FORMAT_KEY);
        final String suffix = fieldInfo.getAttribute(PER_FIELD_SUFFIX_KEY);

        return getSuffix(formatName, suffix);
    }

    public static BytesRef readBytesRef(IndexInput in) throws IOException {
        int numBytes = in.readVInt();
        if (numBytes < 0) {
            throw new CorruptIndexException("invalid bytes length: " + numBytes, in);
        }

        BytesRef bytes = new BytesRef();
        bytes.length = numBytes;
        bytes.bytes = new byte[numBytes];
        in.readBytes(bytes.bytes, 0, numBytes);

        return bytes;
    }

    /**
     * Checks if the term contains positions, and if so, calls .nextPosition() on each term.
     * In our case, this is called with the PostingsEnum positioned to the last document,
     * which makes sure we make reads to the end of the .pos slice, so the
     * tracker will track the end offsets of the .pos for the current field.
     *
     * @param terms    Terms instance for the given field.
     * @param postings PostingsEnum, which in our case, should be positioned to the last document in the postings for the given field.
     * @throws IOException
     */
    public static void readPositions(Terms terms, PostingsEnum postings) throws IOException {
        if (terms.hasPositions()) {
            for (int pos = 0; pos < postings.freq(); pos++) {
                postings.nextPosition();
                // in Lucene90, .nextPosition() already sets the PostingReader's startOffset, endOffset,
                // and payload, and therefore the next 3 calls do not make any seeks to the index
                postings.startOffset();
                postings.endOffset();
                postings.getPayload();
            }
        }
    }
}
