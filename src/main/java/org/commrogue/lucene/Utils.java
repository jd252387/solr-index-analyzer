package org.commrogue.lucene;

import org.apache.lucene.backward_codecs.lucene50.Lucene50PostingsFormat;
import org.apache.lucene.backward_codecs.lucene84.Lucene84PostingsFormat;
import org.apache.lucene.backward_codecs.lucene90.Lucene90PostingsFormat;
import org.apache.lucene.backward_codecs.lucene912.Lucene912PostingsFormat;
import org.apache.lucene.backward_codecs.lucene99.Lucene99PostingsFormat;
import org.apache.lucene.codecs.lucene101.Lucene101PostingsFormat;
import org.apache.lucene.index.*;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;

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

    // necessary since not all codecs expose FPs
    public record BlockTermState(long docStartFP, long posStartFP, long payloadFP) {
        public long distance(BlockTermState other) {
            return this.docStartFP - other.docStartFP + this.posStartFP - other.posStartFP + this.payloadFP - other.payloadFP;
        }
    }

    public static BlockTermState getBlockTermState(TermsEnum termsEnum, BytesRef term) throws IOException {
        if (term != null && termsEnum.seekExact(term)) {
            final TermState termState = termsEnum.termState();
            if (termState instanceof final Lucene101PostingsFormat.IntBlockTermState blockTermState) {
                return new BlockTermState(blockTermState.docStartFP, blockTermState.posStartFP, blockTermState.payStartFP);
            }
            if (termState instanceof final Lucene912PostingsFormat.IntBlockTermState blockTermState) {
                return new BlockTermState(blockTermState.docStartFP, blockTermState.posStartFP, blockTermState.payStartFP);
            }
            if (termState instanceof final Lucene99PostingsFormat.IntBlockTermState blockTermState) {
                return new BlockTermState(blockTermState.docStartFP, blockTermState.posStartFP, blockTermState.payStartFP);
            }
            if (termState instanceof final Lucene90PostingsFormat.IntBlockTermState blockTermState) {
                return new BlockTermState(blockTermState.docStartFP, blockTermState.posStartFP, blockTermState.payStartFP);
            }
            if (termState instanceof final Lucene84PostingsFormat.IntBlockTermState blockTermState) {
                return new BlockTermState(blockTermState.docStartFP, blockTermState.posStartFP, blockTermState.payStartFP);
            }
            if (termState instanceof final Lucene50PostingsFormat.IntBlockTermState blockTermState) {
                return new BlockTermState(blockTermState.docStartFP, blockTermState.posStartFP, blockTermState.payStartFP);
            }
            assert false : "unsupported postings format: " + termState;
        }
        return null;
    }

    public static void readProximity(Terms terms, PostingsEnum postings) throws IOException {
        if (terms.hasPositions()) {
            for (int pos = 0; pos < postings.freq(); pos++) {
                postings.nextPosition();
                postings.startOffset();
                postings.endOffset();
                postings.getPayload();
            }
        }
    }
}
