package org.commrogue.analysis;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.index.*;
import org.apache.lucene.search.DocIdSetIterator;
import org.commrogue.FieldAnalysis;
import org.commrogue.IndexAnalysisResult;
import static org.commrogue.lucene.Utils.*;

import org.commrogue.LuceneFileExtension;
import org.commrogue.tracking.TrackingReadBytesDirectory;

import java.io.IOException;
import java.util.Objects;

import static org.commrogue.lucene.Utils.getBlockTermState;

@RequiredArgsConstructor
public class PostingsAnalysis implements Analysis {
    private final TrackingReadBytesDirectory directory;
    private final SegmentReader segmentReader;
    @Getter
    private final IndexAnalysisResult indexAnalysisResult;

    /**
     * Given a minimum BlockTermState for a term, calculates the distance between it and the maximum BlockTermState.
     * This distance is the sum of deltas between the minimum and maximum file pointers in .doc, .pos, and .pay, which is essentially
     * the size the field takes in these files, assuming we are in the BlockTree implementation of these.
     */
    private void analyzeBlockTermState(TermsEnum termsEnum, Terms terms, BlockTermState minState, String fieldName) throws IOException {
        final BlockTermState maxState = Objects.requireNonNull(
                getBlockTermState(termsEnum, terms.getMax()),
                "can't retrieve the block term state of the max term"
        );

        FieldAnalysis.AggregateSegmentReference invertedIndexReference = indexAnalysisResult.getFieldAnalysis(fieldName).invertedIndex;
        invertedIndexReference.addTrackingByExtension(LuceneFileExtension.DOC, maxState.docStartFP() - minState.docStartFP());
        invertedIndexReference.addTrackingByExtension(LuceneFileExtension.POS, maxState.posStartFP() - minState.posStartFP());
        invertedIndexReference.addTrackingByExtension(LuceneFileExtension.PAY, maxState.payloadFP() - minState.payloadFP());
    }

    /**
     * Analyzes postings on the index/
     * Tracks sizes of .tim, .doc, .pos, and .pay sizes.
     * In a BlockTree implementation, this method takes advantage of the structure,
     * and in order to not iterate over the entire postings list
     * for each term, it attempts to calculate an approximate size by making reads
     * to the start and end file pointers for each field, and relying on the tracker
     * to keep track of these offsets.
     * @throws IOException
     */
    @Override
    public void analyze() throws IOException {
        FieldsProducer postingsReader = segmentReader.getPostingsReader();
        if (postingsReader == null) {
            return;
        }
        postingsReader = postingsReader.getMergeInstance();
        PostingsEnum postings = null;
        for (FieldInfo field : segmentReader.getFieldInfos()) {
            if (field.getIndexOptions() == IndexOptions.NONE) {
                continue;
            }
            directory.resetBytesRead();
            final Terms terms = postingsReader.terms(field.name);
            if (terms == null) {
                continue;
            }
            TermsEnum termsEnum = terms.iterator();
            final BlockTermState minState = getBlockTermState(termsEnum, terms.getMin());
            if (minState != null) {
                // size of .doc, .pos, and .pay
                analyzeBlockTermState(termsEnum, terms, minState, field.name);

                // TODO - didn't we just do this by getBlockTermState?
                termsEnum.seekExact(terms.getMax());

                // TODO - isn't this only needed without the optimization? they are already accounted for by BlockTermState.
                // also, this doesn't do anything since we don't manually advance the PostingsEnum, and also do
                // not make an initial readPositions to the first term, so the start offset in the .pos is not tracked
                postings = termsEnum.postings(postings, PostingsEnum.ALL);
                if (postings.advance(termsEnum.docFreq() - 1) != DocIdSetIterator.NO_MORE_DOCS) {
                    postings.freq();
                    readPositions(terms, postings);
                }
                final long bytesRead = directory.getBytesRead();
                int visitedTerms = 0;
                final long totalTerms = terms.size();
                termsEnum = terms.iterator();
                // Iterate until we really access the first terms, but iterate all if the number of terms is small
                while (termsEnum.next() != null) {
                    ++visitedTerms;
                    if (totalTerms > 1000 && visitedTerms % 50 == 0 && directory.getBytesRead() > bytesRead) {
                        break;
                    }
                }
            } else {
                // if there is no minState, the implementation isn't BlockTree. shouldn't happen in Lucene90.
                // however, if this is the case, then we must iterate over all terms for the field.
                // in Lucene90, termsEnum.next() makes reads to .tim, reading the entire block for each field (which also
                // contains docFreq and totalTermFreq). postings.nextDoc() then makes reads to the postings
                // within .doc with file pointers in each .tim entry.
                while (termsEnum.next() != null) {
                    // in Lucene90, .docFreq() and .totalTermFreq() do not make any reads as
                    // they read from a buffer generated by SegmentTermsEnumFrame.loadBlock(), which
                    // is called upon termsEnum.next(), which DECODES only the term bytes, but does READ
                    // all metadata bytes. in older codecs, calling these may make reads, but I haven't made sure
                    termsEnum.docFreq();
                    termsEnum.totalTermFreq();
                    postings = termsEnum.postings(postings, PostingsEnum.ALL);
                    while (postings.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                        postings.freq();
                        readPositions(terms, postings);
                    }
                }
            }
            indexAnalysisResult.getFieldAnalysis(field.name).invertedIndex.addTrackingByDirectory(directory);
        }
    }
}
