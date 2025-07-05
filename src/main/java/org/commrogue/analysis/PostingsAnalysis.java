package org.commrogue.analysis;

import lombok.RequiredArgsConstructor;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.index.*;
import org.apache.lucene.search.DocIdSetIterator;
import org.commrogue.IndexAnalysisResult;
import static org.commrogue.lucene.Utils.*;
import org.commrogue.tracking.TrackingReadBytesDirectory;

import java.io.IOException;
import java.util.Objects;

import static org.commrogue.lucene.Utils.getBlockTermState;

@RequiredArgsConstructor
public class PostingsAnalysis implements Analysis {
    private final TrackingReadBytesDirectory directory;
    private final SegmentReader segmentReader;
    private final IndexAnalysisResult indexAnalysisResult;

    /**
     * Given a minimum BlockTermState for a term, calculates the distance between it and the maximum BlockTermState.
     * This distance is the sum of deltas between the minimum and maximum file pointers in .doc, .pos, and .pay, which is essentially
     * the size the field takes in these files, assuming we are in the BlockTree implementation of these.
     */
    private long analyzeTermStateStructures(TermsEnum termsEnum, Terms terms, BlockTermState minState) throws IOException {
        final BlockTermState maxState = Objects.requireNonNull(
                getBlockTermState(termsEnum, terms.getMax()),
                "can't retrieve the block term state of the max term"
        );

        return maxState.distance(minState);
    }

    /**
     * Analyzes postings on the index, including positions.
     * In order to not iterate over the entire postings list for each term, this method attempts to calculate an approximate
     * size by making reads to the start and end file pointers for each field, and relying on the tracker
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
                termsEnum.seekExact(terms.getMin());
                postings = termsEnum.postings(postings, PostingsEnum.ALL);
                if (postings.advance(termsEnum.docFreq() - 1) != DocIdSetIterator.NO_MORE_DOCS) {
                    postings.freq();
                    readPositions(terms, postings);
                }
                // size of .doc, .pos, and .pay
                indexAnalysisResult.getFieldAnalysis(field.name).invertedIndexBytes += analyzeTermStateStructures(termsEnum, terms, minState);

                // TODO - didn't we just do this by getBlockTermState?
                termsEnum.seekExact(terms.getMax());

                //
                postings = termsEnum.postings(postings, PostingsEnum.ALL);
                // if we are at the end of the postings lists
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
                // We aren't sure if the optimization can be applied for other implementations rather than the BlockTree
                // based implementation. Hence, we just traverse every postings of all terms in this case.
                while (termsEnum.next() != null) {
                    termsEnum.docFreq();
                    termsEnum.totalTermFreq();
                    postings = termsEnum.postings(postings, PostingsEnum.ALL);
                    while (postings.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                        postings.freq();
                        readPositions(terms, postings);
                    }
                }
            }
            indexAnalysisResult.getFieldAnalysis(field.name).invertedIndexBytes += directory.getBytesRead();
        }
    }
}
