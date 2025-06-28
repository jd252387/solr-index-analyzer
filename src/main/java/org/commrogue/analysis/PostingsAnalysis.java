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
            // It's expensive to look up every term and visit every document of the postings lists of all terms.
            // As we track the min/max positions of read bytes, we just visit the two ends of a partition containing
            // the data. We might miss some small parts of the data, but it's a good trade-off to speed up the process.
            TermsEnum termsEnum = terms.iterator();
            final BlockTermState minState = getBlockTermState(termsEnum, terms.getMin());
            if (minState != null) {
                final BlockTermState maxState = Objects.requireNonNull(
                        getBlockTermState(termsEnum, terms.getMax()),
                        "can't retrieve the block term state of the max term"
                );
                final long skippedBytes = maxState.distance(minState);
                indexAnalysisResult.getFieldAnalysis(field.name).invertedIndexBytes += skippedBytes;
                // TODO - didn't we just do this by getBlockTermState?
                termsEnum.seekExact(terms.getMax());
                postings = termsEnum.postings(postings, PostingsEnum.ALL);
                // if we are at the end of the postings lists
                if (postings.advance(termsEnum.docFreq() - 1) != DocIdSetIterator.NO_MORE_DOCS) {
                    postings.freq();
                    readProximity(terms, postings);
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
                        readProximity(terms, postings);
                    }
                }
            }
            indexAnalysisResult.getFieldAnalysis(field.name).invertedIndexBytes += directory.getBytesRead();
        }
    }
}
