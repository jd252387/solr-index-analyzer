package org.commrogue.analysis.iindex;

import static org.commrogue.lucene.Utils.*;
import static org.commrogue.lucene.Utils.getBlockTermState;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.index.*;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.commrogue.LuceneFileExtension;
import org.commrogue.analysis.exceptions.CorruptedHeaderException;
import org.commrogue.analysis.exceptions.NonBlockTreeException;
import org.commrogue.analysis.Analysis;
import org.commrogue.results.AggregateSegmentReference;
import org.commrogue.results.FieldAnalysis;
import org.commrogue.results.IndexAnalysisResult;
import org.commrogue.results.InvertedIndexFieldAnalysis;
import org.commrogue.tracking.TrackingReadBytesDirectory;

@RequiredArgsConstructor
public class InvertedIndexAnalysis implements Analysis {
    private final TrackingReadBytesDirectory directory;
    private final SegmentReader segmentReader;

    @Getter
    private final IndexAnalysisResult indexAnalysisResult;

    private final boolean allowNonBlockTermState;
    private final TermStructureAnalysisMode termStructuresAnalysisMode;

    private record BlockTermStateAnalysis(long docDelta, long posDelta, long payDelta) {}

    /**
     * Given a minimum BlockTermState for a term, calculates the distance between it and the maximum BlockTermState.
     * This distance is the sum of deltas between the minimum and maximum file pointers in .doc, .pos, and .pay, whichis essentially
     * the size the field takes in these files, assuming we are in the BlockTree implementation of these.
     */
    private BlockTermStateAnalysis analyzeBlockTermState(
            TermsEnum termsEnum, Terms terms, BlockTermState minState, String fieldName) throws IOException {
        final BlockTermState maxState = Objects.requireNonNull(
                getBlockTermState(termsEnum, terms.getMax()), "can't retrieve the block term state of the max term for field " + fieldName);

        return new BlockTermStateAnalysis(
                maxState.docStartFP() - minState.docStartFP(),
                maxState.posStartFP() - minState.posStartFP(),
                maxState.payloadFP() - minState.payloadFP());
    }

    private record TermsAnalysis(long termsIndexSize, long termsDictionarySize) {}

    private Map.Entry<PostingsEnum, TermsAnalysis> analyzeTermsByPartialInstrumentation(
            TermsEnum termsEnum, Terms terms, PostingsEnum reusedPostings) throws IOException {
        directory.resetBytesRead();

        // TODO - didn't we just do this by getBlockTermState?
        termsEnum.seekExact(terms.getMax());

        // TODO - isn't this only needed without the optimization? they are already accounted for by BlockTermState.
        // also, this doesn't do anything since we don't manually advance the PostingsEnum, and also do
        // not make an initial readPositions to the first term, so the start offset in the .pos is not tracked
        reusedPostings = termsEnum.postings(reusedPostings, PostingsEnum.ALL);
        if (reusedPostings.advance(termsEnum.docFreq() - 1) != DocIdSetIterator.NO_MORE_DOCS) {
            reusedPostings.freq();
            readPositions(terms, reusedPostings);
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

        AggregateSegmentReference segmentReference = new AggregateSegmentReference();
        segmentReference.addTrackingByDirectory(directory);

        return Map.entry(
                reusedPostings,
                new TermsAnalysis(
                        segmentReference.fileEntries.get(LuceneFileExtension.TIP),
                        segmentReference.fileEntries.get(LuceneFileExtension.TIM)));
    }

    private long blockCountTermsIndex(IndexInput termsIndexInput) throws IOException {
        // advance file pointer past the header
        CodecUtil.readIndexHeader(termsIndexInput);
        int fieldCount = termsIndexInput.readVInt();

        for (int i = 0; i < fieldCount; i++) {
            int fieldNum = termsIndexInput.readVInt();

        }

        return 0;
    }

    private Map.Entry<PostingsEnum, TermsAnalysis> analyzeTermsByBlockSkipping(IndexInput termsIndexInput, IndexInput termsDictionaryInput) throws IOException {
        blockCountTermsIndex(termsIndexInput);
        return null;
    }

    private Map.Entry<PostingsEnum, InvertedIndexFieldAnalysis> analyzeAllByFullInstrumentation(
            TermsEnum termsEnum, Terms terms, PostingsEnum reusedPostings) throws IOException {
        InvertedIndexFieldAnalysis analysis =
                new InvertedIndexFieldAnalysis(TermStructureAnalysisMode.FULL_INSTRUMENTED_IO);

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
            reusedPostings = termsEnum.postings(reusedPostings, PostingsEnum.ALL);
            while (reusedPostings.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                reusedPostings.freq();
                readPositions(terms, reusedPostings);
            }
        }

        analysis.addTrackingByDirectory(directory);

        return Map.entry(reusedPostings, analysis);
    }

    private IndexInput getBodyPositionedIndexInput(LuceneFileExtension extension) throws IOException {
        IndexInput indexInput = directory.openInput(segmentReader.getSegmentName() + "." + extension.getExtension(), IOContext.READ);
        CodecUtil.readIndexHeader(indexInput);

        return indexInput;
    }

    /**
     * Analyzes postings on the index
     * Tracks sizes of .tip, .tim, .doc, .pos, and .pay sizes.
     * In a BlockTree implementation, this method takes advantage of the structure,
     * and in order to not iterate over the entire postings list
     * for each term, it attempts to calculate an approximate size by making reads
     * to the start and end file pointers for each field, and relying on the tracker
     * to keep track of these offsets.
     *
     * @throws IOException
     */
    @Override
    public void analyze() throws IOException, NonBlockTreeException {
        FieldsProducer postingsReader = segmentReader.getPostingsReader();
        if (postingsReader == null) {
            return;
        }

        IndexInput termsIndexInput = getBodyPositionedIndexInput(LuceneFileExtension.TIP);
        IndexInput termsDictionaryInput = getBodyPositionedIndexInput(LuceneFileExtension.TIM);
        boolean areIndexInputsPositioned = true;

        postingsReader = postingsReader.getMergeInstance();
        PostingsEnum postings = null;
        for (FieldInfo field : segmentReader.getFieldInfos()) {
            FieldAnalysis fieldAnalysis = indexAnalysisResult.getFieldAnalysis(field.getName());
            if (field.getIndexOptions() == IndexOptions.NONE) {
                continue;
            }

            final Terms terms = postingsReader.terms(field.name);
            if (terms == null) {
                continue;
            }

            TermsEnum termsEnum = terms.iterator();
            if (termStructuresAnalysisMode == TermStructureAnalysisMode.FULL_INSTRUMENTED_IO) {
                Map.Entry<PostingsEnum, InvertedIndexFieldAnalysis> fullAnalysis =
                        analyzeAllByFullInstrumentation(termsEnum, terms, postings);
                areIndexInputsPositioned = false;
                postings = fullAnalysis.getKey();
                fieldAnalysis.invertedIndex = fullAnalysis.getValue();
            } else {
                final BlockTermState minState = getBlockTermState(termsEnum, terms.getMin());

                if (minState == null) {
                    if (!allowNonBlockTermState)
                        throw new NonBlockTreeException(field.getName(), segmentReader.getSegmentName());
                    Map.Entry<PostingsEnum, InvertedIndexFieldAnalysis> fullAnalysis =
                            analyzeAllByFullInstrumentation(termsEnum, terms, postings);
                    areIndexInputsPositioned = false;
                    postings = fullAnalysis.getKey();
                    fieldAnalysis.invertedIndex = fullAnalysis.getValue();
                } else {
                    fieldAnalysis.invertedIndex = new InvertedIndexFieldAnalysis(termStructuresAnalysisMode);

                    BlockTermStateAnalysis termStateAnalysis =
                            analyzeBlockTermState(termsEnum, terms, minState, field.name);
                    fieldAnalysis.invertedIndex.addTrackingByExtension(
                            LuceneFileExtension.DOC, termStateAnalysis.docDelta());
                    fieldAnalysis.invertedIndex.addTrackingByExtension(
                            LuceneFileExtension.POS, termStateAnalysis.posDelta());
                    fieldAnalysis.invertedIndex.addTrackingByExtension(
                            LuceneFileExtension.PAY, termStateAnalysis.payDelta());

                    Map.Entry<PostingsEnum, TermsAnalysis> termsAnalysis;

                    if (termStructuresAnalysisMode == TermStructureAnalysisMode.BLOCK_SKIPPING) {
                        if (!areIndexInputsPositioned) throw new IllegalStateException("Block counting analysis mode was requested, and more than 1 field provided BlockTermState, but not all.");
                        termsAnalysis = analyzeTermsByBlockSkipping(termsIndexInput, termsDictionaryInput);
                    } else {
                        termsAnalysis = analyzeTermsByPartialInstrumentation(termsEnum, terms, postings);
                        areIndexInputsPositioned = false;
                    }

                    fieldAnalysis.invertedIndex.addTrackingByExtension(
                            LuceneFileExtension.TIM, termsAnalysis.getValue().termsDictionarySize());
                    fieldAnalysis.invertedIndex.addTrackingByExtension(
                            LuceneFileExtension.TIP, termsAnalysis.getValue().termsIndexSize());
                    postings = termsAnalysis.getKey();
                }
            }
        }
    }
}
