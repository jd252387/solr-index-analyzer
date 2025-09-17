package org.commrogue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.apache.lucene.index.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.AuthorizationContext;
import org.apache.solr.security.PermissionNameProvider;
import org.commrogue.analysis.Analysis;
import org.commrogue.analysis.docvalues.DocValuesAnalysis;
import org.commrogue.analysis.docvalues.DocValuesAnalysisMode;
import org.commrogue.analysis.iindex.InvertedIndexAnalysis;
import org.commrogue.analysis.iindex.TermStructureAnalysisMode;
import org.commrogue.analysis.knn.KnnVectorsAnalysis;
import org.commrogue.analysis.knn.KnnVectorsAnalysisMode;
import org.commrogue.analysis.storedfields.StoredFieldsAnalysis;
import org.commrogue.analysis.storedfields.StoredFieldsAnalysisMode;
import org.commrogue.lucene.Utils;
import org.commrogue.results.IndexAnalysisResult;
import org.commrogue.tracking.DelegatingDirectoryIndexCommit;
import org.commrogue.tracking.TrackingReadBytesDirectory;

public class IndexAnalyzerRequestHandler extends RequestHandlerBase {
    // TODO - make core-specific
    @Override
    public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
        TermStructureAnalysisMode analysisMode = Optional.ofNullable(
                        req.getParams().get("termAnalysisMode"))
                .flatMap(targetMode -> Arrays.stream(TermStructureAnalysisMode.values())
                        .filter(mode -> mode.param.equals(targetMode))
                        .findFirst())
                .orElse(TermStructureAnalysisMode.BLOCK_SKIPPING);

        KnnVectorsAnalysisMode knnAnalysisMode = Optional.ofNullable(
                        req.getParams().get("vectorAnalysisMode"))
                .map(KnnVectorsAnalysisMode::fromParam)
                .orElse(KnnVectorsAnalysisMode.STRUCTURAL);

        DocValuesAnalysisMode docValuesAnalysisMode = Optional.ofNullable(
                        req.getParams().get("docValuesAnalysisMode"))
                .map(DocValuesAnalysisMode::fromParam)
                .orElse(DocValuesAnalysisMode.STRUCTURAL_WITH_FALLBACK);

        StoredFieldsAnalysisMode storedFieldsAnalysisMode = Optional.ofNullable(
                        req.getParams().get("storedFieldsAnalysisMode"))
                .map(StoredFieldsAnalysisMode::fromParam)
                .orElse(StoredFieldsAnalysisMode.STRUCTURAL_WITH_FALLBACK);

        final IndexCommit originalCommit = req.getSearcher().getIndexReader().getIndexCommit();
        final TrackingReadBytesDirectory trackingDirectory =
                new TrackingReadBytesDirectory(originalCommit.getDirectory());
        final IndexCommit trackingCommit = new DelegatingDirectoryIndexCommit(originalCommit) {
            @Override
            public Directory getDirectory() {
                return trackingDirectory;
            }
        };
        IndexAnalysisResult segmentMergedResult;

        // TODO - just open directory instead?
        try (DirectoryReader directoryReader = DirectoryReader.open(trackingCommit)) {
            // technically not needed since analysis will reset
            trackingDirectory.resetBytesRead();
            List<IndexAnalysisResult> results = new ArrayList<>();
            for (LeafReaderContext leafReaderContext : directoryReader.leaves()) {
                final IndexAnalysisResult indexAnalysisResult = new IndexAnalysisResult();
                final SegmentReader segmentReader = Utils.segmentReader(leafReaderContext.reader());
                TrackingReadBytesDirectory targetDirectory;
                SegmentInfo segmentInfo = segmentReader.getSegmentInfo().info;

                if (segmentInfo.getUseCompoundFile()) {
                    targetDirectory = new TrackingReadBytesDirectory(segmentInfo
                            .getCodec()
                            .compoundFormat()
                            .getCompoundReader(trackingDirectory, segmentInfo, IOContext.READONCE));
                } else {
                    targetDirectory = trackingDirectory;
                }

                List<Analysis> analysisList = new ArrayList<>();
                analysisList.add(new InvertedIndexAnalysis(
                        targetDirectory, segmentReader, indexAnalysisResult, false, analysisMode));
                analysisList.add(new DocValuesAnalysis(
                        targetDirectory, segmentReader, indexAnalysisResult, docValuesAnalysisMode));
                analysisList.add(
                        new KnnVectorsAnalysis(targetDirectory, segmentReader, indexAnalysisResult, knnAnalysisMode));
                analysisList.add(new StoredFieldsAnalysis(
                        targetDirectory, segmentReader, indexAnalysisResult, storedFieldsAnalysisMode));

                for (Analysis analysis : analysisList) analysis.analyze();

                results.add(indexAnalysisResult);
            }

            segmentMergedResult = IndexAnalysisResult.byMerging(results);
        }

        rsp.add("analysis", segmentMergedResult.toSimpleOrderedMap());
    }

    @Override
    public String getDescription() {
        return "Analyzes the core's index to determine sizes of Lucene data-structures, e.g. postings, points, term vectors, norms, DocValues, stored, and KNNs.";
    }

    @Override
    public PermissionNameProvider.Name getPermissionName(AuthorizationContext ctx) {
        return Name.CORE_READ_PERM;
    }
}
