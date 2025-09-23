package org.commrogue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.apache.lucene.index.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.solr.common.SolrException;
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
import org.commrogue.analysis.points.PointValuesAnalysis;
import org.commrogue.analysis.points.PointValuesAnalysisMode;
import org.commrogue.analysis.storedfields.StoredFieldsAnalysis;
import org.commrogue.analysis.storedfields.StoredFieldsAnalysisMode;
import org.commrogue.analysis.termvectors.TermVectorsAnalysis;
import org.commrogue.analysis.termvectors.TermVectorsAnalysisMode;
import org.commrogue.lucene.Utils;
import org.commrogue.results.IndexAnalysisResult;
import org.commrogue.tracking.DelegatingDirectoryIndexCommit;
import org.commrogue.tracking.TrackingReadBytesDirectory;

public class IndexAnalyzerRequestHandler extends RequestHandlerBase {
    // TODO - make core-specific
    @Override
    public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
        TermStructureAnalysisMode termStructureAnalysisMode = Optional.ofNullable(
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

        PointValuesAnalysisMode pointValuesAnalysisMode = Optional.ofNullable(
                        req.getParams().get("pointValuesAnalysisMode"))
                .map(PointValuesAnalysisMode::fromParam)
                .orElse(PointValuesAnalysisMode.STRUCTURAL_WITH_FALLBACK);

        StoredFieldsAnalysisMode storedFieldsAnalysisMode = Optional.ofNullable(
                        req.getParams().get("storedFieldsAnalysisMode"))
                .map(StoredFieldsAnalysisMode::fromParam)
                .orElse(StoredFieldsAnalysisMode.STRUCTURAL_WITH_FALLBACK);

        TermVectorsAnalysisMode termVectorsAnalysisMode = Optional.ofNullable(
                        req.getParams().get("termVectorsAnalysisMode"))
                .map(TermVectorsAnalysisMode::fromParam)
                .orElse(TermVectorsAnalysisMode.STRUCTURAL_WITH_FALLBACK);

        List<AnalysisType> requestedAnalysisTypes =
                resolveRequestedAnalyses(req.getParams().get(ANALYSIS_PARAM));

        if (requestedAnalysisTypes.isEmpty()) {
            rsp.add("analysis", new IndexAnalysisResult().toSimpleOrderedMap());
            return;
        }

        AnalysisParams analysisParams = new AnalysisParams(
                termStructureAnalysisMode,
                docValuesAnalysisMode,
                pointValuesAnalysisMode,
                termVectorsAnalysisMode,
                knnAnalysisMode,
                storedFieldsAnalysisMode);

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

                for (AnalysisType analysisType : requestedAnalysisTypes) {
                    Analysis analysis =
                            analysisType.create(targetDirectory, segmentReader, indexAnalysisResult, analysisParams);
                    analysis.analyze();
                }

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

    private static final String ANALYSIS_PARAM = "analysis";

    private static List<AnalysisType> resolveRequestedAnalyses(String rawParam) {
        if (rawParam == null || rawParam.isBlank()) {
            return Collections.emptyList();
        }

        if (rawParam.trim().equalsIgnoreCase("all")) {
            return Arrays.asList(AnalysisType.values());
        }

        Set<AnalysisType> requestedTypes = new LinkedHashSet<>();
        for (String candidate : rawParam.split(",")) {
            String trimmed = candidate.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            if (trimmed.equalsIgnoreCase("all")) {
                return Arrays.asList(AnalysisType.values());
            }
            AnalysisType type = AnalysisType.fromParam(trimmed)
                    .orElseThrow(() -> new SolrException(
                            SolrException.ErrorCode.BAD_REQUEST, "Unknown analysis requested: " + trimmed));
            requestedTypes.add(type);
        }

        return new ArrayList<>(requestedTypes);
    }

    private static class AnalysisParams {
        final TermStructureAnalysisMode termStructureAnalysisMode;
        final DocValuesAnalysisMode docValuesAnalysisMode;
        final PointValuesAnalysisMode pointValuesAnalysisMode;
        final TermVectorsAnalysisMode termVectorsAnalysisMode;
        final KnnVectorsAnalysisMode knnVectorsAnalysisMode;
        final StoredFieldsAnalysisMode storedFieldsAnalysisMode;

        AnalysisParams(
                TermStructureAnalysisMode termStructureAnalysisMode,
                DocValuesAnalysisMode docValuesAnalysisMode,
                PointValuesAnalysisMode pointValuesAnalysisMode,
                TermVectorsAnalysisMode termVectorsAnalysisMode,
                KnnVectorsAnalysisMode knnVectorsAnalysisMode,
                StoredFieldsAnalysisMode storedFieldsAnalysisMode) {
            this.termStructureAnalysisMode = termStructureAnalysisMode;
            this.docValuesAnalysisMode = docValuesAnalysisMode;
            this.pointValuesAnalysisMode = pointValuesAnalysisMode;
            this.termVectorsAnalysisMode = termVectorsAnalysisMode;
            this.knnVectorsAnalysisMode = knnVectorsAnalysisMode;
            this.storedFieldsAnalysisMode = storedFieldsAnalysisMode;
        }
    }

    private enum AnalysisType {
        INVERTED_INDEX("invertedIndex") {
            @Override
            Analysis create(
                    TrackingReadBytesDirectory directory,
                    SegmentReader segmentReader,
                    IndexAnalysisResult indexAnalysisResult,
                    AnalysisParams params) {
                return new InvertedIndexAnalysis(
                        directory, segmentReader, indexAnalysisResult, false, params.termStructureAnalysisMode);
            }
        },
        DOC_VALUES("docValues") {
            @Override
            Analysis create(
                    TrackingReadBytesDirectory directory,
                    SegmentReader segmentReader,
                    IndexAnalysisResult indexAnalysisResult,
                    AnalysisParams params) {
                return new DocValuesAnalysis(
                        directory, segmentReader, indexAnalysisResult, params.docValuesAnalysisMode);
            }
        },
        POINT_VALUES("pointValues") {
            @Override
            Analysis create(
                    TrackingReadBytesDirectory directory,
                    SegmentReader segmentReader,
                    IndexAnalysisResult indexAnalysisResult,
                    AnalysisParams params) {
                return new PointValuesAnalysis(
                        directory, segmentReader, indexAnalysisResult, params.pointValuesAnalysisMode);
            }
        },
        TERM_VECTORS("termVectors") {
            @Override
            Analysis create(
                    TrackingReadBytesDirectory directory,
                    SegmentReader segmentReader,
                    IndexAnalysisResult indexAnalysisResult,
                    AnalysisParams params) {
                return new TermVectorsAnalysis(
                        directory, segmentReader, indexAnalysisResult, params.termVectorsAnalysisMode);
            }
        },
        KNN_VECTORS("knnVectors") {
            @Override
            Analysis create(
                    TrackingReadBytesDirectory directory,
                    SegmentReader segmentReader,
                    IndexAnalysisResult indexAnalysisResult,
                    AnalysisParams params) {
                return new KnnVectorsAnalysis(
                        directory, segmentReader, indexAnalysisResult, params.knnVectorsAnalysisMode);
            }
        },
        STORED_FIELDS("storedFields") {
            @Override
            Analysis create(
                    TrackingReadBytesDirectory directory,
                    SegmentReader segmentReader,
                    IndexAnalysisResult indexAnalysisResult,
                    AnalysisParams params) {
                return new StoredFieldsAnalysis(
                        directory, segmentReader, indexAnalysisResult, params.storedFieldsAnalysisMode);
            }
        };

        private final String param;

        AnalysisType(String param) {
            this.param = param;
        }

        abstract Analysis create(
                TrackingReadBytesDirectory directory,
                SegmentReader segmentReader,
                IndexAnalysisResult indexAnalysisResult,
                AnalysisParams params);

        static Optional<AnalysisType> fromParam(String name) {
            for (AnalysisType type : values()) {
                if (type.param.equalsIgnoreCase(name)) {
                    return Optional.of(type);
                }
            }
            return Optional.empty();
        }
    }
}
