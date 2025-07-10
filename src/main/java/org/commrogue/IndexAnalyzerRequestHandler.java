package org.commrogue;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.store.Directory;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.AuthorizationContext;
import org.apache.solr.security.PermissionNameProvider;
import org.commrogue.analysis.iindex.InvertedIndexAnalysis;
import org.commrogue.analysis.iindex.TermStructureAnalysisMode;
import org.commrogue.lucene.Utils;
import org.commrogue.results.IndexAnalysisResult;
import org.commrogue.tracking.DelegatingDirectoryIndexCommit;
import org.commrogue.tracking.TrackingReadBytesDirectory;

public class IndexAnalyzerRequestHandler extends RequestHandlerBase {

    // TODO - make core-specific
    @Override
    public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
        final IndexCommit originalCommit = req.getSearcher().getIndexReader().getIndexCommit();
        final TrackingReadBytesDirectory trackingDirectory =
                new TrackingReadBytesDirectory(originalCommit.getDirectory());
        final IndexCommit trackingCommit = new DelegatingDirectoryIndexCommit(originalCommit) {
            @Override
            public Directory getDirectory() {
                return trackingDirectory;
            }
        };
        final IndexAnalysisResult indexAnalysisResult = new IndexAnalysisResult();
        // TODO - just open directory instead?
        try (DirectoryReader directoryReader = DirectoryReader.open(trackingCommit)) {
            trackingDirectory.resetBytesRead();
            for (LeafReaderContext leafReaderContext : directoryReader.leaves()) {
                final SegmentReader segmentReader = Utils.segmentReader(leafReaderContext.reader());

                InvertedIndexAnalysis postingsAnalysis =
                        new InvertedIndexAnalysis(trackingDirectory, segmentReader, indexAnalysisResult, false, TermStructureAnalysisMode.BLOCK_SKIPPING);
                postingsAnalysis.analyze();
                System.out.println("done");
            }
        }
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
