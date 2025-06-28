package org.commrogue.tracking;

import lombok.RequiredArgsConstructor;
import lombok.experimental.Delegate;
import org.apache.lucene.index.IndexCommit;

@RequiredArgsConstructor
public class DelegatingDirectoryIndexCommit extends IndexCommit {
    @Delegate
    private final IndexCommit delegateIndexCommit;
}
