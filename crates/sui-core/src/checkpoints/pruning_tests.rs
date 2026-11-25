// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use super::*;

fn highest_pruned(store: &CheckpointStore) -> (CheckpointSequenceNumber, CheckpointDigest) {
    store
        .tables
        .watermarks
        .get(&CheckpointWatermark::HighestPruned)
        .expect("read the highest-pruned watermark")
        .expect("pruning must publish the highest-pruned watermark")
}

/// The pruned watermark's digest is informational, but two stores that prune the
/// same checkpoint must publish the same watermark. A random digest fails here.
#[tokio::test]
async fn prune_checkpoint_data_publishes_the_last_pruned_digest() {
    let first = CheckpointDigest::new([1; 32]);
    let last = CheckpointDigest::new([2; 32]);

    let prune = || {
        let store = CheckpointStore::new_for_tests();
        store
            .prune_checkpoint_data(7, vec![first, last], vec![])
            .expect("prune checkpoint data");
        highest_pruned(&store)
    };

    let watermark = prune();
    assert_eq!(
        watermark,
        (7, last),
        "the pruned watermark must carry the last pruned digest"
    );
    assert_eq!(
        watermark,
        prune(),
        "the same pruning outcome must publish the same watermark in a new store"
    );
}

#[tokio::test]
async fn prune_checkpoint_data_without_digests_publishes_the_default_digest() {
    let store = CheckpointStore::new_for_tests();
    store
        .prune_checkpoint_data(9, vec![], vec![])
        .expect("prune checkpoint data");
    assert_eq!(
        highest_pruned(&store),
        (9, CheckpointDigest::default()),
        "a batch that pruned no digest must publish the default digest"
    );
}
