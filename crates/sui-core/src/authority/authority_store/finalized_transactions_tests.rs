// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use super::*;

use crate::authority::test_authority_builder::TestAuthorityBuilder;

/// The deprecated digest-to-checkpoint mapping must become durable atomically with the
/// checkpoint's transaction outputs. Staging it into the outputs batch is exactly that:
/// the row is invisible until the batch is written, and present afterwards.
#[tokio::test]
async fn finalized_transactions_land_with_the_outputs_batch() {
    let state = TestAuthorityBuilder::new().build().await;
    let store = state.authority_store();
    let epoch = state.epoch_store_for_testing().epoch();
    let digests = [
        TransactionDigest::random(),
        TransactionDigest::random(),
        TransactionDigest::random(),
    ];
    let checkpoint = 7;

    // Any table in the perpetual DB can host the batch; the outputs batch is one of them.
    let mut batch = store.perpetual_tables.objects.batch();
    store
        .set_finalized_transactions_in_batch(&mut batch, &digests, epoch, checkpoint)
        .expect("stage the finalized-transaction mapping");

    for digest in &digests {
        assert_eq!(
            store
                .deprecated_get_transaction_checkpoint(digest)
                .expect("read the staged but unwritten mapping"),
            None,
            "a staged mapping must not be readable before the batch is written"
        );
    }

    batch.write().expect("write the outputs batch");

    for digest in &digests {
        assert_eq!(
            store
                .deprecated_get_transaction_checkpoint(digest)
                .expect("read the written mapping"),
            Some((epoch, checkpoint)),
            "the mapping must be durable once the outputs batch is written"
        );
    }
}
