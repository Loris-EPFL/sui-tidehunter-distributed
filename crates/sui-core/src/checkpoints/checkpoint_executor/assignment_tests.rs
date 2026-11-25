// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use super::*;

use std::cell::{Cell, RefCell};
use sui_types::error::SuiErrorKind;

/// A store read failure while initialising shared versions must reach the caller as an
/// `Err`. Before this, the executor called `expect` here and a storage error became a
/// panic in a spawned checkpoint task.
#[test]
fn shared_version_assignment_errors_are_returned() {
    let calls = Cell::new(0);
    let result = acquire_shared_versions(&[7u8, 8, 9], |batch| -> SuiResult<Vec<u8>> {
        calls.set(calls.get() + 1);
        assert_eq!(
            batch,
            [7, 8, 9],
            "one checkpoint initialisation must cover every first-use object"
        );
        Err(SuiErrorKind::Storage("shared version row is unreadable".to_string()).into())
    });

    assert_eq!(
        calls.get(),
        1,
        "the assignment store read must be attempted exactly once"
    );
    let error = result.expect_err("a storage failure must not be dropped");
    assert!(
        matches!(
            error.as_inner(),
            SuiErrorKind::Storage(message) if message.contains("unreadable")
        ),
        "the caller must receive the storage error, got {error:?}"
    );
}

/// The empty-checkpoint guard is what keeps a checkpoint with no unexecuted transaction
/// from issuing an empty store read.
#[test]
fn empty_checkpoint_issues_no_assignment_call() {
    let calls = Cell::new(0);
    let no_items: &[u8] = &[];
    let result = acquire_shared_versions(no_items, |_| {
        calls.set(calls.get() + 1);
        Ok(vec![0u8])
    });

    assert_eq!(
        calls.get(),
        0,
        "an empty checkpoint must not reach the assignment store"
    );
    assert!(
        result
            .expect("an empty checkpoint has no assignments")
            .is_empty(),
        "an empty checkpoint has no assignments"
    );
}

#[test]
fn assignments_are_concatenated_in_input_order() {
    let items = [1u8, 2, 3, 4];
    let assignments = acquire_shared_versions(&items, |batch| {
        Ok(batch.iter().map(|item| item * 10).collect())
    })
    .expect("assignments must follow input order");
    assert_eq!(
        assignments,
        [10, 20, 30, 40],
        "assignments must be returned in input order"
    );
}

/// A checkpoint that fits one bounded request must be initialised in exactly one call, so
/// the usual path keeps its single allocation instead of copying the manager's vector.
#[test]
fn checkpoint_that_fits_one_request_is_initialised_once() {
    let items: Vec<u16> = (0..SHARED_VERSION_BATCH_KEYS as u16).collect();
    let calls = Cell::new(0);
    let assignments = acquire_shared_versions(&items, |batch| {
        calls.set(calls.get() + 1);
        Ok(batch.to_vec())
    })
    .expect("a checkpoint that fits one request must initialise in one call");

    assert_eq!(
        calls.get(),
        1,
        "SHARED_VERSION_BATCH_KEYS objects fit one bounded request"
    );
    assert_eq!(assignments, items, "the assignments must keep input order");
}

/// A large checkpoint must not turn its first-use shared objects into one very wide store
/// request. Chunking bounds each request and must not reorder the assignments.
#[test]
fn large_checkpoints_initialise_shared_versions_in_bounded_chunks() {
    let items: Vec<u16> = (0..600).collect();
    let chunk_sizes = RefCell::new(Vec::new());
    let assignments = acquire_shared_versions(&items, |batch| {
        chunk_sizes.borrow_mut().push(batch.len());
        Ok(batch.to_vec())
    })
    .expect("chunked assignments must succeed");

    assert_eq!(
        *chunk_sizes.borrow(),
        [SHARED_VERSION_BATCH_KEYS, SHARED_VERSION_BATCH_KEYS, 88],
        "600 first-use objects must be initialised as 256 + 256 + 88"
    );
    assert_eq!(
        assignments, items,
        "chunking must preserve the input order of the assignments"
    );
}
