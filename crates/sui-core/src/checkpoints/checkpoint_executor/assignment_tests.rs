// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use super::*;

use std::cell::Cell;
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
