// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Tests for the `TH_*` rows of [`super::thdb_config`]. They exercise the pure parsers,
//! because the process environment cannot be mutated safely from a parallel test run.

use super::{
    check_index_blob_cache_pair, parse_index_blob_cache_bytes, parse_index_blob_cache_min_bytes,
};

/// Unset must keep upstream Sui's behaviour: `None` disables the engine's index blob
/// cache, so an unpaced, un-tuned arm measures exactly what it measured before the row.
#[test]
fn absent_index_blob_cache_row_keeps_upstream_default() {
    assert_eq!(
        parse_index_blob_cache_bytes(None),
        None,
        "an absent TH_INDEX_BLOB_CACHE_BYTES must leave the cache disabled"
    );
}

#[test]
fn index_blob_cache_row_parses_a_byte_count() {
    assert_eq!(
        parse_index_blob_cache_bytes(Some("67108864")),
        Some(64 * 1024 * 1024),
        "TH_INDEX_BLOB_CACHE_BYTES must parse as a byte count"
    );
    assert_eq!(
        parse_index_blob_cache_bytes(Some("1")),
        Some(1),
        "any non-zero byte count is a valid budget"
    );
}

/// Zero is not "off": the engine's `validate` rejects it, so the row refuses it with the
/// same instruction rather than failing later at open.
#[test]
#[should_panic(expected = "TH_INDEX_BLOB_CACHE_BYTES must be non-zero")]
fn zero_index_blob_cache_row_is_refused() {
    parse_index_blob_cache_bytes(Some("0"));
}

#[test]
#[should_panic(expected = "Failed to parse TH_INDEX_BLOB_CACHE_BYTES")]
fn non_numeric_index_blob_cache_row_is_refused() {
    parse_index_blob_cache_bytes(Some("64MiB"));
}

/// Unset must keep the engine's 64 MiB admission floor, which is what the M4 cell
/// overrides; leaving it unset must change nothing.
#[test]
fn absent_index_blob_cache_floor_keeps_engine_default() {
    assert_eq!(
        parse_index_blob_cache_min_bytes(None),
        None,
        "an absent TH_INDEX_BLOB_CACHE_MIN_BYTES must leave the engine's 64 MiB floor"
    );
}

/// The M4 cell is a 51 MB replay, so its floor must be settable below the blob sizes it
/// produces; zero is the engine's "admit every blob that fits the budget" value.
#[test]
fn index_blob_cache_floor_parses_a_byte_count_including_zero() {
    assert_eq!(
        parse_index_blob_cache_min_bytes(Some("0")),
        Some(0),
        "zero is the engine's admit-everything floor and must be accepted"
    );
    assert_eq!(
        parse_index_blob_cache_min_bytes(Some("1048576")),
        Some(1024 * 1024),
        "TH_INDEX_BLOB_CACHE_MIN_BYTES must parse as a byte count"
    );
}

#[test]
#[should_panic(expected = "Failed to parse TH_INDEX_BLOB_CACHE_MIN_BYTES")]
fn non_numeric_index_blob_cache_floor_is_refused() {
    parse_index_blob_cache_min_bytes(Some("64MiB"));
}

/// The engine rejects a floor above the budget because every blob that qualified would be
/// rejected as oversized; the rows refuse the same pair, naming both.
#[test]
#[should_panic(expected = "TH_INDEX_BLOB_CACHE_MIN_BYTES")]
fn floor_above_budget_is_refused() {
    check_index_blob_cache_pair(Some(1024), Some(2048));
}

#[test]
fn floor_at_or_below_budget_is_accepted() {
    // The engine accepts a floor equal to the budget, and no pair check applies while the
    // budget is unset (the cache is disabled, so the floor is unused).
    check_index_blob_cache_pair(Some(2048), Some(2048));
    check_index_blob_cache_pair(Some(2048), Some(0));
    check_index_blob_cache_pair(None, Some(2048));
}
