// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Tests for the `TH_*` rows of [`super::thdb_config`]. They exercise the pure parsers,
//! because the process environment cannot be mutated safely from a parallel test run.

use super::parse_index_blob_cache_bytes;

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
