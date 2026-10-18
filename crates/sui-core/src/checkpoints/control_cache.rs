// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use super::{CheckpointWatermark, VerifiedCheckpoint};
use lru::LruCache;
use std::num::NonZeroUsize;
use sui_types::digests::CheckpointDigest;
use sui_types::messages_checkpoint::CheckpointSequenceNumber;
use typed_store::TypedStoreError;

pub(super) type StoredWatermark = (CheckpointSequenceNumber, CheckpointDigest);

const SUMMARY_CACHE_ENTRIES: usize = 8;
// Canonical retained row bytes; sequence and epoch fields keep each valid
// summary positive-sized. This does not measure allocator overhead or RSS.
const SUMMARY_CACHE_BYTES: usize = 1024 * 1024;
const SEQUENCE_READ_CACHE_BYTES: usize = SUMMARY_CACHE_BYTES;
const REFILL_TOKEN_ENTRIES: usize = 512;

/// Demand-read summaries have their own lifetime from producer-oriented cache warming.
pub(super) struct SequenceReadCache {
    summaries: LruCache<CheckpointSequenceNumber, CachedSequenceSummary>,
    summary_bytes: usize,
    refill_tokens: LruCache<CheckpointSequenceNumber, std::sync::Arc<()>>,
    active_mutation: Option<CheckpointSequenceNumber>,
}

struct CachedSequenceSummary {
    checkpoint: VerifiedCheckpoint,
    bytes: usize,
}

impl Default for SequenceReadCache {
    fn default() -> Self {
        Self {
            summaries: LruCache::unbounded(),
            summary_bytes: 0,
            refill_tokens: LruCache::new(NonZeroUsize::new(REFILL_TOKEN_ENTRIES).unwrap()),
            active_mutation: None,
        }
    }
}

impl SequenceReadCache {
    pub(super) fn get(
        &mut self,
        sequence_number: CheckpointSequenceNumber,
    ) -> Option<VerifiedCheckpoint> {
        self.summaries
            .get(&sequence_number)
            .map(|summary| summary.checkpoint.clone())
    }

    pub(super) fn begin_refill(
        &mut self,
        sequence_number: CheckpointSequenceNumber,
    ) -> Option<std::sync::Arc<()>> {
        if self.active_mutation == Some(sequence_number) {
            return None;
        }
        if let Some(token) = self.refill_tokens.get(&sequence_number) {
            return Some(token.clone());
        }
        let token = std::sync::Arc::new(());
        self.refill_tokens.put(sequence_number, token.clone());
        Some(token)
    }

    pub(super) fn canonical_bytes(checkpoint: &VerifiedCheckpoint) -> Option<usize> {
        let Ok(bytes) = bcs::serialized_size(checkpoint.serializable_ref()) else {
            return None;
        };
        (bytes <= SEQUENCE_READ_CACHE_BYTES).then_some(bytes)
    }

    pub(super) fn cache_refill(
        &mut self,
        sequence_number: CheckpointSequenceNumber,
        token: &std::sync::Arc<()>,
        checkpoint: VerifiedCheckpoint,
        bytes: usize,
    ) {
        if bytes > SEQUENCE_READ_CACHE_BYTES {
            return;
        }
        if self.active_mutation == Some(sequence_number)
            || !self
                .refill_tokens
                .peek(&sequence_number)
                .is_some_and(|current| std::sync::Arc::ptr_eq(current, token))
        {
            return;
        }
        if let Some(previous) = self.summaries.pop(&sequence_number) {
            self.summary_bytes -= previous.bytes;
        }
        let _ = self
            .summaries
            .put(sequence_number, CachedSequenceSummary { checkpoint, bytes });
        self.summary_bytes += bytes;
        for _ in 0..self.summaries.len() {
            if self.summary_bytes <= SEQUENCE_READ_CACHE_BYTES {
                break;
            }
            let Some((_, evicted)) = self.summaries.pop_lru() else {
                break;
            };
            self.summary_bytes -= evicted.bytes;
        }
    }

    pub(super) fn invalidate_sequence(&mut self, sequence_number: CheckpointSequenceNumber) {
        if let Some(summary) = self.summaries.pop(&sequence_number) {
            self.summary_bytes -= summary.bytes;
        }
        self.refill_tokens.pop(&sequence_number);
    }

    pub(super) fn begin_mutation(&mut self, sequence_number: CheckpointSequenceNumber) {
        self.invalidate_sequence(sequence_number);
        self.active_mutation = Some(sequence_number);
    }

    pub(super) fn finish_mutation(&mut self, sequence_number: CheckpointSequenceNumber) {
        self.invalidate_sequence(sequence_number);
        if self.active_mutation == Some(sequence_number) {
            self.active_mutation = None;
        }
    }

    pub(super) fn clear(&mut self) {
        self.summaries.clear();
        self.summary_bytes = 0;
        self.refill_tokens.clear();
        self.active_mutation = None;
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.summaries.len()
    }

    #[cfg(test)]
    pub(super) fn bytes(&self) -> usize {
        self.summary_bytes
    }
}

/// The outer option distinguishes an unloaded entry from a durable absent entry.
pub(super) struct ControlCache {
    watermarks: [Option<Option<StoredWatermark>>; 4],
    summaries: LruCache<CheckpointDigest, CachedSummary>,
    summary_bytes: usize,
}

// Producer-oriented summaries retain digest-index authority; sequence demand is separate.
struct CachedSummary {
    checkpoint: VerifiedCheckpoint,
    bytes: usize,
}

impl Default for ControlCache {
    fn default() -> Self {
        Self {
            watermarks: [None; 4],
            summaries: LruCache::new(NonZeroUsize::new(SUMMARY_CACHE_ENTRIES).unwrap()),
            summary_bytes: 0,
        }
    }
}

impl ControlCache {
    fn index(watermark: CheckpointWatermark) -> usize {
        match watermark {
            CheckpointWatermark::HighestVerified => 0,
            CheckpointWatermark::HighestSynced => 1,
            CheckpointWatermark::HighestExecuted => 2,
            CheckpointWatermark::HighestPruned => 3,
            CheckpointWatermark::CheckpointForkDetected => {
                unreachable!("retired fork marker is not a checkpoint watermark")
            }
        }
    }

    pub(super) fn read(
        &mut self,
        watermark: CheckpointWatermark,
        load: impl FnOnce() -> Result<Option<StoredWatermark>, TypedStoreError>,
    ) -> Result<Option<StoredWatermark>, TypedStoreError> {
        let entry = &mut self.watermarks[Self::index(watermark)];
        if let Some(value) = *entry {
            return Ok(value);
        }
        let value = load()?;
        *entry = Some(value);
        Ok(value)
    }

    pub(super) fn publish(
        &mut self,
        watermark: CheckpointWatermark,
        value: Option<StoredWatermark>,
    ) {
        self.watermarks[Self::index(watermark)] = Some(value);
    }

    pub(super) fn persist(
        &mut self,
        watermark: CheckpointWatermark,
        value: Option<StoredWatermark>,
        write: impl FnOnce() -> Result<(), TypedStoreError>,
    ) -> Result<(), TypedStoreError> {
        // A failed durable write can have an unknown outcome. The next permitted read
        // must consult storage, rather than trusting either the old or intended value.
        self.watermarks[Self::index(watermark)] = None;
        if let Err(error) = write() {
            self.clear_summaries();
            return Err(error);
        }
        self.publish(watermark, value);
        Ok(())
    }

    pub(super) fn summary(&mut self, digest: &CheckpointDigest) -> Option<VerifiedCheckpoint> {
        self.summaries
            .get(digest)
            .map(|summary| summary.checkpoint.clone())
    }

    pub(super) fn remove_summaries_older_than_epoch(&mut self, epoch: u64) {
        for _ in 0..SUMMARY_CACHE_ENTRIES {
            let digest = {
                self.summaries.iter().find_map(|(digest, summary)| {
                    (summary.checkpoint.epoch() < epoch).then_some(*digest)
                })
            };
            let Some(digest) = digest else {
                break;
            };
            self.remove_summary(&digest);
        }
    }

    #[cfg(test)]
    pub(super) fn contains_summary(&self, digest: &CheckpointDigest) -> bool {
        self.summaries.peek(digest).is_some()
    }

    #[cfg(test)]
    pub(super) fn summary_count(&self) -> usize {
        self.summaries.len()
    }

    #[cfg(test)]
    pub(super) fn summary_bytes(&self) -> usize {
        self.summary_bytes
    }

    pub(super) fn cache_summary(&mut self, checkpoint: &VerifiedCheckpoint) {
        let Ok(bytes) = bcs::serialized_size(checkpoint.serializable_ref()) else {
            return;
        };
        if bytes > SUMMARY_CACHE_BYTES {
            return;
        }
        let digest = *checkpoint.digest();
        if let Some(previous) = self.summaries.pop(&digest) {
            self.summary_bytes -= previous.bytes;
        }
        if let Some((_, evicted)) = self.summaries.push(
            digest,
            CachedSummary {
                checkpoint: checkpoint.clone(),
                bytes,
            },
        ) {
            self.summary_bytes -= evicted.bytes;
        }
        self.summary_bytes += bytes;
        for _ in 0..SUMMARY_CACHE_ENTRIES {
            if self.summary_bytes <= SUMMARY_CACHE_BYTES {
                break;
            }
            if let Some((_, evicted)) = self.summaries.pop_lru() {
                self.summary_bytes -= evicted.bytes;
            } else {
                break;
            }
        }
    }

    pub(super) fn remove_summary(&mut self, digest: &CheckpointDigest) {
        if let Some(summary) = self.summaries.pop(digest) {
            self.summary_bytes -= summary.bytes;
        }
    }

    pub(super) fn clear_summaries(&mut self) {
        self.summaries.clear();
        self.summary_bytes = 0;
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    #[tokio::test]
    async fn absent_watermark_is_loaded_once_and_reset_is_not_monotonic() {
        let mut cache = ControlCache::default();
        let reads = Cell::new(0);
        for _ in 0..2 {
            assert_eq!(
                cache
                    .read(CheckpointWatermark::HighestExecuted, || {
                        reads.set(reads.get() + 1);
                        Ok(None)
                    })
                    .unwrap(),
                None
            );
        }
        assert_eq!(reads.get(), 1);
        let digest = CheckpointDigest::random();
        cache
            .persist(
                CheckpointWatermark::HighestExecuted,
                Some((10, digest)),
                || Ok(()),
            )
            .unwrap();
        cache
            .persist(
                CheckpointWatermark::HighestExecuted,
                Some((3, digest)),
                || Ok(()),
            )
            .unwrap();
        assert_eq!(
            cache
                .read(CheckpointWatermark::HighestExecuted, || panic!(
                    "cache miss"
                ))
                .unwrap(),
            Some((3, digest))
        );
        cache
            .persist(CheckpointWatermark::HighestExecuted, None, || Ok(()))
            .unwrap();
        assert_eq!(
            cache
                .read(CheckpointWatermark::HighestExecuted, || panic!(
                    "cache miss"
                ))
                .unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn failed_write_reloads_the_actual_durable_outcome() {
        let mut cache = ControlCache::default();
        let digest = CheckpointDigest::random();
        cache.publish(CheckpointWatermark::HighestSynced, Some((1, digest)));
        assert!(
            cache
                .persist(
                    CheckpointWatermark::HighestSynced,
                    Some((2, digest)),
                    || {
                        Err(TypedStoreError::SerializationError(
                            "injected durable failure".to_owned(),
                        ))
                    }
                )
                .is_err()
        );
        let reads = Cell::new(0);
        assert_eq!(
            cache
                .read(CheckpointWatermark::HighestSynced, || {
                    reads.set(reads.get() + 1);
                    Ok(Some((2, digest)))
                })
                .unwrap(),
            Some((2, digest))
        );
        assert_eq!(reads.get(), 1);
    }
}
