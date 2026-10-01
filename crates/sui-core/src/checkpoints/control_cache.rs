// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use super::{CheckpointWatermark, VerifiedCheckpoint};
use lru::LruCache;
use std::num::NonZeroUsize;
use sui_types::digests::CheckpointDigest;
use sui_types::messages_checkpoint::CheckpointSequenceNumber;
use typed_store::TypedStoreError;

pub(super) type StoredWatermark = (CheckpointSequenceNumber, CheckpointDigest);

/// The outer option distinguishes an unloaded entry from a durable absent entry.
pub(super) struct ControlCache {
    watermarks: [Option<Option<StoredWatermark>>; 4],
    pub(super) summaries: LruCache<CheckpointDigest, VerifiedCheckpoint>,
}

impl Default for ControlCache {
    fn default() -> Self {
        Self {
            watermarks: [None; 4],
            summaries: LruCache::new(NonZeroUsize::new(8).unwrap()),
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
            self.summaries.clear();
            return Err(error);
        }
        self.publish(watermark, value);
        Ok(())
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
