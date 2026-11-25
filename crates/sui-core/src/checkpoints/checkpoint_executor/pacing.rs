// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Paced checkpoint admission for the campaign's paced cell.
//!
//! A catch-up replay admits as many checkpoints as execution allows, which is not the
//! arrival pattern a validator sees. Setting `SUI_CHECKPOINT_EXECUTION_PACING_MS` turns
//! admission into a paced stream: the k-th checkpoint admitted by one epoch run is
//! admitted no earlier than `start + k * interval`, where `k` counts from the first
//! checkpoint of the run. The wait happens at admission, before the checkpoint touches
//! the pipeline or the store, and it is absent from the run when the variable is unset.

use std::time::Duration;

use sui_types::error::{SuiError, SuiErrorKind, SuiResult};

/// Milliseconds per checkpoint. Absent means an unpaced run; a value that cannot be read
/// as a whole number of milliseconds refuses the run, so a paced arm can never be
/// silently reported as unpaced.
pub(super) const PACING_ENV: &str = "SUI_CHECKPOINT_EXECUTION_PACING_MS";

/// One day per checkpoint is far beyond any campaign arm and keeps every deadline
/// representable: `interval * index` cannot overflow and `start + deadline` stays inside
/// a monotonic clock's range.
const MAX_PACING_MS: u64 = 24 * 60 * 60 * 1000;

/// The admission schedule of one epoch run.
#[derive(Clone, Copy, Debug)]
pub(super) struct CheckpointPacing {
    millis: u64,
    start: tokio::time::Instant,
}

impl CheckpointPacing {
    /// Reads [`PACING_ENV`] and anchors the schedule at `start`, the instant the run
    /// began. Returns `None` when the variable is unset, in which case the executor
    /// keeps its existing admission path with no wait at all.
    pub(super) fn from_env(start: tokio::time::Instant) -> SuiResult<Option<Self>> {
        match std::env::var_os(PACING_ENV) {
            None => Ok(None),
            Some(raw) => match raw.to_str() {
                Some(raw) => Self::parse(Some(raw), start),
                None => Err(invalid(raw.to_string_lossy().as_ref())),
            },
        }
    }

    /// Parses a raw setting; `None` means unpaced. Split from [`Self::from_env`] so tests
    /// can exercise every branch without mutating the process environment.
    pub(super) fn parse(raw: Option<&str>, start: tokio::time::Instant) -> SuiResult<Option<Self>> {
        let Some(raw) = raw else {
            return Ok(None);
        };
        let millis: u64 = raw.trim().parse().map_err(|_| invalid(raw))?;
        if millis > MAX_PACING_MS {
            return Err(invalid(raw));
        }
        Ok(Some(Self { millis, start }))
    }

    /// The configured interval in milliseconds.
    pub(super) fn millis(&self) -> u64 {
        self.millis
    }

    /// Waits until the `index`-th checkpoint of this run may be admitted. `index` 0 is the
    /// first checkpoint and is admitted immediately.
    pub(super) async fn wait_for_admission(&self, index: u64) {
        tokio::time::sleep_until(self.deadline(index)).await;
    }

    fn deadline(&self, index: u64) -> tokio::time::Instant {
        // Clamping the index bounds the deadline even if a run somehow outlives a
        // 32-bit checkpoint count; every campaign arm is far below it.
        let index = index.min(u32::MAX as u64);
        self.start + Duration::from_millis(self.millis.saturating_mul(index))
    }
}

fn invalid(raw: &str) -> SuiError {
    SuiErrorKind::Unknown(format!(
        "{PACING_ENV}={raw:?} is not a whole number of milliseconds per checkpoint"
    ))
    .into()
}
