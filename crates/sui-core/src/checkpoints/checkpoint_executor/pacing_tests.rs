// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use super::pacing::{CheckpointPacing, PACING_ENV};

use std::time::Duration;

/// The schedule is absolute from the run's start, so the k-th admitted checkpoint waits
/// exactly `k * interval`. The paused clock advances only when every task is idle, so this
/// test measures the schedule without any real sleep.
#[tokio::test(start_paused = true)]
async fn admission_waits_follow_the_configured_interval() {
    let start = tokio::time::Instant::now();
    let pacing = CheckpointPacing::parse(Some("245"), start)
        .expect("a whole number of milliseconds must parse")
        .expect("a present setting must pace the run");

    pace_and_assert(&pacing, start, 0, 0).await;
    pace_and_assert(&pacing, start, 1, 245).await;
    pace_and_assert(&pacing, start, 2, 490).await;
    pace_and_assert(&pacing, start, 3, 735).await;
}

async fn pace_and_assert(
    pacing: &CheckpointPacing,
    start: tokio::time::Instant,
    index: u64,
    expected_ms: u64,
) {
    pacing.wait_for_admission(index).await;
    assert_eq!(
        tokio::time::Instant::now(),
        start + Duration::from_millis(expected_ms),
        "checkpoint {index} must be admitted at start + {expected_ms} ms"
    );
}

/// The first checkpoint of a paced run is not delayed: pacing exists to space arrivals,
/// not to add a fixed startup cost.
#[tokio::test(start_paused = true)]
async fn first_checkpoint_is_admitted_immediately() {
    let start = tokio::time::Instant::now();
    let pacing = CheckpointPacing::parse(Some("245"), start)
        .expect("a whole number of milliseconds must parse")
        .expect("a present setting must pace the run");

    pacing.wait_for_admission(0).await;
    assert_eq!(
        tokio::time::Instant::now(),
        start,
        "the first checkpoint must be admitted without a wait"
    );
}

/// Unset must keep the executor's existing admission path: `None` means the executor never
/// calls `wait_for_admission` and never touches the clock.
#[tokio::test(start_paused = true)]
async fn absent_setting_leaves_the_run_unpaced() {
    let start = tokio::time::Instant::now();
    assert!(
        CheckpointPacing::parse(None, start)
            .expect("an absent setting cannot fail to parse")
            .is_none(),
        "an absent setting must return no schedule"
    );
    assert_eq!(
        tokio::time::Instant::now(),
        start,
        "parsing must not move the clock"
    );
}

#[tokio::test(start_paused = true)]
async fn zero_milliseconds_paces_without_delay() {
    let start = tokio::time::Instant::now();
    let pacing = CheckpointPacing::parse(Some("0"), start)
        .expect("zero is a whole number of milliseconds")
        .expect("a present setting must pace the run");
    assert_eq!(pacing.millis(), 0, "the configured value must be kept");
    pace_and_assert(&pacing, start, 5, 0).await;
}

/// A value that is not a whole number of milliseconds refuses the run and names the
/// variable, so a paced arm can never be reported as unpaced by accident.
#[tokio::test(start_paused = true)]
async fn invalid_settings_are_refused() {
    let start = tokio::time::Instant::now();
    for raw in [
        "",
        "abc",
        "-5",
        "1.5",
        "245ms",
        "1e3",
        "99999999999999999999",
    ] {
        let error = CheckpointPacing::parse(Some(raw), start)
            .expect_err("an invalid setting must refuse the run");
        assert!(
            error.to_string().contains(PACING_ENV),
            "the refusal for {raw:?} must name {PACING_ENV}, got {error}"
        );
    }
}

/// Whitespace around the number is tolerated; the stored value is the number itself.
#[tokio::test(start_paused = true)]
async fn surrounding_whitespace_is_tolerated() {
    let start = tokio::time::Instant::now();
    let pacing = CheckpointPacing::parse(Some(" 245 "), start)
        .expect("surrounding whitespace must be tolerated")
        .expect("a present setting must pace the run");
    assert_eq!(pacing.millis(), 245, "the parsed value must be 245 ms");
}

/// An interval beyond one day per checkpoint is refused rather than producing a run that
/// can never finish a replay.
#[tokio::test(start_paused = true)]
async fn intervals_above_one_day_are_refused() {
    let start = tokio::time::Instant::now();
    let error = CheckpointPacing::parse(Some("86400001"), start)
        .expect_err("more than one day per checkpoint must refuse the run");
    assert!(
        error.to_string().contains(PACING_ENV),
        "the refusal must name {PACING_ENV}, got {error}"
    );
}
