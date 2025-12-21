use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

/// Milliseconds since UNIX epoch
pub type UnixMillis = u64;

pub fn unix_millis() -> UnixMillis {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_millis().min(u64::MAX as u128) as u64,
        Err(_) => 0, // clock went backwards; clamp
    }
}

async fn sleep_until(ts_millis: u64) {
    if let Some(deadline) = deadline_instant(ts_millis) {
        tokio::time::sleep_until(deadline).await;
    }
}

pub fn deadline_instant(ts_millis: u64) -> Option<tokio::time::Instant> {
    let now = unix_millis();
    ts_millis
        .checked_sub(now)
        .map(|delta| tokio::time::Instant::now() + Duration::from_millis(delta))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn unix_millis_is_monotonic_enough() {
        let t1 = unix_millis();
        let t2 = unix_millis();
        assert!(t2 >= t1, "unix_millis went backwards");
    }

    #[test]
    fn deadline_instant_returns_none_for_past() {
        let now = unix_millis();
        assert!(deadline_instant(now.saturating_sub(1)).is_none());
    }

    #[test]
    fn deadline_instant_allows_now() {
        let now = unix_millis();
        assert!(deadline_instant(now).is_some());
    }

    #[tokio::test]
    async fn sleep_until_past_returns_immediately() {
        let start = tokio::time::Instant::now();

        sleep_until(unix_millis().saturating_sub(100)).await;

        let elapsed = start.elapsed();
        assert!(
            elapsed < Duration::from_millis(5),
            "sleep_until for past timestamp blocked unexpectedly: {:?}",
            elapsed
        );
    }

    #[tokio::test]
    async fn sleep_until_future_waits_at_least_until_deadline() {
        let start = tokio::time::Instant::now();
        let deadline = unix_millis() + 20;

        sleep_until(deadline).await;

        let elapsed = start.elapsed();
        assert!(
            elapsed >= Duration::from_millis(15),
            "sleep_until returned too early: {:?}",
            elapsed
        );
    }

    #[tokio::test]
    async fn sleep_until_does_not_oversleep_badly() {
        let start = tokio::time::Instant::now();
        let deadline = unix_millis() + 20;

        sleep_until(deadline).await;

        let elapsed = start.elapsed();
        assert!(
            elapsed < Duration::from_millis(200),
            "sleep_until overslept badly: {:?}",
            elapsed
        );
    }
}

pub fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    tracing_subscriber::registry()
        .with(filter)
        .with(
            tracing_subscriber::fmt::layer()
                .with_target(true)
                .with_thread_ids(true)
                .with_line_number(true)
                .with_file(true),
        )
        .init();
}