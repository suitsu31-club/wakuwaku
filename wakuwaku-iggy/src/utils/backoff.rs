use iggy::prelude::IggyError;
use std::time::Duration;

/// Delay before attempt `attempt + 1`. The last delay repeats forever.
pub(crate) fn delay_for(delays: &'static [Duration], attempt: usize) -> Duration {
    delays
        .get(attempt)
        .or_else(|| delays.last())
        .copied()
        .unwrap_or(Duration::ZERO)
}

/// Run `op` until it succeeds, sleeping [`delay_for`] between attempts.
pub(crate) async fn until_ok<T, F, Fut>(
    what: &'static str,
    delays: &'static [Duration],
    mut op: F,
) -> T
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, IggyError>>,
{
    let mut attempt = 0usize;
    loop {
        match op().await {
            Ok(value) => return value,
            Err(e) => {
                let delay = delay_for(delays, attempt);
                tracing::warn!(what, error = %e, ?delay, "iggy operation failed, retrying");
                tokio::time::sleep(delay).await;
                attempt = attempt.saturating_add(1);
            }
        }
    }
}
