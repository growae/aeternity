use std::time::{Instant, SystemTime, UNIX_EPOCH};
#[derive(Clone, Copy)]
pub struct Time {
    pub monotonic: Instant,
    pub unix_microseconds: i128,
}
pub trait Clock: Send + Sync + 'static {
    fn now(&self) -> Time;
}
pub struct SystemClock;
impl Clock for SystemClock {
    fn now(&self) -> Time {
        let unix_microseconds = match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(d) => d.as_micros() as i128,
            Err(e) => -(e.duration().as_micros() as i128),
        };
        Time {
            monotonic: Instant::now(),
            unix_microseconds,
        }
    }
}
// OTP timer:now_diff/2 followed by integer div truncates toward zero. Keep the
// planned wall-clock firing instant until drift exceeds a whole interval, or
// the clock moves backwards. Monotonic time is only the waiting mechanism.
pub(super) fn adjust(period_ms: u64, planned_us: i128, now_us: i128) -> (u64, i128) {
    let elapsed = now_us.saturating_sub(planned_us) / 1000;
    if elapsed < 0 || elapsed > i128::from(period_ms) {
        (period_ms, now_us)
    } else {
        (period_ms - elapsed as u64, planned_us)
    }
}

pub(super) fn next_fire(base_us: i128, period_ms: u64) -> i128 {
    base_us.saturating_add(i128::from(period_ms) * 1000)
}
pub(super) fn exhausted(history: &std::collections::VecDeque<i128>) -> bool {
    let Some(now) = history.front() else {
        return false;
    };
    history.iter().enumerate().any(|(i, t)| {
        (i + 1 >= 3 && now.saturating_sub(*t) / 1000 <= 1)
            || (i + 1 >= 10 && now.saturating_sub(*t) / 1000 <= 30)
    })
}
