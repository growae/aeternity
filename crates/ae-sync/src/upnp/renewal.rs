use std::time::{Duration, Instant};

const MINIMUM_DELAY: Duration = Duration::from_millis(250);
const INITIAL_RETRY: Duration = Duration::from_secs(1);
const MAXIMUM_RETRY: Duration = Duration::from_secs(60);

// Operational scheduling only. A finite lease is renewed halfway through its
// conservative lifetime (RFC 6886 section 3.3), without extending it by response
// latency. Permanent assignments retain the configured maximum interval.
pub(super) struct Schedule {
    maximum: Duration,
    retry: Duration,
}
impl Schedule {
    pub fn new(maximum: Duration) -> Self {
        let mut schedule = Self {
            maximum,
            retry: maximum,
        };
        schedule.reset();
        schedule
    }
    pub fn reset(&mut self) {
        self.retry = INITIAL_RETRY.min(self.maximum);
    }
    pub fn mapped(&mut self, began: Instant, lifetime: Option<u32>, now: Instant) -> Instant {
        self.reset();
        let interval = lifetime
            .map(|seconds| Duration::from_secs(u64::from(seconds)) / 2)
            .unwrap_or(self.maximum)
            .min(self.maximum);
        let earliest = now + MINIMUM_DELAY.min(self.maximum);
        // Late replies may already be past halfway. Avoid immediate retry loops
        // without pretending that the original advertisement lives any longer.
        began
            .checked_add(interval)
            .unwrap_or(earliest)
            .max(earliest)
    }
    pub fn failed(&mut self, now: Instant) -> Instant {
        let next = now + self.retry;
        self.retry = self
            .retry
            .saturating_mul(2)
            .min(MAXIMUM_RETRY)
            .min(self.maximum);
        next
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn granted_lease_controls_renewal_instead_of_requested_lifetime() {
        let now = Instant::now();
        let mut schedule = Schedule::new(Duration::from_secs(7190));
        for (lifetime, delay) in [
            (Some(1), Duration::from_millis(500)),
            (Some(7200), Duration::from_secs(3600)),
            (Some(u32::MAX), Duration::from_secs(7190)),
            (None, Duration::from_secs(7190)),
        ] {
            assert_eq!(schedule.mapped(now, lifetime, now), now + delay);
        }
    }

    #[test]
    fn response_delay_does_not_extend_renewal_and_overdue_results_do_not_spin() {
        let began = Instant::now();
        let mut schedule = Schedule::new(Duration::from_secs(7190));
        let arrived = began + Duration::from_secs(2);
        assert_eq!(
            schedule.mapped(began, Some(10), arrived),
            began + Duration::from_secs(5)
        );
        assert_eq!(
            schedule.mapped(began, Some(1), arrived),
            arrived + Duration::from_millis(250)
        );
        assert_eq!(
            schedule.mapped(began, Some(0), arrived),
            arrived + Duration::from_millis(250)
        );
        let mut short = Schedule::new(Duration::from_millis(20));
        assert_eq!(
            short.mapped(began, Some(1), arrived),
            arrived + Duration::from_millis(20)
        );
    }

    #[test]
    fn retry_backoff_is_bounded_and_success_or_new_listener_resets_it() {
        let now = Instant::now();
        let mut schedule = Schedule::new(Duration::from_secs(7190));
        for seconds in [1, 2, 4, 8, 16, 32, 60, 60] {
            assert_eq!(schedule.failed(now), now + Duration::from_secs(seconds));
        }
        schedule.mapped(now, None, now);
        assert_eq!(schedule.failed(now), now + Duration::from_secs(1));
        schedule.reset();
        assert_eq!(schedule.failed(now), now + Duration::from_secs(1));
        let mut short = Schedule::new(Duration::from_millis(20));
        for _ in 0..4 {
            assert_eq!(short.failed(now), now + Duration::from_millis(20));
        }
    }
}
