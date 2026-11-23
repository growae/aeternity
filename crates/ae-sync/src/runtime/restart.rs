// aec_connection_sup:init/1 (fa2a6d7), OTP26 supervisor:add_restart/3.
// Integer monotonic seconds; entries exactly Period seconds old still count.
#[derive(Default)]
pub(crate) struct Window<const INTENSITY: usize, const PERIOD: u64>(Vec<u64>);
pub(super) type Budget = Window<5, 10>;
impl<const INTENSITY: usize, const PERIOD: u64> Window<INTENSITY, PERIOD> {
    pub fn record(&mut self, now: u64) -> bool {
        self.0.retain(|time| *time >= now.saturating_sub(PERIOD));
        self.0.push(now);
        self.0.len() <= INTENSITY
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn consensus_restart_window_matches_original_otp_thirty_second_boundaries() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../tests/fixtures/erlang_consensus_supervision.json"
        ))
        .unwrap();
        assert_eq!(fixture["policy"]["intensity"], 3);
        assert_eq!(fixture["policy"]["period"], 30);
        for trace in fixture["budgets"].as_array().unwrap() {
            let mut budget = Window::<3, 30>::default();
            for row in trace.as_array().unwrap() {
                assert_eq!(
                    budget.record(row["now"].as_u64().unwrap()),
                    row["allowed"].as_bool().unwrap()
                );
                assert_eq!(serde_json::json!(budget.0), row["retained"]);
            }
        }
    }
    #[test]
    fn retained_attempts_match_original_otp_at_inclusive_second_boundaries() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../tests/fixtures/erlang_connection_owner.json"
        ))
        .unwrap();
        assert_eq!(fixture["policy"]["intensity"], 5);
        assert_eq!(fixture["policy"]["period"], 10);
        for trace in fixture["budgets"].as_array().unwrap() {
            let mut budget = Budget::default();
            for row in trace.as_array().unwrap() {
                assert_eq!(
                    budget.record(row["now"].as_u64().unwrap()),
                    row["allowed"].as_bool().unwrap()
                );
                assert_eq!(serde_json::json!(budget.0), row["retained"]);
            }
        }
    }
}
