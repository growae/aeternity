use ae_stratum::accounting::Date;
#[test]
fn cache_dates_match_original_calendar_utc_and_reject_unrepresentable_years() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/erlang_stratum_feed.json")).unwrap();
    for row in fixture["dates"].as_array().unwrap() {
        let expected = std::array::from_fn(|i| row["utc"][i].as_u64().unwrap() as u32);
        assert_eq!(
            Date::from_unix_millis(row["milliseconds"].as_u64().unwrap()).unwrap(),
            Date(expected)
        );
    }
    assert!(Date::from_unix_millis(253_402_300_800_000).is_err());
    assert!(Date::from_unix_millis(u64::MAX).is_err());
}
