use super::*;
#[test]
fn original_on_demand_expiry_rewards_and_custom_fork_transitions() {
    let data = unpack_fixture(include_str!(
        "../../fixtures/erlang_on_demand_transition.json"
    ));
    let mut count = 0;
    for row in data["rows"]
        .as_array()
        .unwrap()
        .iter()
        .chain(data["custom"].as_array().unwrap())
    {
        let trees = trees(&row["before"]);
        let before = roots(&trees);
        with_context(row, |parent, config| {
            let mut header = key_header(&row["header"]);
            let rules = settings(row);
            let history = history(row);
            let prepared =
                transition::prepare_on_demand(&trees, &header, parent, &history, config, &rules)
                    .unwrap();
            check(&prepared, row);
            header.common.state_hash = prepared.trees().roots().hash().0;
            let mut actual = trees.clone();
            let executed =
                transition::on_demand_key(&header, &mut actual, parent, &history, config, &rules)
                    .unwrap();
            assert_eq!(executed.state_hash(), prepared.trees().roots().hash());
            check_state(&actual, field(&row["result"]["value"], "state"));
            assert_eq!(executed.closed_generation_fees(), prepared.fees());
            assert!(
                transition::pow_key(
                    &header,
                    &mut trees.clone(),
                    parent,
                    &history,
                    config,
                    &rules
                )
                .is_err()
            );
        });
        assert_eq!(roots(&trees), before);
        count += 1;
    }
    assert_eq!(count, 152);
    assert!(
        data["environments"]
            .as_array()
            .unwrap()
            .iter()
            .all(|c| c["difficulty"] == 1338 && c["environment_difficulty"] == 1338)
    );
    println!("On-demand execution: 152 original state transitions across protocols 1–8");
}
