use super::*;

#[test]
fn foreground_on_demand_autostart_advances_height_and_reopens_stopped() {
    let lab = Lab::new("on_demand", false);
    let mut cfg = lab.config();
    cfg["mining"]["autostart"] = json!(true);
    cfg["mining"]["cuckoo"]["miners"] = json!([{
        "executable":"this-program-does-not-exist", "repeats":900000,
        "addressed_instances":[2,2]
    }]);
    lab.write("node.json", &cfg);
    let mut process = Process::spawn(&lab, lab.command(), "automatic");
    let ready = process.event("ready");
    assert_eq!(ready["family"], "OnDemand");
    let address = addresses(&ready)[0];
    let deadline = Instant::now() + Duration::from_secs(15);
    let height = loop {
        let (code, value) = request(
            address,
            "GET /v3/key-blocks/current/height HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        );
        assert_eq!(code, 200, "{value}");
        if let Some(height) = value["height"].as_u64()
            && height >= 3
        {
            break height;
        }
        assert!(
            Instant::now() < deadline,
            "automatic key production: {value}"
        );
        thread::sleep(Duration::from_millis(10));
    };
    process.signal(rustix::process::Signal::TERM);
    assert_eq!(
        process.event("stopped"),
        json!({"event":"stopped","joined":true,"failed":false})
    );
    process.wait(true);
    cfg["mining"]["autostart"] = json!(false);
    lab.write("node.json", &cfg);
    let mut process = Process::spawn(&lab, lab.command(), "reopened");
    let ready = process.event("ready");
    let (code, value) = request(
        addresses(&ready)[0],
        "GET /v3/key-blocks/current HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
    );
    assert_eq!(code, 200);
    assert!(value["height"].as_u64().unwrap() >= height);
    assert_eq!(value["nonce"], 1338);
    assert_eq!(value["pow"], json!(vec![0; 42]));
    process.signal(rustix::process::Signal::INT);
    assert_eq!(
        process.event("stopped"),
        json!({"event":"stopped","joined":true,"failed":false})
    );
    process.wait(true);
}
