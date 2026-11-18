use ae_vm_fate::{Error, Limits, unicode::Profile};
use serde_json::Value as Json;
fn bytes(points: &[u32]) -> Vec<u8> {
    points
        .iter()
        .map(|&c| char::from_u32(c).unwrap())
        .collect::<String>()
        .into_bytes()
}
fn points(j: &Json) -> Vec<u32> {
    j.as_array()
        .unwrap()
        .iter()
        .map(|c| c.as_u64().unwrap() as u32)
        .collect()
}
#[test]
fn unicode15_normalization_and_case_mappings_match_every_otp26_scalar() {
    let reference: Json =
        serde_json::from_str(include_str!("fixtures/otp26_unicode.json")).unwrap();
    let p = Profile::Otp26Unicode15;
    let mut digest_input = Vec::new();
    let mut count = 0;
    for cp in (0..=0x10ffff_u32).filter(|&c| char::from_u32(c).is_some()) {
        count += 1;
        digest_input.extend_from_slice(&cp.to_be_bytes());
        for mapped in [
            p.nfc(&[cp], Limits::default()),
            p.case(&[cp], true, Limits::default()),
            p.case(&[cp], false, Limits::default()),
        ] {
            let b = bytes(&mapped.unwrap().points);
            digest_input.extend_from_slice(&(b.len() as u16).to_be_bytes());
            digest_input.extend_from_slice(&b);
        }
    }
    assert_eq!(count, reference["scalar_count"]);
    assert_eq!(
        hex::encode(ae_crypto::sha256(&digest_input).0),
        reference["scalar_digest"].as_str().unwrap()
    );
    for r in reference["cases"].as_array().unwrap() {
        let input = points(&r["input"]);
        assert_eq!(
            p.nfc(&input, Limits::default()).unwrap().points,
            points(&r["nfc"])
        );
        assert_eq!(
            p.case(&input, true, Limits::default()).unwrap().points,
            points(&r["upper"])
        );
        assert_eq!(
            p.case(&input, false, Limits::default()).unwrap().points,
            points(&r["lower"])
        );
    }
}
#[test]
fn unicode_expansion_and_reordering_have_retryable_work_and_byte_limits() {
    let p = Profile::Otp26Unicode15;
    let input = [0x1fa, 0x315, 0x300, 0x327];
    for limits in [
        Limits {
            max_nodes: 2,
            ..Limits::default()
        },
        Limits {
            max_bytes: 4,
            ..Limits::default()
        },
    ] {
        assert!(matches!(p.nfc(&input, limits), Err(Error::ResourceLimit)));
        assert!(matches!(
            p.case(&[0xfb03, 0xfb03], true, limits),
            Err(Error::ResourceLimit)
        ));
    }
    assert!(p.nfc(&input, Limits::default()).is_ok());
    assert_eq!(
        p.case(&[0xfb03], true, Limits::default()).unwrap().points,
        [70, 70, 73]
    );
}
