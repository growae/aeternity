use ae_chain::block::Block;
use ae_p2p::message::{Kind, Message, Value};
use ae_sync::chain::Direction;
use std::collections::BTreeMap;
pub fn message(key: &Block, micros: &[Block], direction: Direction) -> Message {
    Message::new(
        Kind::Generation,
        1,
        BTreeMap::from([
            ("key_block".into(), Value::Binary(key.encode().unwrap())),
            (
                "micro_blocks".into(),
                Value::List(
                    micros
                        .iter()
                        .map(|b| Value::Binary(b.encode().unwrap()))
                        .collect(),
                ),
            ),
            (
                "forward".into(),
                Value::Boolean(direction == Direction::Forward),
            ),
        ]),
        Default::default(),
    )
    .unwrap()
}
pub fn packet(key: &Block, micros: &[Block], direction: Direction) -> Vec<u8> {
    Message::response_ok(&message(key, micros, direction), Default::default())
        .unwrap()
        .encode_packet(Default::default())
        .unwrap()
}
