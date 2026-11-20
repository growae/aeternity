use super::*;
use FieldType::{Binary as B, Boolean as Bool, Integer as I};
fn list(t: FieldType) -> FieldType {
    FieldType::List(Box::new(t))
}
pub(super) fn fields(fs: Vec<(&str, FieldType)>) -> Vec<Field> {
    fs.into_iter()
        .map(|(name, kind)| Field {
            name: name.into(),
            kind,
        })
        .collect()
}
pub(super) fn peer() -> FieldType {
    FieldType::Record(fields(vec![("host", B), ("port", I), ("pubkey", B)]))
}
pub(super) fn poi() -> FieldType {
    FieldType::Record(fields(vec![
        ("height", I),
        ("root_hash", B),
        ("genesis", B),
        ("top", B),
        ("poi", B),
    ]))
}
pub(super) fn template(kind: Kind, version: u16) -> Result<Vec<Field>, Error> {
    use Kind::*;
    let fs = match (kind, version) {
        (Ping, 1) => vec![
            ("port", I),
            ("share", I),
            ("genesis_hash", B),
            ("difficulty", I),
            ("best_hash", B),
            ("sync_allowed", Bool),
            ("peers", list(B)),
        ],
        (Ping, 2) => vec![
            (
                "versions",
                list(FieldType::Record(fields(vec![
                    ("protocol", B),
                    ("vsns", list(I)),
                ]))),
            ),
            ("port", I),
            ("share", I),
            ("genesis_hash", B),
            ("height", I),
            ("difficulty", I),
            ("best_hash", B),
            ("sync_allowed", Bool),
            ("capabilities", B),
            ("peers", list(B)),
        ],
        (GetHeaderByHash, 1) => vec![("hash", B)],
        (GetHeaderByHeight, 1) => vec![("height", I)],
        (GetHeaderByHeight, 2) => vec![("height", I), ("top_hash", B)],
        (Header, 1) => vec![("hdr", B)],
        (GetSuccessors, 1) => vec![("hash", B), ("n", I)],
        (GetSuccessors, 2) => vec![("from_hash", B), ("target_hash", B), ("n", I)],
        (HeaderHashes, 1) => vec![("header_hashes", list(B))],
        (GetBlockTransactions, 1) => vec![("hash", B), ("tx_hashes", list(B))],
        (BlockTransactions, 1) => vec![("hash", B), ("txs", list(B))],
        (KeyBlock, 1) => vec![("key_block", B)],
        (MicroBlock, 1) => vec![("micro_block", B), ("light", Bool)],
        (GetGeneration, 1) => vec![("hash", B), ("forward", Bool)],
        (Generation, 1) => vec![
            ("key_block", B),
            ("micro_blocks", list(B)),
            ("forward", Bool),
        ],
        (Transactions, 1) => vec![("txs", list(B))],
        (PoolInit | GetNodeInfo | Close, 1) => vec![],
        (PoolUnfold, 1) => vec![("unfolds", list(B))],
        (PoolGet, 1) => vec![("tx_hashes", list(B))],
        (PoolFinish, 1) => vec![("done", Bool)],
        (Response, 1) => vec![("result", Bool), ("type", I), ("reason", B), ("object", B)],
        (NodeInfo, 1) => vec![
            ("version", B),
            ("revision", B),
            ("vendor", B),
            ("os", B),
            ("network_id", B),
            ("verified_peers", I),
            ("unverified_peers", I),
        ],
        _ => return Err(Error::UnsupportedVersion),
    };
    Ok(fields(fs))
}
