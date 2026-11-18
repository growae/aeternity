# Delivery gates

The architecture baseline is the starting point. Implementation advances through evidence gates rather than calendar estimates or a claim that a starting process is a compatible node.

| Gate | Scope | Acceptance evidence |
| --- | --- | --- |
| G0 | Architecture and workflow | Component/data ownership, complete source/interface planning coverage, develop and commit policy |
| G1 | Types codecs crypto headers | Canonical and malformed vectors, signing preimages, independent captured header hashes, initial PoW cases |
| G2 | Trie state genesis storage | Exact roots/proofs, genesis state, atomic write/recovery/read-view prototype |
| G3 | Historical execution | Every transaction family and historical AEVM/FATE behavior, including gas, nested rollback and automatic transitions |
| G4 | Block validation and import | Target/work/timestamps/reorg, fork-boundary replay, verified small export/import and next-state equality |
| G5 | Passive networking and APIs | Noise/message/sync interoperability, SDK/error/schema contracts and bounded public observations |
| G6 | Production validation | Historical replay and shadow validation on dedicated infrastructure; crash, resource, security and performance evidence |
| G7 | Mining and interactive channels | Isolated production/solver checks and full channel signing/reconnect/dispute interoperability |
| G8 | Optional products | Stratum, Hyperchains, active monitoring and additional packaging with separate tests |

Core behavior shared with optional products remains mandatory earlier. For example, all on-chain channel transactions belong in G3 even when the interactive channel service is deferred to G7. Hyperchain-only consensus and flags must never be admitted by the mainnet profile.

The next task is G1: create a small Cargo workspace, pin the toolchain, implement canonical byte types and serialization, and reproduce the existing offline header corpus. The full internal analysis currently records twelve independently reconstructed key-header hashes; those samples do not prove PoW or state execution.

Select exact crypto/Noise/HTTP/big-integer libraries and the RocksDB binding through bounded semantic and platform prototypes. Add a committed lockfile, dependency graph checks, formatting, Clippy and meaningful compatibility tests as code appears. The rewrite's own distribution license is a release decision before implementation code is published; copied upstream material always retains its original notices.

A mainnet compatibility claim requires the relevant G1–G6 evidence and a declared validation range/reference revision. A checkpoint-based node must disclose its trust anchor. Header decoding, API route presence and compilation alone are insufficient.
