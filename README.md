# Aeternity in Rust

An independent Rust implementation of the existing Aeternity network, developed by GrowAE.

The current repository contains the architecture baseline and development rules. It does not yet contain a node, virtual machine or mainnet validator. The first implementation milestone is canonical types, serialization, cryptographic compatibility vectors and offline header verification.

Read the [architecture](docs/architecture.md), [delivery gates](docs/roadmap.md), [reference manifest](reference/upstream.json) and [contribution rules](CONTRIBUTING.md).

The design preserves mainnet history, including Roma through Ceres, key/microblock consensus, exact state commitments, all on-chain transaction families, and historical AEVM/FATE execution. Existing database import is an explicit conversion/verification path; the new database will not open an Erlang data directory in place.

## Development

Use the single branch `develop`. After cloning:

```sh
python3 tools/bootstrap.py
python3 tools/check_policy.py --history
```

Every new commit uses `Badi Ifaoui <badi.ifaoui@zixdev.com>` as both author and committer. Commit messages contain no trailers.

Development uses bounded fixtures and small isolated networks. Do not start full mainnet or UAT nodes or download their large databases on a developer workstation.

## Upstream reference

The behavioral baseline is the [Aeternity Erlang node](https://github.com/aeternity/aeternity/tree/fa2a6d714e7f94c5edb508f9e04dc27b491968c6). This repository has a fresh history and is not a GitHub fork. Copied source, data or fixtures must retain their original notices and exact revision provenance.
