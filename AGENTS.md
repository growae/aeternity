# Project rules

- Develop on `develop` only until the owner changes the rule. Do not create feature or release branches.
- Every new commit must have author and committer `Badi Ifaoui <badi.ifaoui@zixdev.com>`.
- Commit messages must contain no trailers, including `Co-authored-by`, `Signed-off-by`, or attribution footers.
- Use repository-local identity configuration. Run `python3 tools/bootstrap.py` after cloning, and `python3 tools/check_policy.py --history` before pushing.
- Keep original upstream copyright/license notices in copied source and fixtures. Do not rewrite upstream attribution.
- This is an independent Rust implementation of the existing Aeternity network. Preserve canonical bytes, hashes, historical consensus, AEVM/FATE behavior and state roots.
- Distinguish planned, implemented and independently validated capabilities. Passing a codec or header test does not prove full mainnet compatibility.
- Do not run full mainnet/UAT nodes or download their large databases locally. Use bounded public reads, offline fixtures and tiny isolated development networks.
- Keep deterministic execution independent of async I/O, system time, environment variables and mutable operational configuration.
- Keep consensus validity distinct from mempool policy and service resource limits.
- Add crates with their first real behavior; do not fill the repository with empty runtime placeholders.

The full internal analysis is maintained outside this repository in the sibling `internal-docs` workspace directory. Public implementation decisions belong in `docs`.
