# Contributing

Work directly on `develop` with small reviewable commits. Do not introduce feature or release branches until the project owner changes this rule. Pull before starting work and review staged changes before committing.

Run `python3 tools/bootstrap.py` after cloning. It sets only this repository's author/committer defaults and installs the checked-in Git hooks. Environment identity overrides still affect Git; the hooks check the effective identities as well as the branch.

All new commits must use:

```text
Badi Ifaoui <badi.ifaoui@zixdev.com>
```

for both author and committer. Messages contain no trailers, including co-author and sign-off lines. Do not use a GitHub merge or squash action that changes the committer or adds a footer. Source attribution belongs in retained notices and provenance records.

Before pushing, run:

```sh
python3 tools/check_policy.py --history
git diff --check
```

The pre-commit and commit-msg hooks check identity, branch and message policy. The pre-push hook checks actual commit history and restricts branch pushes to `develop`. Hooks are local safeguards, not server-side enforcement; new clones must bootstrap them. The history check is authoritative for the identities and messages actually stored in this fresh repository.

Add a pinned Rust toolchain and committed `Cargo.lock` with the first real implementation slice. Then require formatting, Clippy, targeted unit/differential tests and dependency-boundary checks appropriate to that slice. Do not add tests that merely repeat implementation logic; compare canonical outputs and failure behavior against independent reference evidence.

Use small offline corpora and isolated development chains. Full mainnet/UAT synchronization is not part of local tests. A future large replay job needs dedicated infrastructure and explicit resource planning.
