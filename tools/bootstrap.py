#!/usr/bin/env python3
"""Install repository-local identity and policy hooks; never change global Git config."""
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

def git(*args):
    return subprocess.check_output(['git', '-C', str(ROOT), *args], text=True).strip()

if Path(git('rev-parse', '--show-toplevel')).resolve() != ROOT:
    raise SystemExit('Run bootstrap inside the Aeternity Rust repository.')
if git('symbolic-ref', '--short', 'HEAD') != 'develop':
    raise SystemExit('The project must use develop; bootstrap will not change branches.')
for key, value in {
    'user.name': 'Badi Ifaoui',
    'user.email': 'badi.ifaoui@zixdev.com',
    'user.useConfigOnly': 'true',
    'core.hooksPath': '.githooks',
}.items():
    git('config', '--local', key, value)
for path in (ROOT / '.githooks').iterdir():
    if path.is_file():
        path.chmod(path.stat().st_mode | 0o111)
subprocess.run(['python3', str(ROOT / 'tools/check_policy.py')], check=True)
print('Configured repository-local identity and policy hooks on develop.')
