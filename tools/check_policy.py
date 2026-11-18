#!/usr/bin/env python3
"""Verify the actual Git identity/message/branch policy for this fresh history."""
import argparse
import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
IDENTITY = 'Badi Ifaoui <badi.ifaoui@zixdev.com>'

def git(*args, input=None):
    result = subprocess.run(['git', '-C', str(ROOT), *args], input=input,
                            text=True, capture_output=True)
    if result.returncode:
        raise SystemExit(result.stderr.strip() or f'Git command failed: {args[0]}')
    return result.stdout.strip()

def identity():
    for key in ('GIT_AUTHOR_IDENT', 'GIT_COMMITTER_IDENT'):
        actual = git('var', key)
        if actual.rsplit(' ', 2)[0] != IDENTITY:
            raise SystemExit(f'{key} must be {IDENTITY}')

def message(text, label):
    # Git knows configured trailer aliases; explicit footer detection also catches
    # unknown keys that Git's heuristic would not classify as a trailer block.
    if git('interpret-trailers', '--parse', input=text):
        raise SystemExit(f'{label}: commit trailers are forbidden')
    paragraphs = re.split(r'\n\s*\n', text.strip())
    if len(paragraphs) > 1 and any(
        re.match(r'^[A-Za-z][A-Za-z0-9 -]*:\s*\S', line)
        for line in paragraphs[-1].splitlines()
    ):
        raise SystemExit(f'{label}: trailer-like footer is forbidden')
    if re.search(r'(?im)^\s*(co-authored-by|signed-off-by|reviewed-by|acked-by)\s*:', text):
        raise SystemExit(f'{label}: attribution trailers are forbidden')

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--message', type=Path)
    parser.add_argument('--history', action='store_true')
    parser.add_argument('--pre-push', action='store_true')
    args = parser.parse_args()
    if git('symbolic-ref', '--short', 'HEAD') != 'develop':
        raise SystemExit('Only develop is permitted for project development.')
    identity()
    if args.message:
        message(args.message.read_text(), 'new commit')
    if args.pre_push:
        import sys
        for line in sys.stdin:
            local_ref, local_sha, remote_ref, _ = line.split()
            if local_ref != 'refs/heads/develop' or remote_ref != 'refs/heads/develop' or set(local_sha) == {'0'}:
                raise SystemExit('Only non-deletion pushes of develop to develop are permitted.')
    count = 0
    if args.history or args.pre_push:
        exists = subprocess.run(['git', '-C', str(ROOT), 'rev-parse', '--verify', 'HEAD'],
                                capture_output=True).returncode == 0
        commits = git('rev-list', 'HEAD').splitlines() if exists else []
        for commit in commits:
            author, committer, body = git('show', '-s', '--format=%an <%ae>%n%cn <%ce>%n%B', commit).split('\n', 2)
            if author != IDENTITY or committer != IDENTITY:
                raise SystemExit(f'{commit}: author and committer must be {IDENTITY}')
            message(body, commit)
            count += 1
        print(f'Policy passed for {count} commits on develop.')

if __name__ == '__main__':
    main()
