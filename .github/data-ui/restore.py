"""Restore exact reviewed Git commits from readable patches, then publish verified refs."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys

REPOSITORIES = {'s7cret/openpine', 's7cret/marketdata-provider'}
STAGING = 'ops/rc6-data-ui-20260905'
TARGET = 'release/5.0.0rc6'


def git(*args: str, data: bytes | None = None) -> str:
    return subprocess.check_output(['git', *args], input=data).decode().strip()


def load(path: Path) -> dict:
    plan = json.loads(path.read_text())
    if plan['repository'] not in REPOSITORIES or plan['branch'] != TARGET:
        raise ValueError('unexpected repository or branch')
    if os.environ.get('GITHUB_REPOSITORY', plan['repository']) != plan['repository']:
        raise ValueError('repository mismatch')
    parent = plan['base']
    for commit in plan['commits']:
        for key in ('sha', 'parent', 'tree'):
            if not re.fullmatch('[0-9a-f]{40}', commit[key]):
                raise ValueError('invalid Git identity')
        if commit['parent'] != parent:
            raise ValueError('noncontiguous series')
        raw = commit['raw_commit'].encode()
        if not raw.startswith(f"tree {commit['tree']}\nparent {parent}\nauthor ".encode()):
            raise ValueError('invalid commit header')
        if hashlib.sha1(b'commit ' + str(len(raw)).encode() + b'\0' + raw).hexdigest() != commit['sha']:
            raise ValueError('commit identity mismatch')
        parent = commit['sha']
    if parent != plan['head']:
        raise ValueError('wrong series head')
    return plan


def restore(path: Path, evidence: Path) -> None:
    plan = load(path)
    evidence.mkdir(parents=True, exist_ok=True)
    (evidence / 'manifest.json').write_text(json.dumps(plan, indent=2) + '\n')
    patches = []
    for commit in plan['commits']:
        pieces = []
        for name in commit['parts']:
            if not re.fullmatch(r'[0-9]{2}-[0-9]+\.patch', name):
                raise ValueError('invalid patch path')
            data = (path.parent / name).read_bytes()
            if len(data) > 1_000_000:
                raise ValueError('patch too large')
            pieces.append(data)
            (evidence / name).write_bytes(data)
        patch = b''.join(pieces)
        if hashlib.sha256(patch).hexdigest() != commit['patch_sha256']:
            raise ValueError('patch integrity mismatch')
        patches.append(patch)
    git('checkout', '--detach', plan['base'])
    for commit, patch in zip(plan['commits'], patches, strict=True):
        if git('rev-parse', 'HEAD') != commit['parent']:
            raise ValueError('working parent mismatch')
        git('apply', '--check', '--index', '-', data=patch)
        git('apply', '--index', '--whitespace=nowarn', '-', data=patch)
        if git('write-tree') != commit['tree']:
            raise ValueError('restored tree differs from reviewed tree')
        actual = git('hash-object', '-t', 'commit', '-w', '--stdin', data=commit['raw_commit'].encode())
        if actual != commit['sha']:
            raise ValueError('restored commit mismatch')
        git('checkout', '--detach', actual)
    if git('status', '--porcelain', '--untracked-files=no'):
        raise ValueError('restored source is not clean')
    git('update-ref', 'refs/heads/review-candidate', plan['head'])
    git('bundle', 'create', str((evidence / 'verified.bundle').resolve()), 'refs/heads/review-candidate')
    git('bundle', 'verify', str((evidence / 'verified.bundle').resolve()))
    (evidence / 'restored-head.txt').write_text(git('rev-parse', 'HEAD') + '\n')


def publish(evidence: Path) -> None:
    plan = load(evidence / 'manifest.json')
    if os.environ.get('GITHUB_REF') != 'refs/heads/' + STAGING:
        raise ValueError('publication must run from the expected maintenance branch')
    git('fetch', str((evidence / 'verified.bundle').resolve()), 'refs/heads/review-candidate')
    if git('rev-parse', 'FETCH_HEAD') != plan['head']:
        raise ValueError('verification bundle identity mismatch')
    git('merge-base', '--is-ancestor', plan['base'], plan['head'])
    target = 'refs/heads/' + TARGET
    before = git('ls-remote', '--heads', 'origin')
    remote = git('ls-remote', '--refs', 'origin', target).split()
    if len(remote) != 2 or remote[0] not in (plan['base'], plan['head']):
        raise ValueError('target changed; refusing to overwrite concurrent work')
    if remote[0] == plan['base']:
        git('push', 'origin', plan['head'] + ':' + target)
    if git('ls-remote', '--refs', 'origin', target).split()[0] != plan['head']:
        raise ValueError('published head verification failed')
    (evidence / 'before-heads.txt').write_text(before + '\n')
    (evidence / 'published-head.txt').write_text(plan['head'] + '\n')
    branch, tag = 'refs/heads/' + STAGING, 'refs/tags/' + STAGING
    sha = os.environ['GITHUB_SHA']
    if not re.fullmatch('[0-9a-f]{40}', sha):
        raise ValueError('invalid maintenance head')
    # These leases guard deletion/archive only; the release is never forced.
    git('push', '--atomic', f'--force-with-lease={branch}:{sha}',
        f'--force-with-lease={tag}:', 'origin', sha + ':' + tag, ':' + branch)
    after = git('ls-remote', '--heads', 'origin')
    expected = {ref: value for value, ref in (row.split() for row in before.splitlines())}
    expected.pop(branch)
    expected[target] = plan['head']
    actual = {ref: value for value, ref in (row.split() for row in after.splitlines())}
    if actual != expected or git('ls-remote', '--refs', 'origin', tag).split()[0] != sha:
        raise ValueError('final branch/archive inventory verification failed')
    (evidence / 'final-heads.txt').write_text(after + '\n')
    (evidence / 'maintenance-tag.txt').write_text(tag + ' ' + sha + '\n')


if __name__ == '__main__':
    if sys.argv[1] == 'restore':
        restore(Path(sys.argv[2]).resolve(), Path(sys.argv[3]).resolve())
    elif sys.argv[1] == 'publish':
        publish(Path(sys.argv[2]).resolve())
    else:
        raise ValueError('unknown operation')
