"""Durable request/review/receipt metadata, separate from native OB storage.

This journal is not a memory engine. It is private local control-plane data.
Only the operator CLI calls review(); there is deliberately no MCP review API.
"""
import hashlib
import hmac
import json
from pathlib import Path
import re
import sqlite3
import time
from contextlib import contextmanager

from memory_write_policy import canonical


def _digest_matches(actual, expected):
    return isinstance(expected, str) and expected.isascii() and hmac.compare_digest(actual, expected)


class WriteJournal:
    def __init__(self, path, vault_identity):
        self.path = Path(path)
        if not self.path.is_absolute() or not self.path.parent.is_dir() or self.path.is_symlink():
            raise ValueError('an existing private journal directory is required')
        self.identity = vault_identity
        with self.connect() as db:
            if db.execute('PRAGMA integrity_check').fetchone()[0] != 'ok':
                raise ValueError('write journal is corrupt')
            db.execute('BEGIN IMMEDIATE')
            db.execute('CREATE TABLE IF NOT EXISTS requests('
                       'id TEXT PRIMARY KEY, digest TEXT NOT NULL, body TEXT NOT NULL, '
                       'state TEXT NOT NULL, created REAL NOT NULL, reviewed REAL, receipt TEXT, '
                       'reviewed_digest TEXT)')
            columns = {row['name'] for row in db.execute('PRAGMA table_info(requests)')}
            if 'reviewed_digest' not in columns:
                # A legacy approval has no independently recorded body digest.
                db.execute('ALTER TABLE requests ADD COLUMN reviewed_digest TEXT')

    @contextmanager
    def connect(self):
        db = sqlite3.connect(self.path, timeout=20)
        db.row_factory = sqlite3.Row
        db.execute('PRAGMA synchronous=FULL')
        try:
            with db:
                yield db
        finally:
            db.close()

    def get(self, request_id):
        with self.connect() as db:
            return self._read_request(db, request_id)

    def _read_request(self, db, request_id):
        row = db.execute('SELECT * FROM requests WHERE id=?', (request_id,)).fetchone()
        if row is None:
            raise ValueError('unknown request')
        result = dict(row)
        try:
            body = json.loads(result['body'])
            digest = hashlib.sha256(canonical(body).encode()).hexdigest()
        except (TypeError, ValueError, RecursionError):
            raise PermissionError('invalid request body') from None
        if not isinstance(body, dict) or not _digest_matches(digest, result['digest']):
            raise PermissionError('request body digest does not match')
        if body.get('vault') != self.identity:
            raise PermissionError('journal belongs to another Vault')
        result['body'] = body
        result['receipt'] = json.loads(result['receipt']) if result['receipt'] else None
        return result

    def submit(self, request_id, tool, payload, observations):
        if type(request_id) is not str or not re.fullmatch('[A-Za-z0-9_-]{1,128}', request_id):
            raise ValueError('invalid idempotency request ID')
        body = {'vault': self.identity, 'tool': tool, 'payload': payload, 'observations': observations}
        encoded = canonical(body)
        digest = hashlib.sha256(encoded.encode()).hexdigest()
        with self.connect() as db:
            db.execute('INSERT OR IGNORE INTO requests(id,digest,body,state,created) VALUES(?,?,?,?,?)',
                       (request_id, digest, encoded, 'pending', time.time()))
        row = self.get(request_id)
        # Observation versions are frozen on first submission. Changed source
        # cannot cause a retry to silently approve a different operation.
        if row['body']['tool'] != tool or row['body']['payload'] != payload:
            raise ValueError('request ID already belongs to a different operation')
        return row

    def review(self, request_id, digest, *, approve):
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            row = self._read_request(db, request_id)
            if not _digest_matches(row['digest'], digest) or type(approve) is not bool:
                raise ValueError('review digest does not match')
            if row['state'] != 'pending':
                raise ValueError('request is no longer pending')
            db.execute('UPDATE requests SET state=?,reviewed=?,reviewed_digest=? WHERE id=?',
                       ('approved' if approve else 'rejected', time.time(),
                        row['digest'] if approve else None, request_id))

    def claim(self, request_id):
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            row = self._read_request(db, request_id)
            if row['state'] != 'approved':
                return None
            if not _digest_matches(row['digest'], row['reviewed_digest']):
                raise PermissionError('request approval digest does not match')
            reviewed = row['reviewed']
            now = time.time()
            if not isinstance(reviewed, (int, float)) or not now - 86400 < reviewed <= now:
                return None
            db.execute('UPDATE requests SET state=? WHERE id=?', ('running', request_id))
            row['state'] = 'running'
            return row

    def finish(self, request_id, receipt):
        with self.connect() as db:
            changed = db.execute('UPDATE requests SET state=?,receipt=? WHERE id=? AND state=?',
                                 ('finished', canonical(receipt), request_id, 'running')).rowcount
        if not changed:
            raise ValueError('request was not claimed')

    def progress(self, request_id, receipt):
        with self.connect() as db:
            db.execute('UPDATE requests SET receipt=? WHERE id=? AND state=?',
                       (canonical(receipt), request_id, 'running'))
