"""Durable request/review/receipt metadata, separate from native OB storage.

This journal is not a memory engine. It is private local control-plane data.
Only the operator CLI calls review(); there is deliberately no MCP review API.
"""
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import time
from contextlib import contextmanager

from memory_write_policy import canonical


class WriteJournal:
    def __init__(self, path, vault_identity):
        self.path = Path(path)
        if not self.path.is_absolute() or not self.path.parent.is_dir() or self.path.is_symlink():
            raise ValueError('an existing private journal directory is required')
        self.identity = vault_identity
        with self.connect() as db:
            if db.execute('PRAGMA integrity_check').fetchone()[0] != 'ok':
                raise ValueError('write journal is corrupt')
            db.execute('CREATE TABLE IF NOT EXISTS requests('
                       'id TEXT PRIMARY KEY, digest TEXT NOT NULL, body TEXT NOT NULL, '
                       'state TEXT NOT NULL, created REAL NOT NULL, reviewed REAL, receipt TEXT)')

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
            row = db.execute('SELECT * FROM requests WHERE id=?', (request_id,)).fetchone()
        if row is None:
            raise ValueError('unknown request')
        result = dict(row)
        result['body'] = json.loads(result['body'])
        result['receipt'] = json.loads(result['receipt']) if result['receipt'] else None
        if result['body']['vault'] != self.identity:
            raise PermissionError('journal belongs to another Vault')
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
        row = self.get(request_id)
        if row['digest'] != digest or type(approve) is not bool:
            raise ValueError('review digest does not match')
        with self.connect() as db:
            changed = db.execute('UPDATE requests SET state=?,reviewed=? WHERE id=? AND digest=? AND state=?',
                                 ('approved' if approve else 'rejected', time.time(), request_id, digest, 'pending')).rowcount
        if not changed:
            raise ValueError('request is no longer pending')

    def claim(self, request_id):
        self.get(request_id)
        with self.connect() as db:
            changed = db.execute('UPDATE requests SET state=? WHERE id=? AND state=? AND reviewed>?',
                                 ('running', request_id, 'approved', time.time()-86400)).rowcount
        return self.get(request_id) if changed else None

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
