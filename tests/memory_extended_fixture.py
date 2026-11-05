"""Synthetic-only fixtures; never reads or copies the user's Vault."""
from contextlib import contextmanager
import hashlib
import json
from pathlib import Path
import secrets
import sqlite3
import tempfile
from unittest.mock import patch

import yaml

from embedding_engine import EmbeddingEngine
from test_read_only_read_chains_unittest import _WRITE_ATTEMPTS


ALL_READS = ['memory_bootstrap', 'memory_search', 'memory_read', 'memory_catalog',
             'memory_status', 'memory_feel_search', 'memory_plan_list', 'memory_self_read']


def normal_writer_worker(vault, pipe):
    """Finite normal-mode OB business writer, with no provider or full server."""
    import asyncio
    from bucket_manager import BucketManager

    async def run():
        config = {'read_only': False, 'buckets_dir': vault, 'embedding': {'enabled': False}}
        engine = EmbeddingEngine(config)
        class LocalTestEmbedding:
            async def generate_async(self, text):
                return [1.0] + [0.0] * 1023
        # Real native persistence/locking, deterministic model output, no API.
        engine._backend = LocalTestEmbedding()
        engine.enabled = True
        manager = BucketManager(config, engine)
        conn = sqlite3.connect(engine.db_path, timeout=15)
        try:
            assert conn.execute('PRAGMA journal_mode=WAL').fetchone()[0] == 'wal'
            conn.execute('BEGIN IMMEDIATE')
            conn.execute('UPDATE embeddings SET updated_at=? WHERE bucket_id=?', ('pending-writer', 'ordinary'))
            pipe.send({'ready': True})
            assert pipe.recv() == 'commit'
            conn.commit()
            new_id = await manager.create(content='native writer committed sentinel',
                                          domain=['test'], importance=8, test_data=True)
            pipe.send({'id': new_id, 'integrity': conn.execute('PRAGMA integrity_check').fetchone()[0],
                       'dimension': len(await engine.get_embedding(new_id)),
                       'content_hash': engine.get_content_hash(new_id)})
            assert pipe.recv() == 'finish'
        finally:
            conn.close()

    try:
        asyncio.run(run())
    except Exception as exc:
        pipe.send({'error': type(exc).__name__})
    finally:
        pipe.close()


class ExtendedFixture:
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parents[1])
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.vault = self.root / 'vault'
        self.vault.mkdir()
        self.put('dynamic', 'ordinary', 'dynamic', 'orchard public ordinary sentinel')
        self.put('permanent', 'core', 'permanent', 'public core sentinel', pinned=True)
        self.put('feel', 'feeling', 'feel', 'quiet assistant feeling sentinel',
                 tags=['__feel__'], domain=['feel'], triggered_by='ordinary')
        self.put('plans', 'active-plan', 'plan', 'active plan sentinel', status='active')
        self.put('plans', 'resolved-plan', 'plan', 'resolved plan SECRET', status='resolved')
        self.put('plans', 'abandoned-plan', 'plan', 'abandoned plan SECRET', status='abandoned')
        self.put('dynamic', 'self-entry', 'i', 'formal self sentinel', tags=['__i__'])
        self.put('dynamic', 'candidate', 'dynamic', 'candidate self sentinel',
                 tags=['__i_candidate__'], i_stage='candidate', i_dream_dates=[])
        self.put('letters', 'private-letter', 'letter', 'LETTER PRIVATE SENTINEL',
                 tags=['__letter__'], lock_type='timed', unlock_date='2000-01-01T00:00:00+08:00')
        self.put('dynamic', 'disguised-letter', 'dynamic', 'DISGUISED LETTER SECRET',
                 tags=['__letter__'])
        self.put('archive', 'old-record', 'dynamic', 'ARCHIVED PRIVATE SENTINEL')
        self.put('dynamic', 'misplaced-feel', 'feel', 'MISPLACED FEEL SECRET')
        EmbeddingEngine({'buckets_dir': str(self.vault), 'embedding': {'enabled': False}})
        conn = sqlite3.connect(self.vault / 'embeddings.db')
        try:
            for name in ['ordinary', 'core', 'feeling', 'private-letter', 'misplaced-feel']:
                conn.execute('INSERT INTO embeddings(bucket_id,embedding,updated_at) VALUES (?,?,?)',
                             (name, json.dumps([1.0] + [0.0] * 1023), '2026-11-04T10:00:00+08:00'))
            conn.commit()
        finally:
            conn.close()
        self.token = secrets.token_urlsafe(32)
        self.config = {
            'read_only': True, 'transport': 'stdio', 'buckets_dir': str(self.vault),
            'auth': {'token_sha256': hashlib.sha256(self.token.encode()).hexdigest()},
            'embedding': {'enabled': False, 'api_format': 'ollama', 'model': 'bge-m3',
                          'dim': 1024, 'base_url': 'http://127.0.0.1:11434/v1'},
            'permissions': {'tools': list(ALL_READS), 'write_enabled': False},
        }

    def put(self, directory, name, kind, body, **extra):
        path = self.vault / directory / (name + '.md')
        path.parent.mkdir(exist_ok=True)
        meta = dict(id=name, name=name, type=kind, importance=8, valence=0.5,
                    arousal=0.3, created='2026-11-04T10:00:00+08:00',
                    last_active='2026-11-04T10:00:00+08:00', **extra)
        path.write_text('---\n' + yaml.safe_dump(meta) + '---\n' + body, encoding='utf-8')

    def snapshot(self):
        files = {str(p.relative_to(self.vault)): hashlib.sha256(p.read_bytes()).hexdigest()
                 for p in self.vault.rglob('*') if p.is_file()}
        dirs = sorted(str(p.relative_to(self.vault)) for p in self.vault.rglob('*') if p.is_dir())
        conn = sqlite3.connect((self.vault / 'embeddings.db').as_uri() + '?mode=ro', uri=True)
        try:
            conn.execute('PRAGMA query_only=ON')
            state = {'schema': conn.execute('SELECT type,name,sql FROM sqlite_schema ORDER BY name').fetchall(),
                     'data': conn.execute('SELECT * FROM embeddings ORDER BY bucket_id').fetchall(),
                     'metadata': conn.execute('SELECT * FROM embeddings_meta ORDER BY key').fetchall(),
                     'integrity': conn.execute('PRAGMA integrity_check').fetchone(),
                     'journal_mode': conn.execute('PRAGMA journal_mode').fetchone()}
        finally:
            conn.close()
        return files, dirs, state

    @contextmanager
    def no_writes(self):
        attempts = []
        marker = _WRITE_ATTEMPTS.set(attempts)
        try:
            with patch('asyncio.create_task', side_effect=AssertionError('background task')):
                yield
        finally:
            _WRITE_ATTEMPTS.reset(marker)
            self.assertEqual(attempts, [], 'a swallowed write attempt is still a failure')
