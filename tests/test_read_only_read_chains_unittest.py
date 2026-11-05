"""Real OB read dispatch, synthetic vault, no HTTP/provider/network/processes."""
import asyncio
from contextvars import ContextVar
import hashlib
import json
import logging
import os
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import AsyncMock, Mock, patch

from bucket_manager import BucketManager
from decay_engine import DecayEngine
from deletion_requests import DeletionRequestStore
from embedding_engine import EmbeddingEngine
from tools import _runtime as rt
from tools.breath import dispatch as native_dispatch


_WRITE_ATTEMPTS = ContextVar("read_chain_write_attempts", default=None)


def _audit_reads(event, args):
    attempts = _WRITE_ATTEMPTS.get()
    if attempts is None:
        return
    mutation = event in {"os.mkdir", "os.remove", "os.rmdir", "os.rename", "os.truncate", "os.chmod"}
    if event == "open":
        flags = args[2] or 0
        mutation = bool(flags & (os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_TRUNC | os.O_APPEND))
    if event == "sqlite3.connect":
        mutation = "mode=ro" not in str(args[0])
    if mutation:
        attempts.append((event, str(args)))
        raise PermissionError("read chain attempted filesystem mutation: " + event)


sys.addaudithook(_audit_reads)


async def dispatch(**kwargs):
    attempts = []
    token = _WRITE_ATTEMPTS.set(attempts)
    try:
        return await native_dispatch(**kwargs)
    finally:
        _WRITE_ATTEMPTS.reset(token)
        if attempts:
            raise AssertionError("read-chain write attempts: " + repr(attempts))


class ReadChainsTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parents[1])
        self.root = Path(self.temp.name)
        self.addCleanup(self.temp.cleanup)
        self.config = {
            "read_only": True, "buckets_dir": str(self.root),
            "embedding": {"enabled": False},
            "storage": {"external_change_poll_seconds": 0},
        }
        (self.root / "dynamic").mkdir()
        self.bucket_id = "20261103-120000-audit"
        self.memory = self.root / "dynamic" / (self.bucket_id + ".md")
        self.memory.write_text(
            "---\nid: " + self.bucket_id + "\ntype: dynamic\nimportance: 9\n"
            "created: '2026-11-03T12:00:00+08:00'\n"
            "last_active: '2026-11-03T12:00:00+08:00'\n"
            "domain: [work]\n---\nsynthetic orchard memory", encoding="utf-8",
        )
        self.requests = self.root / ".human_deletion_requests.json"
        self.requests.write_text(json.dumps({"version": 1, "requests": [
            {"id": "pending-audit", "status": "pending", "bucket_id": "missing-target"}
        ]}), encoding="utf-8")
        self.db = self.root / "embeddings.db"
        conn = sqlite3.connect(self.db)
        try:
            conn.executescript(
                "CREATE TABLE embeddings (bucket_id TEXT PRIMARY KEY, embedding TEXT, "
                "meaning_embedding TEXT, content_hash TEXT);"
                "CREATE TABLE embeddings_meta (key TEXT PRIMARY KEY, value TEXT);"
            )
            conn.execute("INSERT INTO embeddings VALUES (?, ?, NULL, '')",
                         (self.bucket_id, '[1.0, 0.0]'))
            conn.commit()
        finally:
            conn.close()
        self.initial = self.snapshot()
        self.engine = EmbeddingEngine(self.config)
        self.manager = BucketManager(self.config, embedding_engine=self.engine)
        self.decay = DecayEngine(self.config, self.manager)
        self.decay.ensure_started = AsyncMock(side_effect=AssertionError("decay started"))
        self.store = DeletionRequestStore(str(self.root), self.manager, self.engine)
        self.events = Mock(side_effect=AssertionError("event/statistics write"))
        self.webhook = AsyncMock(side_effect=AssertionError("webhook"))
        self.queue = Mock()
        for name in ("enqueue", "enqueue_meaning", "discard", "status"):
            setattr(self.queue, name, Mock(side_effect=AssertionError("outbox accessed")))
        self.manager.attach_embedding_outbox(self.queue)
        # Patch the process-local runtime without importing the full server.
        runtime = {
            "config": self.config, "bucket_mgr": self.manager,
            "embedding_engine": self.engine, "embedding_outbox": self.queue,
            "decay_engine": self.decay, "deletion_requests": self.store,
            "mark_op": self.events, "fire_webhook": self.webhook,
            "v3_runtime": Mock(record_tool_event=self.events),
            "them_service": Mock(surface=AsyncMock(side_effect=AssertionError("them accessed"))),
            "logger": logging.getLogger("read-only-test"),
        }
        self.runtime_patch = patch.multiple(rt, **runtime)
        self.runtime_patch.start()
        self.addCleanup(self.runtime_patch.stop)
        # No reads may schedule any task, including BM25 rebuilds.
        self.task_patch = patch("asyncio.create_task", side_effect=AssertionError("background task"))
        self.task_mock = self.task_patch.start()
        self.addCleanup(self.task_patch.stop)
        self.warning_patch = patch("errors.record_error", side_effect=AssertionError("persistent error log"))
        self.warning_mock = self.warning_patch.start()
        self.addCleanup(self.warning_patch.stop)

    def snapshot(self):
        return {
            str(p.relative_to(self.root)): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in self.root.rglob("*") if p.is_file()
        }

    async def asyncTearDown(self):
        self.assertEqual(self.initial, self.snapshot(), "vault files changed")
        self.assertEqual({str(p.relative_to(self.root)) for p in self.root.rglob("*") if p.is_dir()},
                         {"dynamic"}, "new directory/lock directory")
        self.decay.ensure_started.assert_not_called()
        self.events.assert_not_called()
        self.webhook.assert_not_called()
        self.queue.enqueue.assert_not_called()
        self.queue.enqueue_meaning.assert_not_called()
        self.queue.discard.assert_not_called()
        self.queue.status.assert_not_called()
        self.task_mock.assert_not_called()
        self.warning_mock.assert_not_called()

    async def test_bootstrap(self):
        self.assertIn("synthetic orchard memory", await dispatch())

    async def test_empty_bootstrap_has_no_statistics_callback(self):
        self.memory.unlink()
        self.initial = self.snapshot()
        self.assertIn("空", await dispatch())

    async def test_keyword_missing_bm25_does_not_build(self):
        self.assertIsNone(self.manager._bm25)
        text = await dispatch(query="orchard")
        self.assertIn("synthetic orchard memory", text)
        self.assertIn("BM25 自动构建已禁用", text)
        self.assertIsNone(self.manager._bm25)

    async def test_poisoned_bm25_never_used_or_rebuilt(self):
        self.manager._bm25 = Mock(score=Mock(side_effect=AssertionError("BM25 used")))
        self.assertIn("orchard", await dispatch(query="orchard"))
        self.manager._bm25.score.assert_not_called()

    async def test_exact_id_does_not_query_vectors(self):
        with patch.object(self.engine, "search_similar_strict", side_effect=AssertionError("vector lookup")):
            text = await dispatch(query=self.bucket_id)
        self.assertIn("exact_bucket_id:true", text)
        self.assertIn("synthetic orchard memory", text)

    async def test_missing_id_has_no_write(self):
        self.assertNotIn("exact_bucket_id:true", await dispatch(query="20990101-missing"))

    async def test_semantic_real_sqlite_with_deterministic_query_vector(self):
        # No paid API or local provider is called; SQLite/vector scoring is real.
        self.engine.enabled = True
        self.engine._backend = Mock()
        self.engine._backend.generate_async = AsyncMock(return_value=[1.0, 0.0])
        text = await dispatch(query="unrelated-query-xyz")
        self.assertIn("synthetic orchard memory", text)
        self.engine._backend.generate_async.assert_awaited_once()
        self.assertTrue(self.engine._query_cache)  # Memory-only LRU is allowed.

    async def test_pending_deletion_never_reconciles(self):
        with patch.object(self.store, "reconcile_pending", side_effect=AssertionError("reconcile")):
            await dispatch()
            await dispatch(query="orchard")
            await dispatch(query=self.bucket_id)
            self.assertEqual(await self.store.render_pending_batch(), "")
        with self.assertRaises(PermissionError):
            await self.store.reconcile_pending()

    async def test_semantic_database_error_refuses_keyword_fallback(self):
        self.engine.enabled = True
        with patch.object(self.engine, "_connect_db", side_effect=sqlite3.DatabaseError("synthetic failure")):
            with patch.object(self.manager, "search", side_effect=AssertionError("fallback")) as fallback:
                with self.assertRaisesRegex(RuntimeError, "semantic search failed"):
                    await dispatch(query="orchard")
                fallback.assert_not_called()

    async def test_malformed_vector_refuses_fallback(self):
        conn = sqlite3.connect(self.db)
        try:
            conn.execute("UPDATE embeddings SET embedding='invalid-vector'")
            conn.commit()
        finally:
            conn.close()
        self.initial = self.snapshot()
        self.engine.enabled = True
        self.engine._backend = Mock(generate_async=AsyncMock(return_value=[1.0, 0.0]))
        with self.assertRaisesRegex(RuntimeError, "semantic search failed"):
            await dispatch(query="orchard")

    async def test_keyword_failure_is_an_error(self):
        with patch.object(self.manager, "search", side_effect=OSError("synthetic read failure")):
            with self.assertRaisesRegex(RuntimeError, "keyword search failed"):
                await dispatch(query="orchard")

    async def test_external_change_cache_refresh_does_not_repair_indexes(self):
        await dispatch()
        self.memory.write_text(self.memory.read_text(encoding="utf-8").replace("orchard", "garden"), encoding="utf-8")
        self.initial = self.snapshot()  # Test fixture edit, outside the read call.
        self.assertIn("garden", await dispatch())

    async def test_bad_metadata_errors_before_persistent_warning(self):
        self.memory.write_text(self.memory.read_text(encoding="utf-8").replace("importance: 9", "importance: bad"), encoding="utf-8")
        self.initial = self.snapshot()
        with self.assertRaisesRegex(RuntimeError, "bootstrap failed"):
            await dispatch()
        with self.assertRaisesRegex(RuntimeError, "exact memory lookup failed"):
            await dispatch(query=self.bucket_id)

    async def test_vector_dimension_mismatch_errors_without_fallback(self):
        self.engine.enabled = True
        self.engine._backend = Mock(generate_async=AsyncMock(return_value=[1.0]))
        with self.assertRaisesRegex(RuntimeError, "semantic search failed"):
            await dispatch(query="orchard")

    async def test_direct_bm25_rebuild_is_forbidden(self):
        with self.assertRaises(PermissionError):
            await self.manager._rebuild_bm25_async([])

    async def test_real_database_corruption_after_initialization(self):
        self.db.write_bytes(b"synthetic corrupt sqlite")
        self.initial = self.snapshot()
        self.engine.enabled = True
        with self.assertRaisesRegex(RuntimeError, "semantic search failed"):
            await dispatch(query="orchard")

    async def test_external_deletion_does_not_delete_vector(self):
        await dispatch()
        self.memory.unlink()
        self.initial = self.snapshot()
        with patch.object(self.engine, "delete_embedding", side_effect=AssertionError("vector deleted")) as delete:
            await dispatch()
            delete.assert_not_called()

    async def test_runtime_event_recording_is_disabled(self):
        self.assertIsNone(rt.record_v3_tool_event("breath", {"query": "audit"}))

    async def test_unexpected_disabled_engine_refuses_fallback(self):
        self.config["embedding"]["enabled"] = True
        with self.assertRaisesRegex(RuntimeError, "engine unavailable"):
            await dispatch(query="orchard")

    async def test_external_new_id_is_visible_after_cached_negative_index(self):
        self.assertIsNone(await self.manager.get('new-external-id'))
        new_path = self.root / 'dynamic' / 'new-external-id.md'
        new_path.write_text('---\nid: new-external-id\ntype: dynamic\nimportance: 5\n---\nnew external memory', encoding='utf-8')
        self.initial = self.snapshot()
        text = await dispatch(query='new-external-id')
        self.assertIn('[exact_bucket_id:true]', text)
        self.assertIn('new external memory', text)


if __name__ == "__main__":
    unittest.main()
