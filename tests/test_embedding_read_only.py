import sqlite3
import tempfile
import unittest
from unittest.mock import AsyncMock, patch
from pathlib import Path
from embedding_engine import EmbeddingEngine


class EmbeddingReadOnlyTests(unittest.TestCase):
    def config(self, root):
        return {"read_only": True, "buckets_dir": str(root), "embedding": {"enabled": False}}

    def test_missing_fails_closed(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parents[1]) as tmp:
            root = Path(tmp)
            with self.assertRaisesRegex(RuntimeError, "missing"):
                EmbeddingEngine(self.config(root))
            self.assertEqual(list(root.iterdir()), [])

    def test_valid_database_stays_unchanged(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parents[1]) as tmp:
            root = Path(tmp)
            db = root / "embeddings.db"
            conn = sqlite3.connect(db)
            try:
                conn.execute("CREATE TABLE embeddings (bucket_id TEXT, embedding TEXT, meaning_embedding TEXT, content_hash TEXT)")
                conn.execute("CREATE TABLE embeddings_meta (key TEXT, value TEXT)")
                conn.commit()
            finally:
                conn.close()
            before = db.read_bytes()
            engine = EmbeddingEngine(self.config(root))
            self.assertEqual(engine.list_all_ids(), [])
            self.assertEqual(db.read_bytes(), before)
            self.assertEqual({p.name for p in root.iterdir()}, {"embeddings.db"})

    def test_corrupt_database_fails_without_replacement(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parents[1]) as tmp:
            root = Path(tmp)
            db = root / "embeddings.db"
            db.write_bytes(b"corrupt database")
            with self.assertRaises(sqlite3.DatabaseError):
                EmbeddingEngine(self.config(root))
            self.assertEqual(db.read_bytes(), b"corrupt database")
            self.assertEqual({p.name for p in root.iterdir()}, {"embeddings.db"})

    def test_incompatible_schema_fails_without_migration(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parents[1]) as tmp:
            root = Path(tmp)
            db = root / "embeddings.db"
            conn = sqlite3.connect(db)
            conn.execute("CREATE TABLE embeddings (bucket_id TEXT)")
            conn.commit()
            conn.close()
            before = db.read_bytes()
            with self.assertRaisesRegex(RuntimeError, "schema is incompatible"):
                EmbeddingEngine(self.config(root))
            self.assertEqual(db.read_bytes(), before)

    def test_invalid_provider_cannot_write_startup_error_log(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parents[1]) as tmp:
            root = Path(tmp)
            config = self.config(root)
            config["embedding"] = {"enabled": True, "api_key": "invalid-\u4e2d"}
            with patch("embedding_engine._record_startup_e001") as record:
                with self.assertRaisesRegex(RuntimeError, "configuration is invalid"):
                    EmbeddingEngine(config)
                record.assert_not_called()
            self.assertEqual(list(root.iterdir()), [])


class ReadOnlyProviderFailureTests(unittest.IsolatedAsyncioTestCase):
    async def test_real_backend_failure_never_records_persistent_error(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parents[1]) as tmp:
            root = Path(tmp)
            EmbeddingEngine({"buckets_dir": str(root), "embedding": {"enabled": False}})
            config = {"read_only": True, "buckets_dir": str(root), "embedding": {
                "enabled": True, "api_format": "ollama", "model": "bge-m3",
                "base_url": "http://127.0.0.1:11434/v1",
            }}
            engine = EmbeddingEngine(config)
            before = {p.name: p.read_bytes() for p in root.rglob('*') if p.is_file()}
            try:
                with patch.object(engine._backend._client.embeddings, "create", AsyncMock(side_effect=OSError("offline"))):
                    with patch("errors.record_error") as record:
                        with self.assertRaises(RuntimeError):
                            await engine._generate_async("provider failure audit")
                        record.assert_not_called()
                self.assertEqual(before, {p.name: p.read_bytes() for p in root.rglob('*') if p.is_file()})
            finally:
                await engine._backend._client.close()


if __name__ == "__main__":
    unittest.main()
