import hashlib
import json
from pathlib import Path
import secrets
import sys
import tempfile
import unittest
from unittest.mock import patch

from embedding_engine import EmbeddingEngine
from memory_stdio import load_settings, create_server


class MemoryStdioTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parents[1])
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.vault = self.root / 'vault'
        self.vault.mkdir()
        (self.vault / 'dynamic').mkdir()
        (self.vault / 'dynamic' / 'abc123def456.md').write_text(
            '---\nid: abc123def456\ntype: dynamic\nimportance: 8\n---\nstdio synthetic orchard memory', encoding='utf-8')
        EmbeddingEngine({'buckets_dir': str(self.vault), 'embedding': {'enabled': False}})
        self.token = secrets.token_urlsafe(32)
        self.config = {
            'read_only': True, 'transport': 'stdio', 'buckets_dir': str(self.vault),
            'auth': {'token_sha256': hashlib.sha256(self.token.encode()).hexdigest()},
            'embedding': {'enabled': False, 'api_format': 'ollama', 'model': 'bge-m3',
                          'dim': 1024, 'base_url': 'http://127.0.0.1:11434/v1'},
        }
        self.path = self.root / 'memory-test.json'
        self.save()

    def save(self):
        self.path.write_text(json.dumps(self.config), encoding='utf-8')

    def hashes(self):
        return {str(p.relative_to(self.vault)): hashlib.sha256(p.read_bytes()).hexdigest()
                for p in self.vault.rglob('*') if p.is_file()}

    def server(self):
        return create_server(load_settings(self.path, self.token))

    async def test_auth_missing_and_wrong_rejected_before_vault_access(self):
        with patch.dict('os.environ', {}, clear=True), patch('memory_stdio.Path.resolve', side_effect=AssertionError('Vault access before authentication')):
            for token in (None, '', 'incorrect-token-value-that-is-long-enough'):
                with self.assertRaises(PermissionError):
                    load_settings(self.path, token)

    async def test_discovery_is_exactly_three_read_only_strict_tools(self):
        tools = await self.server().list_tools()
        self.assertEqual({t.name for t in tools}, {'memory_bootstrap', 'memory_search', 'memory_read'})
        for tool in tools:
            self.assertTrue(tool.annotations.readOnlyHint)
            self.assertFalse(tool.inputSchema.get('additionalProperties', True))
        self.assertNotIn('server', sys.modules)

    async def test_three_calls_do_not_write_and_missing_id_does_not_search(self):
        before = self.hashes()
        server = self.server()
        async with server.settings.lifespan(server):
            bootstrap = await server.call_tool('memory_bootstrap', {})
            search = await server.call_tool('memory_search', {'query': 'orchard'})
            read = await server.call_tool('memory_read', {'bucket_id': 'abc123def456'})
            for result in (bootstrap, search, read):
                self.assertIn('orchard', str(result))
            from tools import _runtime as rt
            with patch.object(rt.embedding_engine, 'search_similar_strict', side_effect=AssertionError('unexpected vector query')):
                with self.assertRaisesRegex(Exception, 'memory_not_found'):
                    await server.call_tool('memory_read', {'bucket_id': 'does-not-exist'})
            with self.assertRaises(Exception):
                await server.call_tool('hold', {'content': 'forbidden'})
            with self.assertRaises(Exception):
                await server.call_tool('memory_search', {'query': 'orchard', 'delete': True})
            self.assertIsNone(rt.embedding_outbox)
            self.assertFalse(rt.decay_engine.is_running)
            self.assertIsNone(rt.deletion_requests)
        self.assertEqual(before, self.hashes())
        self.assertNotIn('server', sys.modules)

    async def test_special_memory_and_archived_content_are_not_exposed(self):
        (self.vault / 'dynamic' / 'hidden.md').write_text(
            '---\nid: hidden\ntype: self\nimportance: 10\npinned: true\n---\nprivate sentinel', encoding='utf-8')
        (self.vault / 'archive').mkdir()
        (self.vault / 'archive' / 'old-id.md').write_text(
            '---\nid: old-id\ntype: dynamic\nimportance: 8\n---\narchived sentinel', encoding='utf-8')
        server = self.server()
        async with server.settings.lifespan(server):
            self.assertNotIn('private sentinel', str(await server.call_tool('memory_bootstrap', {})))
            self.assertNotIn('private sentinel', str(await server.call_tool('memory_search', {'query': 'sentinel'})))
            with self.assertRaisesRegex(Exception, 'memory_not_found'):
                await server.call_tool('memory_read', {'bucket_id': 'hidden'})
            with self.assertRaisesRegex(Exception, 'memory_not_found'):
                await server.call_tool('memory_read', {'bucket_id': 'old-id'})

    async def test_default_logging_fallback_remains_lazy_and_compatible(self):
        import utils
        with patch.dict('os.environ', {}, clear=True), patch('utils.tempfile.gettempdir', return_value=str(self.root)) as probe:
            self.assertEqual(utils._resolve_log_dir('explicit'), 'explicit')
            probe.assert_not_called()
            self.assertEqual(utils._resolve_log_dir(None), str(self.root / 'ombre_logs'))
            probe.assert_called_once()

    async def test_read_only_false_and_nonstdio_fail_closed(self):
        for field, value in (('read_only', False), ('read_only', 'true'), ('transport', 'streamable-http')):
            original = self.config[field]
            self.config[field] = value
            self.save()
            with self.assertRaises(ValueError):
                load_settings(self.path, self.token)
            self.config[field] = original

    async def test_independent_config_rejects_unknown_options_and_external_provider(self):
        self.config['github_sync'] = {'enabled': True}
        self.save()
        with self.assertRaises(ValueError):
            load_settings(self.path, self.token)
        self.config.pop('github_sync')
        self.config['embedding']['base_url'] = 'https://example.com/v1'
        self.save()
        with self.assertRaises(ValueError):
            load_settings(self.path, self.token)

    async def test_legacy_environment_cannot_override_independent_config(self):
        with patch.dict('os.environ', {'OMBRE_VAULT_DIR': 'invalid', 'OMBRE_OLLAMA_URL': 'https://example.com/v1'}):
            server = self.server()
            async with server.settings.lifespan(server):
                from tools import _runtime as rt
                self.assertEqual(Path(rt.config['buckets_dir']), self.vault)
                self.assertIn('orchard', str(await server.call_tool('memory_read', {'bucket_id': 'abc123def456'})))


if __name__ == '__main__':
    unittest.main()
