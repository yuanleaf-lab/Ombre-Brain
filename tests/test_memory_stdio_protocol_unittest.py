"""Actual MCP handshake/calls through a child process, without HTTP."""
from datetime import timedelta
import hashlib
import json
import os
from pathlib import Path
import secrets
import subprocess
import sys
import tempfile
import unittest

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from embedding_engine import EmbeddingEngine


class MemoryProtocolTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parents[1])
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.vault = self.root / 'vault'
        self.vault.mkdir()
        (self.vault / 'dynamic').mkdir()
        (self.vault / 'dynamic' / 'protocol123.md').write_text(
            '---\nid: protocol123\ntype: dynamic\nimportance: 8\n---\nprotocol orchard sentinel', encoding='utf-8')
        EmbeddingEngine({'buckets_dir': str(self.vault), 'embedding': {'enabled': False}})
        self.token = secrets.token_urlsafe(32)
        self.config = self.root / 'memory.json'
        self.config.write_text(json.dumps({
            'read_only': True, 'transport': 'stdio', 'buckets_dir': str(self.vault),
            'auth': {'token_sha256': hashlib.sha256(self.token.encode()).hexdigest()},
            'embedding': {'enabled': False, 'api_format': 'ollama', 'model': 'bge-m3',
                          'dim': 1024, 'base_url': 'http://127.0.0.1:11434/v1'},
        }), encoding='utf-8')
        self.repo = Path(__file__).resolve().parents[1]
        self.command = [sys.executable, '-B', '-u', str(self.repo / 'src' / 'memory_stdio.py'), '--config', str(self.config)]

    def hashes(self):
        return {str(p.relative_to(self.vault)): hashlib.sha256(p.read_bytes()).hexdigest()
                for p in self.vault.rglob('*') if p.is_file()}

    async def test_actual_stdio_discovery_calls_and_denials(self):
        before = self.hashes()
        params = StdioServerParameters(command=self.command[0], args=self.command[1:], cwd=self.repo,
                    env={'MEMORY_MCP_TOKEN': self.token, 'PYTHONDONTWRITEBYTECODE': '1', 'PYTHONIOENCODING': 'utf-8'})
        with open(os.devnull, 'w') as errors:
            async with stdio_client(params, errlog=errors) as streams:
                async with ClientSession(*streams, read_timeout_seconds=timedelta(seconds=30)) as client:
                    initialized = await client.initialize()
                    self.assertEqual(initialized.serverInfo.name, 'memory')
                    listed = await client.list_tools()
                    self.assertEqual([t.name for t in listed.tools], ['memory_bootstrap', 'memory_search', 'memory_read'])
                    for tool in listed.tools:
                        self.assertTrue(tool.annotations.readOnlyHint)
                        self.assertFalse(tool.inputSchema['additionalProperties'])
                    for name, args in (
                        ('memory_bootstrap', {}), ('memory_search', {'query': 'orchard'}),
                        ('memory_read', {'bucket_id': 'protocol123'}),
                    ):
                        response = await client.call_tool(name, args)
                        self.assertFalse(response.isError, str(response))
                        self.assertIn('orchard sentinel', str(response))
                    for name, args in (
                        ('hold', {'content': 'forbidden'}), ('memory_read', {'bucket_id': 'missing-id'}),
                        ('memory_read', {'bucket_id': '../protocol123'}),
                        ('memory_search', {'query': 'orchard', 'delete': True}),
                        ('memory_bootstrap', {'query': 'unexpected'}),
                    ):
                        response = await client.call_tool(name, args)
                        self.assertTrue(response.isError, name)
                    self.assertEqual((await client.list_resources()).resources, [])
                    self.assertEqual((await client.list_prompts()).prompts, [])
        self.assertEqual(before, self.hashes())
        self.assertEqual({str(p.relative_to(self.vault)) for p in self.vault.rglob('*') if p.is_dir()}, {'dynamic'})

    async def test_missing_or_wrong_launcher_credential_exits_without_protocol_or_writes(self):
        before = self.hashes()
        for token in ('', 'wrong-token-that-is-at-least-32-characters'):
            env = dict(os.environ, MEMORY_MCP_TOKEN=token, PYTHONDONTWRITEBYTECODE='1')
            result = subprocess.run(self.command, input='', text=True, encoding='utf-8',
                                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env, timeout=30)
            self.assertNotEqual(result.returncode, 0)
            self.assertEqual(result.stdout, '')
            self.assertIn('memory startup failed', result.stderr)
            self.assertNotIn(self.token, result.stderr)
        self.assertEqual(before, self.hashes())


if __name__ == '__main__':
    unittest.main()
