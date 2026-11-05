import hashlib
import json
from pathlib import Path
import secrets
import os
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import httpx
from embedding_engine import EmbeddingEngine
from memory_http import load_http_settings, create_http_app, HTTP_TOKEN_ENV


class MemoryHTTPTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parents[1])
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.vault = self.root / 'vault'
        self.vault.mkdir()
        (self.vault / 'dynamic').mkdir()
        (self.vault / 'dynamic' / 'http123.md').write_text(
            '---\nid: http123\ntype: dynamic\nimportance: 8\n---\nHTTP orchard synthetic memory', encoding='utf-8')
        EmbeddingEngine({'buckets_dir': str(self.vault), 'embedding': {'enabled': False}})
        self.token = secrets.token_urlsafe(32)
        self.raw = {
            'read_only': True, 'transport': 'streamable-http', 'host': '127.0.0.1',
            'port': 18765, 'endpoint': '/mcp', 'buckets_dir': str(self.vault),
            'embedding': {'enabled': False, 'api_format': 'ollama', 'model': 'bge-m3',
                          'dim': 1024, 'base_url': 'http://127.0.0.1:11434/v1'},
        }
        self.path = self.root / 'http.json'
        self.save()

    def save(self):
        self.path.write_text(json.dumps(self.raw), encoding='utf-8')

    def hashes(self):
        return {str(p.relative_to(self.vault)): hashlib.sha256(p.read_bytes()).hexdigest()
                for p in self.vault.rglob('*') if p.is_file()}

    async def test_token_required_and_never_accepted_from_config(self):
        with patch.dict('os.environ', {}, clear=True):
            with self.assertRaises(PermissionError):
                load_http_settings(self.path)
        self.raw['token'] = self.token
        self.save()
        with self.assertRaises(ValueError):
            load_http_settings(self.path, self.token)

    async def test_only_loopback_exact_endpoint_and_http_transport(self):
        for key, value in (('host', '0.0.0.0'), ('host', 'localhost'), ('endpoint', '/other'),
                           ('transport', 'stdio'), ('read_only', False), ('port', True)):
            previous = self.raw[key]
            self.raw[key] = value
            self.save()
            with self.assertRaises(ValueError):
                load_http_settings(self.path, self.token)
            self.raw[key] = previous

    async def test_unauthorized_every_method_and_duplicate_header_rejected(self):
        app = create_http_app(load_http_settings(self.path, self.token))
        before = self.hashes()
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url='http://127.0.0.1:18765') as client:
                for method in ('GET', 'POST', 'DELETE', 'OPTIONS'):
                    for headers in ({}, {'Authorization': 'Bearer incorrect-token'}):
                        response = await client.request(method, '/mcp', headers=headers)
                        self.assertEqual(response.status_code, 401)
                        self.assertIn('Bearer', response.headers['www-authenticate'])
                        self.assertNotIn(self.token, response.text)
                response = await client.post('/mcp', headers=[('Authorization', 'Bearer '+self.token), ('Authorization', 'Bearer '+self.token)])
                self.assertEqual(response.status_code, 401)
                self.assertEqual((await client.get('/mcp?token='+self.token)).status_code, 401)
                self.assertEqual((await client.get('/mcp',headers={'Ombre-MCP-Token':self.token})).status_code, 401)
        self.assertEqual(before, self.hashes())

    async def test_initialize_three_tools_calls_and_no_file_changes(self):
        app = create_http_app(load_http_settings(self.path, self.token))
        before = self.hashes()
        headers = {'Authorization': 'Bearer '+self.token, 'Accept': 'application/json, text/event-stream'}
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url='http://127.0.0.1:18765', headers=headers) as client:
                async def rpc(method, params=None):
                    response = await client.post('/mcp', json={'jsonrpc': '2.0', 'id': 1, 'method': method, 'params': params or {}})
                    self.assertEqual(response.status_code, 200, response.text)
                    return response.json()
                initialized = await rpc('initialize', {'protocolVersion':'2025-03-26', 'capabilities':{}, 'clientInfo':{'name':'unittest','version':'1'}})
                self.assertEqual(initialized['result']['serverInfo']['name'], 'memory')
                listed = await rpc('tools/list')
                self.assertEqual([t['name'] for t in listed['result']['tools']], ['memory_bootstrap','memory_search','memory_read'])
                for name, arguments in (('memory_bootstrap',{}), ('memory_search',{'query':'orchard'}), ('memory_read',{'bucket_id':'http123'})):
                    response = await rpc('tools/call', {'name':name,'arguments':arguments})
                    self.assertFalse(response['result']['isError'])
                    self.assertIn('HTTP orchard', str(response))
                denied = await rpc('tools/call', {'name':'hold','arguments':{'content':'forbidden'}})
                self.assertTrue(denied['result']['isError'])
                bad_host = await client.post('/mcp', headers={'Host':'evil.example'}, json={'jsonrpc':'2.0','id':1,'method':'tools/list'})
                self.assertEqual(bad_host.status_code, 421)
                bad_origin = await client.post('/mcp',headers={'Origin':'https://evil.example'},json={'jsonrpc':'2.0','id':1,'method':'tools/list'})
                self.assertEqual(bad_origin.status_code, 403)
                large = await client.post('/mcp',content=b'x'*70000,headers={'Content-Type':'application/json'})
                self.assertEqual(large.status_code, 413)
        self.assertEqual(before, self.hashes())
        self.assertEqual({str(p.relative_to(self.vault)) for p in self.vault.rglob('*') if p.is_dir()}, {'dynamic'})

    async def test_missing_environment_token_fails_before_listener_or_vault(self):
        before = self.hashes()
        command=[sys.executable,'-B',str(Path(__file__).resolve().parents[1]/'src'/'memory_http.py'),'--config',str(self.path)]
        result=subprocess.run(command,capture_output=True,text=True,encoding='utf-8',timeout=30,
                              env=dict(os.environ, MEMORY_MCP_HTTP_TOKEN='',PYTHONDONTWRITEBYTECODE='1'))
        self.assertNotEqual(result.returncode,0)
        self.assertEqual(result.stdout,'')
        self.assertIn('memory HTTP startup failed',result.stderr)
        self.assertNotIn(self.token,result.stderr)
        self.assertEqual(before,self.hashes())


if __name__ == '__main__':
    unittest.main()
