"""Real MCP stdio and in-process authenticated HTTP; never opens a listener."""
import json
import asyncio
import hashlib
import multiprocessing
import os
from pathlib import Path
import sys
import unittest

import httpx
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

from memory_extended_fixture import ALL_READS, ExtendedFixture, normal_writer_worker
from memory_http import create_http_app, load_http_settings


CALLS = [('memory_bootstrap', {}, 'public core'),
         ('memory_search', {'query': 'orchard'}, 'public ordinary'),
         ('memory_read', {'bucket_id': 'ordinary'}, 'public ordinary'),
         ('memory_catalog', {}, 'ordinary'),
         ('memory_status', {}, 'read_only'),
         ('memory_feel_search', {'query': 'quiet'}, 'assistant feeling'),
         ('memory_plan_list', {}, 'active plan sentinel'),
         ('memory_self_read', {}, 'formal self sentinel')]


class ExtendedProtocolTests(ExtendedFixture, unittest.IsolatedAsyncioTestCase):
    async def test_stdio_handshake_eight_tools_and_read_guard(self):
        path = self.root / 'stdio.json'
        path.write_text(json.dumps(self.config), encoding='utf-8')
        parameters = StdioServerParameters(
            command=sys.executable,
            args=['-B', str(Path(__file__).resolve().parents[1] / 'src' / 'memory_stdio.py'),
                  '--config', str(path)],
            env=dict(os.environ, MEMORY_MCP_TOKEN=self.token, PYTHONDONTWRITEBYTECODE='1'))
        before = self.snapshot()
        async with stdio_client(parameters) as (read, write):
            async with ClientSession(read, write) as session:
                initialized = await session.initialize()
                self.assertEqual(initialized.serverInfo.name, 'memory')
                self.assertEqual([t.name for t in (await session.list_tools()).tools], ALL_READS)
                for name, arguments, expected in CALLS:
                    result = await session.call_tool(name, arguments)
                    self.assertFalse(result.isError, name)
                    self.assertIn(expected, str(result))
                for name in ['hold', 'trace', 'letter_read', 'memory_write']:
                    self.assertTrue((await session.call_tool(name, {})).isError)
                forged = await session.call_tool('memory_self_read', {'promote': 'candidate', 'approved': True})
                self.assertTrue(forged.isError)
        self.assertEqual(before, self.snapshot())

    async def test_stdio_reader_and_normal_native_writer_coexist(self):
        path = self.root / 'concurrent-stdio.json'
        path.write_text(json.dumps(self.config), encoding='utf-8')
        context = multiprocessing.get_context('spawn')
        parent, child = context.Pipe()
        writer = context.Process(target=normal_writer_worker, args=(str(self.vault), child))
        writer.start()
        child.close()

        async def receive():
            self.assertTrue(await asyncio.to_thread(parent.poll, 20), 'native writer timed out')
            result = parent.recv()
            self.assertNotIn('error', result)
            return result

        def application_files():
            # Filter coordination files BEFORE opening them: Windows may deny
            # raw reads of a locked SHM byte range during a normal transaction.
            files = {}
            for path in self.vault.rglob('*'):
                if path.is_file() and not path.name.endswith(('-wal', '-shm')):
                    files[str(path.relative_to(self.vault))] = hashlib.sha256(path.read_bytes()).hexdigest()
            dirs = sorted(str(p.relative_to(self.vault)) for p in self.vault.rglob('*') if p.is_dir())
            return files, dirs

        try:
            self.assertTrue((await receive())['ready'])
            before = application_files()
            parameters = StdioServerParameters(command=sys.executable,
                args=['-B', str(Path(__file__).resolve().parents[1] / 'src' / 'memory_stdio.py'),
                      '--config', str(path)],
                env=dict(os.environ, MEMORY_MCP_TOKEN=self.token, PYTHONDONTWRITEBYTECODE='1'))
            async with stdio_client(parameters) as (read, write):
                async with ClientSession(read, write) as session:
                    await session.initialize()
                    for name, args, expected in CALLS:
                        result = await session.call_tool(name, args)
                        self.assertFalse(result.isError, name)
                        self.assertIn(expected, str(result))
                    # The writer still owns an uncommitted WAL transaction.
                    self.assertEqual(before, application_files())
                    parent.send('commit')
                    committed = await receive()
                    self.assertEqual(committed['integrity'], 'ok')
                    self.assertEqual(committed['dimension'], 1024)
                    self.assertEqual(committed['content_hash'],
                                     hashlib.sha256(b'native writer committed sentinel').hexdigest())
                    after_writer = application_files()
                    read_new = await session.call_tool('memory_read', {'bucket_id': committed['id']})
                    self.assertFalse(read_new.isError)
                    self.assertIn('native writer committed sentinel', str(read_new))
                    self.assertEqual(after_writer, application_files())
            parent.send('finish')
            await asyncio.to_thread(writer.join, 10)
            self.assertEqual(writer.exitcode, 0)
            state = self.snapshot()[2]
            self.assertEqual(state['integrity'], ('ok',))
            rows = [row for row in state['data'] if row[0] == 'ordinary']
            self.assertEqual(rows[0][2], 'pending-writer')
        finally:
            if writer.is_alive():
                writer.terminate()
                await asyncio.to_thread(writer.join, 5)
            parent.close()

    async def test_http_same_grants_authentication_and_denials(self):
        raw = dict(self.config, transport='streamable-http', host='127.0.0.1', port=18765, endpoint='/mcp')
        raw.pop('auth')
        # A restricted credential/service profile: no Feel permission.
        raw['permissions'] = {'tools': [n for n in ALL_READS if n != 'memory_feel_search'],
                              'write_enabled': False}
        path = self.root / 'http.json'
        path.write_text(json.dumps(raw), encoding='utf-8')
        app = create_http_app(load_http_settings(path, self.token))
        headers = {'Authorization': 'Bearer ' + self.token, 'Accept': 'application/json, text/event-stream'}
        before = self.snapshot()
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app),
                                         base_url='http://127.0.0.1:18765', headers=headers) as client:
                async def rpc(method, params=None):
                    response = await client.post('/mcp', json={'jsonrpc': '2.0', 'id': 1,
                                                               'method': method, 'params': params or {}})
                    self.assertEqual(response.status_code, 200)
                    return response.json()['result']
                await rpc('initialize', {'protocolVersion': '2025-03-26', 'capabilities': {},
                                         'clientInfo': {'name': 'permission-test', 'version': '1'}})
                expected_names = [n for n in ALL_READS if n != 'memory_feel_search']
                listed = await rpc('tools/list')
                self.assertEqual([t['name'] for t in listed['tools']], expected_names)
                for name, arguments, expected in CALLS:
                    if name not in expected_names:
                        continue
                    result = await rpc('tools/call', {'name': name, 'arguments': arguments})
                    self.assertFalse(result['isError'], name)
                    self.assertIn(expected, str(result))
                for name in ['memory_feel_search', 'letter_read', 'memory_write', 'trace']:
                    result = await rpc('tools/call', {'name': name, 'arguments': {'query': 'quiet'}})
                    self.assertTrue(result['isError'])
                for credential in ['', 'Bearer wrong-token']:
                    response = await client.post('/mcp', headers={'Authorization': credential},
                                                 json={'jsonrpc': '2.0', 'id': 1, 'method': 'tools/list'})
                    self.assertEqual(response.status_code, 401)
        self.assertEqual(before, self.snapshot())


if __name__ == '__main__':
    unittest.main()
