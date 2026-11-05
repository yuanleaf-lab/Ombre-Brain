"""Real MCP subprocess, local reviewer boundary and isolated native worker."""
import asyncio
from datetime import timedelta
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import unittest

import httpx
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

from memory_extended_fixture import ExtendedFixture, ALL_READS
from memory_http import create_http_app
from memory_write_entry import create_write_server, validate_write_settings
from memory_write_journal import WriteJournal
from memory_write_policy import WRITE_TOOLS
from memory_write_service import VersionedReadRuntime


def body(result):
    return '\n'.join(block.text for block in result.content if hasattr(block, 'text'))


class WriteTransportTests(ExtendedFixture, unittest.IsolatedAsyncioTestCase):
    def setup_writes(self):
        self.config['writes'] = {'enabled': True, 'tools': list(WRITE_TOOLS),
                                 'journal_path': str(self.root/'review.sqlite3')}
        self.path = self.root/'write-config.json'
        self.path.write_text(json.dumps(self.config), encoding='utf-8')
        self.entry = Path(__file__).resolve().parents[1]/'src'/'memory_write_entry.py'
        self.settings = validate_write_settings(self.config, self.token)

    async def test_real_stdio_review_native_worker_and_receipt_retry(self):
        self.setup_writes()
        parameters = StdioServerParameters(command=sys.executable,
            args=['-B', str(self.entry), 'serve', '--config', str(self.path)],
            env=dict(os.environ, MEMORY_MCP_TOKEN=self.token, PYTHONDONTWRITEBYTECODE='1'))
        before = self.snapshot()
        async with stdio_client(parameters) as streams:
            async with ClientSession(*streams, read_timeout_seconds=timedelta(seconds=30)) as client:
                await client.initialize()
                listed = (await client.list_tools()).tools
                self.assertEqual({t.name for t in listed}, set(ALL_READS)|set(WRITE_TOOLS))
                self.assertTrue(all(t.inputSchema['additionalProperties'] is False for t in listed))
                for name in ['hold', 'trace', 'letter_write', 'memory_review', 'memory_approve']:
                    self.assertTrue((await client.call_tool(name, {})).isError)
                read = await client.call_tool('memory_read', {'bucket_id': 'ordinary'})
                self.assertFalse(read.isError)
                self.assertRegex(body(read), r'\[memory_version:[a-f0-9]{64}\]')
                plans = await client.call_tool('memory_plan_list', {})
                self.assertIn('plan_versions', body(plans))
                self.assertIn('active-plan', body(plans))
                args = {'request_id': 'transport-1', 'content': 'new reviewed transport event'}
                pending = await client.call_tool('memory_write', args)
                self.assertFalse(pending.isError, body(pending))
                self.assertEqual(json.loads(body(pending))['state'], 'pending')
                self.assertEqual(before, self.snapshot())
                forged = await client.call_tool('memory_write', dict(args, confirmed=True))
                self.assertTrue(forged.isError)
                journal = WriteJournal(self.settings.journal, self.settings.identity)
                row = journal.get('transport-1')
                # Local operator authority, never an argument on an MCP call.
                journal.review('transport-1', row['digest'], approve=True)
                completed = await client.call_tool('memory_write', args)
                self.assertFalse(completed.isError, body(completed))
                receipt = json.loads(body(completed))
                self.assertEqual(receipt['state'], 'finished')
                self.assertEqual(receipt['receipt']['status'], 'succeeded')
                new_id = receipt['receipt']['items'][0]['mutations'][0]['bucket_id']
                self.assertIn('new reviewed transport event', body(await client.call_tool(
                    'memory_read', {'bucket_id': new_id})))
                after = self.snapshot()
                repeated = await client.call_tool('memory_write', args)
                self.assertEqual(body(repeated), body(completed))
                self.assertEqual(after, self.snapshot())

    async def test_noninteractive_review_is_denied_without_approval(self):
        self.setup_writes()
        journal = WriteJournal(self.settings.journal, self.settings.identity)
        journal.submit('transport-1', 'memory_write', {'content': 'pending'}, {})
        result = await asyncio.to_thread(subprocess.run,
            [sys.executable, '-B', str(self.entry), 'review', '--config', str(self.path),
             '--request', 'transport-1'], input='APPROVE anything\n',
            text=True, capture_output=True, timeout=15)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(journal.get('transport-1')['state'], 'pending')

    async def test_missing_launcher_token_creates_no_journal(self):
        self.setup_writes()
        for token in ['', 'incorrect-token-'+'x'*40]:
            result = await asyncio.to_thread(subprocess.run,
                [sys.executable, '-B', str(self.entry), 'serve', '--config', str(self.path)],
                env=dict(os.environ, MEMORY_MCP_TOKEN=token), input='',
                text=True, capture_output=True, timeout=15)
            self.assertNotEqual(result.returncode, 0)
            self.assertNotIn(self.token, result.stdout+result.stderr)
            self.assertFalse(self.settings.journal.exists())

    async def test_http_bearer_denial_and_authenticated_discovery(self):
        self.setup_writes()
        raw = dict(self.config, transport='streamable-http', host='127.0.0.1', port=18765, endpoint='/mcp')
        raw.pop('auth')
        self.path.write_text(json.dumps(raw), encoding='utf-8')
        settings = validate_write_settings(raw, self.token)
        app = create_http_app(settings.http, runtime_factory=VersionedReadRuntime,
            server_factory=lambda _read, **kwargs: create_write_server(settings, config_path=self.path, **kwargs))
        before = self.snapshot()
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://127.0.0.1:18765') as client:
                for headers in ({}, {'Authorization': 'Bearer incorrect'}):
                    response = await client.post('/mcp', headers=headers, json={
                        'jsonrpc': '2.0', 'id': 1, 'method': 'tools/list'})
                    self.assertEqual(response.status_code, 401)
                headers = {'Authorization': 'Bearer '+self.token,
                           'Accept': 'application/json, text/event-stream'}
                response = await client.post('/mcp', headers=headers, json={
                    'jsonrpc': '2.0', 'id': 2, 'method': 'initialize', 'params': {
                        'protocolVersion': '2025-03-26', 'capabilities': {},
                        'clientInfo': {'name': 'synthetic-test', 'version': '1'}}})
                self.assertEqual(response.status_code, 200)
                response = await client.post('/mcp', headers=headers, json={
                    'jsonrpc': '2.0', 'id': 3, 'method': 'tools/list'})
                self.assertEqual(response.status_code, 200)
                self.assertEqual({t['name'] for t in response.json()['result']['tools']},
                                 set(ALL_READS)|set(WRITE_TOOLS))
                response = await client.post('/mcp', headers=headers, json={
                    'jsonrpc': '2.0', 'id': 4, 'method': 'tools/call',
                    'params': {'name': 'memory_plan_create', 'arguments': {
                        'request_id': 'http-plan', 'content': 'synthetic pending HTTP plan'}}})
                self.assertEqual(response.status_code, 200)
                self.assertIn('pending', response.text)
                self.assertEqual(before, self.snapshot())
                journal = WriteJournal(settings.journal, settings.identity)
                pending = journal.get('http-plan')
                journal.review('http-plan', pending['digest'], approve=True)
                response = await client.post('/mcp', headers=headers, json={
                    'jsonrpc': '2.0', 'id': 5, 'method': 'tools/call',
                    'params': {'name': 'memory_plan_create', 'arguments': {
                        'request_id': 'http-plan', 'content': 'synthetic pending HTTP plan'}}})
                self.assertEqual(response.status_code, 200)
                receipt = journal.get('http-plan')
                self.assertEqual(receipt['state'], 'finished')
                self.assertEqual(receipt['receipt']['status'], 'succeeded')


if __name__ == '__main__':
    unittest.main()
