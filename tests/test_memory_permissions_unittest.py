import copy
import json
import logging
import sqlite3
import unittest
from unittest.mock import AsyncMock, Mock, patch

from memory_extended_fixture import ALL_READS, ExtendedFixture
from memory_stdio import MemorySettings, MemoryRuntime, create_server, validate_settings


class PermissionTests(ExtendedFixture, unittest.IsolatedAsyncioTestCase):
    async def test_opt_in_discovery_matches_calls_and_defaults_stay_three(self):
        self.config['permissions']['tools'] = ['memory_catalog', 'memory_status']
        settings = validate_settings(self.config, self.token)
        server = create_server(settings)
        self.assertEqual([t.name for t in await server.list_tools()], ['memory_catalog', 'memory_status'])
        before = self.snapshot()
        async with server.settings.lifespan(server):
            for name, args in [('memory_search', {'query': 'orchard'}),
                               ('memory_feel_search', {'query': 'quiet'}), ('trace', {}),
                               ('letter_read', {}), ('memory_write', {})]:
                with self.assertRaises(Exception):
                    await server.call_tool(name, args)
            self.assertIn('ordinary', str(await server.call_tool('memory_catalog', {})))
            self.assertIn('read_only', str(await server.call_tool('memory_status', {})))
        self.assertEqual(before, self.snapshot())
        self.config.pop('permissions')
        default = create_server(validate_settings(self.config, self.token))
        self.assertEqual([t.name for t in await default.list_tools()],
                         ['memory_bootstrap', 'memory_search', 'memory_read'])

    async def test_write_unknown_grants_and_fake_approval_fail_before_vault_open(self):
        for permission in [{'write_enabled': True}, {'write_enabled': 'false'},
                           {'tools': ['trace']}, {'tools': ['memory_write']},
                           {'tools': ['letter_read']}, {'approved': True},
                           {'tools': 'memory_status'}, {'tools': ['memory_status', 'memory_status']}]:
            config = copy.deepcopy(self.config)
            config['permissions'] = permission
            with patch('memory_stdio.Path.resolve', side_effect=AssertionError('premature Vault access')):
                with self.assertRaises(ValueError):
                    validate_settings(config, self.token)

    async def test_direct_runtime_call_cannot_bypass_discovery_policy(self):
        self.config['permissions']['tools'] = ['memory_status']
        runtime = MemoryRuntime(validate_settings(self.config, self.token))
        # Authorization must happen before any opened runtime is required.
        with self.assertRaises(PermissionError):
            await runtime.invoke('memory_feel_search', query='quiet')

    async def test_request_cannot_change_policy_or_claim_approval(self):
        server = create_server(MemorySettings(self.config))
        async with server.settings.lifespan(server):
            for args in [{'query': 'orchard', 'approved': True},
                         {'query': 'orchard', 'permissions': {'tools': ALL_READS}},
                         {'query': 'orchard', 'delete': True}]:
                with self.assertRaises(Exception):
                    await server.call_tool('memory_search', args)
        for tool in await server.list_tools():
            self.assertTrue(tool.annotations.readOnlyHint)
            self.assertFalse(tool.annotations.destructiveHint)
            self.assertFalse(tool.inputSchema['additionalProperties'])

    async def test_mutating_source_config_does_not_expand_frozen_grant(self):
        self.config['permissions']['tools'] = ['memory_status']
        settings = MemorySettings(self.config)
        runtime = MemoryRuntime(settings)
        server = create_server(settings, runtime=runtime)
        self.config['permissions']['tools'].append('memory_feel_search')
        self.assertEqual([t.name for t in await server.list_tools()], ['memory_status'])
        with self.assertRaises(PermissionError):
            await runtime.invoke('memory_feel_search', query='quiet')

    async def test_empty_grant_exposes_and_executes_nothing(self):
        self.config['permissions']['tools'] = []
        server = create_server(validate_settings(self.config, self.token))
        self.assertEqual(await server.list_tools(), [])
        with self.assertRaises(Exception):
            await server.call_tool('memory_bootstrap', {})


class ExtendedReadTests(ExtendedFixture, unittest.IsolatedAsyncioTestCase):
    def server(self):
        return create_server(MemorySettings(self.config))

    async def test_eight_real_chains_leave_files_schema_and_data_unchanged(self):
        before = self.snapshot()
        server = self.server()
        self.assertEqual([t.name for t in await server.list_tools()], ALL_READS)
        with self.no_writes():
            async with server.settings.lifespan(server):
                expected = [
                    ('memory_bootstrap', {}, 'public core'),
                    ('memory_search', {'query': 'orchard'}, 'public ordinary'),
                    ('memory_read', {'bucket_id': 'ordinary'}, 'public ordinary'),
                    ('memory_catalog', {}, 'ordinary'),
                    ('memory_status', {}, 'read_only'),
                    ('memory_feel_search', {'query': 'quiet'}, 'assistant feeling'),
                    ('memory_plan_list', {}, 'active plan sentinel'),
                    ('memory_self_read', {}, 'formal self sentinel'),
                ]
                for name, args, text in expected:
                    result = str(await server.call_tool(name, args))
                    self.assertIn(text, result, name)
                    for secret in ['LETTER PRIVATE SENTINEL', 'DISGUISED LETTER SECRET',
                                   'ARCHIVED PRIVATE SENTINEL', 'MISPLACED FEEL SECRET']:
                        self.assertNotIn(secret, result, name)
                from tools import _runtime as rt
                self.assertIsNone(rt.embedding_outbox)
                self.assertIsNone(rt.dehydrator)
                self.assertIsNone(rt.deletion_requests)
                self.assertFalse(rt.decay_engine.is_running)
        self.assertEqual(before, self.snapshot())

    async def test_status_contains_only_allowlisted_ordinary_aggregates(self):
        server = self.server()
        async with server.settings.lifespan(server):
            result = await server.call_tool('memory_status', {})
            blocks = result[0] if isinstance(result, tuple) else result
            data = json.loads(blocks[0].text)
            self.assertEqual(set(data), {'read_only', 'counts', 'embedding', 'maintenance'})
            self.assertEqual(data['counts'], {'dynamic': 1, 'permanent': 1})
            self.assertEqual(set(data['embedding']), {'enabled', 'model', 'dim'})
            self.assertNotIn(str(self.vault), str(result))
            self.assertNotIn(self.token, str(result))

    async def test_special_scope_does_not_expand_ordinary_read_or_catalog(self):
        server = self.server()
        async with server.settings.lifespan(server):
            for name in ['feeling', 'self-entry', 'candidate', 'private-letter', 'disguised-letter']:
                with self.assertRaises(Exception):
                    await server.call_tool('memory_read', {'bucket_id': name})
            catalog = str(await server.call_tool('memory_catalog', {}))
            for name in ['candidate', 'self-entry', 'feeling', 'private-letter']:
                self.assertNotIn(name, catalog)
            self.assertNotIn('feeling', str(await server.call_tool('memory_catalog', {'domain': 'feel'})))
            with self.assertRaises(Exception):
                await server.call_tool('memory_catalog', {'memory_types': ['feel']})

    async def test_plans_mean_active_only_and_self_candidates_are_explicit(self):
        server = self.server()
        async with server.settings.lifespan(server):
            plan = str(await server.call_tool('memory_plan_list', {}))
            self.assertIn('active plan sentinel', plan)
            self.assertNotIn('resolved plan SECRET', plan)
            self.assertNotIn('abandoned plan SECRET', plan)
            own = str(await server.call_tool('memory_self_read', {}))
            self.assertIn('candidate self sentinel', own)
            self.assertIn('这些还不是自我认知', own)
            self.assertIn('0/3', own)
            feel = str(await server.call_tool('memory_feel_search', {'query': 'quiet'}))
            self.assertIn('legacy_unspecified', feel)

    async def test_conflicting_self_and_other_special_markers_are_never_declassified(self):
        self.put('dynamic', 'conflicting-i', 'i', 'CONFLICTING I CANDIDATE SECRET',
                 tags=['__i__', '__i_candidate__'], i_stage='candidate')
        self.put('dynamic', 'conflicting-feel', 'dynamic', 'CONFLICTING FEEL CANDIDATE SECRET',
                 tags=['__feel__', '__i_candidate__'], i_stage='candidate')
        before = self.snapshot()
        server = self.server()
        with self.no_writes():
            async with server.settings.lifespan(server):
                for name, args in [('memory_self_read', {}), ('memory_catalog', {}),
                                   ('memory_bootstrap', {}), ('memory_search', {'query': 'CONFLICTING'})]:
                    text = str(await server.call_tool(name, args))
                    self.assertNotIn('CONFLICTING I CANDIDATE SECRET', text)
                    self.assertNotIn('CONFLICTING FEEL CANDIDATE SECRET', text)
        self.assertEqual(before, self.snapshot())

    async def test_feel_provider_and_database_failures_refuse_literal_fallback(self):
        settings = MemorySettings(self.config)
        runtime = MemoryRuntime(settings)
        server = create_server(settings, runtime=runtime)
        before = self.snapshot()
        with self.no_writes():
            async with server.settings.lifespan(server):
                from tools import _runtime as rt
                rt.config['embedding']['enabled'] = True
                engine = runtime.engine
                with patch.object(engine, 'enabled', True):
                    for error in [sqlite3.DatabaseError('private error body'), RuntimeError('provider unavailable')]:
                        with patch.object(engine, 'search_similar_strict', side_effect=error):
                            with self.assertRaisesRegex(Exception, 'read-only feel'):
                                await server.call_tool('memory_feel_search', {'query': 'quiet'})
        self.assertEqual(before, self.snapshot())

    async def test_real_vector_scoring_is_limited_before_ranking(self):
        settings = MemorySettings(self.config)
        runtime = MemoryRuntime(settings)
        server = create_server(settings, runtime=runtime)
        before = self.snapshot()
        async with server.settings.lifespan(server):
            from tools import _runtime as rt
            # Only the external model is replaced; native SQLite decoding,
            # candidate filtering, cosine scoring and rendering are exercised.
            backend = Mock(generate_async=AsyncMock(return_value=[1.0] + [0.0] * 1023))
            with patch.object(runtime.engine, 'enabled', True), patch.object(runtime.engine, '_backend', backend):
                rt.config['embedding']['enabled'] = True
                result = str(await server.call_tool('memory_feel_search', {'query': 'unrelated semantic query'}))
                self.assertIn('assistant feeling sentinel', result)
                self.assertNotIn('PRIVATE SENTINEL', result)
                self.assertNotIn('MISPLACED', result)
                backend.generate_async.return_value = [0.0, 1.0] + [0.0] * 1022
                empty = str(await server.call_tool('memory_feel_search', {'query': 'valid zero similarity query'}))
                self.assertIn('没有和', empty)
                self.assertNotIn('assistant feeling sentinel', empty)
        self.assertEqual(before, self.snapshot())

    async def test_native_default_feel_failure_still_has_its_original_fallback(self):
        from tools import _runtime as rt
        from tools.breath.feel import _semantic_hits
        engine = Mock(enabled=True, search_similar=AsyncMock(side_effect=RuntimeError('offline')))
        with patch.multiple(rt, config={'read_only': False}, embedding_engine=engine,
                            logger=logging.getLogger('normal-feel-test')):
            scores, notice = await _semantic_hits('quiet', {'feeling'})
            self.assertEqual(scores, {})
            self.assertTrue(notice)

    async def test_extended_read_errors_are_not_successful_empty_results(self):
        settings = MemorySettings(self.config)
        runtime = MemoryRuntime(settings)
        server = create_server(settings, runtime=runtime)
        before = self.snapshot()
        with self.no_writes():
            async with server.settings.lifespan(server):
                with patch.object(runtime.view.manager, 'list_all', side_effect=OSError('PRIVATE PATH OR SECRET')):
                    for name, args in [('memory_catalog', {}), ('memory_status', {}),
                                       ('memory_feel_search', {'query': 'quiet'}),
                                       ('memory_plan_list', {}), ('memory_self_read', {})]:
                        with self.assertRaises(Exception) as caught:
                            await server.call_tool(name, args)
                        self.assertNotIn('PRIVATE PATH OR SECRET', str(caught.exception))
        self.assertEqual(before, self.snapshot())

    async def test_feel_unexpected_disabled_engine_fails_closed(self):
        server = self.server()
        async with server.settings.lifespan(server):
            from tools import _runtime as rt
            rt.config['embedding']['enabled'] = True
            with self.assertRaisesRegex(Exception, 'read-only feel'):
                await server.call_tool('memory_feel_search', {'query': 'quiet'})

    async def test_unauthorized_vectors_cannot_crowd_out_ordinary_results(self):
        conn = sqlite3.connect(self.vault / 'embeddings.db')
        try:
            for number in range(80):
                conn.execute('INSERT INTO embeddings(bucket_id,embedding,updated_at) VALUES (?,?,?)',
                             (f'hidden-vector-{number}', json.dumps([1.0] + [0.0] * 1023), '2026-11-04'))
            conn.execute('UPDATE embeddings SET embedding=? WHERE bucket_id IN (?,?)',
                         (json.dumps([0.9, 0.1] + [0.0] * 1022), 'ordinary', 'core'))
            conn.commit()
        finally:
            conn.close()
        before = self.snapshot()
        settings = MemorySettings(self.config)
        runtime = MemoryRuntime(settings)
        server = create_server(settings, runtime=runtime)
        async with server.settings.lifespan(server):
            backend = Mock(generate_async=AsyncMock(return_value=[1.0] + [0.0] * 1023))
            with patch.object(runtime.engine, 'enabled', True), patch.object(runtime.engine, '_backend', backend):
                runtime.config['embedding']['enabled'] = True
                from memory_read_tools import ScopedEmbeddings
                pairs = await ScopedEmbeddings(runtime.engine, runtime.view).search_similar_strict(
                    'unrelated vector query', top_k=2)
                self.assertEqual({pair[0] for pair in pairs}, {'ordinary', 'core'})
                text = str(await server.call_tool('memory_search', {'query': 'unrelated vector query', 'max_results': 1}))
                self.assertIn('public core sentinel', text)
                self.assertNotIn('hidden-vector', text)
        self.assertEqual(before, self.snapshot())


if __name__ == '__main__':
    unittest.main()
