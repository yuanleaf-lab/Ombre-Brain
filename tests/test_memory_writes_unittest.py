"""Synthetic temporary Vaults only; no installed services or paid providers."""
import tempfile
from pathlib import Path
import unittest
from contextlib import closing
import copy
import asyncio
import hashlib
import json
import logging
import sqlite3
import os
import subprocess
import sys
from unittest.mock import patch, AsyncMock

from memory_extended_fixture import ExtendedFixture, ALL_READS
from memory_stdio import MemoryRuntime, validate_settings
from memory_write_service import WriteCoordinator, WriteSettings
from memory_write_executor import execute_approved
from memory_write_executor import ReviewedManager, ExistingEmbeddingEngine
from memory_write_policy import WRITE_TOOLS, revision
from memory_write_entry import validate_write_settings, create_write_server

from memory_write_policy import validate_request
from memory_write_journal import WriteJournal


class WritePolicyTests(unittest.TestCase):
    def test_delimiter_whitespace_and_control_characters_cannot_smuggle_special_tags(self):
        for payload in [
            {'tags': ['normal,__i__']}, {'tags': [' __plan__']},
            {'tags': ['\x00__letter__']}, {'domain': ['plan ']},
        ]:
            with self.assertRaises(ValueError):
                validate_request('memory_write', {'content': 'synthetic ordinary event', **payload})

    def test_forged_approval_and_native_passthrough_are_rejected(self):
        for extra in ({'confirmed': True}, {'approved': True}, {'feel': True},
                      {'pinned': True}, {'delete': True}, {'metadata': {'type': 'letter'}}):
            with self.assertRaises(ValueError):
                validate_request('memory_write', {'content': 'synthetic event', **extra})

    def test_updates_require_full_id_revision_and_whitelist(self):
        with self.assertRaises(ValueError):
            validate_request('memory_update', {'bucket_id': 'event', 'content': 'change'})
        with self.assertRaises(ValueError):
            validate_request('memory_update', {'bucket_id': '../event', 'expected_version': 'a'*64,
                                                'changes': {'content': 'change'}})
        with self.assertRaises(ValueError):
            validate_request('memory_plan_update', {'bucket_id': 'event', 'expected_version': 'a'*64,
                                                     'changes': {'delete': True}})


class JournalTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parents[1])
        self.addCleanup(self.temp.cleanup)
        self.store = WriteJournal(Path(self.temp.name)/'reviews.sqlite3', 'synthetic-vault')

    def test_request_retry_is_idempotent_and_payload_is_bound(self):
        row = self.store.submit('req-1', 'memory_write', {'content': 'event'}, {})
        again = self.store.submit('req-1', 'memory_write', {'content': 'event'}, {})
        self.assertEqual(row, again)
        with self.assertRaises(ValueError):
            self.store.submit('req-1', 'memory_write', {'content': 'different'}, {})
        self.assertIsNone(self.store.claim('req-1'))


    def test_only_reviewed_digest_can_be_approved_and_claimed_once(self):
        row = self.store.submit('req-1', 'memory_write', {'content': 'event'}, {})
        with self.assertRaises(ValueError):
            self.store.review('req-1', 'f'*64, approve=True)
        self.store.review('req-1', row['digest'], approve=True)
        self.assertIsNotNone(self.store.claim('req-1'))
        self.assertIsNone(self.store.claim('req-1'))
        self.assertEqual(self.store.get('req-1')['state'], 'running')

    def test_receipt_is_durable_and_rejected_requests_do_not_execute(self):
        row = self.store.submit('req-1', 'memory_write', {'content': 'event'}, {})
        self.store.review('req-1', row['digest'], approve=False)
        self.assertIsNone(self.store.claim('req-1'))
        other = WriteJournal(self.store.path, 'synthetic-vault')
        self.assertEqual(other.get('req-1')['state'], 'rejected')

    def test_expired_approval_cannot_be_claimed(self):
        row = self.store.submit('req-1', 'memory_write', {'content': 'event'}, {})
        self.store.review('req-1', row['digest'], approve=True)
        with self.store.connect() as db:
            db.execute('UPDATE requests SET reviewed=0')
        self.assertIsNone(self.store.claim('req-1'))
        self.assertEqual(WriteCoordinator.public(self.store.get('req-1'))['state'], 'approval_expired')

    def test_crash_after_claim_cannot_be_replayed_by_a_new_process(self):
        row = self.store.submit('req-1', 'memory_write', {'content': 'event'}, {})
        self.store.review('req-1', row['digest'], approve=True)
        self.assertIsNotNone(self.store.claim('req-1'))
        other = WriteJournal(self.store.path, 'synthetic-vault')
        self.assertIsNone(other.claim('req-1'))

    def test_concurrent_claims_have_only_one_winner(self):
        from concurrent.futures import ThreadPoolExecutor
        row = self.store.submit('req-1', 'memory_write', {'content': 'event'}, {})
        self.store.review('req-1', row['digest'], approve=True)
        with ThreadPoolExecutor(max_workers=4) as pool:
            results = list(pool.map(lambda _: self.store.claim('req-1'), range(4)))
        self.assertEqual(sum(r is not None for r in results), 1)


class NativeWriteTests(ExtendedFixture, unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.settings = WriteSettings(validate_settings(self.config, self.token),
                                      self.root/'reviews.sqlite3', frozenset(WRITE_TOOLS))
        self.runtime = MemoryRuntime(self.settings.read)
        await self.runtime.open()
        self.coordinator = WriteCoordinator(self.settings, self.runtime)

    async def asyncTearDown(self):
        await self.runtime.close()

    async def execute(self, request_id):
        row = self.coordinator.journal.get(request_id)
        self.coordinator.journal.review(request_id, row['digest'], approve=True)
        await self.runtime.close()
        try:
            await execute_approved(self.settings, request_id)
        finally:
            await self.runtime.open()
        return self.coordinator.journal.get(request_id)

    async def test_pending_hold_has_no_vault_effect_then_approved_hold_is_native(self):
        before = self.snapshot()
        result = await self.coordinator.submit('memory_write', 'event-1', {'content': 'brand new synthetic memory'})
        self.assertEqual(result['state'], 'pending')
        self.assertEqual(before, self.snapshot())
        row = await self.execute('event-1')
        self.assertEqual(row['state'], 'finished')
        self.assertEqual(row['receipt']['status'], 'succeeded')
        bucket_id = row['receipt']['items'][0]['mutations'][0]['bucket_id']
        bucket = await self.runtime.view.get(bucket_id)
        self.assertEqual(bucket['metadata']['source_tool'], 'hold')
        self.assertEqual(bucket['content'], 'brand new synthetic memory')
        after = self.snapshot()
        again = await self.coordinator.submit('memory_write', 'event-1', {'content': 'brand new synthetic memory'})
        self.assertEqual(again['receipt'], row['receipt'])
        self.assertEqual(after, self.snapshot())

    async def test_version_conflict_rejects_trace_and_preserves_concurrent_edit(self):
        bucket = await self.runtime.view.get('ordinary')
        await self.coordinator.submit('memory_update', 'update-1', {
            'bucket_id': 'ordinary', 'expected_version': revision(bucket), 'changes': {'content': 'intended replacement'}})
        self.put('dynamic', 'ordinary', 'dynamic', 'concurrent writer content')
        row = await self.execute('update-1')
        self.assertEqual(row['receipt']['status'], 'conflict')
        self.assertEqual((await self.runtime.view.get('ordinary'))['content'], 'concurrent writer content')

    async def test_trace_update_and_plan_create_update_reuse_native_business(self):
        bucket = await self.runtime.view.get('ordinary')
        await self.coordinator.submit('memory_update', 'update-1', {'bucket_id': 'ordinary',
            'expected_version': revision(bucket), 'changes': {'content': 'revised ordinary content'}})
        row = await self.execute('update-1')
        self.assertEqual(row['receipt']['status'], 'succeeded')
        await self.coordinator.submit('memory_plan_create', 'plan-1', {'content': 'synthetic reviewed promise'})
        row = await self.execute('plan-1')
        plan_id = row['receipt']['items'][0]['mutations'][0]['bucket_id']
        plan = await self.runtime.view.manager.get(plan_id)
        self.assertEqual(plan['metadata']['type'], 'plan')
        await self.coordinator.submit('memory_plan_update', 'plan-2', {'bucket_id': plan_id,
            'expected_version': revision(plan), 'changes': {'status': 'abandoned'}})
        row = await self.execute('plan-2')
        self.assertEqual(row['receipt']['status'], 'succeeded')
        plan = await self.runtime.view.manager.get(plan_id)
        self.assertEqual(plan['metadata']['status'], 'abandoned')
        self.assertTrue(plan['metadata']['change_log'])

    async def test_unapproved_execution_and_special_memory_update_are_denied(self):
        await self.coordinator.submit('memory_write', 'event-1', {'content': 'pending event'})
        before = self.snapshot()
        self.assertIsNone(await execute_approved(self.settings, 'event-1'))
        self.assertEqual(before, self.snapshot())
        letter = await self.runtime.view.manager.get('private-letter')
        with self.assertRaises(PermissionError):
            await self.coordinator.submit('memory_update', 'letter-1', {'bucket_id': 'private-letter',
                'expected_version': revision(letter), 'changes': {'content': 'forbidden'}})

    async def test_grow_returns_individual_outcomes_and_preserves_partial_success(self):
        await self.coordinator.submit('memory_grow', 'batch-1', {'items': [
            {'content': 'synthetic successful grow item'}, {'content': 'synthetic failing grow item'}]})
        original = ReviewedManager.create
        async def fail_one(manager, *args, **kwargs):
            if 'failing' in kwargs.get('content', ''):
                raise OSError('synthetic storage failure')
            return await original(manager, *args, **kwargs)
        with patch.object(ReviewedManager, 'create', fail_one):
            row = await self.execute('batch-1')
        self.assertEqual(row['receipt']['status'], 'partial')
        self.assertEqual(len(row['receipt']['items']), 2)
        self.assertEqual(row['receipt']['items'][0]['status'], 'succeeded')
        self.assertEqual(row['receipt']['items'][1]['status'], 'indeterminate')
        created = row['receipt']['items'][0]['mutations'][0]['bucket_id']
        self.assertEqual((await self.runtime.view.get(created))['metadata']['source_tool'], 'grow')

    async def test_grow_duplicate_items_reuse_native_dedup(self):
        await self.coordinator.submit('memory_grow', 'batch-1', {'items': [
            {'content': 'same synthetic grow text'}, {'content': 'same synthetic grow text'}]})
        row = await self.execute('batch-1')
        self.assertEqual(row['receipt']['status'], 'succeeded')
        self.assertEqual(row['receipt']['items'][1]['outcome'], 'native_duplicate')

    async def test_exception_after_commit_is_indeterminate_and_never_retried(self):
        payload = {'content': 'committed before synthetic receipt failure'}
        await self.coordinator.submit('memory_write', 'event-1', payload)
        original = ReviewedManager.create
        async def fail_after(manager, *args, **kwargs):
            await original(manager, *args, **kwargs)
            raise OSError('synthetic post-commit failure')
        with patch.object(ReviewedManager, 'create', fail_after):
            row = await self.execute('event-1')
        self.assertEqual(row['receipt']['status'], 'partial')
        before = self.snapshot()
        await self.coordinator.submit('memory_write', 'event-1', payload)
        self.assertEqual(before, self.snapshot())

    async def test_native_plan_duplicate_is_a_verified_noop(self):
        payload = {'content': 'active plan sentinel'}
        await self.coordinator.submit('memory_plan_create', 'plan-1', payload)
        row = await self.execute('plan-1')
        self.assertEqual(row['receipt']['status'], 'succeeded')
        self.assertEqual(row['receipt']['items'][0]['outcome'], 'native_duplicate')

    async def test_cas_is_checked_inside_native_bucket_lock(self):
        bucket = await self.runtime.view.get('ordinary')
        await self.coordinator.submit('memory_update', 'update-1', {'bucket_id': 'ordinary',
            'expected_version': revision(bucket), 'changes': {'content': 'must not overwrite'}})
        self.coordinator.journal.review('update-1', self.coordinator.journal.get('update-1')['digest'], approve=True)
        await self.runtime.close()
        original = ReviewedManager._update_locked
        async def race(manager, bucket_id, **kwargs):
            self.put('dynamic', 'ordinary', 'dynamic', 'concurrent edit after initial validation')
            return await original(manager, bucket_id, **kwargs)
        try:
            with patch.object(ReviewedManager, '_update_locked', race):
                await execute_approved(self.settings, 'update-1')
        finally:
            await self.runtime.open()
        row = self.coordinator.journal.get('update-1')
        self.assertEqual(row['receipt']['status'], 'conflict')
        self.assertEqual((await self.runtime.view.get('ordinary'))['content'],
                         'concurrent edit after initial validation')

    async def test_real_sqlite_index_uses_native_store_with_existing_vector_provider(self):
        class LocalProvider:
            _client = type('Client', (), {'close': AsyncMock()})()
            async def generate_async(self, content):
                return [1.0]+[0.0]*1023
        class LocalEngine(ExistingEmbeddingEngine):
            def __init__(self, config):
                super().__init__(config)
                self._backend = LocalProvider()
                self.enabled = True
        await self.coordinator.submit('memory_write', 'vector-1', {'content': 'vector indexed synthetic event'})
        with patch('memory_write_executor.ExistingEmbeddingEngine', LocalEngine), \
                patch('embedding_engine.EmbeddingEngine._init_db', side_effect=AssertionError('rebuild')):
            row = await self.execute('vector-1')
        self.assertEqual(row['receipt']['status'], 'succeeded')
        bucket_id = row['receipt']['items'][0]['mutations'][0]['bucket_id']
        self.assertEqual(row['receipt']['index'][bucket_id], 'ready')
        with closing(sqlite3.connect(self.vault/'embeddings.db')) as db:
            self.assertEqual(db.execute('PRAGMA integrity_check').fetchone()[0], 'ok')
            digest = db.execute('SELECT content_hash FROM embeddings WHERE bucket_id=?', (bucket_id,)).fetchone()[0]
            self.assertEqual(digest, hashlib.sha256(b'vector indexed synthetic event').hexdigest())
        self.assertEqual(len(await self.runtime.engine.get_embedding(bucket_id)), 1024)

    async def test_no_maintenance_or_unreviewed_suggestion_tasks(self):
        await self.coordinator.submit('memory_grow', 'no-tasks', {'items': [{'content': 'approved isolated event'}]})
        with patch('tools.grow.core.check_plan_resolution', new_callable=AsyncMock) as plans, \
                patch('tools.grow.core.check_duplicate_for', new_callable=AsyncMock) as duplicates, \
                patch('ombrebrain.storage.embedding_outbox.EmbeddingOutbox.start', new_callable=AsyncMock) as queue, \
                patch('bucket_manager.BucketManager._rebuild_bm25_async', new_callable=AsyncMock) as bm25:
            row = await self.execute('no-tasks')
            await asyncio.sleep(0)
            self.assertEqual(row['receipt']['status'], 'succeeded')
            plans.assert_not_called()
            duplicates.assert_not_called()
            queue.assert_not_called()
            bm25.assert_not_called()
        self.assertFalse((self.vault/'dehydration_cache.db').exists())

    async def assert_reviewed_create_skips_relations(self, tool, payload):
        existing = self.vault/'dynamic'/'ordinary.md'
        before = existing.read_bytes()
        await self.coordinator.submit(tool, 'no-auto-relations', payload)
        with patch('tools._common.link_new_bucket', new_callable=AsyncMock) as link:
            row = await self.execute('no-auto-relations')
            link.assert_not_called()

        self.assertEqual(row['receipt']['status'], 'succeeded')
        mutation, = row['receipt']['items'][0]['mutations']
        self.assertEqual(mutation['action'], 'created')
        self.assertEqual(existing.read_bytes(), before)
        created = await self.runtime.view.get(mutation['bucket_id'])
        self.assertFalse(created['metadata'].get('relation_links'))

    async def test_reviewed_memory_write_does_not_link_new_bucket(self):
        await self.assert_reviewed_create_skips_relations(
            'memory_write', {'content': 'reviewed hold without automatic relationships'})

    async def test_reviewed_memory_grow_does_not_link_new_bucket(self):
        await self.assert_reviewed_create_skips_relations(
            'memory_grow', {'items': [{'content': 'reviewed grow without automatic relationships'}]})

    async def test_native_create_still_persists_both_automatic_relation_links(self):
        from bucket_manager import BucketManager
        from tools import _runtime as rt
        from tools._common import merge_or_create
        from tools._relation_link import link_new_bucket

        config = dict(self.settings.read.config, read_only=False)
        manager = BucketManager(config, None)
        completed = asyncio.Event()

        async def track_link(bucket_id, content):
            try:
                await link_new_bucket(bucket_id, content)
            finally:
                completed.set()

        inferred = [{'target_bucket_id': 'ordinary', 'type': 'related_to', 'label': '',
                     'status': 'active', 'auto': True, 'score': 0.9}]
        with patch.multiple(rt, config=config, bucket_mgr=manager, embedding_engine=None,
                            logger=logging.getLogger('synthetic-native-relation')), \
                patch('tools._relation_link.infer_links_for', return_value=inferred), \
                patch('tools._common.link_new_bucket', side_effect=track_link) as link:
            content = 'native memory with automatic relationships'
            bucket_id, merged, _ = await merge_or_create(content, [], 5, ['test'], .5, .3)
            self.assertFalse(merged)
            link.assert_called_once_with(bucket_id, content)
            await asyncio.wait_for(completed.wait(), timeout=5)

        for source, target in ((bucket_id, 'ordinary'), ('ordinary', bucket_id)):
            stored = await manager.get(source)
            relation, = stored['metadata']['relation_links']
            self.assertEqual(relation['target_bucket_id'], target)
            self.assertTrue(relation['auto'])

    async def test_adapter_short_grow_does_not_start_suggestion_tasks(self):
        from tools import _runtime as rt
        from tools.grow.shortpath import grow_shortpath

        metadata = type('Metadata', (), {'analyze': AsyncMock(return_value={})})()
        with patch.multiple(rt, config={'memory_write_adapter': True}, dehydrator=metadata,
                            logger=logging.getLogger('synthetic-short-grow')), \
                patch('tools.grow.shortpath.merge_or_create', return_value=('new-id', False, '')), \
                patch('tools.grow.shortpath.check_plan_resolution') as plans, \
                patch('tools.grow.shortpath.check_duplicate_for') as duplicates:
            await grow_shortpath('short synthetic event')
            plans.assert_not_called()
            duplicates.assert_not_called()

    async def test_plan_resolution_cascade_requires_reviewed_link_versions(self):
        self.put('plans', 'linked-plan', 'plan', 'reviewed linked promise', status='active', related_bucket='ordinary')
        plan = await self.runtime.view.manager.get('linked-plan')
        await self.coordinator.submit('memory_plan_update', 'resolve-1', {'bucket_id': 'linked-plan',
            'expected_version': revision(plan), 'changes': {'status': 'resolved'}})
        observed = self.coordinator.journal.get('resolve-1')['body']['observations']['versions']
        self.assertIn('ordinary', observed)
        row = await self.execute('resolve-1')
        self.assertEqual(row['receipt']['status'], 'succeeded')
        self.assertTrue((await self.runtime.view.get('ordinary'))['metadata']['resolved'])

    async def test_legacy_hold_default_keeps_native_post_write_tasks(self):
        from tools import _runtime as rt
        from tools.hold.core import store_core
        original = rt.config, rt.dehydrator
        try:
            rt.config = {}
            rt.dehydrator = type('Metadata', (), {'analyze': AsyncMock(return_value={
                'domain': ['test'], 'tags': [], 'valence': .5, 'arousal': .3})})()
            with patch('tools.hold.core.merge_or_create', new_callable=AsyncMock,
                       return_value=('synthetic-native-id', False, '')), \
                    patch('tools.hold.core.check_plan_resolution', new_callable=AsyncMock) as plans, \
                    patch('tools.hold.core.check_duplicate_for', new_callable=AsyncMock) as duplicates:
                await store_core('synthetic', [], 5, .5, .3, '')
                await asyncio.sleep(0)
                plans.assert_awaited_once()
                duplicates.assert_awaited_once()
        finally:
            rt.config, rt.dehydrator = original

    async def test_corrupt_vector_data_fails_closed_without_rebuild_or_memory_changes(self):
        await self.coordinator.submit('memory_write', 'bad-index', {'content': 'must remain pending storage'})
        with closing(sqlite3.connect(self.vault/'embeddings.db')) as db:
            db.execute('UPDATE embeddings SET embedding=? WHERE bucket_id=?', ('invalid-vector', 'ordinary'))
            db.commit()
        before = self.snapshot()
        row = await self.execute('bad-index')
        self.assertEqual(row['receipt']['status'], 'failed')
        self.assertEqual(before, self.snapshot())

    async def test_unavailable_vector_provider_retains_a_durable_native_queue_receipt(self):
        await self.coordinator.submit('memory_write', 'pending-index', {'content': 'retained pending native vector'})
        row = await self.execute('pending-index')
        bucket_id = row['receipt']['items'][0]['mutations'][0]['bucket_id']
        self.assertEqual(row['receipt']['index'][bucket_id], 'pending_native_outbox')
        self.assertIsNotNone(await self.runtime.view.get(bucket_id))


class WriteProtocolTests(ExtendedFixture, unittest.IsolatedAsyncioTestCase):
    async def test_preflight_errors_do_not_reflect_native_private_messages(self):
        self.config['writes'] = {'enabled': True, 'tools': ['memory_write'],
                                 'journal_path': str(self.root/'review.sqlite3')}
        server = create_write_server(validate_write_settings(self.config, self.token))
        async with server.settings.lifespan(server):
            with patch('memory_write_entry.WriteCoordinator.submit', new_callable=AsyncMock,
                       side_effect=RuntimeError('SYNTHETIC_PRIVATE_PROVIDER_BODY')):
                with self.assertRaises(Exception) as error:
                    await server.call_tool('memory_write', {'request_id': 'r', 'content': 'event'})
                self.assertNotIn('SYNTHETIC_PRIVATE_PROVIDER_BODY', str(error.exception))

    async def test_defaults_remain_read_only_and_do_not_open_a_journal(self):
        settings = validate_write_settings(self.config, self.token)
        server = create_write_server(settings)
        self.assertEqual({t.name for t in await server.list_tools()}, set(ALL_READS))
        before = self.snapshot()
        async with server.settings.lifespan(server):
            with self.assertRaises(Exception):
                await server.call_tool('memory_write', {'request_id': 'r', 'content': 'event'})
        self.assertEqual(before, self.snapshot())

    async def test_explicit_writes_share_discovery_and_call_permissions(self):
        self.config['writes'] = {'enabled': True, 'tools': ['memory_write'],
                                 'journal_path': str(self.root/'review.sqlite3')}
        server = create_write_server(validate_write_settings(self.config, self.token))
        self.assertEqual({t.name for t in await server.list_tools()}, set(ALL_READS)|{'memory_write'})
        before = self.snapshot()
        async with server.settings.lifespan(server):
            for name, args in [('memory_plan_create', {'request_id': 'p', 'content': 'promise'}),
                               ('memory_write', {'request_id': 'r', 'content': 'event', 'confirmed': True}),
                               ('memory_write', {'request_id': 'r', 'content': 'event', 'pinned': True})]:
                with self.assertRaises(Exception):
                    await server.call_tool(name, args)
            result = await server.call_tool('memory_write', {'request_id': 'r', 'content': 'event'})
            self.assertIn('pending', str(result))
        self.assertEqual(before, self.snapshot())

    async def test_missing_auth_and_journal_inside_vault_fail_closed(self):
        self.config['writes'] = {'enabled': True, 'tools': list(WRITE_TOOLS),
                                 'journal_path': str(self.vault/'review.sqlite3')}
        with self.assertRaises(PermissionError):
            validate_write_settings(self.config, 'incorrect')
        with self.assertRaises(ValueError):
            validate_write_settings(self.config, self.token)


if __name__ == '__main__':
    unittest.main()
