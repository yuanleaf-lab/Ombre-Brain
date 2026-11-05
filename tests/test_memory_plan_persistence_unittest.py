"""Plan writes against one synthetic Vault, including two independent clients."""
import asyncio
from contextlib import asynccontextmanager
import json
import logging
from logging.handlers import RotatingFileHandler
import multiprocessing
import unittest
from unittest.mock import AsyncMock, patch

import frontmatter
import errors

from bucket_manager import BucketManager
from memory_extended_fixture import ExtendedFixture
from memory_write_executor import NoMaintenance, ReviewedManager, execute_approved
import test_memory_writes_unittest as write_tests
from tools import _runtime as rt
from tools import _common
from tools.plan import plan_create


CONTENT = 'synthetic shared plan for both clients\nsecond reviewed line'


def plan_process(settings, role, leader, ready, start, entered, competing, release, pipe):
    async def run():
        original_create = BucketManager.create
        original_turn = _common._filesystem_content_turn

        async def gated_create(manager, *args, **kwargs):
            if kwargs.get('bucket_type') == 'plan':
                if leader:
                    entered.set()
                    if not await asyncio.to_thread(release.wait, 20):
                        raise RuntimeError('synthetic race did not reach the contention point')
                else:
                    # Before the fix, the second client reaches create after
                    # independently observing an empty active-plan snapshot.
                    competing.set()
            return await original_create(manager, *args, **kwargs)

        @asynccontextmanager
        async def observed_turn(key):
            if not leader:
                competing.set()
            async with original_turn(key):
                yield

        if role == 'native':
            config = dict(settings.read.config, read_only=False,
                          storage={'external_change_poll_seconds': 3600})
            manager = BucketManager(config, None)
            rt.init(config=config, bucket_mgr=manager, decay_engine=NoMaintenance(),
                    logger=logging.getLogger('synthetic-plan-native'))
            # Exercise a stale cache in the long-lived native client.
            await manager.list_all()
        ready.set()
        if not await asyncio.to_thread(start.wait, 20):
            raise RuntimeError('synthetic race start timed out')
        with patch.object(BucketManager, 'create', gated_create), \
                patch.object(_common, '_filesystem_content_turn', observed_turn):
            if role == 'native':
                windows_content = CONTENT.replace('\n', '\r\n')
                return {'native': await plan_create('  ' + windows_content + '  ')}
            return {'receipt': await execute_approved(settings, 'race')}

    try:
        pipe.send(asyncio.run(run()))
    except Exception as exc:
        pipe.send({'error': type(exc).__name__})
    finally:
        pipe.close()


class PlanPersistenceTests(ExtendedFixture, unittest.IsolatedAsyncioTestCase):
    asyncSetUp = write_tests.NativeWriteTests.asyncSetUp
    asyncTearDown = write_tests.NativeWriteTests.asyncTearDown
    execute = write_tests.NativeWriteTests.execute

    async def propose(self, request='plan-write'):
        await self.coordinator.submit('memory_plan_create', request, {'content': CONTENT})

    def stored_plans(self):
        return [p for path in (self.vault/'plans').rglob('*.md')
                if (p := frontmatter.load(path)).content == CONTENT]

    async def race(self, leader_role):
        await self.propose('race')
        journal = self.coordinator.journal
        journal.review('race', journal.get('race')['digest'], approve=True)
        context = multiprocessing.get_context('spawn')
        entered, competing, release = (context.Event() for _ in range(3))
        actors = {}
        try:
            for role in ('native', 'adapter'):
                ready, start = context.Event(), context.Event()
                receiver, sender = context.Pipe(duplex=False)
                process = context.Process(target=plan_process, args=(
                    self.settings, role, role == leader_role, ready, start,
                    entered, competing, release, sender))
                process.start()
                sender.close()
                actors[role] = process, receiver, ready, start
            for _, _, ready, _ in actors.values():
                self.assertTrue(await asyncio.to_thread(ready.wait, 20))
            actors[leader_role][3].set()
            self.assertTrue(await asyncio.to_thread(entered.wait, 20))
            follower = 'adapter' if leader_role == 'native' else 'native'
            actors[follower][3].set()
            self.assertTrue(await asyncio.to_thread(competing.wait, 20))
            release.set()
            results = {}
            for role, (process, receiver, _, _) in actors.items():
                self.assertTrue(await asyncio.to_thread(receiver.poll, 25), role)
                results[role] = receiver.recv()
                self.assertNotIn('error', results[role])
                await asyncio.to_thread(process.join, 5)
                self.assertEqual(process.exitcode, 0)
            plans = self.stored_plans()
            self.assertEqual(len(plans), 1, 'both clients created the same active plan')
            self.assertEqual(plans[0].metadata['status'], 'active')
            self.assertEqual(plans[0].metadata['change_log'][0]['action'], 'created')
            receipt = results['adapter']['receipt']
            self.assertEqual(receipt['status'], 'succeeded')
            if follower == 'adapter':
                self.assertEqual(receipt['items'][0]['outcome'], 'native_duplicate')
                self.assertEqual(receipt['items'][0]['existing_bucket_id'], plans[0].metadata['id'])
                self.assertEqual(receipt['items'][0]['mutations'], [])
            else:
                self.assertIn('完全重复', results['native']['native'])
            self.assertEqual(self.snapshot()[2]['integrity'], ('ok',))
        finally:
            release.set()
            for process, receiver, _, start in actors.values():
                start.set()
                if process.is_alive():
                    process.terminate()
                await asyncio.to_thread(process.join, 5)
                receiver.close()
                if not process.is_alive():
                    process.close()

    async def test_cyberboss_native_first_adapter_deduplicates(self):
        await self.race('native')

    async def test_adapter_first_cyberboss_bypasses_stale_plan_cache(self):
        await self.race('adapter')

    async def test_state_update_exception_leaves_partial_receipt(self):
        await self.propose()
        with patch.object(ReviewedManager, 'update', new_callable=AsyncMock,
                          side_effect=OSError('synthetic state update error')):
            row = await self.execute('plan-write')
        self.assertEqual(row['receipt']['status'], 'partial')
        plans = self.stored_plans()
        self.assertEqual(len(plans), 1)
        self.assertNotIn('change_log', plans[0].metadata)
        self.assertEqual(row['receipt']['items'][0]['bucket_id'], plans[0].metadata['id'])
        before = self.snapshot()
        self.assertIsNone(await execute_approved(self.settings, 'plan-write'))
        self.assertEqual(before, self.snapshot())

    async def test_success_ack_without_persistence_is_partial(self):
        await self.propose()
        with patch.object(ReviewedManager, 'update', new_callable=AsyncMock, return_value=True):
            row = await self.execute('plan-write')
        self.assertEqual(row['receipt']['status'], 'partial')
        self.assertNotIn('change_log', self.stored_plans()[0].metadata)

    async def test_disk_error_in_metadata_update_is_partial(self):
        await self.propose()
        with patch('bucket_manager._atomic_write_text', side_effect=OSError('synthetic disk write failure')):
            row = await self.execute('plan-write')
        self.assertEqual(row['receipt']['status'], 'partial')
        self.assertEqual(len(self.stored_plans()), 1)
        self.assertNotIn('change_log', self.stored_plans()[0].metadata)
        self.assertEqual(self.snapshot()[2]['integrity'], ('ok',))

    async def test_disk_error_in_initial_create_is_indeterminate(self):
        await self.propose()
        with patch('bucket_manager._atomic_create_text', side_effect=OSError('synthetic create failure')):
            row = await self.execute('plan-write')
        self.assertEqual(row['receipt']['status'], 'indeterminate')
        self.assertEqual(self.stored_plans(), [])

    async def test_post_update_read_failure_is_indeterminate_and_never_retried(self):
        await self.propose()
        original_get = ReviewedManager.get
        fail_once = True

        async def unavailable_read(manager, bucket_id):
            nonlocal fail_once
            bucket = await original_get(manager, bucket_id)
            if fail_once and bucket and bucket['metadata'].get('change_log'):
                fail_once = False
                raise OSError('synthetic readback unavailable')
            return bucket

        with patch.object(ReviewedManager, 'get', unavailable_read):
            row = await self.execute('plan-write')
        self.assertEqual(row['receipt']['status'], 'indeterminate')
        self.assertEqual(len(self.stored_plans()), 1)
        self.assertTrue(self.stored_plans()[0].metadata['change_log'])
        before = self.snapshot()
        self.assertIsNone(await execute_approved(self.settings, 'plan-write'))
        self.assertEqual(before, self.snapshot())

    async def test_native_success_keeps_resolved_status_and_link_fields(self):
        config = dict(self.settings.read.config, read_only=False)
        manager = BucketManager(config, None)
        with patch.object(rt, 'bucket_mgr', manager), patch.object(rt, 'decay_engine', NoMaintenance()):
            response = await plan_create(
                CONTENT, status='resolved', related_bucket='ordinary',
                weight=0.7, why_remembered='synthetic reason')
        plan = self.stored_plans()[0]
        self.assertEqual(response, f"📋plan→{plan.metadata['id']} [resolved]")
        self.assertEqual(plan.metadata['status'], 'resolved')
        self.assertEqual(plan.metadata['related_bucket'], 'ordinary')
        self.assertEqual(plan.metadata['weight'], 0.7)
        self.assertEqual(plan.metadata['why_remembered'], 'synthetic reason')
        self.assertEqual(plan.metadata['change_log'][0]['to'], 'resolved')

    async def test_native_incomplete_response_has_no_success_marker(self):
        config = dict(self.settings.read.config, read_only=False)
        manager = BucketManager(config, None)
        with patch.object(rt, 'bucket_mgr', manager), patch.object(rt, 'decay_engine', NoMaintenance()), \
                patch.object(manager, 'update', new_callable=AsyncMock, return_value=False):
            response = await plan_create(CONTENT)
        self.assertIn('[partial]', response)
        self.assertNotIn('📋plan→', response)
        self.assertNotIn('change_log', self.stored_plans()[0].metadata)

    async def test_unreadable_dedup_snapshot_cannot_create_a_plan(self):
        await self.propose()
        original_load = frontmatter.load

        def fail_for_existing_plan(path, *args, **kwargs):
            if str(path).endswith('active-plan.md'):
                raise OSError('synthetic plan scan failure')
            return original_load(path, *args, **kwargs)

        with patch('bucket_manager.frontmatter.load', side_effect=fail_for_existing_plan):
            row = await self.execute('plan-write')
        self.assertEqual(row['receipt']['status'], 'indeterminate')
        self.assertEqual(row['receipt']['items'][0]['mutations'], [])
        self.assertEqual(self.stored_plans(), [])

    async def test_incomplete_native_plan_is_not_a_successful_duplicate(self):
        await self.propose()
        with patch.object(ReviewedManager, 'update', new_callable=AsyncMock, return_value=True):
            first = await self.execute('plan-write')
        self.assertEqual(first['receipt']['status'], 'partial')
        await self.propose('repeat')
        repeat = await self.execute('repeat')
        self.assertEqual(repeat['receipt']['status'], 'partial')
        item = repeat['receipt']['items'][0]
        self.assertEqual(item['outcome'], 'native_duplicate')
        self.assertEqual(item['existing_bucket_id'], first['receipt']['items'][0]['bucket_id'])
        self.assertEqual(item['mutations'], [])
        self.assertEqual(len(self.stored_plans()), 1)

    async def test_unreadable_new_bucket_is_indeterminate(self):
        await self.propose()
        with patch.object(ReviewedManager, 'get', new_callable=AsyncMock, return_value=None):
            row = await self.execute('plan-write')
        self.assertEqual(row['receipt']['status'], 'indeterminate')
        self.assertEqual(len(self.stored_plans()), 1)

    async def test_strict_plan_scan_rejects_bad_weight_without_warnings_or_writes(self):
        path = self.vault/'plans'/'active-plan.md'
        post = frontmatter.load(path)
        post['weight'] = 'invalid numeric weight'
        path.write_text(frontmatter.dumps(post), encoding='utf-8')
        config = dict(self.settings.read.config, read_only=False)
        manager = BucketManager(config, None)

        with patch.object(errors, '_errors_path', None):
            errors.configure_errors_path(str(self.vault))
            before = self.snapshot()
            with self.assertNoLogs(level='WARNING'):
                with self.assertRaisesRegex(ValueError, 'weight'):
                    manager.find_exact_content(post.content, active_plan=True)
            self.assertEqual(before, self.snapshot())

            with patch.object(rt, 'bucket_mgr', manager), patch.object(rt, 'config', config), \
                    patch.object(rt, 'decay_engine', NoMaintenance()), self.assertNoLogs(level='WARNING'):
                with self.assertRaisesRegex(ValueError, 'weight'):
                    await plan_create(CONTENT)

        after_files, after_dirs, after_db = self.snapshot()
        before_files, before_dirs, before_db = before
        new_files = set(after_files) - set(before_files)
        locks = list((self.vault/'.locks').glob('*.lock'))
        self.assertEqual(len(locks), 1)
        self.assertEqual(new_files, {str(locks[0].relative_to(self.vault))})
        self.assertEqual(json.loads(locks[0].read_text('ascii'))['state'], 'released')
        self.assertEqual(set(after_dirs) - set(before_dirs), {'.locks'})
        self.assertEqual({name: after_files[name] for name in before_files}, before_files)
        self.assertEqual(after_db, before_db)
        self.assertEqual(self.stored_plans(), [])
        self.assertFalse((self.vault/'.logs'/'errors.jsonl').exists())

    async def test_strict_numeric_checks_precede_all_warning_normalizers(self):
        path = self.vault/'plans'/'active-plan.md'
        original = frontmatter.load(path)
        manager = BucketManager(dict(self.settings.read.config, read_only=False), None)
        invalid_values = (
            ('importance', 4.5), ('valence', True), ('arousal', -0.2),
            ('model_valence', float('inf')), ('weight', None), ('activation_count', -1),
        )
        for field, value in invalid_values:
            with self.subTest(field=field):
                post = frontmatter.Post(original.content, **dict(original.metadata, **{field: value}))
                path.write_text(frontmatter.dumps(post), encoding='utf-8')
                with patch.object(manager, '_sanitize_float_field', side_effect=AssertionError('float normalizer')), \
                        patch('bucket_manager._clamp_importance', side_effect=AssertionError('importance normalizer')), \
                        self.assertNoLogs(level='WARNING'):
                    with self.assertRaisesRegex(ValueError, field):
                        manager._load_bucket(str(path), strict=True)

    async def test_strict_read_uses_validated_numeric_string_value(self):
        path = self.vault/'plans'/'active-plan.md'
        post = frontmatter.load(path)
        post['weight'] = '1e-1'
        path.write_text(frontmatter.dumps(post), encoding='utf-8')
        manager = BucketManager(dict(self.settings.read.config, read_only=False), None)
        with self.assertNoLogs(level='WARNING'):
            bucket = manager.find_exact_content(post.content, active_plan=True)
        self.assertEqual(bucket['metadata']['weight'], 0.1)

    async def test_read_only_non_strict_load_rejects_bad_metadata_without_log_writes(self):
        path = self.vault/'plans'/'active-plan.md'
        post = frontmatter.load(path)
        post['weight'] = 'invalid numeric weight'
        path.write_text(frontmatter.dumps(post), encoding='utf-8')
        manager = BucketManager(dict(self.settings.read.config, read_only=True), None)
        log_path = self.vault/'.logs'/'server.log'
        log_path.parent.mkdir()
        logger = logging.getLogger('ombre_brain.bucket')

        for existing_log in (False, True):
            with self.subTest(existing_log=existing_log):
                if existing_log:
                    log_path.write_text('existing server log\n', encoding='utf-8')
                before = self.snapshot()
                handler = RotatingFileHandler(log_path, encoding='utf-8', delay=True)
                logger.addHandler(handler)
                try:
                    with self.assertNoLogs(level='WARNING'):
                        with self.assertRaisesRegex(ValueError, 'invalid memory metadata: weight'):
                            manager._load_bucket(str(path), strict=False)
                finally:
                    logger.removeHandler(handler)
                    handler.close()
                self.assertEqual(log_path.exists(), existing_log)
                self.assertEqual(self.snapshot(), before)

    async def test_ordinary_write_mode_still_repairs_and_warns_on_bad_numbers(self):
        path = self.vault/'plans'/'active-plan.md'
        post = frontmatter.load(path)
        post['importance'] = 'invalid'
        post['weight'] = 'invalid'
        path.write_text(frontmatter.dumps(post), encoding='utf-8')
        manager = BucketManager(dict(self.settings.read.config, read_only=False), None)
        original_bytes = path.read_bytes()
        with patch.object(errors, '_errors_path', None), self.assertLogs('errors', level='WARNING'):
            errors.configure_errors_path(str(self.vault))
            bucket = manager._load_bucket(str(path), strict=False)
        self.assertEqual(bucket['metadata']['importance'], 5)
        self.assertEqual(bucket['metadata']['weight'], 0.5)
        self.assertEqual(path.read_bytes(), original_bytes)
        log = self.vault/'.logs'/'errors.jsonl'
        entries = [json.loads(line) for line in log.read_text('utf-8').splitlines() if line.strip()]
        self.assertEqual([entry['code'] for entry in entries], ['OB-W001', 'OB-W001'])
