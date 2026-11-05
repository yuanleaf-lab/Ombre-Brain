"""Short-lived native OB writer; no full server, maintenance, or second engine."""
import asyncio
from copy import deepcopy
import hashlib
import logging
import math
import os
from pathlib import Path
import sqlite3

from bucket_manager import BucketManager, _filesystem_turn
from dehydrator import Dehydrator
from embedding_engine import EmbeddingEngine, _norm_model
from memory_write_journal import WriteJournal
from memory_write_policy import revision, validate_request
from memory_write_service import editable
from ombrebrain.storage.embedding_outbox import EmbeddingOutbox
from ombrebrain.storage.source_store import SourceStore
from ombrebrain.storage.vector_codec import decode_vector
from tools import _runtime as rt


class WriteConflict(RuntimeError):
    pass


class NoMaintenance:
    async def ensure_started(self):
        return None


class ExistingEmbeddingEngine(EmbeddingEngine):
    def _init_read_only_db(self):
        super()._init_read_only_db()
        db = self._connect_db()
        try:
            meta = dict(db.execute('SELECT key,value FROM embeddings_meta'))
            if meta.get('model_name') and _norm_model(meta['model_name']) != _norm_model('bge-m3'):
                raise ValueError('existing vector model is incompatible')
            if meta.get('vector_dim') and str(meta['vector_dim']) != '1024':
                raise ValueError('existing vector dimension is incompatible')
            rows = db.execute('SELECT embedding,meaning_embedding FROM embeddings')
            while batch := rows.fetchmany(32):
                for row in batch:
                    for raw in row:
                        if raw:
                            vector = decode_vector(raw)
                            if vector.size != 1024 or not all(math.isfinite(float(x)) for x in vector):
                                raise ValueError('existing vector index is incompatible')
        finally:
            db.close()

    def _connect_db(self):
        if self.read_only:
            return super()._connect_db()
        # An existing database disappearing must not create a replacement.
        return sqlite3.connect(Path(self.db_path).resolve().as_uri() + '?mode=rw', uri=True)

    def _store_embedding(self, bucket_id, embedding, content_hash=''):
        if len(embedding) != 1024:
            raise ValueError('unexpected local embedding dimension')
        return super()._store_embedding(bucket_id, embedding, content_hash)


class ReviewedManager(BucketManager):
    """Use native locking/storage/indexes; add an under-lease compare-and-swap.

    A sticky denial also blocks native 'merge failed -> create' fallback. Native
    business functions catch some exceptions, so exception-only guards are not
    sufficient permission enforcement.
    """
    def __init__(self, config, engine, row):
        super().__init__(config, engine)
        self._bm25 = None
        self.expected = dict(row['body']['observations']['versions'])
        self.tool = row['body']['tool']
        self.denied = None
        self.mutations = []

    def _reconcile_external_changes(self, previous, current):
        # No unrelated external-change repair during an approved request.
        return None

    def _find_bucket_file(self, bucket_id):
        with self._bucket_path_index_guard:
            self._bucket_path_index.clear()
            self._bucket_path_index_ready = False
        return super()._find_bucket_file(bucket_id)

    def deny(self, message):
        self.denied = message
        raise WriteConflict(message)

    def check(self):
        if self.denied:
            raise WriteConflict(self.denied)

    async def get(self, bucket_id):
        bucket = await super().get(bucket_id)
        if bucket is None:
            return None
        kind = bucket['metadata'].get('type', 'dynamic')
        return bucket if editable(bucket, self, kind) else None

    async def list_all(self, include_archive=False):
        return [b for b in await super().list_all(False)
                if editable(b, self, b['metadata'].get('type', 'dynamic'))]

    async def search(self, query, **kwargs):
        self.check()
        kwargs['allowed_bucket_ids'] = {b['id'] for b in await self.list_all()
                                       if b['metadata'].get('type', 'dynamic') == 'dynamic'}
        kwargs['include_archive'] = False
        return await super().search(query, **kwargs)

    def find_exact_content(self, content, domain_filter=None, *, active_plan=False):
        bucket = super().find_exact_content(content, domain_filter=domain_filter, active_plan=active_plan)
        kind = 'plan' if active_plan else 'dynamic'
        if bucket and not editable(bucket, self, kind):
            self.deny('native dedup target is outside the permitted memory type')
        return bucket

    async def create(self, *args, **kwargs):
        self.check()
        for target, expected in self.expected.items():
            current = await self.get(target)
            if current is None or revision(current) != expected:
                self.deny('reviewed merge or update target changed before creation')
        kind = 'plan' if self.tool == 'memory_plan_create' else 'dynamic'
        if kwargs.get('bucket_type', 'dynamic') != kind or kwargs.get('pinned') or kwargs.get('protected'):
            self.deny('native creation crossed the approved type boundary')
        if any(not isinstance(tag, str) or (tag.strip().startswith('__') and
               not (kind == 'plan' and tag == '__plan__')) for tag in kwargs.get('tags') or []):
            self.deny('native metadata crossed the approved special-tag boundary')
        bucket_id = await super().create(*args, **kwargs)
        bucket = await self.get(bucket_id)
        if bucket is None:
            if kind == 'plan':
                # Native create returned after its disk write. A failed
                # readback is uncertain persistence, not a pre-write denial.
                raise OSError('created plan cannot be read back')
            self.deny('created record is not readable within the approved scope')
        self.expected[bucket_id] = revision(bucket)
        self.mutations.append({'action': 'created', 'bucket_id': bucket_id})
        # Review provenance stays in the control journal. Native storage has
        # an explicit metadata field allowlist; do not invent a second schema
        # or perform a misleading no-op update of an unsupported field.
        return bucket_id

    async def _update_locked(self, bucket_id, **kwargs):
        self.check()
        path = self._find_bucket_file(bucket_id)
        bucket = self._load_bucket(path) if path else None
        if bucket is None or not editable(bucket, self, bucket['metadata'].get('type', 'dynamic')):
            self.deny('update target left the permitted type or location')
        if self.expected.get(bucket_id) != revision(bucket):
            self.deny('record changed since the reviewed full snapshot')
        committed = await super()._update_locked(bucket_id, **kwargs)
        if not committed:
            self.deny('native update did not commit')
        current = self._load_bucket(self._find_bucket_file(bucket_id))
        self.expected[bucket_id] = revision(current)
        self.mutations.append({'action': 'updated', 'bucket_id': bucket_id,
                               'version': self.expected[bucket_id]})
        return committed


async def execute_approved(settings, request_id):
    journal = WriteJournal(settings.journal, settings.identity)
    row = journal.claim(request_id)
    if row is None:
        return None
    # A crash after claim leaves 'running' and is never blindly re-executed.
    receipt = {'status': 'failed', 'items': [], 'index': {},
               'metadata_mode': 'native_local_defaults_no_paid_llm', 'automatic_retry': False,
               'request_id': row['id'], 'review_digest': row['digest'],
               'reviewed_at': row['reviewed']}
    engine = None
    dehydrator = None
    try:
        tool = row['body']['tool']
        if tool not in settings.tools:
            raise PermissionError('write grant was withdrawn')
        payload = validate_request(tool, row['body']['payload'])
        if rt.bucket_mgr is not None:
            raise RuntimeError('native writes require an isolated process runtime')
        # Validate an existing DB with mode=ro first; do not run native startup
        # schema creation, backfill, metadata migration, quarantine or rebuild.
        config = deepcopy(settings.read.config)
        prior_url = os.environ.pop('OMBRE_OLLAMA_URL', None)
        try:
            engine = ExistingEmbeddingEngine(config)
        finally:
            if prior_url is not None:
                os.environ['OMBRE_OLLAMA_URL'] = prior_url
        manager = ReviewedManager(config, engine, row)
        for bucket_id, expected in manager.expected.items():
            bucket = await manager.get(bucket_id)
            if bucket is None or revision(bucket) != expected:
                raise WriteConflict('reviewed record changed before execution')
        engine.read_only = False
        config.update(read_only=False, memory_write_adapter=True)
        # Existing native outbox is durable, but no worker is started here.
        outbox = EmbeddingOutbox(config, manager, engine)
        manager.attach_embedding_outbox(outbox)
        dehydrator = Dehydrator(config, initialize_cache=False)
        rt.init(config=config, bucket_mgr=manager, embedding_engine=engine,
                decay_engine=NoMaintenance(), dehydrator=dehydrator,
                embedding_outbox=outbox, source_store=SourceStore(config['buckets_dir']),
                deletion_requests=None, mark_op=None, fire_webhook=None, v3_runtime=None,
                logger=logging.getLogger('memory-writer'))
        items = payload['items'] if tool == 'memory_grow' else [payload]
        # Same adapter worker lease for every request/Vault; native bucket and
        # content leases continue to coordinate with unmodified Cyberboss.
        async with _filesystem_turn(str(Path(settings.journal).parent), settings.identity):
            for index, item in enumerate(items):
                start = len(manager.mutations)
                manager.denied = None
                result = {'index': index, 'status': 'failed', 'mutations': []}
                receipt['items'].append(result)
                journal.progress(request_id, dict(receipt, status='running'))
                plan_result = None
                try:
                    if tool == 'memory_write':
                        from tools.hold import dispatch
                        args = dict(item)
                        if 'tags' in args:
                            args['tags'] = ','.join(args['tags'])
                        await dispatch(**args)
                    elif tool == 'memory_grow':
                        from tools.grow import dispatch
                        # Native pre-split grow, no second splitter or dedup.
                        await dispatch(content=payload.get('content', ''), items=[item])
                    elif tool == 'memory_plan_create':
                        from tools.plan import plan_create
                        plan_result = await plan_create(**item, structured=True)
                        result['status'] = plan_result.status
                        if plan_result.duplicate:
                            result['outcome'] = 'native_duplicate'
                            result['existing_bucket_id'] = plan_result.bucket_id
                        elif plan_result.bucket_id:
                            result['bucket_id'] = plan_result.bucket_id
                        if plan_result.error:
                            result['error'] = plan_result.error
                        continue
                    else:
                        from tools.trace import dispatch
                        changes = dict(payload['changes'])
                        for key in ('tags', 'domain'):
                            if key in changes:
                                changes[key] = ','.join(changes[key])
                        await dispatch(bucket_id=payload['bucket_id'], **changes)
                    manager.check()
                    if len(manager.mutations) == start:
                        matches = [b for b in await manager.list_all()
                                   if b['content'].strip() == item['content'].strip()
                                   and b['id'] in manager.expected
                                   and revision(b) == manager.expected[b['id']]] if tool == 'memory_grow' else []
                        if not matches:
                            raise RuntimeError('native call produced no verified mutation')
                        result['outcome'] = 'native_duplicate'
                        result['existing_bucket_id'] = matches[0]['id']
                    result['status'] = 'succeeded'
                except Exception as exc:
                    # Native storage can fail after a Markdown commit. A text
                    # error alone cannot prove rollback or authorize a retry.
                    result['status'] = 'conflict' if isinstance(exc, WriteConflict) else 'indeterminate'
                    result['error'] = 'version_or_scope_conflict' if manager.denied else 'native_operation_failed'
                finally:
                    result['mutations'] = manager.mutations[start:]
                    if plan_result is None and result['status'] != 'succeeded' and result['mutations']:
                        result['status'] = 'partial'
                    journal.progress(request_id, dict(receipt, status='running'))
            for bucket_id in {m['bucket_id'] for m in manager.mutations}:
                if tool == 'memory_plan_create' and receipt['items'][0]['status'] != 'succeeded':
                    receipt['index'][bucket_id] = 'index_unconfirmed'
                    continue
                bucket = await manager.get(bucket_id)
                if bucket is None:
                    receipt['index'][bucket_id] = 'record_changed'
                    continue
                current_hash = hashlib.sha256(bucket['content'].encode()).hexdigest()
                ready = engine.enabled and engine.get_content_hash(bucket_id) == current_hash
                if ready:
                    outbox.complete_content(bucket_id, bucket['content'])
                receipt['index'][bucket_id] = 'ready' if ready else (
                    'pending_native_outbox' if outbox.is_pending(bucket_id) else 'index_unconfirmed')
            statuses = [item['status'] for item in receipt['items']]
            receipt['status'] = 'succeeded' if all(s == 'succeeded' for s in statuses) else (
                'partial' if any(s in ('succeeded', 'partial') for s in statuses) else
                'indeterminate' if any(s == 'indeterminate' for s in statuses) else
                'conflict' if any(s == 'conflict' for s in statuses) else 'failed')
    except Exception as exc:
        receipt['status'] = 'conflict' if isinstance(exc, WriteConflict) else 'failed'
        receipt['error'] = 'reviewed_version_conflict' if isinstance(exc, WriteConflict) else 'isolated_writer_failed'
    finally:
        if dehydrator is not None:
            dehydrator.close()
        if engine is not None and engine._backend is not None:
            await engine._backend._client.close()
        # Do not clear another process/runtime's globals after an early denial.
        if rt.bucket_mgr is locals().get('manager'):
            rt.init(**{name: None for name in (
                'config', 'bucket_mgr', 'embedding_engine', 'decay_engine', 'dehydrator',
                'embedding_outbox', 'source_store', 'deletion_requests', 'mark_op',
                'fire_webhook', 'v3_runtime', 'logger')})
    journal.finish(request_id, receipt)
    return receipt
