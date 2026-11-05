"""Proposal-only MCP control plane. Native mutations run in an isolated worker."""
from dataclasses import dataclass
import hashlib
from pathlib import Path
import time

from memory_permissions import memory_kind
from memory_stdio import MemoryRuntime, MemorySettings, _ReadView
from memory_write_journal import WriteJournal
from memory_write_policy import WRITE_TOOLS, canonical, revision, validate_request


@dataclass(frozen=True)
class WriteSettings:
    read: MemorySettings
    journal: Path | None
    tools: frozenset[str]
    http: object = None

    def __post_init__(self):
        if not self.tools:
            if self.journal is not None:
                raise ValueError('disabled writes must not configure a journal')
            return
        vault = Path(self.read.config['buckets_dir']).resolve()
        path = Path(self.journal).resolve()
        if path.is_relative_to(vault) or not path.parent.is_dir() or set(self.tools)-set(WRITE_TOOLS):
            raise ValueError('write journal must be outside the Vault and tools must be explicitly granted')

    @property
    def identity(self):
        return hashlib.sha256(canonical({'read': self.read.config, 'tools': sorted(self.tools)}).encode()).hexdigest()


def editable(bucket, manager, kind):
    if bucket is None or memory_kind(bucket) != kind or kind not in ('dynamic', 'plan'):
        return False
    if not _ReadView(manager, frozenset({kind})).visible(bucket):
        return False
    meta = bucket['metadata']
    return not any(meta.get(key) for key in ('pinned', 'protected', 'anchor', 'tombstone', 'deleted_at'))


class VersionedReadRuntime(MemoryRuntime):
    async def read(self, bucket_id):
        # Same native exact-ID rendering, plus a version for subsequent writes.
        before = await self.view.get(bucket_id)
        body = await super().read(bucket_id)
        after = await self.view.get(bucket_id)
        if before is None or after is None or revision(before) != revision(after):
            raise RuntimeError('memory changed during full read; read again')
        return body + '\n[memory_version:' + revision(after) + ']'

    async def invoke(self, name, **arguments):
        body = await super().invoke(name, **arguments)
        if name == 'memory_plan_list':
            plans = await _ReadView(self.view.manager, frozenset({'plan'})).list_all()
            versions = {b['id']: revision(b) for b in plans
                        if b['metadata'].get('status', 'active') == 'active' and b['id'] in body}
            body += '\n[plan_versions:' + canonical(versions) + ']'
        return body


class WriteCoordinator:
    def __init__(self, settings, runtime):
        self.settings = settings
        self.runtime = runtime
        self.journal = WriteJournal(settings.journal, settings.identity)

    async def submit(self, tool, request_id, payload):
        if tool not in self.settings.tools:
            raise PermissionError('write tool is not authorized')
        payload = validate_request(tool, payload)
        try:
            prior = self.journal.get(request_id)
        except ValueError:
            prior = None
        if prior is not None:
            if prior['body']['tool'] != tool or prior['body']['payload'] != payload:
                raise ValueError('request ID already belongs to another payload')
            return self.public(prior)
        if self.runtime.view is None:
            raise RuntimeError('read runtime is not open')
        manager = self.runtime.view.manager
        observations = {'versions': {}, 'records': {}, 'notes': [
            'hold/grow retain native automatic merge and exact-content deduplication',
            'plan resolution may update the reviewed linked ordinary events',
            'no decay, dream, sync, suggestion or embedding queue workers',
        ]}

        def observe(bucket, kind):
            if not editable(bucket, manager, kind):
                raise PermissionError('memory type, protection or physical location forbids this write')
            observations['versions'][bucket['id']] = revision(bucket)
            observations['records'][bucket['id']] = {
                'id': bucket['id'], 'content': bucket['content'], 'metadata': bucket['metadata']}

        if tool in ('memory_update', 'memory_plan_update'):
            kind = 'plan' if tool == 'memory_plan_update' else 'dynamic'
            bucket = await manager.get(payload['bucket_id'])
            observe(bucket, kind)
            if revision(bucket) != payload['expected_version']:
                raise ValueError('version conflict; read the complete current record again')
            if tool == 'memory_plan_update' and payload['changes'].get('status') == 'resolved':
                for key in ('related_bucket', 'resolved_by'):
                    related = bucket['metadata'].get(key)
                    if related and related not in ('manual', 'llm_judge'):
                        linked = await manager.get(related)
                        if linked is not None and not linked['metadata'].get('resolved'):
                            observe(linked, 'dynamic')
        elif tool in ('memory_write', 'memory_grow'):
            items = payload['items'] if tool == 'memory_grow' else [payload]
            view = _ReadView(manager, frozenset({'dynamic'}))
            for item in items:
                for hit in await view.search(item['content'], limit=5):
                    observe(hit, 'dynamic')
                exact = manager.find_exact_content(item['content'], domain_filter=None)
                if exact:
                    observe(exact, 'dynamic')
        elif tool == 'memory_plan_create':
            duplicate = manager.find_exact_content(payload['content'], active_plan=True)
            if duplicate:
                observe(duplicate, 'plan')
        return self.public(self.journal.submit(request_id, tool, payload, observations))

    @staticmethod
    def public(row):
        expired = row['state'] == 'approved' and (row['reviewed'] or 0) <= time.time()-86400
        return {'request_id': row['id'], 'digest': row['digest'],
                'state': 'approval_expired' if expired else row['state'],
                'approval_required': row['state'] == 'pending', 'receipt': row['receipt'],
                'retry': ('use a new request ID and obtain a new review' if expired else
                          'repeat the identical request; running attempts are never restarted')}
