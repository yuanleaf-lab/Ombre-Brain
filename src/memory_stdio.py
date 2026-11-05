"""Independent authenticated, read-only memory MCP over stdio only.

Does not import server.py, load the normal OB config, or start maintenance.
"""
import sys
sys.dont_write_bytecode = True

import argparse
from contextlib import asynccontextmanager
from dataclasses import dataclass
import hashlib
import hmac
import logging
import math
import os
from pathlib import Path
import re
from typing import Annotated
from urllib.parse import urlsplit

import yaml
from mcp.server.fastmcp import FastMCP
from mcp.types import ToolAnnotations
from pydantic import Field, StrictInt

from memory_permissions import DEFAULT_TOOLS, ORDINARY, MemoryPermissions, memory_kind


TOOLS = DEFAULT_TOOLS  # Compatibility: old configurations still advertise three.
TOKEN_ENV = 'MEMORY_MCP_TOKEN'


@dataclass(frozen=True)
class MemorySettings:
    config: dict


def load_settings(path: Path, token: str | None = None) -> MemorySettings:
    """Authenticate the local launcher before inspecting/opening its Vault."""
    raw = yaml.safe_load(Path(path).read_text(encoding='utf-8'))
    return validate_settings(raw, token)


def validate_settings(raw, token: str | None = None, *, expected_transport='stdio') -> MemorySettings:
    """Shared validation; stdio still accepts only its original transport."""
    if not isinstance(raw, dict):
        raise ValueError('memory configuration must be a mapping')
    auth = raw.get('auth')
    expected = auth.get('token_sha256') if isinstance(auth, dict) else None
    supplied = os.environ.get(TOKEN_ENV, '') if token is None else token
    if (not isinstance(expected, str) or not re.fullmatch(r'[a-f0-9]{64}', expected)
            or not isinstance(supplied, str) or len(supplied) < 32
            or not hmac.compare_digest(hashlib.sha256(supplied.encode()).hexdigest(), expected)):
        raise PermissionError('memory authorization failed')
    if set(raw) - {'read_only', 'transport', 'buckets_dir', 'embedding', 'auth', 'surfacing', 'permissions'}:
        raise ValueError('unsupported memory configuration option')
    permissions = MemoryPermissions.from_config(raw.get('permissions'))
    if set(auth) != {'token_sha256'}:
        raise ValueError('unsupported authentication option')
    if (expected_transport not in ('stdio', 'streamable-http') or
            raw.get('read_only') is not True or raw.get('transport') != expected_transport):
        raise ValueError('memory requires explicit read_only=true and its selected transport')
    directory = raw.get('buckets_dir')
    if not isinstance(directory, str) or not Path(directory).is_absolute():
        raise ValueError('an explicit absolute Vault path is required')
    root = Path(directory).resolve(strict=True)
    if not root.is_dir() or not (root / 'embeddings.db').is_file():
        raise ValueError('existing Vault and embeddings.db are required')
    embedding = raw.get('embedding')
    if not isinstance(embedding, dict) or set(embedding) - {
        'enabled', 'api_format', 'model', 'dim', 'base_url', 'timeout_seconds'
    }:
        raise ValueError('unsupported embedding configuration')
    if (type(embedding.get('enabled')) is not bool or embedding.get('api_format') != 'ollama'
            or embedding.get('model') not in ('bge-m3', 'bge-m3:latest')
            or embedding.get('dim') != 1024):
        raise ValueError('memory requires explicit existing local bge-m3/1024 configuration')
    address = urlsplit(embedding.get('base_url', ''))
    if (address.scheme != 'http' or address.hostname not in ('127.0.0.1', 'localhost', '::1')
            or address.username or address.password or address.query or address.fragment
            or address.path.rstrip('/') != '/v1' or not address.port):
        raise ValueError('only an explicit loopback Ollama /v1 endpoint is allowed')
    timeout = embedding.get('timeout_seconds', 120)
    if type(timeout) not in (int, float) or not math.isfinite(timeout) or not 0 < timeout <= 300:
        raise ValueError('invalid embedding timeout')
    surfacing = raw.get('surfacing', {})
    if not isinstance(surfacing, dict) or set(surfacing) - {'breath_max_results', 'breath_max_tokens'}:
        raise ValueError('unsupported surfacing configuration')
    for key, maximum in (('breath_max_results', 50), ('breath_max_tokens', 40000)):
        value = surfacing.get(key, 10 if key == 'breath_max_results' else 20000)
        if type(value) is not int or not 1 <= value <= maximum:
            raise ValueError('invalid surfacing budget')
    return MemorySettings({
        'read_only': True, 'transport': expected_transport, 'buckets_dir': str(root),
        'embedding': dict(embedding, timeout_seconds=timeout),
        'surfacing': dict(surfacing), 'storage': {'external_change_poll_seconds': 0},
        'permissions': permissions.to_config(),
    })


def _visible(bucket, memory_types=ORDINARY):
    if not bucket:
        return False
    return memory_kind(bucket) in memory_types


class _ReadView:
    """Restrict logical kinds AND physical paths; never expose mutation methods."""
    def __init__(self, manager, memory_types=ORDINARY):
        self.manager = manager
        self.memory_types = memory_types
        self.exact_snapshot = None
        root = Path(manager.base_dir).resolve()
        self.active_roots = (root / 'dynamic', root / 'permanent')
        self.kind_roots = {
            'dynamic': (root / 'dynamic',), 'permanent': (root / 'permanent',),
            'feel': (root / 'feel',), 'plan': (root / 'plans',),
            'i': self.active_roots, 'self_candidate': self.active_roots,
        }

    def visible(self, bucket):
        if not _visible(bucket, self.memory_types):
            return False
        path = Path(bucket['path']).resolve(strict=True)
        return any(path.is_relative_to(root) for root in self.kind_roots.get(memory_kind(bucket), ()))

    async def list_all(self, include_archive=False):
        return [b for b in await self.manager.list_all(False) if self.visible(b)]

    async def get(self, bucket_id):
        if self.exact_snapshot is not None and self.exact_snapshot['id'] == bucket_id:
            return self.exact_snapshot
        bucket = await self.manager.get(bucket_id)
        return bucket if self.visible(bucket) and bucket['id'] == bucket_id else None

    get_including_archive = get

    async def search(self, query, **kwargs):
        kwargs['include_archive'] = False
        kwargs['allowed_bucket_ids'] = {b['id'] for b in await self.list_all()}
        return [b for b in await self.manager.search(query, **kwargs) if self.visible(b)]

    def footprint_snapshot(self):
        return self.manager.footprint_snapshot()

    async def get_stats(self):
        # An empty ordinary bootstrap must not disclose private bucket counts.
        counts = dict.fromkeys(('permanent_count', 'dynamic_count', 'archive_count',
                                'feel_count', 'plan_count', 'letter_count'), 0)
        for bucket in await self.list_all():
            key = bucket['metadata'].get('type', 'dynamic') + '_count'
            if key in counts:
                counts[key] += 1
        return dict(counts, total_size_kb=0.0, domains={})


class _ScoringOnly:
    def __init__(self, engine):
        self._engine = engine

    def calculate_score(self, metadata):
        return self._engine.calculate_score(metadata)

    @property
    def is_running(self):
        return False


class MemoryRuntime:
    def __init__(self, settings):
        self.config = settings.config
        self.permissions = MemoryPermissions.from_config(self.config.get('permissions'))
        self.engine = None
        self.view = None

    async def open(self):
        if (self.config.get('read_only') is not True or
                self.config.get('transport') not in ('stdio', 'streamable-http')):
            raise ValueError('memory runtime requires read_only=true and an approved transport')
        from bucket_manager import BucketManager
        from decay_engine import DecayEngine
        from embedding_engine import EmbeddingEngine
        from tools import _runtime as rt
        if rt.bucket_mgr is not None:
            raise RuntimeError('memory requires its own process runtime')
        # OB's embedding constructor accepts this legacy override. Its value
        # must not override our independently validated endpoint.
        prior = os.environ.pop('OMBRE_OLLAMA_URL', None)
        try:
            self.engine = EmbeddingEngine(self.config)
        finally:
            if prior is not None:
                os.environ['OMBRE_OLLAMA_URL'] = prior
        try:
            self.view = _ReadView(BucketManager(self.config, self.engine))
            rt.init(config=self.config, bucket_mgr=self.view, embedding_engine=self.engine,
                    decay_engine=_ScoringOnly(DecayEngine(self.config, self.view)),
                    embedding_outbox=None, deletion_requests=None, dehydrator=None,
                    import_engine=None, source_store=None, you_service=None, them_service=None,
                    v3_runtime=None, mark_op=None, fire_webhook=None,
                    logger=logging.getLogger('memory'))
        except BaseException:
            await self.close()
            raise

    async def close(self):
        from tools import _runtime as rt
        if self.engine is not None and self.engine._backend is not None:
            await self.engine._backend._client.close()
        if self.view is not None and rt.bucket_mgr is self.view:
            rt.init(**{name: None for name in (
                'config', 'bucket_mgr', 'embedding_engine', 'decay_engine', 'embedding_outbox',
                'deletion_requests', 'dehydrator', 'import_engine', 'source_store',
                'you_service', 'them_service', 'v3_runtime', 'mark_op', 'fire_webhook', 'logger',
            )})
        self.engine = None
        self.view = None

    async def dispatch(self, **kwargs):
        if self.view is None:
            raise RuntimeError('memory runtime is not open')
        from tools.breath import dispatch
        return await dispatch(**kwargs)

    async def invoke(self, name, **arguments):
        rule = self.permissions.require(name)
        if self.view is None:
            raise RuntimeError('memory runtime is not open')
        from tools import _runtime as rt
        from memory_read_tools import ScopedEmbeddings, read_extended
        view = self.view if rule.memory_types == ORDINARY else _ReadView(self.view.manager, rule.memory_types)
        previous = rt.bucket_mgr, rt.embedding_engine
        rt.bucket_mgr, rt.embedding_engine = view, ScopedEmbeddings(self.engine, view)
        try:
            if name == 'memory_bootstrap':
                return await self.dispatch()
            if name == 'memory_search':
                return await self.dispatch(with_ids=True, **arguments)
            if name == 'memory_read':
                return await self.read(arguments['bucket_id'])
            return await read_extended(name, arguments, self, view)
        finally:
            rt.bucket_mgr, rt.embedding_engine = previous

    async def read(self, bucket_id):
        if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,159}', bucket_id) or '..' in bucket_id:
            raise ValueError('invalid full memory ID')
        if self.view is None:
            raise RuntimeError('memory runtime is not open')
        bucket = await self.view.get(bucket_id)
        if bucket is None:
            raise ValueError('memory_not_found_or_not_readable')
        # Render one coherent exact-ID snapshot even if another process moves
        # or edits the file while the native dispatcher is executing.
        self.view.exact_snapshot = bucket
        try:
            result = await self.dispatch(query=bucket_id, max_tokens=40000, max_results=1)
            if '[exact_bucket_id:true]' not in result:
                raise RuntimeError('memory_read_failed_or_budget_exceeded')
            return result
        finally:
            self.view.exact_snapshot = None


def create_server(settings: MemorySettings, *, runtime=None, manage_runtime_lifespan=True) -> FastMCP:
    import asyncio
    runtime = runtime if runtime is not None else MemoryRuntime(settings)
    permissions = runtime.permissions
    if permissions != MemoryPermissions.from_config(settings.config.get('permissions')):
        raise ValueError('runtime and discovery permissions must match')
    lock = asyncio.Lock()

    @asynccontextmanager
    async def lifespan(_):
        if not manage_runtime_lifespan:
            yield {}
            return
        await runtime.open()
        try:
            yield {}
        finally:
            await runtime.close()

    server = FastMCP('memory', log_level='WARNING', lifespan=lifespan,
                     instructions='Read-only external memory. Use memory_bootstrap for initial context; '
                     'use memory_search to find ordinary memories and memory_read for an exact ID.')
    annotations = ToolAnnotations(readOnlyHint=True, destructiveHint=False,
                                  idempotentHint=False, openWorldHint=True)

    async def memory_bootstrap() -> str:
        """Read important initial memories through OB's native breath logic."""
        async with lock:
            return await runtime.invoke('memory_bootstrap')

    async def memory_search(
        query: Annotated[str, Field(min_length=1, max_length=4096)],
        max_results: Annotated[StrictInt, Field(ge=1, le=50)] = 10,
    ) -> str:
        """Search active ordinary memories using native keyword and semantic retrieval."""
        if not query.strip():
            raise ValueError('query must not be blank')
        async with lock:
            return await runtime.invoke('memory_search', query=query, max_results=max_results)

    async def memory_read(bucket_id: Annotated[str, Field(min_length=1, max_length=160)]) -> str:
        """Read one complete ordinary memory ID; missing or private IDs return an error."""
        async with lock:
            return await runtime.invoke('memory_read', bucket_id=bucket_id)

    async def memory_catalog(
        domain: str = '', tags: str = '',
        max_results: Annotated[StrictInt, Field(ge=1, le=50)] = 20,
        date_from: str = '', date_to: str = '',
    ) -> str:
        """Catalog active ordinary dynamic/permanent memories; metadata only, no writes."""
        async with lock:
            return await runtime.invoke('memory_catalog', domain=domain, tags=tags,
                                        max_results=max_results, date_from=date_from, date_to=date_to)

    async def memory_status() -> str:
        """Read ordinary memory counts and safe model settings; no bodies, IDs or paths."""
        async with lock:
            return await runtime.invoke('memory_status')

    async def memory_feel_search(
        query: Annotated[str, Field(min_length=1, max_length=4096)],
        max_tokens: Annotated[StrictInt, Field(ge=1, le=40000)] = 20000,
        date_from: str = '', date_to: str = '',
    ) -> str:
        """Search active Feel only. Legacy subject is unspecified, never assumed user emotion/fact."""
        if not query.strip():
            raise ValueError('query must not be blank')
        async with lock:
            return await runtime.invoke('memory_feel_search', query=query, max_tokens=max_tokens,
                                        date_from=date_from, date_to=date_to)

    async def memory_plan_list(
        max_tokens: Annotated[StrictInt, Field(ge=1, le=40000)] = 20000,
    ) -> str:
        """Read active plans only; excludes resolved/abandoned plans. No status changes."""
        async with lock:
            return await runtime.invoke('memory_plan_list', max_tokens=max_tokens)

    async def memory_self_read(
        limit: Annotated[StrictInt, Field(ge=1, le=100)] = 20,
        max_tokens: Annotated[StrictInt, Field(ge=1, le=40000)] = 20000,
    ) -> str:
        """Read native I and explicitly labelled candidates; no promotion, witnessing or decay."""
        async with lock:
            return await runtime.invoke('memory_self_read', limit=limit, max_tokens=max_tokens)

    handlers = (memory_bootstrap, memory_search, memory_read, memory_catalog,
                memory_status, memory_feel_search, memory_plan_list, memory_self_read)
    for handler in handlers:
        if permissions.allows(handler.__name__):
            server.tool(annotations=annotations)(handler)

    # Match the OB SDK compatibility boundary: unknown arguments must fail,
    # rather than silently dropping a purported write/manage option.
    for name in permissions.to_config()['tools']:
        tool = server._tool_manager.get_tool(name)
        model = tool.fn_metadata.arg_model
        model.model_config = dict(model.model_config, extra='forbid', strict=True)
        model.model_rebuild(force=True)
        tool.parameters = model.model_json_schema()
    return server


def install_read_guard():
    """Defense in depth for this dedicated process; not an OS security sandbox.

    SQLite C-level WAL/SHM coordination is allowed by mode=ro, not immutable.
    """
    def guard(event, args):
        forbidden = event in {'os.mkdir', 'os.rename', 'os.remove', 'os.rmdir',
                              'os.truncate', 'os.chmod', 'subprocess.Popen', 'socket.bind'}
        if event == 'open':
            forbidden = bool((args[2] or 0) & (os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_TRUNC | os.O_APPEND))
        if event == 'sqlite3.connect':
            forbidden = 'mode=ro' not in str(args[0])
        if forbidden:
            raise PermissionError('memory process forbids operation: ' + event)
    sys.addaudithook(guard)


def main():
    parser = argparse.ArgumentParser(description='Independent read-only memory MCP (stdio only)')
    parser.add_argument('--config', type=Path, required=True)
    args = parser.parse_args()
    try:
        settings = load_settings(args.config)
        os.environ.pop(TOKEN_ENV, None)
        logging.basicConfig(level=logging.WARNING, stream=sys.stderr, force=True)
        server = create_server(settings)
        import asyncio

        async def run():
            # Windows creates an internal socketpair while constructing the
            # event loop. Install the guard afterwards, before Vault/runtime
            # initialization. No application listener is ever started.
            install_read_guard()
            await server.run_stdio_async()

        asyncio.run(run())
        return 0
    except Exception:
        # Never log configuration, credential, raw memory, or arbitrary paths.
        print('memory startup failed (authorization, configuration, or read-only boundary)', file=sys.stderr)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
