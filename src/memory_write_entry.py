"""Opt-in authenticated read/propose MCP plus a separate trusted operator CLI.

The original memory_stdio.py / memory_http.py remain read-only. This entry
requires an independent writes grant before opening a private review journal.
"""
import sys
sys.dont_write_bytecode = True

import argparse
import asyncio
from copy import deepcopy
import hashlib
import logging
import os
from pathlib import Path
import secrets
import socket
from typing import Annotated

from mcp.types import ToolAnnotations
from pydantic import Field, StrictInt
import yaml

from memory_stdio import MemoryRuntime, create_server, install_read_guard, validate_settings
from memory_http import create_http_app, validate_http_settings
from memory_write_journal import WriteJournal
from memory_write_policy import WRITE_TOOLS, canonical
from memory_write_service import VersionedReadRuntime, WriteCoordinator, WriteSettings


def validate_write_settings(raw, token=None, *, operator=False):
    if type(raw) is not dict:
        raise ValueError('configuration must be a mapping')
    raw = deepcopy(raw)
    control = raw.pop('writes', {})
    # Authenticate before opening journal files or validating their paths.
    if operator:
        token = secrets.token_urlsafe(40)
        if raw.get('transport') == 'stdio':
            raw['auth'] = {'token_sha256': hashlib.sha256(token.encode()).hexdigest()}
    http = None
    if raw.get('transport') == 'streamable-http':
        http = validate_http_settings(raw, token)
        read = http.memory
    else:
        read = validate_settings(raw, token)
    if type(control) is not dict or set(control)-{'enabled', 'tools', 'journal_path'}:
        raise ValueError('unsupported write authorization configuration')
    if type(control.get('enabled', False)) is not bool:
        raise ValueError('writes.enabled must be a boolean')
    if not control.get('enabled', False):
        if set(control)-{'enabled'}:
            raise ValueError('disabled writes cannot grant tools or configure state')
        return WriteSettings(read, None, frozenset(), http)
    tools = control.get('tools')
    if (type(tools) is not list or not tools or len(set(tools)) != len(tools) or
            any(type(t) is not str or t not in WRITE_TOOLS for t in tools)):
        raise ValueError('writes.tools must explicitly list unique supported tools')
    path = control.get('journal_path')
    if type(path) is not str or not Path(path).is_absolute():
        raise ValueError('writes.journal_path must be an explicit private absolute path')
    return WriteSettings(read, Path(path), frozenset(tools), http)


def load_write_settings(path, *, operator=False):
    return validate_write_settings(yaml.safe_load(Path(path).read_text('utf-8')), operator=operator)


def create_write_server(settings, *, runtime=None, manage_runtime_lifespan=True, config_path=None):
    factory = VersionedReadRuntime if settings.tools else MemoryRuntime
    runtime = runtime or factory(settings.read)
    server = create_server(settings.read, runtime=runtime,
                           manage_runtime_lifespan=manage_runtime_lifespan,
                           instructions=('Use memory_bootstrap for context. Write tools first return a pending '
                           'request. Only an independent local user can review the exact digest. Repeat the '
                           'same request ID and payload after approval to execute or retrieve its receipt. '
                           'Never claim a pending, partial, or indeterminate request succeeded. '
                           'Updates require a full read and its memory_version or plan_versions token.')
                           if settings.tools else None)
    if not settings.tools:
        return server
    coordinator = WriteCoordinator(settings, runtime)
    lock = asyncio.Lock()

    async def execute_call(tool, request_id, payload):
        async with lock:
            result = await coordinator.submit(tool, request_id, payload)
            if result['state'] == 'approved':
                if config_path is None:
                    raise RuntimeError('isolated writer configuration is unavailable')
                # No payload, credential, user command or arbitrary script is
                # passed to a shell. The worker revalidates config and approval.
                process = await asyncio.create_subprocess_exec(
                    sys.executable, '-B', str(Path(__file__).resolve()), 'execute',
                    '--config', str(Path(config_path).resolve()), '--request', request_id,
                    stdin=asyncio.subprocess.DEVNULL, stdout=asyncio.subprocess.DEVNULL,
                    stderr=asyncio.subprocess.DEVNULL)
                await process.wait()
                result = coordinator.public(coordinator.journal.get(request_id))
            return canonical(result)

    async def call(tool, request_id, payload):
        try:
            return await execute_call(tool, request_id, payload)
        except Exception:
            # Native YAML/provider/filesystem errors may contain source text,
            # paths or credentials. Never reflect those into MCP tool errors.
            raise RuntimeError('memory write request rejected (permission, input, review or source state)') from None

    async def memory_write(
        request_id: Annotated[str, Field(min_length=1, max_length=128)],
        content: Annotated[str, Field(min_length=1, max_length=40000)],
        title: str | None = None, tags: list[str] | None = None,
        domain: list[str] | None = None, importance: StrictInt | None = None,
        why_remembered: str | None = None,
    ) -> str:
        args = dict(content=content, title=title, tags=tags, domain=domain,
                    importance=importance, why_remembered=why_remembered)
        return await call('memory_write', request_id, {k: v for k, v in args.items() if v is not None})

    async def memory_grow(
        request_id: Annotated[str, Field(min_length=1, max_length=128)],
        items: Annotated[list[dict], Field(min_length=1, max_length=16)],
        content: Annotated[str, Field(max_length=200000)] = '',
    ) -> str:
        return await call('memory_grow', request_id, dict(items=items, **({'content': content} if content else {})))

    async def memory_update(request_id: str, bucket_id: str, expected_version: str, changes: dict) -> str:
        return await call('memory_update', request_id,
                          dict(bucket_id=bucket_id, expected_version=expected_version, changes=changes))

    async def memory_plan_create(request_id: str, content: str, weight: float | None = None,
                                 why_remembered: str | None = None) -> str:
        args = dict(content=content, weight=weight, why_remembered=why_remembered)
        return await call('memory_plan_create', request_id, {k: v for k, v in args.items() if v is not None})

    async def memory_plan_update(request_id: str, bucket_id: str, expected_version: str, changes: dict) -> str:
        return await call('memory_plan_update', request_id,
                          dict(bucket_id=bucket_id, expected_version=expected_version, changes=changes))

    for handler in (memory_write, memory_grow, memory_update, memory_plan_create, memory_plan_update):
        if handler.__name__ in settings.tools:
            server.tool(annotations=ToolAnnotations(readOnlyHint=False, destructiveHint=True,
                        idempotentHint=True, openWorldHint=True))(handler)
            tool = server._tool_manager.get_tool(handler.__name__)
            model = tool.fn_metadata.arg_model
            model.model_config = dict(model.model_config, extra='forbid', strict=True)
            model.model_rebuild(force=True)
            tool.parameters = model.model_json_schema()
    return server


def review_request(settings, request_id):
    if not sys.stdin.isatty() or not sys.stdout.isatty():
        raise PermissionError('review requires a local interactive operator terminal')
    journal = WriteJournal(settings.journal, settings.identity)
    row = journal.get(request_id)
    print(canonical({'request_id': row['id'], 'digest': row['digest'], 'state': row['state'],
                     'body': row['body']}))
    print('Review the entire payload, original records and native merge/cascade effects above.')
    answer = input('Type APPROVE followed by the complete digest to approve; anything else rejects: ')
    journal.review(request_id, row['digest'], approve=(answer == 'APPROVE '+row['digest']))


async def serve(settings, config_path):
    if settings.http is None:
        server = create_write_server(settings, config_path=config_path)
        if not settings.tools:
            install_read_guard()
        await server.run_stdio_async()
        return
    import uvicorn
    app = create_http_app(
        settings.http,
        runtime_factory=VersionedReadRuntime if settings.tools else MemoryRuntime,
        server_factory=lambda _read, **kw: create_write_server(settings, config_path=config_path, **kw))
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        if os.name == 'nt':
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
        listener.bind((settings.http.host, settings.http.port))
        if not settings.tools:
            install_read_guard()
        await uvicorn.Server(uvicorn.Config(
            app, host=settings.http.host, port=settings.http.port, access_log=False,
            log_config=None, proxy_headers=False, ws='none', lifespan='on',
            server_header=False, limit_concurrency=16)).serve(sockets=[listener])


def main():
    parser = argparse.ArgumentParser(description='Opt-in reviewed native OB writes')
    parser.add_argument('command', choices=('serve', 'review', 'execute'))
    parser.add_argument('--config', type=Path, required=True)
    parser.add_argument('--request')
    args = parser.parse_args()
    try:
        settings = load_write_settings(args.config, operator=args.command != 'serve')
        if args.command != 'serve' and (not settings.tools or not args.request):
            raise PermissionError('an enabled write grant and a request are required')
        if args.command == 'review':
            review_request(settings, args.request)
        elif args.command == 'execute':
            # Native messages can contain provider details; keep them out of
            # MCP logs. The durable receipt uses fixed, non-sensitive errors.
            logging.disable(logging.CRITICAL)
            from memory_write_executor import execute_approved
            asyncio.run(execute_approved(settings, args.request))
        else:
            for name in ('MEMORY_MCP_TOKEN', 'MEMORY_MCP_HTTP_TOKEN'):
                os.environ.pop(name, None)
            logging.basicConfig(level=logging.WARNING, stream=sys.stderr, force=True)
            asyncio.run(serve(settings, args.config))
        return 0
    except Exception:
        print('memory write control failed (authorization, review, configuration or isolated worker)', file=sys.stderr)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
