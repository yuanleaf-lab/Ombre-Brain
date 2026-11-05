"""Loopback-only authenticated Streamable HTTP for the three read-only tools."""
import sys
sys.dont_write_bytecode = True

import argparse
import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
import hashlib
import hmac
import logging
import os
from pathlib import Path
import re
import socket

import yaml
from mcp.server.transport_security import TransportSecuritySettings

from memory_stdio import MemoryRuntime, MemorySettings, create_server, install_read_guard, validate_settings


HTTP_TOKEN_ENV = 'MEMORY_MCP_HTTP_TOKEN'


@dataclass(frozen=True)
class HTTPSettings:
    memory: MemorySettings
    host: str
    port: int
    endpoint: str
    token_digest: bytes = field(repr=False)


def load_http_settings(path: Path, token: str | None = None) -> HTTPSettings:
    raw = yaml.safe_load(Path(path).read_text(encoding='utf-8'))
    return validate_http_settings(raw, token)


def validate_http_settings(raw, token: str | None = None) -> HTTPSettings:
    if not isinstance(raw, dict):
        raise ValueError('HTTP memory configuration must be a mapping')
    supplied = os.environ.get(HTTP_TOKEN_ENV, '') if token is None else token
    if not isinstance(supplied, str) or not re.fullmatch(r'[A-Za-z0-9._~-]{32,256}', supplied):
        raise PermissionError('HTTP memory token is required in the environment')
    if set(raw) - {'read_only', 'transport', 'host', 'port', 'endpoint', 'buckets_dir', 'embedding', 'surfacing', 'permissions'}:
        raise ValueError('unsupported HTTP memory configuration option')
    host, port, endpoint = raw.get('host'), raw.get('port'), raw.get('endpoint')
    if host != '127.0.0.1' or type(port) is not int or not 1024 <= port <= 65535 or endpoint != '/mcp':
        raise ValueError('HTTP memory requires host=127.0.0.1, explicit port and endpoint=/mcp')
    shared = {k: v for k, v in raw.items() if k not in ('host', 'port', 'endpoint')}
    shared['auth'] = {'token_sha256': hashlib.sha256(supplied.encode('ascii')).hexdigest()}
    memory = validate_settings(shared, supplied, expected_transport='streamable-http')
    return HTTPSettings(memory, host, port, endpoint, hashlib.sha256(supplied.encode('ascii')).digest())


class BearerMiddleware:
    """Authenticate all HTTP methods before any routing or MCP processing."""
    def __init__(self, app, *, token_digest):
        self.app = app
        self.token_digest = token_digest

    async def __call__(self, scope, receive, send):
        if scope['type'] == 'websocket':
            await send({'type': 'websocket.close', 'code': 1008})
            return
        if scope['type'] != 'http':
            await self.app(scope, receive, send)
            return
        headers = [v for k, v in scope.get('headers', ()) if k.lower() == b'authorization']
        valid = False
        if len(headers) == 1 and len(headers[0]) <= 1024:
            scheme, separator, credential = headers[0].partition(b' ')
            if scheme.lower() == b'bearer' and separator and credential:
                valid = hmac.compare_digest(hashlib.sha256(credential).digest(), self.token_digest)
        if not valid:
            body = b'{"error":"unauthorized"}'
            await send({'type': 'http.response.start', 'status': 401, 'headers': [
                (b'content-type', b'application/json'), (b'www-authenticate', b'Bearer realm="memory"'),
                (b'cache-control', b'no-store'), (b'content-length', str(len(body)).encode()),
            ]})
            await send({'type': 'http.response.body', 'body': body})
            return
        await self.app(scope, receive, send)


def create_http_app(settings: HTTPSettings, *, server_factory=None, runtime_factory=None):
    runtime = (runtime_factory or MemoryRuntime)(settings.memory)
    # HTTP sessions have their own SDK lifespans; shared read runtime must be
    # opened exactly once at application startup, not once per request/session.
    server = (server_factory or create_server)(settings.memory, runtime=runtime, manage_runtime_lifespan=False)
    server.settings.host = settings.host
    server.settings.port = settings.port
    server.settings.streamable_http_path = settings.endpoint
    server.settings.json_response = True
    server.settings.stateless_http = True
    server.settings.max_request_body_size = 65536
    server.settings.transport_security = TransportSecuritySettings(
        enable_dns_rebinding_protection=True,
        allowed_hosts=[f'127.0.0.1:{settings.port}', f'localhost:{settings.port}'],
        allowed_origins=[f'http://127.0.0.1:{settings.port}', f'http://localhost:{settings.port}'],
    )
    app = server.streamable_http_app()
    sdk_lifespan = app.router.lifespan_context

    @asynccontextmanager
    async def lifespan(application):
        await runtime.open()
        try:
            async with sdk_lifespan(application):
                yield
        finally:
            await runtime.close()

    app.router.lifespan_context = lifespan
    app.add_middleware(BearerMiddleware, token_digest=settings.token_digest)
    return app


def main():
    parser = argparse.ArgumentParser(description='Local read-only memory MCP, Streamable HTTP')
    parser.add_argument('--config', type=Path, required=True)
    args = parser.parse_args()
    try:
        settings = load_http_settings(args.config)
        os.environ.pop(HTTP_TOKEN_ENV, None)
        logging.basicConfig(level=logging.WARNING, stream=sys.stderr, force=True)
        import uvicorn
        app = create_http_app(settings)
        server = uvicorn.Server(uvicorn.Config(
            app, host=settings.host, port=settings.port, access_log=False, log_config=None,
            log_level='warning', proxy_headers=False, ws='none', lifespan='on',
            server_header=False, timeout_keep_alive=5, limit_concurrency=16,
        ))
        # Bind the one explicitly configured loopback listener before applying
        # the read guard. All subsequent application bind/write attempts fail.
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
            if os.name == 'nt':
                listener.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
            listener.bind((settings.host, settings.port))

            async def run():
                install_read_guard()
                await server.serve(sockets=[listener])

            asyncio.run(run())
        return 0
    except Exception:
        print('memory HTTP startup failed (credential, configuration, port, or read-only boundary)', file=sys.stderr)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
