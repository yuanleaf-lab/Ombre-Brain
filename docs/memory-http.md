# Local read-only HTTP memory MCP

Entry: `src/memory_http.py`. Template: `config/memory-http.example.yaml`.
Only Streamable HTTP at `/mcp` is supported. The host must be exactly
`127.0.0.1`; wildcard, IPv6 and hostname bindings are rejected. The configuration
must explicitly select `read_only: true` and `transport: streamable-http`.

## Start and authenticate

Provide a random URL-safe token of 32–256 characters in the child environment
variable `MEMORY_MCP_HTTP_TOKEN`, then use the existing Python environment:

```powershell
.venv\Scripts\python.exe -B -u src\memory_http.py --config C:\absolute\path\memory-http.yaml
```

The config does not accept a token or auth section. Nothing reads the normal
OB config or changes Cyberboss's stdio configuration. Missing/invalid startup
credentials fail before binding a listener. The raw token is removed from the
server environment after loading; request checking retains a SHA-256 digest.

Requests must carry `Authorization: Bearer <credential supplied by the client>`.
Every HTTP method is authenticated, including GET, POST, DELETE and OPTIONS.
Missing, wrong or duplicate Authorization headers return 401 and a Bearer
challenge. Query-string tokens, cookies and alternate token headers do not
authenticate. Comparison uses hmac.compare_digest on token digests. Auth failure
does not print the credential. This is a local static Bearer scheme, not OAuth.

For a hidden Windows process, `scripts/start_memory_http.ps1` accepts a separate
configuration, a Windows-DPAPI encrypted credential file and an external log
directory. It decrypts only in memory, passes the token through the process
environment and restores the launcher's environment afterwards. Do not put the
credential file or logs inside a Vault or Git repository. No credential is
provided in a command argument. The local acceptance setup stores only DPAPI
ciphertext outside the repository; it does not store a plaintext token.

## Runtime and protocol

The HTTP entry reuses `create_server` and `MemoryRuntime` from memory_stdio.py;
it registers exactly memory_bootstrap, memory_search and memory_read, with the
same strict schemas and native read logic. It never imports the full server.py.
There is no dashboard, maintenance API, OAuth route, write tool or legacy SSE
endpoint.

The app opens one read-only runtime at startup. SDK HTTP request/session
lifespans do not recreate or close that runtime. Normal stdio retains its
original independent lifespan. Read operations are serialized by the same
tool lock to keep exact-ID snapshots separate between clients.

The existing SDK handles Streamable HTTP in stateless JSON response mode.
Host and Origin validation is enabled for the configured local address/port;
forwarded headers are not trusted, CORS is not enabled, bodies are limited to
64 KiB and the local server has a concurrency limit of 16. Access logs are
disabled and runtime logs use stderr, never a file inside the Vault.

The one configured local socket is bound before installing the existing
read-only audit guard. Windows also initializes its internal event-loop
socketpair first. The guard is then installed before the app opens the Vault,
blocking subsequent application binds, subprocess launches and Python file
writes. SQLite is still mode=ro/query_only, with normal WAL/SHM coordination;
immutable=1 is not used. The guard is not an OS sandbox.

## Verification

```powershell
$env:PYTHONPATH='src'
.venv\Scripts\python.exe -B -m unittest discover -s tests -p 'test_memory_http_unittest.py' -v
```

These standard-library tests use synthetic Vaults and in-memory ASGI requests.
They cover authentication, handshake, exact tool discovery, all three calls,
unknown writes, Host/Origin rejection, the body limit and unchanged files.

`tests/memory_http_acceptance.py` is an opt-in real TCP acceptance script. It
requires a successful sixth-stage report and verifies the already accepted
test copy before use. It launches temporary local HTTP and stdio processes,
uses real bge-m3, tests unauthorized requests and runs three finite normal OB
writer cycles on that same TEST Vault. It does not copy or rebuild a Vault.

The running stage-seven service uses an independent test configuration and
encrypted credential outside the repo. The service is not installed as a
Windows startup task. Its listener PID is recorded separately from the Python
venv launcher PID; confirm the process identity before stopping it.

Do not expose this local endpoint publicly or publish a tunnel as part of this
stage. Remote HTTPS/authentication is a later separately approved step.
