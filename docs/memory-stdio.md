# Independent read-only memory MCP

Entry: `src/memory_stdio.py`. Configuration template: `config/memory-stdio.example.yaml`.
This phase supports stdio only. It does not load `src/server.py` or the normal OB
configuration, register maintenance tools, or create/copy a Vault.

## Launch

Use the existing virtual environment and pass a separate absolute configuration:

```powershell
.venv\Scripts\python.exe -B -u src\memory_stdio.py --config C:\absolute\path\memory-stdio.yaml
```

The launching client must supply `MEMORY_MCP_TOKEN` in the child environment.
Generate a fresh random token with `secrets.token_urlsafe(32)` or equivalent;
put only its lowercase SHA-256 hex digest in `auth.token_sha256` in the separate
configuration. Keep the raw token in the launching client's credential handling,
not in command arguments, source code, reports, or the existing Cyberboss config.
The example contains deliberately invalid placeholders and will not start.

Missing/wrong credentials stop the process before Vault path inspection and
runtime creation. This is a local stdio launcher gate, not OAuth, HTTP Bearer
authentication, or protection against the Windows account owner. An authorized
client controls its private process pipes. Per-request network identity and
authorization belong to the later HTTP phase.

The required configuration has `read_only: true`, `transport: stdio`, an existing
absolute `buckets_dir`, and explicit local Ollama/bge-m3/1024 settings. Unknown
settings, cloud endpoints, write mode and other transports are rejected. The
normal OB config and its environment overrides are not loaded. A legacy
`OMBRE_OLLAMA_URL` cannot redirect this entry's validated local endpoint.

## Public interface

| Tool | Arguments | Native implementation |
|---|---|---|
| memory_bootstrap | none | breath dispatch without a query |
| memory_search | query, optional max_results (1–50, default 10) | breath_search dispatch, with result ID block |
| memory_read | full bucket_id | validated exact-ID snapshot rendered by native dispatch |

All three advertise readOnlyHint=true, destructiveHint=false and strict schemas.
Unknown arguments and tool names fail. No resources or prompts expose alternate
data paths. Public memory scope is active ordinary dynamic/permanent buckets;
special types and archived paths are excluded. An absent/unreadable ID is an
error, never a semantic fallback. Large exact reads retain native token limits
and fail if a complete exact rendering cannot be produced.

The default bootstrap may vary because native OB sampling is retained; tools
therefore do not claim idempotence. Tool instructions suggest bootstrap for
initial context but cannot force a client to invoke it in every new chat.

## Runtime isolation

The dedicated process constructs only the read-only embedding engine, native
bucket manager behind a read view, and a scoring-only decay facade. No lifecycle
starts decay, queues, dream, sync, tunnel, updater, importer or model child
processes. Existing Ollama is used; models are not downloaded. No file logger or
persistent error-log path is configured. Logs use stderr and MCP uses stdout.

CLI execution disables bytecode writes and installs a process audit guard before
business imports and Vault initialization. It rejects Python filesystem writes,
non-read-only SQLite connections, child-process launches and application socket
binding. Windows initializes its event loop before the guard because its
internal socketpair uses a short-lived loopback bind. The entry never starts an
application listener or HTTP server.

The audit guard is defense in depth, not an OS sandbox. SQLite remains mode=ro
with query_only and normal locking; C-level WAL/SHM coordination can occur and
is distinguished from application data changes. No immutable=1 is used.

## Verification

Standard-library tests:

```powershell
$env:PYTHONPATH='src'
.venv\Scripts\python.exe -B -m unittest discover -s tests -p 'test_memory_stdio_unittest.py' -v
.venv\Scripts\python.exe -B -m unittest discover -s tests -p 'test_memory_stdio_protocol_unittest.py' -v
```

The first uses synthetic Vaults and direct runtime calls. The second launches
the real entry through the installed MCP SDK and tests discovery, calls,
unknown/write commands and credential denial. It needs no pytest or model.

`tests/memory_stdio_acceptance.py` is opt-in real bge-m3 acceptance on a previously
verified independent test copy. It validates baseline hashes, calls the real
stdio MCP and runs a finite normal OB writer on the same TEST Vault. Repeated
runs require the preceding successful result as `--baseline-report`; unexpected
baseline changes fail closed. It never launches the full 16-tool server.

No resident service or client configuration is installed by these tests. An
actual Cyberboss session's OB read path is a separate product integration check;
normal native OB writer concurrency is the tested surrogate workload.
