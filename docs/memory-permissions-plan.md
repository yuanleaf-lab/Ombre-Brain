# Memory permission foundation implementation plan

The user's approved inventory and next-stage request define this scope. Execute
in this session; do not commit, deploy, install dependencies or change production
configuration/data. Existing 28 uncommitted files have independent snapshots.

**Goal:** add an explicit, fail-closed permission foundation and five opt-in
read tools to the existing independent memory service.

**Architecture:** a frozen registry declares each tool's operation and logical
memory types. The same immutable grant determines registration and runtime
authorization. Scoped native BucketManager views restrict both record rendering
and vector candidate selection. No full server import or maintenance lifecycle.

**Stack:** existing Python, MCP SDK, Pydantic, SQLite and unittest.

## Constraints and review focus

- Default grants remain bootstrap/search/read. No write grant is supported in
  this release; write_enabled=true and unknown settings fail before Vault open.
- Never forward arbitrary native tool names, trace parameters or approval flags.
- Reject Letter markers even when a record claims an ordinary type; reject
  archived, deleted and physically misplaced records.
- Self candidates remain explicitly candidates and are excluded from ordinary
  reads. Legacy Feel records have unspecified subject, never inferred user facts.
- Empty results differ from failures. Semantic provider/index failures in Feel
  must raise in read-only mode. Normal native behavior remains unchanged.
- Plan means active plans only; self read means native I plus candidates, with
  native current/disputed/superseded distinctions. No expiry-unlock path.
- Status is an aggregate allowlist; never return memory bodies, names, IDs,
  domains, paths, endpoints, tokens, lock metadata or raw exceptions.

## Task 1: permissions and discovery

Files: new src/memory_permissions.py; update src/memory_stdio.py and
src/memory_http.py; new tests/test_memory_permissions_unittest.py.

- [x] Write and run failing tests for opt-in discovery, direct-call denial,
  unknown tools, forged approvals and rejected write permissions.
- [x] Implement immutable ToolRule/MemoryPermissions and strict configuration.
- [x] Register only granted wrappers and reauthorize every runtime invocation.
- [x] Re-run existing three-tool/HTTP authentication tests unchanged.

## Task 2: scoped extended native reads

Files: new src/memory_read_tools.py; update memory_stdio.py and only explicit
read_only branches in tools/breath/feel.py, catalog.py and surface.py.

- [x] Write and run failing tests for catalog, aggregate status, Feel, active
  plans and native self read; include hidden/mislabelled/special records.
- [x] Reuse native renderers/read functions behind per-tool typed views and
  vector candidate restrictions. Bypass I's mutating dispatcher.
- [x] Fail closed on native read errors; preserve all normal-mode branches.
- [x] Compare all fixture file hashes/lists, SQLite schema/data and integrity;
  reject attempted filesystem writes, events and background tasks.

## Task 3: transport and regression evidence

Files: new tests/test_memory_extended_protocol_unittest.py; new
docs/memory-permissions.md and separate opt-in configuration examples.

- [x] Exercise real stdio discovery/calls in a subprocess and authenticated
  HTTP through in-process ASGI (no listening port).
- [x] Run all existing 43 standard-library tests plus new tests with -B.
- [x] Review changes against snapshots; verify production Vault/config hashes.
- [x] Deliver a phase-only diff, permission matrix, failure cases, test results
  and complete side-effect audit. No live service restart or activation.
