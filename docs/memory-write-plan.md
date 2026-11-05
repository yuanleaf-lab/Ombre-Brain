# First write batch: implementation plan

The user's implementation request authorizes this work in the existing isolated
feature worktree. No commit, push, installation, deployment or production data
access is part of this phase.

## Design

Keep the existing read-only entries and their immutable eight-read registry.
Add an explicitly enabled, independently configured proposal entry which reuses
those reads and adds exactly five write tools. Its Vault runtime remains
read-only. Proposals and receipts live in a separate local SQLite journal,
outside the Vault. No arbitrary operation, approval flag, metadata, filesystem
path or native trace argument is accepted from MCP.

A local operator CLI displays and approves the exact request digest, observed
versions and native side effects. It has no MCP counterpart. A subsequent
identical tool request executes an approved proposal in a separate short-lived
process, through native hold/grow/trace/plan functions. The journal claims work
atomically before execution; replay returns the persisted receipt. A crash after
claim is indeterminate and never automatically re-executed.

Updates require an exact ID and revision returned by a full read. Check that
revision again inside the native cross-process bucket lease. Restrict mutation
to reviewed logical types and targets. Native merge/dedup/search remain native;
conflicts and unreviewed targets fail closed, including native fallback paths.
Do not start decay, dream, sync, native full-server or embedding queue workers.
Optional post-write suggestions are disabled only in this adapter runtime.

Grow initially uses native pre-split items, without a second splitter or a paid
LLM. Each item is a native grow call with an individual durable receipt; the
adapter request groups the item receipts. Never retry an ambiguous item.

## Tasks and verification

- [x] Write failing unittest cases for strict schemas, default denial,
  idempotency, operator-only review and version conflicts.
- [x] Implement the separate journal, immutable write policy and reviewer CLI.
- [x] Implement native write execution with scoped storage, version checks,
  no maintenance and durable per-item outcomes.
- [x] Add authenticated stdio/HTTP opt-in entry; retain original read entries.
- [x] Exercise real native storage/index paths in temporary Vaults, partial
  batches, concurrent requests, failure-after-commit and read compatibility.
- [x] Run the original 57 + 6 unittest cases and all new cases with existing
  dependencies, and deliver permission, diff, log and side-effect evidence.
