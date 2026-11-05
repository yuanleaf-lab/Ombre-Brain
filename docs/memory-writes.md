# Reviewed native writes, first batch

This phase adds a separate `src/memory_write_entry.py`. The original
`memory_stdio.py` and `memory_http.py` remain read-only. No installation,
production activation, service restart, migration, commit or push is performed.

## Permission table

| Tool | Native implementation | Allowed scope | Default |
|---|---|---|---|
| memory_bootstrap | breath | Existing ordinary read scope | Existing read grant |
| memory_search | breath_search | Existing ordinary read scope | Existing read grant |
| memory_read | breath_search exact ID | Existing ordinary read scope; adds version in write entry | Existing read grant |
| memory_catalog | breath catalog | Existing ordinary read scope | Existing read grant |
| memory_status | aggregate allowlist | No body, private path or credentials | Existing read grant |
| memory_feel_search | native Feel read | Existing separate Feel read grant | Existing read grant |
| memory_plan_list | breath plan channel | Active plans; write entry appends read versions | Existing read grant |
| memory_self_read | native self read | Existing separate self read grant | Existing read grant |
| memory_write | hold dispatch | New dynamic event; native merge into reviewed dynamic targets | Denied |
| memory_grow | grow dispatch, pre-split items | 1–16 ordinary items; per-item native calls and receipts | Denied |
| memory_update | trace dispatch | Dynamic records; content/title/tags/domain/importance/why_remembered | Denied |
| memory_plan_create | plan_create | Active plan; content/weight/why_remembered | Denied |
| memory_plan_update | trace dispatch | Plan content/status/weight/why_remembered; reviewed linked-event resolution | Denied |

Protected/pinned/anchored memories, permanent/core writes, Feel writes, self
promotion, Letter access, deletion, archive, restoration, arbitrary metadata,
media, relation edits and generic trace are not write grants in this batch.
Importance is limited to integer 1–9. Neither top-level nor nested parameters
can add an approval, change permissions, select native functions or grant types.
Tag/domain values containing delimiter commas, reserved names, control or format
characters are rejected before native normalization. Native trace cannot clear
tags/domain with an empty list, so that ambiguous operation is also rejected.

## Configuration and authentication

Use a private copy of `config/memory-writes.example.yaml` and an existing test
Vault. `writes.enabled` defaults to false. Only an explicit nonempty list of
the five supported names can authorize proposals. Discovery and invocation use
the same frozen grant. The private journal must have an existing parent outside
the Vault; create that parent with permissions restricted to the local owner.
The journal contains private proposal content and is never publishable data.

`read_only: true` still applies to the MCP Vault reader. The independent write
grant permits proposal-journal writes and execution of approved requests in a
short-lived subprocess. It does not relax the original read-only entries or
their storage mode. The legacy `permissions.write_enabled` remains false.

Stdio launcher authentication uses `MEMORY_MCP_TOKEN` and its configured SHA-256.
HTTP uses the existing `MEMORY_MCP_HTTP_TOKEN`, Bearer authentication, loopback
binding and `/mcp`; copy the HTTP example and add the independent writes section.
No caller can set an `operator` flag through MCP. Never put tokens in arguments,
Git or the journal. Operator validation is a local CLI boundary, not an HTTP
authentication bypass.
HTTP retains the existing 64 KiB request-body limit; split larger submissions
into separately reviewable requests instead of assuming the field limits raise it.

Run with the already installed OB Python interpreter, without installing a new
environment or pointing at production data:

```text
<existing-python> -B src/memory_write_entry.py serve --config <private-config>
```

This entry must run in its own process. It does not import the native 16-tool
server. The HTTP application factory reuses the existing authenticated SDK app.

## Review flow

1. Call a granted write tool with a unique `request_id` and strict payload.
   This freezes its payload, native candidate snapshots, versions and digest.
   Only the separate control journal changes; the Vault does not.
2. A trusted local operator inspects that exact pending request:

   ```text
   <existing-python> -B src/memory_write_entry.py review --config <private-config> --request <request-id>
   ```

   The CLI requires an interactive terminal, displays the complete request and
   original records, and asks for `APPROVE <full-digest>`. Anything else rejects.
   There is no MCP review tool. A ChatGPT UI confirmation, `confirmed=true`, a
   model statement or a supplied approval token cannot substitute for review.
3. Repeat the identical write request. An approved request is atomically claimed
   and sent to a fixed isolated native worker, without a shell or inline content
   on the command line. Approval expires after 24 hours. An expired approval
   requires a new request and review; it cannot execute automatically.
4. The durable receipt is returned. Further identical calls return that receipt
   without calling OB again. Reusing an ID for another payload is rejected.

The host account, private config and journal directory form the trust boundary.
Untrusted automation must not have filesystem access to that directory or an
interactive operator terminal. This is not a sandbox against a compromised
local account. The review CLI is intentionally separate from the MCP surface.

## Versions, native side effects and receipts

The write entry's full `memory_read` appends `memory_version`; plan list appends
`plan_versions` for active returned IDs. Send that version with an exact ID and
`changes` to the corresponding update tool. The proposal reads the full current
record again, and the worker checks it both before execution and inside OB's
existing cross-process bucket lease. An external change is a conflict; it is
never silently overwritten. Special kinds and physically misplaced records
remain excluded even if metadata falsely labels them ordinary.

Hold/grow retain native search, exact-content detection, same-event decisions
and merge behavior. Candidate originals are part of user review. A denial is
sticky across the native merge-to-create fallback. The adapter does not copy
deduplication, splitting, storage or vector-index algorithms. Native source_tool,
grow_batch_id, tags, domains, links and plan change_log remain native. Exact
request/review provenance and mutation IDs stay in the separate durable journal
and receipts; no new field is added to OB's native memory data structure.

This first batch accepts ChatGPT-prepared `memory_grow.items`, invoking the
native pre-split path once per item. Its request groups individual receipts;
native grow batch IDs are per call, not one common batch ID for the whole adapter
request. Automatic LLM diary splitting is not added or claimed here. Metadata
uses native neutral fallback without a configured paid LLM. This is explicit in
receipts and is not a second metadata/Feel/Dream engine.

Receipts distinguish pending, rejected, running, finished and item outcomes:

- `succeeded`: verified native mutation, or verified native duplicate no-op.
- `conflict`: reviewed version or scope no longer permits the operation.
- `partial`: some mutations/items committed while another step failed.
- `indeterminate`: an error cannot prove whether native storage committed.
- `failed`: startup/validation prevented execution.

A claimed request left `running` may still be executing or may have crashed.
It is never automatically claimed a second time. If a crash happens between
Markdown commit and receipt persistence, local inspection is required. The
adapter does not promise distributed exactly-once commit across SQLite and
Markdown, or atomic rollback of a whole batch. It provides at most one attempt
per approved ID and explicit uncertain/partial outcomes.

SQLite is validated read-only before writer initialization; malformed vectors,
wrong model metadata, incompatible schema or corruption fail closed. Existing
DB writes use SQLite `mode=rw`, never replacement database creation. Normal WAL
locks are retained, without `immutable=1`. Native indexes, source evidence,
footprints, ledger events and lock files may be written after approval.

No decay, dream, sync, BM25 rebuild, embedding queue worker, duplicate-marking
suggestion or plan-resolution suggestion starts in the adapter writer. Explicit
plan resolution still performs its reviewed native linked-event cascade.
Vectors use native inline generation and durable outbox. `index` separately
reports ready, pending_native_outbox, index_unconfirmed or record_changed;
memory success does not pretend vector generation succeeded. Failed indexing
never causes a repeat of the memory write. A normal authorized OB outbox worker
can later process the native queue; this phase starts no such service.

## Verification

Run the existing 57 unittest cases plus new write/protocol cases, then the
existing 6 SQLite read-only cases, with the existing interpreter and `src` on
PYTHONPATH. Fixtures are synthetic temporary Vaults, not production copies.
The protocol tests cover actual stdio initialize/discovery/calls, local review,
an isolated real native writer, HTTP ASGI handshake and authentication, and
replay receipts. Index tests use a deterministic 1024-dimensional provider and
the real native SQLite storage. They are not real Ollama or remote ChatGPT
acceptance, and no such result is claimed.
