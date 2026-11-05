# Independent memory service: permission foundation and extended reads

This phase adds code and tests only. Existing launch configurations are not
changed; no running service is restarted. No production Vault is selected.

## Administrative configuration

Keep the original independent stdio/HTTP config and its authentication. Add the
optional section in `config/memory-permissions.example.yaml` to a separate TEST
config to opt in. It is a section, not a replacement launch configuration.
Without it, discovery and invocation still permit exactly the original three
tools. An empty `tools: []` grants nothing. Ordering is canonical registry order.

The authenticated service instance has one fixed local-administrator profile;
this release is not a per-user OAuth/multi-token role server. Restart/recreate
the instance to change grants. Requests cannot change its immutable grant.
Unknown/duplicate names, unknown permission fields, non-boolean write flags and
`write_enabled: true` fail before Vault open. Writes cannot be enabled at all in
this release; any future write implementation needs a separate explicit grant,
trusted human approval and new acceptance. No model-side approval flag exists.

## Permission and data matrix

| Tool | Fixed operation | Allowed active logical kinds | Default |
| --- | --- | --- | --- |
| memory_bootstrap | surface | dynamic, permanent | yes |
| memory_search | search | dynamic, permanent | yes |
| memory_read | exact full-ID read | dynamic, permanent | yes |
| memory_catalog | metadata catalog | dynamic, permanent | opt in |
| memory_status | aggregate status | dynamic, permanent | opt in |
| memory_feel_search | search | feel | opt in |
| memory_plan_list | list active plans | plan | opt in |
| memory_self_read | read native I and candidates | i, self_candidate | opt in |

The same `MemoryPermissions` object drives registration and invocation. Runtime
invocation rechecks the grant; each rule selects a scoped read view and vector
facade. Dedicated kinds require the matching physical active directory, not
just a metadata label. Letter markers override misleading ordinary types.
Archive/deletion markers and self candidates are excluded from ordinary reads.
Conflicting dedicated-kind/candidate markers are excluded from all views; the
adapter does not repair or promote such records.
There is no arbitrary `trace`, native tool-name forwarding, Letter read/unlock,
write/delete/archive/restore, index repair, sync or configuration operation.

## Tool inputs and native call chains

- `memory_bootstrap()`: native breath dispatcher -> surface_default.
- `memory_search(query,max_results=10)`: native breath dispatcher ->
  surface_search -> native vector/BucketManager search. Result IDs remain on.
- `memory_read(bucket_id)`: the existing exact-ID coherent snapshot path; a
  hidden or missing ID is an error, never a semantic fallback.
- `memory_catalog(domain='',tags='',max_results=20,date_from='',date_to='')`:
  native breath catalog branch. Domain/tags are comma-separated ordinary-memory
  filters, not a way to select another private memory kind. No body rendering.
- `memory_status()`: safe aggregate projection over the scoped BucketManager
  read result, rather than the full native pulse dispatcher. Returns only
  read_only, ordinary counts, embedding enabled/model/dim and maintenance state.
  No bodies, record names/IDs, domain lists, paths, URLs, credentials or locks.
- `memory_feel_search(query,max_tokens=20000,date_from='',date_to='')`: native
  Feel channel -> scoped native SQLite/vector scoring -> native Feel renderer.
  Read-only provider/index failures raise; only explicit embedding.enabled=false
  permits a labelled literal-only query. A valid zero-hit semantic query may use
  literal matching. Unexpected disabled engine is an error. Legacy attribution
  is labelled unspecified, never silently treated as user fact.
- `memory_plan_list(max_tokens=20000)`: native Plan channel. Returns ACTIVE
  plans only, newest first; no resolved/abandoned/archive search is promised.
  Whole records omitted by the native token budget remain disclosed as omitted.
- `memory_self_read(limit=20,max_tokens=20000)`: directly calls native `_read_i`,
  bypassing i_core's mark_op/decay startup. Keeps current, disputed, superseded
  and unconfirmed candidate distinctions. Native limit is per formal section,
  not a global pending-candidate limit; oversized output fails, never truncates
  a candidate into a fact. No witness/promotion/superseding write is invoked.

Native Feel/catalog/Plan error changes are conditional on read_only=true only.
All normal-mode branches and the native 16-tool registry are retained.
Native read displays may include metadata and links already attached to an
authorized record; permissions do not declassify the linked private target.

## Side-effect boundary

All eight tools use the existing isolated runtime: SQLite mode=ro/query_only,
no immutable=1, no index creation/migration/backfill/quarantine, no decay
lifecycle, embedding queue, dream, sync, Ollama spawn, events, webhooks, deletion
reconciliation or disk caches. Query embeddings/LRU/cache refresh may occur in
memory only. Candidate IDs are restricted before vector ranking. Normal SQLite
WAL/SHM coordination remains allowed; it is distinct from application writes.
The subprocess entry's existing audit guard remains installed. It is defense in
depth, not an OS security sandbox. MCP requests are serialized while native
module-global runtime views are temporarily scoped and restored in finally.

## Emotion attribution: proposal only, no stored schema changes

The native Feel path models assistant first-person feelings and optionally
updates the source bucket's digested/model_valence. It does not provide trusted
user/assistant/inference attribution. This phase does not enable that writer,
infer subjects from text, add metadata fields, or backfill old records.

Proposed minimum future extension, subject to separate approval:

| Meaning | emotion_subject | emotion_basis |
| --- | --- | --- |
| User describes their own feeling | user | self_report |
| Assistant first-person feeling | assistant | self_report |
| Contextual guess about a feeling | user or assistant, explicitly selected | inference |
| Existing ambiguous Feel | unspecified | unspecified |

Preserve source_bucket and its source/time. Inference needs separate provenance
and, if used, confidence; it must never be promoted to self_report/fact by the
model. A future trusted writer must distinguish a user event/emotion record from
assistant Feel: recording user emotion must not automatically digest the source
or set model_valence. No changes to production data are authorized by this doc.

## Verification

Run the six original unittest modules plus test_memory_permissions_unittest and
test_memory_extended_protocol_unittest with PYTHONPATH=src;tests and Python -B.
Fixtures contain only synthetic sentinels, including an expired locked Letter,
disguised Letter, active/resolved/abandoned plans, I candidates and malformed
scope examples. Checks compare complete file/directory manifests, SHA-256,
SQLite schema/data/meta, journal state and integrity, and intercept attempted
writes even if a native exception handler swallows them.

The protocol tests use a real stdio child with its read guard and HTTP ASGI
in-process transport, with no HTTP listener. Semantic unit tests replace only
the external model while retaining native 1024-dimensional SQLite decoding,
cosine scoring and rendering. These are not evidence of a fresh live Ollama
or product-side Cyberboss/ChatGPT test. Existing production services are untouched.
