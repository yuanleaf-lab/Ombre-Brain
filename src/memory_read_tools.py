"""Pure read adapters over native OB functions, never the full server."""
import json


class ScopedEmbeddings:
    """Restrict native vector candidates before ranking, without an index copy."""
    def __init__(self, engine, view):
        self._engine = engine
        self._view = view

    @property
    def enabled(self):
        return self._engine.enabled

    async def search_similar_strict(self, query, top_k=5, allowed_bucket_ids=None):
        permitted = {bucket['id'] for bucket in await self._view.list_all()}
        if allowed_bucket_ids is not None:
            permitted.intersection_update(allowed_bucket_ids)
        if not permitted:
            return []
        return await self._engine.search_similar_strict(
            query, top_k=top_k, allowed_bucket_ids=permitted)

    async def search_similar(self, query, top_k=5, allowed_bucket_ids=None):
        return await self.search_similar_strict(query, top_k, allowed_bucket_ids)


async def read_extended(name, arguments, runtime, view):
    """Only fixed read operations; no arbitrary native dispatch or trace input."""
    try:
        if name == 'memory_catalog':
            return await runtime.dispatch(catalog=True, **arguments)
        if name == 'memory_status':
            counts = {'dynamic': 0, 'permanent': 0}
            for bucket in await view.list_all():
                counts[bucket['metadata'].get('type', 'dynamic')] += 1
            embedding = runtime.config['embedding']
            return json.dumps({
                'read_only': True, 'counts': counts,
                'embedding': {'enabled': runtime.engine.enabled,
                              'model': embedding['model'], 'dim': embedding['dim']},
                'maintenance': 'disabled',
            }, sort_keys=True)
        if name == 'memory_feel_search':
            body = await runtime.dispatch(domain='feel', **arguments)
            # The native schema has no trustworthy user/assistant/inference
            # attribution. Do not retroactively assign a subject to old records.
            return '[emotion_subject=legacy_unspecified; emotion_basis=unspecified; not_user_fact]\n' + body
        if name == 'memory_plan_list':
            return await runtime.dispatch(domain='plan', **arguments)
        if name == 'memory_self_read':
            from tools.i.core import _read_i
            from utils import count_tokens_approx
            # Bypass i_core: that dispatcher marks operations and starts decay.
            body = await _read_i(arguments['limit'])
            if count_tokens_approx(body) > arguments['max_tokens']:
                raise RuntimeError('self read exceeds output budget')
            return body
    except Exception:
        label = {'memory_feel_search': 'feel', 'memory_plan_list': 'plan',
                 'memory_self_read': 'self', 'memory_catalog': 'catalog',
                 'memory_status': 'status'}[name]
        # MCP errors must not reflect database paths, provider bodies or secrets.
        raise RuntimeError(f'read-only {label} retrieval failed') from None
    raise PermissionError('unsupported memory read operation')
