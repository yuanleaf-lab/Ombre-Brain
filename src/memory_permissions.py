"""Local administrator grants for the independent memory service only.

These declarations are shared by discovery, invocation and storage views.
No request argument can grant permissions, approve a memory, or enable writes.
"""
from dataclasses import dataclass
from types import MappingProxyType


ORDINARY = frozenset({'dynamic', 'permanent'})
DEFAULT_TOOLS = ('memory_bootstrap', 'memory_search', 'memory_read')


@dataclass(frozen=True)
class ToolRule:
    name: str
    operation: str
    memory_types: frozenset[str]


RULES = MappingProxyType({rule.name: rule for rule in (
    ToolRule('memory_bootstrap', 'surface', ORDINARY),
    ToolRule('memory_search', 'search', ORDINARY),
    ToolRule('memory_read', 'read', ORDINARY),
    ToolRule('memory_catalog', 'catalog', ORDINARY),
    ToolRule('memory_status', 'aggregate_status', ORDINARY),
    ToolRule('memory_feel_search', 'search', frozenset({'feel'})),
    ToolRule('memory_plan_list', 'list_active', frozenset({'plan'})),
    ToolRule('memory_self_read', 'read', frozenset({'i', 'self_candidate'})),
)})


@dataclass(frozen=True)
class MemoryPermissions:
    tools: frozenset[str] = frozenset(DEFAULT_TOOLS)

    @classmethod
    def from_config(cls, raw=None):
        if raw is None:
            return cls()
        if not isinstance(raw, dict) or set(raw) - {'tools', 'write_enabled'}:
            raise ValueError('unsupported memory permission setting')
        if type(raw.get('write_enabled', False)) is not bool or raw.get('write_enabled', False):
            raise ValueError('write permissions are unavailable in this read-only release')
        tools = raw.get('tools', list(DEFAULT_TOOLS))
        if (not isinstance(tools, list) or any(type(name) is not str or name not in RULES for name in tools)
                or len(set(tools)) != len(tools)):
            raise ValueError('permissions.tools must contain unique supported read tools')
        return cls(frozenset(tools))

    def allows(self, name):
        return name in self.tools and name in RULES

    def require(self, name):
        if not self.allows(name):
            raise PermissionError('memory tool is not authorized')
        return RULES[name]

    def to_config(self):
        return {'tools': [name for name in RULES if self.allows(name)], 'write_enabled': False}


def memory_kind(bucket):
    """Logical kind, with dedicated markers overriding misleading type values."""
    from tools.plan.core import is_letter_bucket
    if not bucket or is_letter_bucket(bucket):
        return 'excluded'
    meta = bucket.get('metadata') or {}
    if meta.get('deleted_at') or meta.get('tombstone'):
        return 'excluded'
    kind = meta.get('type', 'dynamic')
    tags = meta.get('tags') or []
    # Legacy/special markers must not turn private records into ordinary reads.
    if '__i__' in tags and kind != 'i':
        return 'excluded'
    if '__feel__' in tags and kind != 'feel':
        return 'excluded'
    if '__plan__' in tags and kind != 'plan':
        return 'excluded'
    if meta.get('i_stage') or '__i_candidate__' in tags:
        # Native _read_i uses type=i for its formal section independently of
        # candidate markers. Reject conflicting kinds, never promote by label.
        if kind not in ORDINARY:
            return 'excluded'
        return 'self_candidate'
    return kind
