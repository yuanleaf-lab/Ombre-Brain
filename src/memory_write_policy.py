"""Strict argument conversion, never a generic native-tool proxy."""
import hashlib
import json
import math
import re
import unicodedata


WRITE_TOOLS = ('memory_write', 'memory_grow', 'memory_update',
               'memory_plan_create', 'memory_plan_update')
UPDATE_FIELDS = {'content', 'title', 'tags', 'domain', 'importance', 'why_remembered'}
PLAN_FIELDS = {'content', 'status', 'weight', 'why_remembered'}
COMMON = {'content', 'title', 'tags', 'domain', 'importance', 'why_remembered'}


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'),
                      allow_nan=False, default=str)


def revision(bucket):
    """Full parsed source snapshot, not timestamps or a model supplied version."""
    return hashlib.sha256(canonical({'id': bucket['id'], 'content': bucket['content'],
                                     'metadata': bucket['metadata']}).encode()).hexdigest()


def valid_id(value):
    if type(value) is not str or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,159}', value) or '..' in value:
        raise ValueError('a complete memory ID is required')
    return value


def text(value, *, maximum=40000):
    if type(value) is not str or not value.strip() or len(value) > maximum:
        raise ValueError('non-empty bounded text is required')
    return value


def fields(raw, allowed):
    if type(raw) is not dict or not raw or set(raw) - allowed:
        raise ValueError('unsupported or empty memory parameters')
    result = dict(raw)
    for name, value in result.items():
        if name in {'content', 'title', 'why_remembered'}:
            text(value, maximum={'title': 200, 'why_remembered': 500}.get(name, 40000))
        elif name in {'tags', 'domain'}:
            if (type(value) is not list or len(value) > 16 or
                    any(type(v) is not str or not v.strip() or len(v) > 64 or
                        ',' in v or any(unicodedata.category(c) in ('Cc', 'Cf') for c in v) or
                        v.strip().startswith('__') or v.strip().casefold() in {'feel', 'plan', 'letter', 'i'} for v in value)):
                raise ValueError('only ordinary bounded tags/domains are allowed')
        elif name == 'importance':
            if type(value) is not int or not 1 <= value <= 9:
                raise ValueError('importance must be an integer from 1 to 9')
        elif name == 'weight':
            if type(value) not in (float, int) or not math.isfinite(value) or not 0 <= value <= 1:
                raise ValueError('weight must be finite and between zero and one')
        elif name == 'status' and value not in ('active', 'resolved', 'abandoned'):
            raise ValueError('unsupported plan status')
    return result


def validate_request(tool, payload):
    if tool not in WRITE_TOOLS or type(payload) is not dict:
        raise ValueError('unsupported write tool')
    if tool == 'memory_grow':
        if set(payload) - {'content', 'items'}:
            raise ValueError('unsupported grow parameters')
        items = payload.get('items')
        if type(items) is not list or not 1 <= len(items) <= 16:
            raise ValueError('native pre-split grow requires 1 to 16 items')
        result = {'items': [fields(item, COMMON) for item in items]}
        if any(not item.get('content') for item in result['items']):
            raise ValueError('each grow item needs content')
        if payload.get('content'):
            result['content'] = text(payload['content'], maximum=200000)
        return result
    if tool in ('memory_update', 'memory_plan_update'):
        if set(payload) != {'bucket_id', 'expected_version', 'changes'}:
            raise ValueError('updates require an ID, read version and whitelisted changes')
        valid_id(payload['bucket_id'])
        if type(payload['expected_version']) is not str or not re.fullmatch('[a-f0-9]{64}', payload['expected_version']):
            raise ValueError('expected_version must come from a full read')
        changes = fields(payload['changes'], PLAN_FIELDS if tool == 'memory_plan_update' else UPDATE_FIELDS)
        if any(changes.get(key) == [] for key in ('tags', 'domain')):
            raise ValueError('empty tag/domain replacement is not supported by native trace')
        return dict(payload, changes=changes)
    result = fields(payload, COMMON if tool == 'memory_write' else {'content', 'weight', 'why_remembered'})
    if not result.get('content'):
        raise ValueError('content is required')
    return result
