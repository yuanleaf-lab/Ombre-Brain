"""Opt-in local acceptance on a private full Vault copy; no listening sockets.

WAL workers use saved vectors explicitly (not live Ollama inference). Live
Ollama is tested separately and its failure prevents full acceptance.
"""
import argparse
import asyncio
from contextvars import ContextVar
import hashlib
import json
import logging
import multiprocessing as mp
import os
from pathlib import Path
import shutil
import sqlite3
import sys
import time
import traceback
from urllib.request import urlopen

from bucket_manager import BucketManager
from decay_engine import DecayEngine
from embedding_engine import EmbeddingEngine
from ombrebrain.storage.vector_codec import decode_vector
from tools import _runtime as rt
from tools.breath import dispatch


def production_vault_path():
    """Return the explicitly configured production Vault used only as a safety guard."""
    configured = os.environ.get("OB_PRODUCTION_VAULT_PATH", "").strip()
    if not configured:
        raise RuntimeError(
            "Set OB_PRODUCTION_VAULT_PATH to the existing production Vault path "
            "before running live acceptance scripts."
        )
    path = Path(configured).expanduser().resolve(strict=True)
    if not path.is_dir():
        raise RuntimeError("OB_PRODUCTION_VAULT_PATH must point to an existing directory")
    return path


def manifest(root, coordination=False):
    result = {}
    for p in root.rglob('*'):
        if not p.is_file():
            continue
        name = str(p.relative_to(root))
        if coordination and name in {'embeddings.db-shm', 'embeddings.db-wal'}:
            # Windows byte-range locks can make raw SHM reads fail. Observe
            # coordination metadata; never bypass a live SQLite lock to hash it.
            result[name] = 'sqlite-coordination-bytes:' + str(p.stat().st_size)
        else:
            try:
                result[name] = hashlib.sha256(p.read_bytes()).hexdigest()
            except PermissionError as exc:
                raise PermissionError('cannot hash application file: ' + name) from exc
    return result


def reader_changes(before, after):
    changed = sorted(name for name in set(before) | set(after) if before.get(name) != after.get(name))
    application = [name for name in changed if name not in {'embeddings.db-wal', 'embeddings.db-shm'}]
    if application:
        raise AssertionError('reader changed application files: ' + repr(application))
    return changed


def inspect_db(root):
    uri = (root / 'embeddings.db').resolve().as_uri() + '?mode=ro'
    conn = sqlite3.connect(uri, uri=True)
    try:
        rows = conn.execute('SELECT bucket_id, embedding, meaning_embedding, content_hash FROM embeddings ORDER BY bucket_id').fetchall()
        dimensions = set()
        for _, content, meaning, _ in rows:
            for value in (content, meaning):
                if value:
                    dimensions.add(int(decode_vector(value).size))
        return {
            'integrity': conn.execute('PRAGMA integrity_check').fetchone()[0],
            'journal_mode': conn.execute('PRAGMA journal_mode').fetchone()[0],
            'schema': conn.execute("SELECT name, type, sql FROM sqlite_master ORDER BY name").fetchall(),
            'rows': len(rows), 'dimensions': sorted(dimensions),
            'logical_hash': hashlib.sha256(repr(rows).encode()).hexdigest(),
        }
    finally:
        conn.close()


def config_for(root, readonly, enabled):
    return {'read_only': readonly, 'buckets_dir': str(root),
            'embedding': {'enabled': enabled, 'api_format': 'ollama',
                          'model': 'bge-m3', 'dim': 1024,
                          'base_url': 'http://127.0.0.1:11434/v1', 'timeout_seconds': 3},
            'storage': {'external_change_poll_seconds': 0}}


def bind_runtime(config, manager, engine):
    rt.init(config=config, bucket_mgr=manager, embedding_engine=engine,
            decay_engine=DecayEngine(config, manager), embedding_outbox=None,
            deletion_requests=None, them_service=None, you_service=None,
            v3_runtime=None, mark_op=None, fire_webhook=None,
            logger=logging.getLogger('acceptance'))


_GUARD = ContextVar('acceptance_read_guard', default=None)


def audit(event, args):
    writes = _GUARD.get()
    if writes is None:
        return
    mutation = event in {'os.mkdir', 'os.rename', 'os.remove', 'os.rmdir', 'os.truncate', 'os.chmod'}
    if event == 'open':
        mutation = bool((args[2] or 0) & (os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_TRUNC | os.O_APPEND))
    if event == 'sqlite3.connect':
        mutation = 'mode=ro' not in str(args[0])
    if mutation:
        writes.append(event)
        raise PermissionError('reader mutation rejected: ' + event)


async def local_reads(root):
    before = manifest(root)
    db_before = inspect_db(root)
    writes = []
    token = _GUARD.set(writes)
    engine = None
    result = {}
    try:
        config = config_for(root, True, True)
        engine = EmbeddingEngine(config)
        engine._backend._client.max_retries = 0
        manager = BucketManager(config, engine)
        bind_runtime(config, manager, engine)
        buckets = await manager.list_all()
        ordinary = [b for b in buckets if b['metadata'].get('type') in ('dynamic', 'permanent', None)]
        if not ordinary:
            raise RuntimeError('no ordinary bucket to test')
        exact_id = ordinary[0]['id']
        text = await dispatch(query=exact_id)
        result['exact_id'] = '[exact_bucket_id:true]' in text
        result['bootstrap'] = bool(await dispatch())
        for label, query in [('semantic', '长期记忆与共同经历'), ('fusion', '记忆')]:
            try:
                pairs = await engine.search_similar_strict(query) if label == 'semantic' else None
                text = await dispatch(query=query) if label == 'fusion' else ''
                result[label] = {'passed': True, 'matches': len(pairs) if pairs is not None else len(text)}
            except Exception as exc:
                result[label] = {'passed': False, 'error': type(exc).__name__, 'message': str(exc)}
        result['bm25'] = {'instantiated': manager._bm25 is not None, 'rebuild_running': manager._bm25_rebuilding}
        result['decay_running'] = rt.decay_engine.is_running
        result['outbox_present'] = rt.embedding_outbox is not None
    finally:
        if engine and engine._backend:
            await engine._backend._client.close()
        _GUARD.reset(token)
    result['write_attempts'] = writes
    result['files_unchanged'] = before == manifest(root)
    result['database_unchanged'] = db_before == inspect_db(root)
    return result


def writer(root_text, pipe):
    root = Path(root_text)
    try:
        config = config_for(root, False, False)
        engine = EmbeddingEngine(config)
        manager = BucketManager(config, engine)
        conn = sqlite3.connect(engine.db_path, timeout=10)
        try:
            conn.execute('PRAGMA journal_mode=WAL')
            vector = conn.execute("SELECT embedding FROM embeddings WHERE embedding IS NOT NULL AND length(embedding)>2 LIMIT 1").fetchone()[0]
            vector = decode_vector(vector).tolist()
            pipe.send({'ready': True, 'mode': 'WAL', 'dimensions': len(vector)})
            ids = []
            for number in range(3):
                if pipe.recv() != 'write':
                    raise RuntimeError('unexpected writer command')
                content = 'OB offline WAL acceptance test ' + str(number)
                bid = asyncio.run(manager.create(content=content, domain=['acceptance'], test_data=True, defer_derived_index=True))
                engine._store_embedding(bid, vector, hashlib.sha256(content.encode()).hexdigest())
                ids.append(bid)
                # Hold an uncommitted transaction while reader uses old snapshot.
                conn.execute('BEGIN IMMEDIATE')
                conn.execute('UPDATE embeddings SET updated_at=? WHERE bucket_id=?',
                             ('2099-01-01T00:00:0' + str(number) + '+00:00', bid))
                pipe.send({'id': bid, 'transaction_open': True})
                if pipe.recv() != 'commit':
                    raise RuntimeError('missing commit command')
                conn.commit()
                pipe.send({'committed': bid})
            if pipe.recv() != 'finish':
                raise RuntimeError('missing finish')
            pipe.send({'done': True, 'ids': ids,
                       'integrity': conn.execute('PRAGMA integrity_check').fetchone()[0]})
        finally:
            conn.close()
    except Exception as exc:
        pipe.send({'error': type(exc).__name__, 'message': str(exc)})
    finally:
        pipe.close()


def reader(root_text, pipe):
    sys.addaudithook(audit)
    writes = []
    token = _GUARD.set(writes)
    root = Path(root_text)
    try:
        config = config_for(root, True, False)
        engine = EmbeddingEngine(config)
        manager = BucketManager(config, engine)
        bind_runtime(config, manager, engine)
        pipe.send({'ready': True})
        while True:
            command = pipe.recv()
            if command['op'] == 'finish':
                pipe.send({'done': True, 'write_attempts': writes,
                           'bm25_instantiated': manager._bm25 is not None,
                           'decay_running': rt.decay_engine.is_running})
                break
            bid = command['id']
            async def read():
                text = await dispatch(query=bid)
                keyword = await dispatch(query='OB offline WAL acceptance test')
                conn = engine._connect_db()
                try:
                    updated_at = conn.execute('SELECT updated_at FROM embeddings WHERE bucket_id=?', (bid,)).fetchone()[0]
                finally:
                    conn.close()
                return {'exact': '[exact_bucket_id:true]' in text,
                        'keyword_hit': bid in keyword,
                        'vector_present': bool(await engine.get_embedding(bid)),
                        'content_hash': engine.get_content_hash(bid), 'updated_at': updated_at}
            pipe.send(asyncio.run(read()))
    except Exception as exc:
        pipe.send({'error': type(exc).__name__, 'message': str(exc), 'write_attempts': writes})
    finally:
        _GUARD.reset(token)
        pipe.close()


def receive(pipe, timeout=30):
    if not pipe.poll(timeout):
        raise TimeoutError('worker deadline exceeded')
    result = pipe.recv()
    if result.get('error'):
        raise RuntimeError('worker failed: ' + json.dumps(result))
    return result


def wal_test(root):
    before = manifest(root)
    old_db = inspect_db(root)
    ctx = mp.get_context('spawn')
    wp, wc = ctx.Pipe()
    rp, rc = ctx.Pipe()
    w = ctx.Process(target=writer, args=(str(root), wc))
    r = ctx.Process(target=reader, args=(str(root), rc))
    cycles = []
    try:
        w.start()
        writer_ready = receive(wp)
        # B owns WAL connection before A opens the readonly connection.
        reader_start_before = manifest(root, coordination=True)
        r.start()
        receive(rp)
        reader_start_changes = reader_changes(reader_start_before, manifest(root, coordination=True))
        for number in range(3):
            wp.send('write')
            opened = receive(wp)
            bid = opened['id']
            during_before = manifest(root, coordination=True)
            rp.send({'op': 'read', 'id': bid})
            during = receive(rp)
            during_changes = reader_changes(during_before, manifest(root, coordination=True))
            wp.send('commit')
            receive(wp)
            after_before = manifest(root, coordination=True)
            rp.send({'op': 'read', 'id': bid})
            after = receive(rp)
            after_changes = reader_changes(after_before, manifest(root, coordination=True))
            expected = hashlib.sha256(('OB offline WAL acceptance test ' + str(number)).encode()).hexdigest()
            assert during['exact'] and during['keyword_hit'] and during['vector_present'], json.dumps(during)
            assert after['content_hash'] == expected, json.dumps(after)
            assert during['updated_at'] != after['updated_at'], 'reader did not observe transaction commit'
            cycles.append({'id': bid, 'during': during, 'after': after,
                           'reader_file_changes_during': during_changes,
                           'reader_file_changes_after': after_changes})
        # Exit reader first, then writer; do not manually checkpoint.
        rp.send({'op': 'finish'})
        reader_final = receive(rp)
        wp.send('finish')
        writer_final = receive(wp)
        r.join(10)
        w.join(10)
        assert not r.is_alive() and not w.is_alive()
        assert r.exitcode == 0 and w.exitcode == 0
        coordination_before_check = {p.name: p.stat().st_size for p in root.glob('embeddings.db-*')}
        final_db = inspect_db(root)
        final = manifest(root)
        original_md = [name for name in before if name.endswith('.md')]
        assert all(before[name] == final.get(name) for name in original_md)
        assert final_db['integrity'] == 'ok'
        assert old_db['schema'] == final_db['schema']
        assert final_db['rows'] == old_db['rows'] + 3
        return {'passed': True, 'scope': 'offline existing-vector WAL coordination, not live Ollama',
                'writer_ready': writer_ready, 'reader': reader_final, 'writer': writer_final,
                'reader_start_file_changes': reader_start_changes,
                'cycles': cycles, 'final_database': final_db,
                'original_markdown_unchanged': True,
                'added_files': sorted(set(final)-set(before)),
                'changed_files': sorted(name for name in before if before[name] != final.get(name)),
                'coordination_before_integrity_check': coordination_before_check,
                'coordination_after_integrity_check': {p.name: p.stat().st_size for p in root.glob('embeddings.db-*')}}
    finally:
        for process in (r, w):
            if process.pid is not None and process.is_alive():
                process.terminate()
                process.join(5)
        wp.close()
        rp.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    source, output = args.source.resolve(), args.output.resolve()
    if output.exists() or source == output or source in output.parents:
        raise RuntimeError('output must be a new directory outside source Vault')
    output.mkdir(parents=True)
    root = output / 'private-vault-copy'
    report = {'source_vault_mutated_by_test': False}
    try:
        initial = manifest(source)
        shutil.copytree(source, root)
        if initial != manifest(source) or initial != manifest(root):
            raise RuntimeError('source changed during copy or full copy verification failed')
        report['full_copy_verified'] = True
        report['source_files'] = len(initial)
        report['baseline_database'] = inspect_db(root)
        try:
            with urlopen('http://127.0.0.1:11434/api/tags', timeout=5) as response:
                models = json.load(response)['models']
            report['ollama_models'] = [m['name'] for m in models if m['name'].split(':')[0] == 'bge-m3']
        except Exception as exc:
            report['ollama_probe'] = {'passed': False, 'error': type(exc).__name__}
        sys.addaudithook(audit)
        report['real_read_checks'] = asyncio.run(local_reads(root))
        report['offline_wal_checks'] = wal_test(root)
        report['source_manifest_unchanged'] = initial == manifest(source)
        reads = report['real_read_checks']
        report['all_passed'] = bool(reads['semantic']['passed'] and reads['fusion']['passed']
                                    and reads['files_unchanged'] and reads['database_unchanged']
                                    and report['offline_wal_checks']['passed'])
    except Exception as exc:
        report['all_passed'] = False
        report['failure'] = {'error': type(exc).__name__, 'message': str(exc), 'traceback': traceback.format_exc()}
    if 'initial' in locals():
        report['source_manifest_unchanged'] = initial == manifest(source)
    (output / 'results.json').write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps({'report': str(output / 'results.json'), 'all_passed': report.get('all_passed'),
                      'failure': report.get('failure')}, ensure_ascii=False))


if __name__ == '__main__':
    main()
