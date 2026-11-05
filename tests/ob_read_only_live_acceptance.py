"""Live Ollama acceptance on an already verified private Vault copy only."""
import argparse
import asyncio
import hashlib
import json
import math
import multiprocessing as mp
from pathlib import Path
import sqlite3
import sys
import traceback

from bucket_manager import BucketManager
from embedding_engine import EmbeddingEngine
from tools import _runtime as rt
from tools.breath import dispatch
from ob_read_only_acceptance import (
    audit, _GUARD, bind_runtime, config_for, inspect_db, manifest,
    reader_changes, production_vault_path, receive,
)


def live_config(root, readonly):
    config = config_for(root, readonly, True)
    config['embedding']['timeout_seconds'] = 120
    return config


async def close_engine(engine):
    if engine is not None and engine._backend is not None:
        await engine._backend._client.close()


async def read_only_checks(root):
    before_files = manifest(root, coordination=True)
    before_database = inspect_db(root)
    writes = []
    token = _GUARD.set(writes)
    engine = None
    try:
        config = live_config(root, True)
        engine = EmbeddingEngine(config)
        engine._backend._client.max_retries = 0
        manager = BucketManager(config, engine)
        bind_runtime(config, manager, engine)
        probe = 'OB live bge-m3 read-only acceptance probe'
        generated = await engine._generate_async(probe)
        assert len(generated) == 1024 and all(math.isfinite(v) for v in generated)
        assert any(v != 0 for v in generated)
        buckets = await manager.list_all()
        ordinary = [b for b in buckets if b['metadata'].get('type') in ('dynamic', 'permanent', None)]
        assert ordinary
        exact_id = ordinary[0]['id']
        exact = await dispatch(query=exact_id)
        bootstrap = await dispatch()
        semantic = await engine.search_similar_strict('长期记忆与共同经历', top_k=5)
        fusion = await dispatch(query='记忆', with_ids=True)
        assert '[exact_bucket_id:true]' in exact
        assert bootstrap and semantic and 'ombre:result-ids' in fusion
        assert not manager._bm25_rebuilding and manager._bm25 is None
        assert rt.embedding_outbox is None and not rt.decay_engine.is_running
        result = {
            'generated_dimension': len(generated), 'generated_finite_nonzero': True,
            'memory_bootstrap_business_chain': True,
            'memory_search_business_chain': True,
            'memory_read_business_chain': True,
            'semantic_top_k_count': len(semantic), 'fusion_returned_id_block': True,
            'bm25_instantiated': False, 'decay_running': False, 'queue_present': False,
        }
    finally:
        await close_engine(engine)
        _GUARD.reset(token)
    result['write_attempts'] = writes
    result['application_file_changes'] = reader_changes(before_files, manifest(root, coordination=True))
    result['database_unchanged'] = before_database == inspect_db(root)
    assert not writes and result['database_unchanged']
    return result


async def live_writer(root, pipe):
    engine = None
    conn = None
    try:
        config = live_config(root, False)
        engine = EmbeddingEngine(config)
        engine._backend._client.max_retries = 0
        manager = BucketManager(config, engine)
        conn = sqlite3.connect(engine.db_path, timeout=15)
        assert conn.execute('PRAGMA journal_mode=WAL').fetchone()[0] == 'wal'
        pipe.send({'ready': True})
        ids = []
        for number in range(3):
            assert pipe.recv() == 'write'
            content = 'OB real bge-m3 WAL acceptance synthetic memory cycle ' + str(number)
            bid = await manager.create(content=content, domain=['acceptance-live'], importance=7,
                                       source_tool='acceptance', test_data=True)
            digest = hashlib.sha256(content.encode()).hexdigest()
            stored = await engine.get_embedding(bid)
            assert stored and len(stored) == 1024
            assert engine.get_content_hash(bid) == digest
            ids.append(bid)
            conn.execute('BEGIN IMMEDIATE')
            conn.execute('UPDATE embeddings SET updated_at=? WHERE bucket_id=?',
                         ('2098-01-01T00:00:0' + str(number) + '+00:00', bid))
            pipe.send({'id': bid, 'content': content, 'hash': digest, 'dimension': len(stored)})
            assert pipe.recv() == 'commit'
            conn.commit()
            pipe.send({'committed': True})
        assert pipe.recv() == 'finish'
        pipe.send({'done': True, 'ids': ids, 'integrity': conn.execute('PRAGMA integrity_check').fetchone()[0]})
    finally:
        if conn is not None:
            conn.close()
        await close_engine(engine)


async def live_reader(root, pipe):
    writes = []
    token = _GUARD.set(writes)
    engine = None
    try:
        config = live_config(root, True)
        engine = EmbeddingEngine(config)
        engine._backend._client.max_retries = 0
        manager = BucketManager(config, engine)
        bind_runtime(config, manager, engine)
        pipe.send({'ready': True})
        while True:
            command = pipe.recv()
            if command['op'] == 'finish':
                pipe.send({'done': True, 'write_attempts': writes,
                           'decay_running': rt.decay_engine.is_running,
                           'queue_present': rt.embedding_outbox is not None,
                           'bm25_instantiated': manager._bm25 is not None})
                return
            bid, content = command['id'], command['content']
            exact = await dispatch(query=bid)
            fused = await dispatch(query=content, domain='acceptance-live', with_ids=True)
            semantic = dict(await engine.search_similar_strict(content, top_k=5))
            conn = engine._connect_db()
            try:
                updated_at = conn.execute('SELECT updated_at FROM embeddings WHERE bucket_id=?', (bid,)).fetchone()[0]
            finally:
                conn.close()
            pipe.send({'exact': '[exact_bucket_id:true]' in exact,
                       'fusion_hit': bid in fused, 'semantic_score': semantic.get(bid),
                       'content_hash': engine.get_content_hash(bid), 'updated_at': updated_at,
                       'bootstrap_returned': bool(await dispatch())})
    finally:
        await close_engine(engine)
        _GUARD.reset(token)


def worker(kind, root, pipe):
    if kind == 'reader':
        sys.addaudithook(audit)
    try:
        asyncio.run(live_reader(Path(root), pipe) if kind == 'reader' else live_writer(Path(root), pipe))
    except Exception as exc:
        pipe.send({'error': type(exc).__name__, 'message': str(exc), 'traceback': traceback.format_exc()})
    finally:
        pipe.close()


def live_wal(root):
    initial_files = manifest(root, coordination=True)
    initial_db = inspect_db(root)
    ctx = mp.get_context('spawn')
    wp, wc = ctx.Pipe()
    rp, rc = ctx.Pipe()
    w = ctx.Process(target=worker, args=('writer', str(root), wc))
    r = ctx.Process(target=worker, args=('reader', str(root), rc))
    cycles = []
    try:
        w.start()
        receive(wp, 180)
        before_reader = manifest(root, coordination=True)
        r.start()
        receive(rp, 180)
        start_changes = reader_changes(before_reader, manifest(root, coordination=True))
        for _ in range(3):
            wp.send('write')
            record = receive(wp, 180)
            command = {'op': 'read', 'id': record['id'], 'content': record['content']}
            before = manifest(root, coordination=True)
            rp.send(command)
            during = receive(rp, 180)
            during_changes = reader_changes(before, manifest(root, coordination=True))
            wp.send('commit')
            receive(wp)
            before = manifest(root, coordination=True)
            rp.send(command)
            after = receive(rp, 180)
            after_changes = reader_changes(before, manifest(root, coordination=True))
            for reading in (during, after):
                assert reading['exact'] and reading['fusion_hit'] and reading['bootstrap_returned']
                assert reading['semantic_score'] is not None and reading['semantic_score'] > 0.99
                assert reading['content_hash'] == record['hash']
            assert during['updated_at'] != after['updated_at']
            cycles.append({'id': record['id'], 'dimension': record['dimension'],
                           'during': during, 'after': after,
                           'reader_changes_during': during_changes, 'reader_changes_after': after_changes})
        rp.send({'op': 'finish'})
        reader = receive(rp)
        wp.send('finish')
        writer = receive(wp)
        r.join(15)
        w.join(15)
        assert not r.is_alive() and not w.is_alive() and r.exitcode == 0 and w.exitcode == 0
        final_db = inspect_db(root)
        final_files = manifest(root, coordination=True)
        assert final_db['integrity'] == 'ok' and writer['integrity'] == 'ok'
        assert final_db['schema'] == initial_db['schema']
        assert final_db['rows'] == initial_db['rows'] + 3
        assert final_db['dimensions'] == [1024]
        assert not reader['write_attempts'] and not reader['decay_running'] and not reader['queue_present']
        assert all(final_files.get(name) == digest for name, digest in initial_files.items() if name.endswith('.md'))
        return {'passed': True, 'live_ollama_both_processes': True,
                'initial_rows': initial_db['rows'], 'final_database': final_db,
                'reader_start_changes': start_changes, 'cycles': cycles,
                'reader': reader, 'writer': writer, 'original_markdown_unchanged': True,
                'coordination_files': {p.name: p.stat().st_size for p in root.glob('embeddings.db-*')}}
    finally:
        for process in (r, w):
            if process.pid is not None and process.is_alive():
                process.terminate()
                process.join(10)
        wp.close()
        rp.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--existing-run', type=Path, required=True)
    parser.add_argument('--report', type=Path, required=True)
    args = parser.parse_args()
    root = args.existing_run.resolve() / 'private-vault-copy'
    prior = json.loads((args.existing_run / 'results.json').read_text(encoding='utf-8'))
    if not prior.get('full_copy_verified') or root.name != 'private-vault-copy':
        raise RuntimeError('an already verified private test copy is required')
    current_db = inspect_db(root)
    assert current_db['logical_hash'] == prior['offline_wal_checks']['final_database']['logical_hash']
    assert current_db['integrity'] == 'ok'
    source = production_vault_path()
    if root == source or source in root.parents:
        raise RuntimeError('production Vault cannot be a test target')
    source_before = manifest(source)
    sys.addaudithook(audit)
    report = {'already_verified_copy': str(root), 'initial_database': current_db}
    try:
        report['live_read_checks'] = asyncio.run(read_only_checks(root))
        report['live_wal_checks'] = live_wal(root)
        report['all_passed'] = True
    except Exception as exc:
        report['all_passed'] = False
        report['failure'] = {'error': type(exc).__name__, 'message': str(exc), 'traceback': traceback.format_exc()}
    report['source_manifest_unchanged'] = source_before == manifest(source)
    if not report['source_manifest_unchanged']:
        report['all_passed'] = False
    if args.report.exists():
        raise RuntimeError('refusing to overwrite an acceptance report')
    args.report.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps({'all_passed': report['all_passed'], 'report': str(args.report),
                      'failure': report.get('failure')}, ensure_ascii=False))
    return 0 if report['all_passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
