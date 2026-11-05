"""Real stdio MCP acceptance plus concurrent normal OB writes on a test copy."""
import argparse
import asyncio
from datetime import timedelta
import hashlib
import json
import multiprocessing as mp
import os
from pathlib import Path
import secrets
import sqlite3
import subprocess
import sys
import traceback

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from bucket_manager import BucketManager
from embedding_engine import EmbeddingEngine
from ob_read_only_acceptance import inspect_db, manifest, production_vault_path, reader_changes, receive
from ob_read_only_live_acceptance import live_config, close_engine


async def normal_writer_async(root, pipe):
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
            command = pipe.recv()
            content = command['content']
            bid = await manager.create(content=content, domain=['memory-stdio-acceptance'],
                                       importance=8, test_data=True)
            expected = hashlib.sha256(content.encode()).hexdigest()
            vector = await engine.get_embedding(bid)
            assert len(vector) == 1024 and engine.get_content_hash(bid) == expected
            ids.append(bid)
            conn.execute('BEGIN IMMEDIATE')
            conn.execute('UPDATE embeddings SET updated_at=? WHERE bucket_id=?',
                         ('2097-01-01T00:00:0' + str(number) + '+00:00', bid))
            pipe.send({'id': bid, 'content_hash': expected, 'dimension': len(vector)})
            assert pipe.recv() == 'commit'
            conn.commit()
            pipe.send({'committed': True})
        assert pipe.recv() == 'finish'
        pipe.send({'done': True, 'ids': ids, 'integrity': conn.execute('PRAGMA integrity_check').fetchone()[0]})
    finally:
        if conn is not None:
            conn.close()
        await close_engine(engine)


def normal_writer(root, pipe):
    try:
        asyncio.run(normal_writer_async(Path(root), pipe))
    except Exception as exc:
        pipe.send({'error': type(exc).__name__, 'message': str(exc)})
    finally:
        pipe.close()


async def checked_call(client, name, arguments):
    response = await client.call_tool(name, arguments)
    if response.isError:
        raise RuntimeError('MCP tool failed: ' + name + ': ' + str(response))
    return '\n'.join(block.text for block in response.content if hasattr(block, 'text'))


async def acceptance(root, output, prior, baseline=None):
    before_files = manifest(root, coordination=True)
    before_db = inspect_db(root)
    expected = baseline or prior['live_wal_checks']['final_database']
    if before_db['logical_hash'] != expected['logical_hash'] or before_db['integrity'] != 'ok':
        raise RuntimeError('test copy no longer matches the verified baseline')
    token = secrets.token_urlsafe(32)
    config = live_config(root, True)
    config.pop('storage')
    config.update(transport='stdio', auth={'token_sha256': hashlib.sha256(token.encode()).hexdigest()})
    config_path = output / 'memory-stdio.test.yaml'
    config_path.write_text(json.dumps(config, indent=2), encoding='utf-8')
    repo = Path(__file__).resolve().parents[1]
    command = [sys.executable, '-B', '-u', str(repo / 'src' / 'memory_stdio.py'), '--config', str(config_path)]
    env = {'MEMORY_MCP_TOKEN': token, 'PYTHONIOENCODING': 'utf-8', 'PYTHONDONTWRITEBYTECODE': '1'}
    result = {'transport': 'stdio', 'authentication': 'launcher environment token validated against SHA-256',
              'database_before': before_db, 'existing_test_copy': str(root)}
    # Launch denial must happen before the native runtime/Vault is initialized.
    for bad in ('', 'incorrect-token-at-least-thirty-two-characters'):
        rejected = await asyncio.to_thread(subprocess.run, command, input='', capture_output=True,
                            text=True, encoding='utf-8', timeout=30,
                            env=dict(os.environ, MEMORY_MCP_TOKEN=bad, PYTHONDONTWRITEBYTECODE='1'))
        assert rejected.returncode != 0 and rejected.stdout == ''
        assert token not in rejected.stderr
    result['missing_and_wrong_credentials_rejected'] = True
    params = StdioServerParameters(command=command[0], args=command[1:], cwd=repo, env=env)
    context = mp.get_context('spawn')
    parent, child = context.Pipe()
    writer = context.Process(target=normal_writer, args=(str(root), child))
    cycles = []
    try:
        with open(os.devnull, 'w') as errors:
            async with stdio_client(params, errlog=errors) as streams:
                async with ClientSession(*streams, read_timeout_seconds=timedelta(seconds=180)) as client:
                    handshake = await client.initialize()
                    result['server_name'] = handshake.serverInfo.name
                    listed = await client.list_tools()
                    assert [t.name for t in listed.tools] == ['memory_bootstrap', 'memory_search', 'memory_read']
                    result['discovered_tools'] = [t.model_dump(mode='json', exclude_none=True) for t in listed.tools]
                    assert not (await client.list_resources()).resources
                    assert not (await client.list_prompts()).prompts
                    bootstrap = await checked_call(client, 'memory_bootstrap', {})
                    known_id = prior['live_wal_checks']['writer']['ids'][0]
                    exact = await checked_call(client, 'memory_read', {'bucket_id': known_id})
                    search = await checked_call(client, 'memory_search', {
                        'query': 'OB real bge-m3 WAL acceptance synthetic memory cycle 0', 'max_results': 50})
                    assert bootstrap and '[exact_bucket_id:true]' in exact and known_id in search
                    result['three_tools_and_real_semantics'] = True
                    for name, arguments in (
                        ('hold', {'content': 'denied'}), ('trace', {'delete': True}),
                        ('memory_read', {'bucket_id': 'nonexistent-memory-id'}),
                        ('memory_search', {'query': 'test', 'delete': True}),
                    ):
                        denied = await client.call_tool(name, arguments)
                        assert denied.isError
                    result['write_unknown_and_missing_id_calls_rejected'] = True
                    result['read_only_file_changes'] = reader_changes(before_files, manifest(root, coordination=True))
                    assert before_db == inspect_db(root)
                    result['read_only_database_unchanged'] = True
                    writer.start()
                    await asyncio.to_thread(receive, parent, 180)
                    for number in range(3):
                        content = 'memory stdio live compatibility ' + secrets.token_hex(8) + ' cycle ' + str(number)
                        parent.send({'content': content})
                        record = await asyncio.to_thread(receive, parent, 180)
                        before = manifest(root, coordination=True)
                        exact = await checked_call(client, 'memory_read', {'bucket_id': record['id']})
                        found = await checked_call(client, 'memory_search', {'query': content, 'max_results': 50})
                        assert content in exact and record['id'] in found
                        during_changes = reader_changes(before, manifest(root, coordination=True))
                        parent.send('commit')
                        await asyncio.to_thread(receive, parent, 30)
                        before = manifest(root, coordination=True)
                        exact = await checked_call(client, 'memory_read', {'bucket_id': record['id']})
                        assert content in exact
                        after_changes = reader_changes(before, manifest(root, coordination=True))
                        cycles.append({'id': record['id'], 'dimension': record['dimension'],
                                       'read_during_transaction': True, 'read_after_commit': True,
                                       'reader_changes_during': during_changes, 'reader_changes_after': after_changes})
                    parent.send('finish')
                    writer_final = await asyncio.to_thread(receive, parent, 30)
                    writer.join(15)
                    assert not writer.is_alive() and writer.exitcode == 0
                    result['writer'] = writer_final
                    # Attribute shutdown effects separately after writer exits.
                    before_shutdown = manifest(root, coordination=True)
            result['reader_shutdown_changes'] = reader_changes(before_shutdown, manifest(root, coordination=True))
        after_db = inspect_db(root)
        after_files = manifest(root, coordination=True)
        assert after_db['integrity'] == 'ok' and writer_final['integrity'] == 'ok'
        assert after_db['schema'] == before_db['schema'] and after_db['rows'] == before_db['rows'] + 3
        assert after_db['dimensions'] == [1024]
        assert all(after_files.get(p) == digest for p, digest in before_files.items() if p.endswith('.md'))
        result['database_after'] = after_db
        result['same_vault_normal_ob_writer_compatibility'] = {'passed': True, 'cycles': cycles,
            'scope': 'native normal OB writer as Cyberboss workload surrogate; not the Cyberboss UI'}
        result['existing_markdown_unchanged'] = True
        result['all_executed_checks_passed'] = True
        return result
    finally:
        if writer.pid is not None and writer.is_alive():
            writer.terminate()
            writer.join(10)
        parent.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--prior-report', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--baseline-report', type=Path)
    args = parser.parse_args()
    if args.output.exists():
        raise RuntimeError('output directory must be new')
    prior = json.loads(args.prior_report.read_text(encoding='utf-8'))
    if not prior.get('all_passed'):
        raise RuntimeError('verified prior acceptance is required')
    root = Path(prior['already_verified_copy']).resolve(strict=True)
    source = production_vault_path()
    if root == source or source in root.parents:
        raise RuntimeError('only an existing independent TEST Vault is allowed')
    baseline = None
    if args.baseline_report:
        previous = json.loads(args.baseline_report.read_text(encoding='utf-8'))
        if (not previous.get('all_executed_checks_passed') or
                not previous.get('formal_vault_hashes_unchanged') or
                Path(previous['existing_test_copy']).resolve() != root):
            raise RuntimeError('baseline must be a verified result for this exact test copy')
        baseline = previous['database_after']
    source_before = manifest(source)
    args.output.mkdir(parents=True)
    try:
        result = asyncio.run(acceptance(root, args.output, prior, baseline))
    except Exception as exc:
        result = {'all_executed_checks_passed': False, 'failure': {
            'error': type(exc).__name__, 'message': str(exc), 'traceback': traceback.format_exc()}}
    result['formal_vault_hashes_unchanged'] = source_before == manifest(source)
    if not result['formal_vault_hashes_unchanged']:
        result['all_executed_checks_passed'] = False
    (args.output / 'results.json').write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps({'passed': result['all_executed_checks_passed'], 'report': str(args.output / 'results.json'),
                      'failure': result.get('failure')}, ensure_ascii=False))
    return 0 if result['all_executed_checks_passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
