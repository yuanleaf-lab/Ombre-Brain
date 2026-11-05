"""Real local HTTP MCP, simultaneous stdio, and test-Vault writer acceptance."""
import argparse
import asyncio
from datetime import timedelta
import hashlib
import json
import multiprocessing as mp
import os
from pathlib import Path
import secrets
import socket
import subprocess
import sys
import traceback

import httpx
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from mcp.client.streamable_http import streamable_http_client
from memory_stdio_acceptance import normal_writer, checked_call
from ob_read_only_acceptance import inspect_db, manifest, production_vault_path, reader_changes, receive
from ob_read_only_live_acceptance import live_config


async def wait_ready(url, process):
    async with httpx.AsyncClient(trust_env=False, timeout=2) as client:
        for _ in range(100):
            if process.poll() is not None:
                raise RuntimeError('HTTP process exited during startup')
            try:
                if (await client.get(url)).status_code == 401:
                    return
            except httpx.TransportError:
                pass
            await asyncio.sleep(0.2)
    raise TimeoutError('HTTP server startup deadline exceeded')


async def execute(root, output, prior):
    before = manifest(root, coordination=True)
    initial_db = inspect_db(root)
    assert initial_db['logical_hash'] == prior['database_after']['logical_hash']
    assert initial_db['integrity'] == 'ok'
    token, stdio_token = secrets.token_urlsafe(40), secrets.token_urlsafe(40)
    with socket.socket() as probe:
        probe.bind(('127.0.0.1', 0))
        port = probe.getsockname()[1]
    url = f'http://127.0.0.1:{port}/mcp'
    config = live_config(root, True)
    config.pop('storage')
    config.update(transport='streamable-http', host='127.0.0.1', port=port, endpoint='/mcp')
    config_path = output / 'memory-http.test.yaml'
    config_path.write_text(json.dumps(config, indent=2), encoding='utf-8')
    stdio_config = {k:v for k,v in config.items() if k not in ('host','port','endpoint')}
    stdio_config.update(transport='stdio', auth={'token_sha256':hashlib.sha256(stdio_token.encode()).hexdigest()})
    stdio_path = output / 'memory-stdio.test.yaml'
    stdio_path.write_text(json.dumps(stdio_config, indent=2), encoding='utf-8')
    repo = Path(__file__).resolve().parents[1]
    command = [sys.executable,'-B','-u',str(repo/'src'/'memory_http.py'),'--config',str(config_path)]
    process = subprocess.Popen(command, cwd=repo,
        env=dict(os.environ, MEMORY_MCP_HTTP_TOKEN=token, PYTHONDONTWRITEBYTECODE='1', PYTHONIOENCODING='utf-8'),
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    context = mp.get_context('spawn')
    parent, child = context.Pipe()
    writer = context.Process(target=normal_writer, args=(str(root),child))
    result = {'url':url,'transport':'Streamable HTTP','database_before':initial_db,
              'test_copy':str(root),'token_persisted':False}
    try:
        await wait_ready(url, process)
        async with httpx.AsyncClient(trust_env=False,timeout=5) as unauthorized:
            for method in ('GET','POST','DELETE','OPTIONS'):
                for headers in ({},{'Authorization':'Bearer wrong-token'}):
                    response = await unauthorized.request(method,url,headers=headers)
                    assert response.status_code == 401
                    assert 'Bearer' in response.headers['www-authenticate']
            duplicate = await unauthorized.get(url,headers=[('Authorization','Bearer '+token),('Authorization','Bearer '+token)])
            assert duplicate.status_code == 401
        result['unauthorized_rejected'] = {'missing':401,'wrong':401,'duplicate':401,'methods':['GET','POST','DELETE','OPTIONS']}
        parameters = StdioServerParameters(command=sys.executable,
            args=['-B','-u',str(repo/'src'/'memory_stdio.py'),'--config',str(stdio_path)],cwd=repo,
            env={'MEMORY_MCP_TOKEN':stdio_token,'PYTHONDONTWRITEBYTECODE':'1','PYTHONIOENCODING':'utf-8'})
        with open(os.devnull,'w') as errors:
            async with httpx.AsyncClient(headers={'Authorization':'Bearer '+token},trust_env=False,timeout=180) as http:
                async with streamable_http_client(url,http_client=http) as streams:
                    async with ClientSession(*streams[:2],read_timeout_seconds=timedelta(seconds=180)) as client:
                        initialized = await client.initialize()
                        result['initialize'] = initialized.model_dump(mode='json',exclude_none=True)
                        listed = await client.list_tools()
                        assert [t.name for t in listed.tools] == ['memory_bootstrap','memory_search','memory_read']
                        result['tools'] = [t.model_dump(mode='json',exclude_none=True) for t in listed.tools]
                        async with stdio_client(parameters,errlog=errors) as stdio_streams:
                            async with ClientSession(*stdio_streams,read_timeout_seconds=timedelta(seconds=180)) as stdio:
                                await stdio.initialize()
                                assert [t.name for t in (await stdio.list_tools()).tools] == [t.name for t in listed.tools]
                                known_id = prior['writer']['ids'][0]
                                boot, found, exact, stdio_exact = await asyncio.gather(
                                    checked_call(client,'memory_bootstrap',{}),
                                    checked_call(client,'memory_search',{'query':'memory stdio live compatibility','max_results':50}),
                                    checked_call(client,'memory_read',{'bucket_id':known_id}),
                                    checked_call(stdio,'memory_read',{'bucket_id':known_id}))
                                assert boot and known_id in found
                                assert '[exact_bucket_id:true]' in exact and exact == stdio_exact
                                result['three_tools_and_real_semantics'] = True
                                result['simultaneous_stdio'] = True
                                for name, arguments in (('hold',{'content':'forbidden'}),
                                                        ('memory_search',{'query':'test','delete':True}),
                                                        ('memory_read',{'bucket_id':'missing-id'})):
                                    assert (await client.call_tool(name,arguments)).isError
                                result['initial_read_file_changes'] = reader_changes(before,manifest(root,coordination=True))
                                assert initial_db == inspect_db(root)
                                result['initial_read_database_unchanged'] = True
                                writer.start()
                                await asyncio.to_thread(receive,parent,180)
                                cycles=[]
                                for number in range(3):
                                    content = 'HTTP memory live acceptance ' + secrets.token_hex(8) + ' ' + str(number)
                                    parent.send({'content':content})
                                    record = await asyncio.to_thread(receive,parent,180)
                                    window = manifest(root,coordination=True)
                                    exact, found, stdio_exact = await asyncio.gather(
                                        checked_call(client,'memory_read',{'bucket_id':record['id']}),
                                        checked_call(client,'memory_search',{'query':content,'max_results':50}),
                                        checked_call(stdio,'memory_read',{'bucket_id':record['id']}))
                                    assert content in exact and record['id'] in found and content in stdio_exact
                                    during_changes = reader_changes(window,manifest(root,coordination=True))
                                    parent.send('commit')
                                    await asyncio.to_thread(receive,parent,30)
                                    window = manifest(root,coordination=True)
                                    assert content in await checked_call(client,'memory_read',{'bucket_id':record['id']})
                                    after_changes = reader_changes(window,manifest(root,coordination=True))
                                    cycles.append({'id':record['id'],'dimension':record['dimension'],
                                                   'http_and_stdio_read_during_transaction':True,
                                                   'http_read_after_commit':True,
                                                   'reader_changes_during':during_changes,'reader_changes_after':after_changes})
                                parent.send('finish')
                                result['writer'] = await asyncio.to_thread(receive,parent,30)
                                writer.join(15)
                                assert not writer.is_alive() and writer.exitcode == 0
                                result['concurrent_writer_cycles'] = cycles
                                before_shutdown = manifest(root,coordination=True)
        process.terminate()
        await asyncio.to_thread(process.wait,15)
        result['reader_shutdown_changes'] = reader_changes(before_shutdown,manifest(root,coordination=True))
        final_db = inspect_db(root)
        assert final_db['integrity'] == 'ok' and final_db['schema'] == initial_db['schema']
        assert final_db['rows'] == initial_db['rows']+3 and final_db['dimensions'] == [1024]
        after = manifest(root,coordination=True)
        assert all(after.get(name)==digest for name,digest in before.items() if name.endswith('.md'))
        result['database_after'] = final_db
        result['existing_markdown_unchanged'] = True
        result['passed'] = True
        return result
    finally:
        if process.poll() is None:
            process.terminate()
            process.wait(15)
        if writer.pid is not None and writer.is_alive():
            writer.terminate()
            writer.join(10)
        parent.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--prior-report',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    if args.output.exists():
        raise RuntimeError('output directory must be new')
    prior=json.loads(args.prior_report.read_text(encoding='utf-8'))
    assert prior['all_executed_checks_passed'] and prior['formal_vault_hashes_unchanged']
    root=Path(prior['existing_test_copy']).resolve(strict=True)
    source=production_vault_path()
    assert root!=source and source not in root.parents
    before=manifest(source)
    args.output.mkdir(parents=True)
    try:
        result=asyncio.run(execute(root,args.output,prior))
    except Exception as exc:
        result={'passed':False,'failure':{'error':type(exc).__name__,'message':str(exc),'traceback':traceback.format_exc()}}
    result['formal_vault_unchanged']=before==manifest(source)
    result['passed']=result['passed'] and result['formal_vault_unchanged']
    (args.output/'results.json').write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf-8')
    print(json.dumps({'passed':result['passed'],'report':str(args.output/'results.json'),'failure':result.get('failure')},ensure_ascii=False))
    return 0 if result['passed'] else 1


if __name__=='__main__':
    raise SystemExit(main())
