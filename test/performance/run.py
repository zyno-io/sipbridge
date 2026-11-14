#!/usr/bin/env python3
"""Local, isolated Docker comparison. Never connects to Redis or a real SIP peer."""
import argparse
import datetime
import http.client
import hashlib
import json
import os
import platform
import socket
import statistics
import subprocess
import time
import urllib.request
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
HERE = Path(__file__).resolve().parent
DRACHTIO = 'ghcr.io/zynoconsulting/drachtio-server@sha256:84f36badae17cc4fd2458153b3262d6a3ec7dfffefaff459aa6e131a5ebba9d8'
OLD = 'gcr.s24.dev/signal24/oss/sipbridge:73e01e5f451d2fb4430260cffa42408510202d09@sha256:9b44cf8e37470dfba1a12a66a16f985fc788dd4851bb2cc3117e3ffac0d1ee2d'
RESIDENT_METRICS = {'contexts': 'zt_sipbridge_resident_contexts',
                    'retainedBytes': 'zt_sipbridge_resident_retained_bytes',
                    'completionReservedBytes': 'zt_sipbridge_resident_completion_reserved_bytes'}


def command(args, **kwargs):
    result = subprocess.run(args, text=True, capture_output=True, timeout=kwargs.pop('timeout', 120), **kwargs)
    if result.returncode:
        raise RuntimeError(f'{args[0:3]} failed: {result.stderr[-2000:]}')
    return result.stdout.strip()


class DockerHTTP(http.client.HTTPConnection):
    def __init__(self, endpoint):
        super().__init__('localhost', timeout=15)
        self.endpoint = endpoint

    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(self.timeout)
        self.sock.connect(self.endpoint)


class Docker:
    def __init__(self):
        endpoint = command(['docker', 'context', 'inspect', '--format', '{{.Endpoints.docker.Host}}'])
        if not endpoint.startswith('unix://'):
            raise ValueError('This localhost harness requires a local Unix Docker endpoint')
        self.endpoint = endpoint[7:]

    def get(self, path):
        connection = DockerHTTP(self.endpoint)
        try:
            connection.request('GET', '/v1.45' + path)
            response = connection.getresponse()
            data = response.read()
            if response.status != 200:
                raise RuntimeError('Docker evidence request failed: ' + str(response.status))
            return json.loads(data)
        finally:
            connection.close()

    def sample(self, name):
        stats = self.get(f'/containers/{name}/stats?stream=false&one-shot=true')
        top = self.get(f'/containers/{name}/top?ps_args=-eo%20pid,rss,comm')
        rss_index = top['Titles'].index('RSS')
        return {'cpuNS': stats['cpu_stats']['cpu_usage']['total_usage'],
                'rssBytes': sum(int(row[rss_index])*1024 for row in top.get('Processes', [])),
                'cgroupBytes': stats.get('memory_stats', {}).get('usage'),
                'processes': top.get('Processes', [])}


def json_url(port):
    with urllib.request.urlopen(f'http://127.0.0.1:{port}/stats', timeout=3) as response:
        return json.load(response)


def port(name, target):
    value = command(['docker', 'port', name, str(target)+'/tcp'])
    return int(value.rsplit(':', 1)[1])


def wait(predicate, deadline=30):
    end = time.monotonic()+deadline
    while time.monotonic() < end:
        try:
            result = predicate()
            if result:
                return result
        except (OSError, ValueError, RuntimeError):
            pass
        time.sleep(.2)
    raise TimeoutError('Isolated fixture readiness timed out')


def native_metrics(admin_port):
    if admin_port is None:
        return None
    with urllib.request.urlopen(f'http://127.0.0.1:{admin_port}/metrics', timeout=5) as response:
        text = response.read().decode()
    return {line.split()[0]: float(line.split()[1]) for line in text.splitlines()
            if len(line.split()) == 2 and (line.startswith('zt_sipbridge_runtime_') or line.split()[0] in RESIDENT_METRICS.values())}


def resident_evidence(before, after):
    before, after = before or {}, after or {}
    missing = [metric for metric in RESIDENT_METRICS.values() if metric not in before or metric not in after]
    return {'available': not missing, 'missingMetrics': missing,
            'before': {key: before.get(metric) for key,metric in RESIDENT_METRICS.items()},
            'after': {key: after.get(metric) for key,metric in RESIDENT_METRICS.items()},
            'delta': {key: after[metric]-before[metric] for key,metric in RESIDENT_METRICS.items()} if not missing else None}


def measure(docker, core, app, seconds, native_admin, start_work=None):
    started = time.monotonic()
    before = {name: docker.sample(name) for name in (core, app)}
    heap_before = native_metrics(native_admin)
    samples = {name: [before[name]] for name in before}
    if start_work is not None:
        start_work()
    while time.monotonic()-started < seconds:
        time.sleep(min(5, max(0, seconds-(time.monotonic()-started))))
        for name in samples:
            samples[name].append(docker.sample(name))
    elapsed = time.monotonic()-started
    heap_after = native_metrics(native_admin)
    result = {'elapsedSeconds': elapsed}
    for role, name in [('core', core), ('controller', app)]:
        rows = samples[name]
        cpu = (rows[-1]['cpuNS']-rows[0]['cpuNS'])/1e9
        result[role] = {'cpuSeconds': cpu, 'cpuMillicores': cpu/elapsed*1000,
                        'rssMeanBytes': statistics.mean(row['rssBytes'] for row in rows),
                        'rssMaxBytes': max(row['rssBytes'] for row in rows),
                        'rssFinalBytes': rows[-1]['rssBytes'], 'samples': rows}
    if heap_before is not None:
        result['core']['runtimeBefore'] = heap_before
        result['core']['runtimeAfter'] = heap_after
        result['core']['allocationBytesPerSecond'] = (heap_after['zt_sipbridge_runtime_heap_alloc_bytes_total']-heap_before['zt_sipbridge_runtime_heap_alloc_bytes_total'])/elapsed
        result['core']['allocationObjectsPerSecond'] = (heap_after['zt_sipbridge_runtime_heap_alloc_objects_total']-heap_before['zt_sipbridge_runtime_heap_alloc_objects_total'])/elapsed
        result['core']['gcCyclesPerSecond'] = (heap_after['zt_sipbridge_runtime_gc_cycles_total']-heap_before['zt_sipbridge_runtime_gc_cycles_total'])/elapsed
        result['core']['liveHeapBytes'] = heap_after['zt_sipbridge_runtime_heap_live_bytes']
        result['core']['gcCPUSeconds'] = heap_after['zt_sipbridge_runtime_gc_cpu_seconds_total']-heap_before['zt_sipbridge_runtime_gc_cpu_seconds_total']
    else:
        result['core']['allocationBytesPerSecond'] = None
        result['core']['allocationEvidence'] = 'The production Drachtio image exposes no cumulative allocator counter; RSS is measured, allocation rate is unavailable.'
    result['core']['residentRecords'] = resident_evidence(heap_before, heap_after)
    result['totalApplicationAndCoreMillicores'] = result['core']['cpuMillicores']+result['controller']['cpuMillicores']
    return result


def certificates(path):
    path.mkdir()
    config = path/'ca.cnf'
    config.write_text('[req]\ndistinguished_name=dn\nx509_extensions=v3\n[dn]\nCN=SIP performance fixture CA\n[v3]\nbasicConstraints=critical,CA:TRUE\nkeyUsage=critical,keyCertSign,cRLSign\nsubjectKeyIdentifier=hash\nauthorityKeyIdentifier=keyid:always\n')
    command(['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-days', '1', '-subj', '/CN=SIP performance fixture CA', '-config',str(config),'-keyout', str(path/'ca.key'), '-out', str(path/'ca.crt')])
    for name, san, usage in [('server', 'DNS:core,IP:127.0.0.1', 'serverAuth'), ('client', 'URI:spiffe://performance/application', 'clientAuth')]:
        command(['openssl', 'req', '-newkey', 'rsa:2048', '-nodes', '-subj', '/CN='+name, '-keyout', str(path/(name+'.key')), '-out', str(path/(name+'.csr'))])
        ext = path/(name+'.ext')
        ext.write_text(f'subjectAltName={san}\nextendedKeyUsage={usage}\nbasicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature,keyEncipherment\nsubjectKeyIdentifier=hash\nauthorityKeyIdentifier=keyid,issuer\n')
        command(['openssl', 'x509', '-req', '-days', '1', '-in', str(path/(name+'.csr')), '-CA', str(path/'ca.crt'), '-CAkey', str(path/'ca.key'), '-CAcreateserial', '-extfile', str(ext), '-out', str(path/(name+'.crt'))])
    # Synthetic one-day fixture material is readable by the unprivileged containers.
    path.chmod(0o755)
    for entry in path.iterdir():
        entry.chmod(0o644)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--candidate', help='Explicit prebuilt Resident Memory image; no image is built or deployed implicitly')
    parser.add_argument('--only', choices=('drachtio','checkpoint-memory','resident-memory'), help='Functional diagnosis of one backend; no comparison gate')
    parser.add_argument('--old-image', default=OLD)
    parser.add_argument('--drachtio-image', default=DRACHTIO)
    parser.add_argument('--phones', default='17,130')
    parser.add_argument('--histories', default='0,2')
    parser.add_argument('--duration', type=float, default=120)
    parser.add_argument('--cadence', type=float, default=15)
    parser.add_argument('--idle-seconds', type=float, default=15)
    parser.add_argument('--warmup', type=float, default=5)
    parser.add_argument('--platform', default='linux/amd64')
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--skip-build', action='store_true', help='Reuse only this harness\'s controller images')
    parser.add_argument('--debug', action='store_true', help='Verbose Sofia diagnostics; CPU results are not comparable')
    args = parser.parse_args()
    args.output = args.output.resolve()
    phones = [int(v) for v in args.phones.split(',')]
    histories = [int(v) for v in args.histories.split(',')]
    assert all(v in (1,17,130) for v in phones) and all(0 <= v <= 10 for v in histories)
    assert 1 <= args.cadence <= args.duration <= 3600 and 0 < args.idle_seconds <= 300
    args.output.mkdir(parents=True, exist_ok=False)
    args.output.chmod(0o700)
    docker = Docker()
    if not args.skip_build:
        for role in ('controller', 'drachtio-app'):
            command(['docker', 'build', '--platform', args.platform, '-f', str(HERE/(role+'.Dockerfile')), '-t', 'sipbridge-perf-'+role, str(ROOT)], timeout=600)
    manifest = {'createdUTC': datetime.datetime.now(datetime.timezone.utc).isoformat(), 'arguments': {**vars(args), 'output': str(args.output)},
                'hostArchitecture': platform.machine(), 'dockerPlatform': args.platform,
                'dockerArchitecture': command(['docker','info','--format','{{.Architecture}}']),
                'harnessSHA256': {str(path.relative_to(HERE)):hashlib.sha256(path.read_bytes()).hexdigest() for path in HERE.rglob('*') if path.is_file() and '__pycache__' not in path.parts},
                'applicationImages': {role:json.loads(command(['docker','image','inspect','sipbridge-perf-'+role]))[0]['Id'] for role in ('controller','drachtio-app')},
                'limits': {'core': {'cpus':1,'memoryBytes':536870912}, 'controller': {'cpus':1,'memoryBytes':268435456}},
                'capture': 'disabled on both cores', 'controllerTransports': {'native':'canonical protobuf WebSocket with mTLS','drachtio':'drachtio-srf admin TCP'},
                'logging': 'Default server loggers; Sofia SIP tracing disabled unless debug flag is explicitly set; no fixture per-message logs',
                'authentication': 'Fixture application accepts synthetic identities; digest challenge/backend policy work excluded equally',
                'nativeState': 'Memory only, no Redis; logical admission cap64GiB avoids old per-group64MiB reservation, physical limit512MiB unchanged',
                'sources': {'drachtioTLS':'https://drachtio.org/docs/drachtio-server', 'drachtioApplication':'https://drachtio.org/api'},
                'results': []}
    images = [('drachtio', args.drachtio_image), ('checkpoint-memory', args.old_image)]
    if args.candidate:
        images.append(('resident-memory', args.candidate))
    manifest['status'] = 'RUNNING'
    (args.output/'evidence.json').write_text(json.dumps(manifest,indent=2)+'\n')
    if args.only:
        images = [entry for entry in images if entry[0] == args.only]
        assert images, '--only resident-memory requires --candidate'
    for count in phones:
        for history in histories:
            for label, image in images:
                identity = 'sipperf-'+uuid.uuid4().hex[:10]
                network, core, app, load = (identity+suffix for suffix in ('-net','-core','-app','-load'))
                directory = args.output/f'{label}-{count}-history{history}'
                directory.mkdir()
                certs = directory/'certs'
                certificates(certs)
                shared = directory/'load'
                shared.mkdir()
                tracked = []
                try:
                    command(['docker', 'network', 'create', '--label', 'sipbridge.performance='+identity, network])
                    base = ['docker','create','--platform',args.platform,'--network',network,'--label','sipbridge.performance='+identity]
                    common = ['--cpus','1','--memory','512m','--name',core,'--network-alias','core']
                    native_admin = None
                    if label == 'drachtio':
                        opts = ['-p','127.0.0.1::9022',image,'drachtio','-f','/etc/performance.xml']
                        if args.debug: opts.extend(['--stdout','--loglevel','debug','--sofia-loglevel','9'])
                    else:
                        opts = ['-p','127.0.0.1::8800','-e','SIPBRIDGE_STATE_BACKEND=memory','-e','SIPBRIDGE_MEMORY_MAX_BYTES=68719476736','-e','SIPBRIDGE_MEMORY_MAX_GROUPS=8192','-e','GOMAXPROCS=1',image,
                                '--instance=perf-core','--advertise=core:5061','--sip-plain-listen=','--sip-cert=/certs/server.crt','--sip-key=/certs/server.key',
                                '--control-cert=/certs/server.crt','--control-key=/certs/server.key','--client-ca=/certs/ca.crt','--allowed-client-uri=spiffe://performance/application',
                                '--admin-listen=0.0.0.0:8800','--sip-withdrawal-grace=0s','--sip-drain-timeout=2s']
                    tracked.append(core)
                    command(base+common+opts)
                    command(['docker','cp',str(certs),core+':/certs'])
                    if label == 'drachtio': command(['docker','cp',str(HERE/'drachtio.conf.xml'),core+':/etc/performance.xml'])
                    command(['docker','start',core])
                    control_port = port(core, 9022 if label=='drachtio' else 8800)
                    def listening():
                        with socket.create_connection(('127.0.0.1',control_port),timeout=1): return True
                    wait(listening)
                    if label != 'drachtio': native_admin = control_port
                    tracked.append(app)
                    command(base+['--name',app,'--cpus','1','--memory','256m','-e','GOMAXPROCS=1','-p','127.0.0.1::8801',
                                  'sipbridge-perf-drachtio-app' if label=='drachtio' else 'sipbridge-perf-controller'])
                    command(['docker','cp',str(certs),app+':/certs'])
                    command(['docker','start',app])
                    app_port = port(app,8801)
                    wait(lambda: json_url(app_port))
                    empty_idle = measure(docker,core,app,args.idle_seconds,native_admin)
                    tracked.append(load)
                    command(base+['--name',load,'--cpus','1','--memory','256m',
                                  'python:3.13-alpine','python','/load.py','--phones',str(count),'--history',str(history),'--duration',str(args.duration),
                                  '--cadence',str(args.cadence),'--warmup',str(args.warmup)])
                    command(['docker','cp',str(certs),load+':/certs'])
                    command(['docker','cp',str(HERE/'load.py'),load+':/load.py'])
                    command(['docker','start',load])
                    def artifact(name):
                        copied=subprocess.run(['docker','cp',load+':/output/'+name,str(shared/name)],capture_output=True)
                        if copied.returncode: return False
                        json.loads((shared/name).read_text())
                        return True
                    def load_ready():
                        if artifact('result.json'):
                            result=json.loads((shared/'result.json').read_text())
                            if result['status']=='FAIL': raise AssertionError(str(result))
                        return artifact('ready.json')
                    wait(load_ready,120+count*2)
                    setup = json.loads((shared/'ready.json').read_text())
                    wait(lambda: all(json_url(app_port)[key] == value for key,value in setup['expected'].items() if key != 'notify200'),5)
                    populated_idle = measure(docker,core,app,args.idle_seconds,native_admin)
                    app_before = json_url(app_port)
                    (shared/'start').touch()
                    active = measure(docker,core,app,args.duration,native_admin,lambda:command(['docker','cp',str(shared/'start'),load+':/output/start']))
                    wait(lambda:artifact('result.json'),15)
                    workload = json.loads((shared/'result.json').read_text())
                    if workload['status'] != 'PASS': raise ValueError(workload)
                    wait(lambda:json_url(app_port)['notify']-app_before['notify'] == workload['expected']['notify'],5)
                    app_after = json_url(app_port)
                    assert app_before['failures'] == app_after['failures'] == 0, app_after
                    counts = {key: app_after[key]-app_before[key] for key in ('register','subscribe','notify','failures')}
                    assert counts['failures']==0 and all(counts[key]==value for key,value in workload['expected'].items()), counts
                    result = {'backend':label,'phones':count,'history':history,'image':image,'imageInspection':json.loads(command(['docker','image','inspect',image]))[0],
                              'emptyIdle':empty_idle,'populatedIdle':populated_idle,'active':active,'setup':setup,'workload':workload,'controllerCounts':counts,'controllerAllPhases':app_after}
                    manifest['results'].append(result)
                    (args.output/'evidence.json').write_text(json.dumps(manifest,indent=2)+'\n')
                    print(json.dumps({'backend':label,'phones':count,'history':history,'coreMillicores':active['core']['cpuMillicores'],'controllerMillicores':active['controller']['cpuMillicores'],'status':'PASS'}),flush=True)
                except BaseException as error:
                    manifest['status'] = 'FAIL'
                    manifest['failure'] = {'backend':label,'phones':count,'history':history,'type':type(error).__name__,'error':str(error)}
                    (args.output/'evidence.json').write_text(json.dumps(manifest,indent=2)+'\n')
                    raise
                finally:
                    for name in reversed(tracked):
                        logs=subprocess.run(['docker','logs',name],capture_output=True,text=True)
                        (directory/(name+'.log')).write_text(logs.stdout+logs.stderr)
                        subprocess.run(['docker','rm','-f',name],capture_output=True)
                    subprocess.run(['docker','network','rm',network],capture_output=True)
    gates = []
    for result in manifest['results']:
        if result['backend'] != 'resident-memory' or args.only: continue
        baseline=next(v for v in manifest['results'] if v['backend']=='drachtio' and v['phones']==result['phones'] and v['history']==result['history'])
        cpp=baseline['active']['core']['cpuMillicores']; native=result['active']['core']['cpuMillicores']; limit=max(cpp*1.25,cpp+2)
        gates.append({'phones':result['phones'],'history':result['history'],'drachtioCoreMillicores':cpp,'residentCoreMillicores':native,'limitMillicores':limit,'pass':native<=limit})
    history_comparisons = []
    for result in manifest['results']:
        if result['history'] == 0: continue
        fresh = next((v for v in manifest['results'] if v['backend']==result['backend'] and v['phones']==result['phones'] and v['history']==0), None)
        if fresh is None: continue
        comparison = {'backend':result['backend'],'phones':result['phones'],'history':result['history'],
                      'historyAgeSeconds':result['workload']['historicalGenerationAgeAtEndSeconds'],
                      'coreRSSFinalDeltaBytes':result['active']['core']['rssFinalBytes']-fresh['active']['core']['rssFinalBytes'],
                      'controllerRSSFinalDeltaBytes':result['active']['controller']['rssFinalBytes']-fresh['active']['controller']['rssFinalBytes'],
                      'populatedIdleCoreMillicoresDelta':result['populatedIdle']['core']['cpuMillicores']-fresh['populatedIdle']['core']['cpuMillicores'],
                      'activeCoreMillicoresDelta':result['active']['core']['cpuMillicores']-fresh['active']['core']['cpuMillicores']}
        if 'liveHeapBytes' in result['active']['core']:
            comparison['coreLiveHeapDeltaBytes'] = result['active']['core']['liveHeapBytes']-fresh['active']['core']['liveHeapBytes']
        resident = resident_evidence(fresh['active']['core'].get('runtimeAfter'), result['active']['core'].get('runtimeAfter'))
        resident['noGrowthObserved'] = all(delta <= 0 for delta in resident['delta'].values()) if resident['available'] else None
        comparison['residentHistory'] = resident
        history_comparisons.append(comparison)
    manifest['historyComparisons'] = history_comparisons
    manifest['historyQualification'] = 'Finite historical-generation comparison only. Optional resident accounting gauges compare the final fresh and historical populations exactly; noGrowthObserved requires no increase in all three gauges. Missing metrics are unavailable, not zero. RSS includes allocator retention and live heap is the latest GC observation; neither proves records retired or bounded growth.'
    manifest['status']='PASS'
    manifest['cpuGates']=gates
    manifest['qualification']='Local controlled evidence only. Emulated-platform runs, one-phone smoke and short windows are not a staging CPU gate.'
    (args.output/'evidence.json').write_text(json.dumps(manifest,indent=2)+'\n')


if __name__ == '__main__':
    os.umask(0o077)
    main()
