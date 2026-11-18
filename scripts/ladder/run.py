#!/usr/bin/env python3
"""Run the local M staircase against a fresh copy of the Sprint 5 fixture."""
import argparse
from contextlib import ExitStack
import hashlib
import json
import os
from pathlib import Path
import re
import resource
import secrets
import subprocess
import sys
import tempfile
import threading
import time
from urllib.error import URLError
from urllib.request import build_opener, ProxyHandler, Request

from inboxes import prepare

ROOT = Path(__file__).resolve().parents[2]
ROLES = ('core', 'feed-rebuilder', 'feed-query', 'feed-worker', 'relay', 'like-worker')
HTTP = build_opener(ProxyHandler({}))
SEED = 5


def command(*args, **kwargs):
    return subprocess.check_output(args, cwd=ROOT, text=True, timeout=30, **kwargs).strip()


def docker(service, *args):
    return command('docker', 'compose', 'exec', '-T', service, *args)


def redis(*args):
    return docker('redis', 'redis-cli', '-e', '--raw', '-n', '15', *args)


def broker(*args):
    return docker('rabbitmq', 'rabbitmqctl', '-q', *args)


def sql(statement, database=None):
    return command('mysql', '--protocol=tcp', '-h127.0.0.1', '-uroot', '-BN',
                   *([database] if database else []), input=statement)


def get(port, path, token=None):
    request = Request(f'http://127.0.0.1:{port}{path}')
    if token:
        request.add_header('Authorization', 'Bearer ' + token)
    with HTTP.open(request, timeout=3) as response:
        return json.load(response)


def write_json(path, obj):
    path.write_text(json.dumps(obj, indent=2) + '\n')


def terminate(process):
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()


def clear_redis(namespace):
    keys = redis('--scan', '--pattern', '*' + namespace + '*').splitlines()
    for i in range(0, len(keys), 100):
        redis('UNLINK', *keys[i:i + 100])


class Resources:
    """Sample process RSS, Docker working sets and disk growth through preparation and load."""

    def __init__(self, work, output, namespace):
        self.namespace = namespace
        self.collect_metrics = False
        self.work = work
        self.output = output
        self.mysql_dir = sql('SELECT @@datadir')
        self.baseline = self.disks()
        self.guard = work / 'stop'
        self.done = threading.Event()
        self.thread = threading.Thread(target=self.monitor, daemon=True)

    def disks(self):
        mysql = int(command('du', '-sk', self.mysql_dir).split()[0]) * 1024
        volumes = int(docker('redis', 'df', '-k', '/data').splitlines()[-1].split()[2]) * 1024
        files = sum(p.stat().st_size for root in (self.work, self.output)
                    for p in root.rglob('*') if p.is_file())
        return mysql, volumes, files

    def sample(self):
        table = [line.split(None, 3) for line in command('ps', '-axo', 'pid=,ppid=,rss=,comm=').splitlines()]
        pids = {os.getpid()}
        while True:
            children = {int(pid) for pid, parent, _, _ in table if int(parent) in pids}
            if children <= pids:
                break
            pids.update(children)
        rss = sum(int(size) * 1024 for pid, _, size, name in table
                  if int(pid) in pids or name.endswith('/mysqld'))
        stats = command('docker', 'stats', '--no-stream', '--format', '{{.Name}} {{.MemUsage}}',
                        'notex-redis-1', 'notex-rabbitmq-1', 'notex-etcd-1')
        containers = {}
        for line in stats.splitlines():
            name, value, _, _ = line.split()
            size, unit = re.fullmatch(r'([\d.]+)(\w+)', value).groups()
            containers[name] = int(float(size) * {'B': 1, 'KiB': 1024, 'MiB': 2**20, 'GiB': 2**30}[unit])
        growth = sum(max(0, now - before) for now, before in zip(self.disks(), self.baseline))
        total = rss + sum(containers.values())
        info = dict(line.split(':', 1) for line in redis('INFO', 'all').splitlines() if ':' in line)
        sample = dict(time=time.time(), total_bytes=total, rss_bytes=rss, container_bytes=containers,
                      disk_growth_bytes=growth, redis={k: info.get(k) for k in
                      ('used_memory', 'maxmemory', 'errorstat_OOM', 'evicted_keys')})
        if self.collect_metrics:
            sample['metrics_time'] = time.time()
            sample['pools'] = get(18500, '/debug/pools')
            sample['feed_paths'] = get(18502, '/debug/feed-paths')
            sample['mysql'] = self.mysql_sample()
        with (self.output / 'resources.jsonl').open('a') as f:
            print(json.dumps(sample), file=f)
        if total >= 4 * 2**30 or growth >= 2 * 2**30:
            raise RuntimeError('resource limit exceeded')

    def mysql_sample(self):
        rows = sql(f"""
            SELECT VARIABLE_NAME, VARIABLE_VALUE FROM performance_schema.global_status
            WHERE VARIABLE_NAME IN ('Threads_connected','Threads_running','Innodb_row_lock_time',
                'Innodb_row_lock_waits','Connection_errors_max_connections');
            SELECT CONCAT(EVENT_NAME,'/',op), CASE op
                WHEN 'writes' THEN COUNT_WRITE WHEN 'write_ps' THEN SUM_TIMER_WRITE
                WHEN 'reads' THEN COUNT_READ WHEN 'read_ps' THEN SUM_TIMER_READ END
            FROM performance_schema.file_summary_by_event_name
            CROSS JOIN (SELECT 'writes' op UNION ALL SELECT 'write_ps' UNION ALL SELECT 'reads'
                UNION ALL SELECT 'read_ps') ops
            WHERE EVENT_NAME IN ('wait/io/file/innodb/innodb_data_file','wait/io/file/innodb/innodb_log_file');
            SELECT 'app_connections', COUNT(*) FROM performance_schema.threads
            WHERE PROCESSLIST_USER='{self.namespace}';
            SELECT 'app_lock_waits',COUNT(*) FROM performance_schema.data_lock_waits w
            JOIN performance_schema.threads t ON t.THREAD_ID=w.REQUESTING_THREAD_ID
            WHERE t.PROCESSLIST_USER='{self.namespace}';
            SELECT CONCAT('fetch/',OBJECT_NAME),COUNT_FETCH
            FROM performance_schema.table_io_waits_summary_by_table WHERE OBJECT_SCHEMA='{self.namespace}';
            SELECT CONCAT('active/',CASE
                WHEN LOWER(t.PROCESSLIST_INFO) LIKE '%exists%from following%' THEN 'feed_page'
                WHEN LOWER(t.PROCESSLIST_INFO) LIKE '%join following f on f.user_id=u.id%' THEN 'relations'
                WHEN LOWER(t.PROCESSLIST_INFO) LIKE '%for update%' THEN 'for_update'
                WHEN LOWER(t.PROCESSLIST_INFO) LIKE '%sessions%' THEN 'session'
                ELSE COALESCE(t.PROCESSLIST_STATE,'other') END), COUNT(*)
            FROM performance_schema.threads t WHERE PROCESSLIST_USER='{self.namespace}'
            AND PROCESSLIST_COMMAND<>'Sleep' GROUP BY 1
        """)
        data = {k: int(v) for k, v in (row.split('\t') for row in rows.splitlines())}
        times = command('ps', '-axo', 'time=,comm=')
        cpu = next(line.split()[0] for line in times.splitlines() if line.endswith('/mysqld'))
        seconds = 0
        for part in cpu.split(':'):
            seconds = seconds * 60 + float(part)
        data['mysqld_cpu_seconds'] = seconds
        return data

    def monitor(self):
        try:
            while not self.done.is_set():
                self.sample()
        except Exception as exc:
            self.guard.write_text(str(exc))

    def close(self):
        self.done.set()
        self.thread.join()


def run(args, output, work, report, stack):
    namespace = report['namespace']
    source = json.loads(args.fixture.read_text())['database']
    if not re.fullmatch(r'notex_s5_[0-9a-f]+', source):
        raise ValueError('expected an isolated Sprint 5 fixture')
    report.update(source=source, seed=SEED, roles=ROLES, commit=command('git', 'rev-parse', 'HEAD'))
    def cleanup(label, func, *params):
        func(*params)
        report.setdefault('cleaned', []).append(label)

    monitor = Resources(work, output, namespace)
    monitor.sample()
    env = dict(os.environ, GOMAXPROCS='2', GOMEMLIMIT='128MiB', GIN_MODE='release',
               MYSQL_DSN=f'{namespace}:{namespace}@tcp(127.0.0.1:3306)/{namespace}',
               FEED_MODE='hybrid', FEED_PULL_AUTHORS='2', LIKE_MODE='adaptive',
               REDIS_URL='redis://127.0.0.1:6380/15', RABBITMQ_QUEUE_PREFIX=namespace,
               RABBITMQ_URL='amqp://notex:notex-local@127.0.0.1:5673/'+namespace,
               ETCD_ENDPOINTS='http://127.0.0.1:23790', RPC_TOKEN=secrets.token_hex(32),
               CORE_RPC_ADDR='127.0.0.1:19400', REBUILDER_RPC_ADDR='127.0.0.1:19401')
    for name, package in [('notex', './src'), ('control', './src/cmd/control'), ('load', './scripts/ladder/load.go')]:
        subprocess.run(['go', 'build', '-p', '2', '-o', work / name, package], cwd=ROOT, env=env, check=True)
    report['redis_config'] = {key: redis('CONFIG', 'GET', key).splitlines()[-1]
                              for key in ('maxmemory', 'maxmemory-policy')}
    if report['redis_config'] != {'maxmemory': '1073741824', 'maxmemory-policy': 'noeviction'}:
        raise RuntimeError('Redis needs 1 GiB maxmemory and noeviction')
    sql(f'CREATE DATABASE {namespace}')
    stack.callback(cleanup, 'database', sql, f'DROP DATABASE {namespace}')
    for migration in sorted((ROOT / 'migrations').glob('*.sql')):
        sql(migration.read_text(), namespace)
    for table in ('users', 'sessions', 'following', 'posts'):
        sql(f'INSERT INTO {table} SELECT * FROM {source}.{table}', namespace)
    # Renew only the deterministic test sessions in this disposable copy.
    sql(f"""UPDATE sessions SET expires_on=UNIX_TIMESTAMP()+86400
            WHERE token_hash=UNHEX(SHA2(SHA2(CONCAT('{source}/',user_id),256),256))""", namespace)
    valid = sql(f"""SELECT DISTINCT user_id FROM sessions WHERE expires_on>UNIX_TIMESTAMP()+600
                   AND token_hash=UNHEX(SHA2(SHA2(CONCAT('{source}/',user_id),256),256))
                   AND user_id BETWEEN 1 AND 1003 ORDER BY user_id""", namespace)
    if list(map(int, valid.split())) != list(range(1, 1004)):
        raise RuntimeError('fixture must contain the three authors and 1000 authenticated readers')
    readers = list(range(4, 1004))
    posts = sql('SELECT id FROM posts WHERE deleted_on IS NULL ORDER BY id', namespace).split()
    if not posts:
        raise RuntimeError('fixture has no visible posts')
    write_json(work/'fixture.json', dict(source=source, seed=SEED, readers=readers, posts=posts))
    sql(f"CREATE USER '{namespace}'@'127.0.0.1' IDENTIFIED BY '{namespace}'")
    stack.callback(cleanup, 'sql user', sql, f"DROP USER '{namespace}'@'127.0.0.1'")
    sql(f"GRANT SELECT,INSERT,UPDATE,DELETE ON {namespace}.* TO '{namespace}'@'127.0.0.1'")
    broker('add_vhost', namespace)
    stack.callback(cleanup, 'vhost', broker, 'delete_vhost', namespace)
    broker('set_permissions', '-p', namespace, 'notex', '.*', '.*', '.*')
    command(work/'control', 'init', '-workers', 'worker-a', '-active', 'worker-a', env=env)
    stack.callback(cleanup, 'etcd', docker, 'etcd', 'etcdctl', 'del', '--prefix', '/notex/'+namespace+'/')
    stack.callback(cleanup, 'redis', clear_redis, namespace)
    services = {}
    def check():
        if monitor.guard.exists():
            raise RuntimeError(monitor.guard.read_text())
        for role, proc in services.items():
            if proc.poll() is not None:
                raise RuntimeError(f'{role} exited with status {proc.returncode}')

    ordinary = {'core': args.core_conns, 'feed-worker': 4, 'relay': 2, 'like-worker': 10}
    report['mysql_pools'] = dict(ordinary=ordinary, core_read=args.core_read_conns,
                                 total=sum(ordinary.values()) + args.core_read_conns,
                                 server_max=int(sql('SELECT @@max_connections')))
    for i, role in enumerate(ROLES):
        port = 18400 + i
        settings = dict(env, INSTANCE_ID='worker-a' if role == 'feed-worker' else role,
                        NOTEX_ROLE=role, HTTP_ADDR=f'127.0.0.1:{port}', NOTEX_PPROF_ADDR=f'127.0.0.1:{18500+i}')
        if role in ordinary:
            settings['MYSQL_MAX_OPEN_CONNS'] = str(ordinary[role])
        if role == 'core':
            settings['CORE_READ_MAX_OPEN_CONNS'] = str(args.core_read_conns)
        with (work/(role+'.log')).open('w') as log:
            services[role] = subprocess.Popen([work/'notex'], env=settings, stdout=log, stderr=log)
        stack.callback(terminate, services[role])
        for attempt in range(150):
            check()
            try:
                get(port, '/readyz')
                break
            except (URLError, TimeoutError):
                time.sleep(.1)
        else:
            raise RuntimeError(f'{role} readiness timeout')
    monitor.sample()
    monitor.collect_metrics = True
    monitor.thread.start()
    stack.callback(monitor.close)
    def token(user):
        return hashlib.sha256(f'{source}/{user}'.encode()).hexdigest()

    report['core_probe'] = get(18400, '/api/v1/posts/'+posts[0], token(readers[0]))['id']
    rows = sql('SELECT user_id,id FROM following ORDER BY user_id,id', namespace)
    policy = hashlib.sha256(b'hybrid:2').hexdigest()
    evidence = prepare(readers, SEED, namespace, policy, rows,
                       lambda reader: get(18402, '/api/v1/feed?limit=20', token(reader)), redis, check,
                       lambda data: write_json(output/'initial-inboxes.json', data))
    report['initial_inboxes'] = dict(ready=len(evidence['ready_ids']), cold=len(evidence['cold_ids']))
    check()
    if args.prepare_only:
        return

    with (output/'load.json').open('w') as f:
        load = subprocess.Popen([work/'load', work/'fixture.json', monitor.guard],
                                env=dict(env, GOMAXPROCS='4'), stdout=f)
    stack.callback(terminate, load)
    samplers = []
    def sample(label, cmd):
        with (output/label).open('wb') as f:
            sampler = subprocess.Popen(cmd, cwd=ROOT, stdout=f)
        samplers.append((label, sampler))
        stack.callback(terminate, sampler)
    sample('sql-window.json', [sys.executable, 'scripts/mysql_digest_window.py', namespace, '--seconds', '30'])
    sample('core-cpu.pb.gz', ['curl', '--noproxy', '*', '-fsS', 'http://127.0.0.1:18500/debug/pprof/profile?seconds=30'])
    started = time.monotonic()
    traced = False
    while load.poll() is None:
        for role, proc in services.items():
            if proc.poll() is not None:
                monitor.guard.write_text(role+' exited')
        if not traced and time.monotonic()-started >= 32:
            sample('core-trace.out', ['curl', '--noproxy', '*', '-fsS', 'http://127.0.0.1:18500/debug/pprof/trace?seconds=5'])
            traced = True
        time.sleep(.02)
    report['end_state'] = {role: p.poll() for role, p in services.items()}
    report['samplers'] = {name: proc.poll() for name, proc in samplers}
    (output/'queues.tsv').write_text(broker('list_queues', '-p', namespace, 'name', 'messages_ready',
                                            'messages_unacknowledged')+'\n')
    if load.returncode:
        raise RuntimeError(f'load generator exited {load.returncode}')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', required=True, type=Path, help='new batch directory')
    parser.add_argument('--fixture', type=Path, default=ROOT/'notes/fanout/runs/sprint5/fixture.json')
    parser.add_argument('--core-conns', type=int, default=32, metavar='1..100')
    parser.add_argument('--core-read-conns', type=int, default=4, metavar='1..100')
    parser.add_argument('--prepare-only', action='store_true', help='verify initial state and clean up')
    args = parser.parse_args()
    if not all(1 <= n <= 100 for n in (args.core_conns, args.core_read_conns)):
        parser.error('connection pool limits must be in 1..100')
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    report = dict(namespace='notex_m_'+secrets.token_hex(8), prepare_only=args.prepare_only, started=time.time())
    try:
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        if soft < 4096:
            resource.setrlimit(resource.RLIMIT_NOFILE, (min(4096, hard) if hard > 0 else 4096, hard))
        with tempfile.TemporaryDirectory(prefix='notex-ladder-') as tmp, ExitStack() as stack:
            run(args, output, Path(tmp), report, stack)
    except BaseException as exc:
        report['error'] = repr(exc)
        raise
    finally:
        report['finished'] = time.time()
        write_json(output/'run.json', report)


if __name__ == '__main__':
    main()
