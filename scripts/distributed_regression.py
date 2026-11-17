#!/usr/bin/env python3
"""Local Sprint 5 regression. Reuses one database and the existing containers.

Requires prebuilt API/control binaries, mysql CLI, and the governance profile.
Artifacts and the fixture are retained, including after failure. Run --phase
smoke first, then performance/faults against the SAME --output directory.
"""
import argparse
import base64
import concurrent.futures
import hashlib
import json
import math
import os
from pathlib import Path
import secrets
import select
import signal
import socket
import subprocess
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
HTTP = urllib.request.build_opener(urllib.request.ProxyHandler({}))
WORKERS = ['worker-a', 'worker-b', 'worker-c']
PORTS = dict(core=18100, rebuilder=18101, query=18102, relay=18103,
             **{'worker-a': 18104, 'worker-b': 18105, 'worker-c': 18106})


def command(args, **kwargs):
    result = subprocess.run(args, capture_output=True, text=True, timeout=60, **kwargs)
    if result.returncode:
        raise RuntimeError(f'{args[0]} exited {result.returncode}: {result.stderr[-2000:]}')
    return result.stdout.strip()


def request(url, data=None, headers=None, method=None, timeout=10):
    req = urllib.request.Request(url, data=None if data is None else json.dumps(data).encode(),
                                 headers=headers or {}, method=method)
    with HTTP.open(req, timeout=timeout) as response:
        return json.load(response)


def b64(value):
    return base64.b64encode(value.encode()).decode()


def percentile(values, p):
    return sorted(values)[max(0, math.ceil(len(values) * p / 100) - 1)] if values else None


class EtcdProxy:
    """Disconnect only worker-a from etcd while other clients keep advancing it."""
    def __init__(self):
        self.blocked = threading.Event()
        self.closed = threading.Event()
        self.listener = socket.socket()
        self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listener.bind(('127.0.0.1', 23791))
        self.listener.listen()
        self.listener.settimeout(.2)
        self.thread = threading.Thread(target=self.accept, daemon=True)
        self.thread.start()

    def accept(self):
        while not self.closed.is_set():
            try:
                client, _ = self.listener.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            if self.blocked.is_set():
                client.close()
            else:
                threading.Thread(target=self.forward, args=(client,), daemon=True).start()

    def forward(self, client):
        with client:
            try:
                with socket.create_connection(('127.0.0.1', 23790), timeout=2) as upstream:
                    while not self.closed.is_set() and not self.blocked.is_set():
                        ready, _, _ = select.select([client, upstream], [], [], .1)
                        for source in ready:
                            data = source.recv(65536)
                            if not data:
                                return
                            (upstream if source is client else client).sendall(data)
            except OSError:
                return

    def close(self):
        self.closed.set()
        self.listener.close()
        self.thread.join(timeout=2)


class Regression:
    def __init__(self, args):
        self.args = args
        self.output = Path(args.output).resolve()
        self.output.mkdir(parents=True, exist_ok=True)
        self.state_path = self.output / 'fixture.json'
        self.processes = {}
        self.logs = []
        self.paused = set()
        self.stop = threading.Event()
        self.resource_error = None
        self.phase = 'setup'
        self.started = time.monotonic()
        self.results = []
        self.samples = []
        self.proxy = EtcdProxy()
        self.mysql = ['mysql', '--protocol=tcp', '-h' + os.getenv('TEST_MYSQL_HOST', '127.0.0.1'),
                      '-P' + os.getenv('TEST_MYSQL_PORT', '3306'), '-u' + os.getenv('TEST_MYSQL_USER', 'root'),
                      '--batch', '--raw', '--skip-column-names', '--connect-timeout=5']
        self.admin_env = dict(os.environ, MYSQL_PWD=os.getenv('TEST_MYSQL_PASSWORD', ''))
        self.state = json.loads(self.state_path.read_text()) if self.state_path.exists() else None
        self.database = self.state['database'] if self.state else 'notex_s5_' + secrets.token_hex(4)
        self.prefix = self.database
        self.runtime_key = '/notex/' + self.prefix + '/runtime'
        self.base_env = dict(os.environ, GIN_MODE='release', GOMAXPROCS='2', GOMEMLIMIT='128MiB',
                             MYSQL_DSN=f'{self.database}:{self.database}@tcp(127.0.0.1:3306)/{self.database}',
                             REDIS_URL='redis://127.0.0.1:6380/0', FEED_MODE='hybrid', FEED_PULL_AUTHORS='2',
                             RABBITMQ_URL='amqp://notex:notex-local@127.0.0.1:5673/',
                             RABBITMQ_QUEUE_PREFIX=self.prefix, ETCD_ENDPOINTS='http://127.0.0.1:23790',
                             RPC_TOKEN=hashlib.sha256(self.prefix.encode()).hexdigest(),
                             CORE_RPC_ADDR='127.0.0.1:19100', REBUILDER_RPC_ADDR='127.0.0.1:19101')
        self.run_id = time.strftime('%Y%m%d-%H%M%S')
        self.datadir = Path(self.sql('SELECT @@datadir', selected=False))
        self.disk_baseline = self.disk_usage()
        self.cache_reserve = sum(path.stat().st_size for path in Path('/tmp/notex-grpc-build-cache').rglob('*') if path.is_file())

    def sql(self, statement, selected=True):
        return command(self.mysql + ([self.database] if selected else []), input=statement, env=self.admin_env)

    def check(self):
        if self.resource_error:
            raise RuntimeError(self.resource_error)

    def wait(self, predicate, description, seconds=60):
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            self.check()
            result = predicate()
            if result:
                return result
            time.sleep(.2)
        raise RuntimeError('deadline waiting for ' + description)

    def record(self, name, **values):
        result = dict(name=name, elapsed_s=round(time.monotonic() - self.started, 3), **values)
        self.results.append(result)
        with (self.output / (self.run_id + '-results.jsonl')).open('a') as stream:
            stream.write(json.dumps(result) + '\n')
        print(json.dumps(result), flush=True)

    def etcd(self, path, data):
        return request('http://127.0.0.1:23790/v3/' + path, data,
                       {'Content-Type': 'application/json'})

    def runtime(self):
        kv = self.etcd('kv/range', {'key': b64(self.runtime_key)})['kvs'][0]
        return json.loads(base64.b64decode(kv['value'])), int(kv['mod_revision'])

    def put_runtime(self, config):
        return int(self.etcd('kv/put', {'key': b64(self.runtime_key),
                                       'value': b64(json.dumps(config))})['header']['revision'])

    def control(self, *args):
        return command([self.args.control, *args], env=self.base_env)

    def setup(self):
        if self.state is None:
            self.sql(f'CREATE DATABASE {self.database}', selected=False)
            for migration in sorted((ROOT / 'migrations').glob('*.sql')):
                self.sql(migration.read_text())
            self.sql(f"CREATE USER '{self.database}'@'127.0.0.1' IDENTIFIED BY '{self.database}' WITH MAX_USER_CONNECTIONS 52;"
                     f"GRANT SELECT,INSERT,UPDATE,DELETE ON {self.database}.* TO '{self.database}'@'127.0.0.1';", selected=False)
            users = ','.join(f"({i},'s5_user_{i}','fixture',1700000000)" for i in range(1, 1004))
            self.sql('INSERT INTO users(id,username,password_hash,created_on) VALUES ' + users)
            edges = ','.join(f'({reader},{author},1700000000)' for reader in range(4, 1004) for author in (1, 2, 3))
            self.sql('INSERT INTO following(user_id,follow_id,created_on) VALUES ' + edges)
            self.control('init', '-active', 'worker-a', '-shards', '4', '-sql', '12', '-concurrency', '6')
            self.state = dict(database=self.database, readers=1000, seed=5, disk_baseline=self.disk_baseline)
            self.state_path.write_text(json.dumps(self.state, indent=2) + '\n')
        # Deterministic fixture sessions are extended, preserving all business rows.
        if int(self.sql(f'SELECT COUNT(*) FROM sessions WHERE expires_on>{int(time.time())+3600}')) == 1003:
            return
        values = []
        for user in range(1, 1004):
            token = hashlib.sha256(f'{self.prefix}/{user}'.encode()).hexdigest()
            values.append(f"(UNHEX('{hashlib.sha256(token.encode()).hexdigest()}'),{user},{int(time.time()) + 86400})")
        self.sql('INSERT INTO sessions VALUES ' + ','.join(values) + ' ON DUPLICATE KEY UPDATE expires_on=VALUES(expires_on)')

    def api(self, name, path, user=None, data=None, method=None, key=None):
        self.check()
        headers = {'Content-Type': 'application/json'}
        if user:
            headers['Authorization'] = 'Bearer ' + hashlib.sha256(f'{self.prefix}/{user}'.encode()).hexdigest()
        if data is not None:
            headers['Idempotency-Key'] = key or secrets.token_hex(16)
        return request(f'http://127.0.0.1:{PORTS[name]}' + path, data, headers, method)

    def launch(self, name, parallelism=2, memory=128):
        role = {'rebuilder': 'feed-rebuilder', 'query': 'feed-query'}.get(name, name)
        if name in WORKERS:
            role = 'feed-worker'
        env = dict(self.base_env, NOTEX_ROLE=role, INSTANCE_ID=name, HTTP_ADDR=f'127.0.0.1:{PORTS[name]}',
                   GOMAXPROCS=str(parallelism), GOMEMLIMIT=f'{memory}MiB')
        if name == 'worker-a':
            env['ETCD_ENDPOINTS'] = 'http://127.0.0.1:23791'
        log = (self.output / f'{self.run_id}-{name}.log').open('a')
        self.logs.append(log)
        process = subprocess.Popen([self.args.binary], env=env, stdout=log, stderr=log)
        self.processes[name] = process

        def ready():
            if process.poll() is not None:
                raise RuntimeError(f'{name} exited {process.returncode}; inspect {log.name}')
            try:
                return self.api(name, '/readyz')
            except (urllib.error.URLError, TimeoutError):
                return False
        self.wait(ready, name + ' ready', 30)

    def stop_process(self, name, hard=False):
        process = self.processes.pop(name, None)
        if process and process.poll() is None:
            process.send_signal(signal.SIGCONT)
            process.send_signal(signal.SIGKILL if hard else signal.SIGTERM)
            try:
                process.wait(timeout=18)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
                raise RuntimeError(name + ' exceeded graceful shutdown deadline')

    def applied(self, revision, workers=None):
        names = workers or [name for name in WORKERS if name in self.processes]
        return all(self.api(name, '/controlz')['applied_revision'] == revision for name in names)

    def registrations_applied(self, revision, workers):
        entries = json.loads(self.control('status'))
        return all(any(item['key'].endswith('/feed-worker/' + name)
                       and item['value']['applied_revision'] >= revision for item in entries) for name in workers)

    def configure(self, count, expanded=False, restart=True):
        names = WORKERS[:count]
        parallelism = 2 if expanded else 6 // count
        sql = 4 if expanded else 12 // count
        memory = 128 if expanded else 384 // count
        if restart:
            for name in WORKERS:
                self.stop_process(name)
            for name in names:
                self.launch(name, parallelism, memory)
        config, revision = self.runtime()
        self.wait(lambda: self.registrations_applied(revision, names), 'registered revision')
        config.update(preferred=[names[i % count] for i in range(4)], worker_sql=sql, concurrency=parallelism)
        path = self.output / 'runtime.json'
        path.write_text(json.dumps(config))
        self.control('apply', '-file', str(path), '-expect', str(revision))
        _, revision = self.runtime()
        self.wait(lambda: self.applied(revision), 'runtime applied')
        subscribers = sum(name in self.processes for name in WORKERS)
        self.wait(lambda: self.ownership(subscribers), 'SAC ownership', 30)
        return dict(workers=count, worker_sql=sql, concurrency=parallelism, gomaxprocs=parallelism,
                    gomemlimit_mib=memory, revision=revision)

    def rabbit(self, path):
        return request('http://127.0.0.1:15673/api/' + path,
                       headers={'Authorization': 'Basic ' + b64('notex:notex-local')})

    def queues(self):
        return [row for row in self.rabbit('queues/%2F') if row['name'].startswith(self.prefix + '.')]

    def ownership(self, count, preferred=True):
        queues = self.queues()
        shards = [row for row in queues if '.feed.chunk.s' in row['name']]
        if len(shards) != 4:
            return False
        for row in shards:
            details = self.rabbit('queues/%2F/' + row['name'])
            consumers = details.get('consumer_details', [])
            active = [item for item in consumers if item['active']]
            if len(consumers) != count or len(active) != 1:
                return False
            if preferred and active[0]['arguments'].get('x-priority') != 100:
                return False
        return True

    def batch(self, count=24, author=1):
        def publish(index):
            start = time.monotonic()
            item = self.api('core', '/api/v1/posts', author,
                            {'content': f's5 {self.run_id} {self.phase} {index}'})
            return dict(id=item['id'], created_on=item['created_on'], start=start,
                        publish_ms=(time.monotonic() - start) * 1000)
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            return list(pool.map(publish, range(count)))

    def settled(self):
        values = self.sql("SELECT (SELECT COUNT(*) FROM outbox_events WHERE published_on IS NULL),"
                          "(SELECT COUNT(*) FROM outbox_events o LEFT JOIN consumption_jobs j ON j.event_id=o.id "
                          f"AND j.consumer='{self.prefix}.feed' WHERE j.id IS NULL),"
                          "(SELECT COUNT(*) FROM consumption_jobs WHERE status<>'completed'),"
                          "(SELECT COUNT(*) FROM feed_tasks WHERE kind='feed.plan' AND children IS NULL),"
                          "(SELECT COUNT(*) FROM feed_tasks t LEFT JOIN consumption_jobs j ON j.event_id=t.id "
                          f"AND j.consumer LIKE '{self.prefix}.feed.%' WHERE j.id IS NULL)").split('\t')
        return all(int(value) == 0 for value in values)

    def drain(self, seconds=120):
        self.wait(self.settled, 'durable Feed jobs complete', seconds)

    def verify(self, readers=(4, 5, 6, 7), limit=37):
        for reader in readers:
            authors = {reader}
            authors.update(map(int, self.sql(f'SELECT follow_id FROM following WHERE user_id={reader}').splitlines()))
            rows = self.sql('SELECT id,author_id,created_on,content FROM posts WHERE deleted_on IS NULL AND author_id IN (' +
                            ','.join(map(str, authors)) + ') ORDER BY created_on DESC,id DESC')
            expected = [row.split('\t') for row in rows.splitlines()]
            actual, cursor = [], ''
            while True:
                page = self.api('query', '/api/v1/feed?' + urllib.parse.urlencode({'limit': limit, 'cursor': cursor}), reader)
                actual.extend([[str(p['id']), str(p['author']['id']), str(p['created_on']), p['content']] for p in page['items']])
                if not page['has_more']:
                    assert page['next_cursor'] == ''
                    break
                assert page['next_cursor'] and page['next_cursor'] != cursor
                cursor = page['next_cursor']
            assert actual == expected, f'Feed mismatch for reader {reader}'

    def warm(self):
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(lambda reader: self.api('query', '/api/v1/feed?limit=1', reader), range(4, 1004)))

    def verify_indexes(self):
        rows = self.sql('SELECT created_on,id FROM posts WHERE deleted_on IS NULL AND author_id IN (1,3) ORDER BY created_on DESC,id DESC LIMIT 500')
        members = [f'{int(created):020d}:{int(post):020d}' for created,post in (line.split('\t') for line in rows.splitlines())]
        script = ("local r={} for i,key in ipairs(KEYS) do local meta=string.gsub(key,':posts$',':cache') "
                  "r[i]={redis.call('HGET',meta,'floor') or '',redis.sha1hex(table.concat(redis.call('ZREVRANGE',key,0,-1),'\\n'))} end return r")
        for first in range(4, 1004, 32):
            keys = [f'{self.prefix}:reader:{{{reader}}}:posts' for reader in range(first,min(first+32,1004))]
            states = self.redis('EVAL',script,len(keys),*keys)
            for offset,(floor,digest) in enumerate(states):
                retained = [member for member in members if member >= floor]
                expected = hashlib.sha1('\n'.join(retained).encode()).hexdigest()
                assert digest == expected, f'index mismatch at reader {first+offset}, floor {floor}'

    def redis(self, *args):
        # Each call is a bounded, independent connection so pause/recovery cannot
        # leave a shared protocol stream misaligned.
        encoded = [str(arg).encode() for arg in args]
        wire = f'*{len(args)}\r\n'.encode() + b''.join(b'$' + str(len(arg)).encode() + b'\r\n' + arg + b'\r\n' for arg in encoded)
        with socket.create_connection(('127.0.0.1', 6380), timeout=3) as conn:
            conn.sendall(wire)
            with conn.makefile('rb') as stream:
                def read():
                    line = stream.readline().rstrip(b'\r\n')
                    kind, value = line[:1], line[1:]
                    if kind == b'-':
                        raise RuntimeError(value.decode())
                    if kind == b'*':
                        return [read() for _ in range(int(value))]
                    if kind == b'$':
                        if int(value) == -1:
                            return None
                        result = stream.read(int(value))
                        stream.read(2)
                        return result.decode()
                    return int(value) if kind == b':' else value.decode()
                return read()

    def disk_usage(self):
        paths = [self.output, self.datadir, Path('/tmp/notex-grpc-build-cache'),
                 Path(self.args.binary), Path(self.args.control)]
        local = sum(p.stat().st_size for path in paths for p in ([path] if path.is_file() else path.rglob('*')) if p.is_file())
        volumes = sum(int(command(['docker', 'compose', 'exec', '-T', name, 'du', '-sk', directory]).split()[0]) * 1024
                      for name, directory in [('redis', '/data'), ('rabbitmq', '/var/lib/rabbitmq')])
        return local + volumes

    def monitor(self):
        next_disk = 0
        disk_growth = 0
        with (self.output / (self.run_id + '-resources.jsonl')).open('w') as stream:
            while not self.stop.is_set():
                try:
                    rows = [line.split(None, 4) for line in command(['ps', '-axo', 'pid=,ppid=,rss=,%cpu=,comm=']).splitlines()]
                    descendants = {os.getpid()}
                    for _ in range(4):
                        descendants.update(int(row[0]) for row in rows if int(row[1]) in descendants)
                    own = [row for row in rows if int(row[0]) in descendants or Path(row[4]).name == 'mysqld']
                    containers = [json.loads(line) for line in command(['docker', 'stats', '--no-stream', '--format', '{{json .}}',
                                                                        'notex-redis-1', 'notex-rabbitmq-1', 'notex-etcd-1']).splitlines()]
                    def memory(row):
                        value = row['MemUsage'].split(' / ')[0]
                        for unit, multiplier in [('GiB', 1024**3), ('MiB', 1024**2), ('KiB', 1024), ('B', 1)]:
                            if value.endswith(unit):
                                return float(value[:-len(unit)]) * multiplier
                        raise ValueError(value)
                    total = sum(int(row[2]) * 1024 for row in own) + sum(map(memory, containers))
                    if time.monotonic() >= next_disk and not self.paused:
                        baseline = self.state['disk_baseline'] if self.state else self.disk_baseline
                        # Reserve 384 MiB for etcd backend/WAL plus 64 MiB for its
                        # image and logs. Include binaries even if built beforehand.
                        disk_growth = max(0, self.disk_usage() - baseline) + 448 * 1024**2
                        disk_growth += Path(self.args.binary).stat().st_size + Path(self.args.control).stat().st_size
                        # Include the entire reused build cache conservatively,
                        # covering compilation that preceded fixture creation.
                        disk_growth += self.cache_reserve
                        next_disk = time.monotonic() + 20
                    connections = int(self.sql(f"SELECT COUNT(*) FROM information_schema.processlist WHERE USER='{self.database}'", selected=False))
                    row = dict(phase=self.phase, elapsed_s=time.monotonic()-self.started, total_bytes=total,
                               disk_growth_upper_bytes=disk_growth, db_connections=connections,
                               native_cpu=sum(float(row[3]) for row in own), native=own, containers=containers)
                    row['queues'] = [{key:queue.get(key) for key in ('name','messages_ready','messages_unacknowledged','consumers')}
                                     for queue in self.queues()]
                    self.samples.append(row)
                    stream.write(json.dumps(row) + '\n')
                    stream.flush()
                    if total > 4 * 1024**3 or disk_growth > 2 * 1024**3:
                        raise RuntimeError('4 GiB memory or 2 GiB incremental disk budget exceeded')
                except Exception as exc:
                    self.resource_error = str(exc)
                    for process in list(self.processes.values()):
                        if process.poll() is None:
                            process.send_signal(signal.SIGCONT)
                            process.terminate()
                    return
                self.stop.wait(2)

    def performance(self):
        existing = int(self.sql('SELECT COUNT(*) FROM posts WHERE author_id=1 AND deleted_on IS NULL'))
        if existing < 500:
            self.configure(1)
            self.batch(500-existing)
            self.drain(seconds=300)
        for expanded in (False, True):
            for count in (1, 2, 3):
                case = f'{"expanded" if expanded else "fixed"}-{count}'
                if self.args.cases and case not in self.args.cases:
                    continue
                budget = self.configure(count, expanded)
                self.warm()
                self.batch(4)
                self.drain()
                self.verify_indexes()
                for round_number in range(1, 4):
                    self.phase = f'{"expanded" if expanded else "fixed"}-{count}-{round_number}'
                    started = time.monotonic()
                    items = self.batch(96)
                    pending = {item['id']: item for item in items}
                    visibility = []
                    read_latency = []
                    sql_ids = ','.join(str(item['id']) for item in items)
                    worker_started = None
                    while pending:
                        self.check()
                        start = time.monotonic()
                        page = self.api('query', '/api/v1/feed?limit=100', 4)
                        read_latency.append((time.monotonic()-start)*1000)
                        now = time.monotonic()
                        if worker_started is None and int(self.sql("SELECT COUNT(*) FROM feed_tasks WHERE JSON_EXTRACT(body,'$.post_id') IN (" + sql_ids + ')')):
                            worker_started = now
                        for item in page['items']:
                            original = pending.pop(item['id'], None)
                            if original:
                                visibility.append((now-original['start'])*1000)
                        if now-started > 120:
                            raise RuntimeError('visibility deadline')
                        time.sleep(.02)
                    self.drain()
                    elapsed = time.monotonic() - started
                    worker_elapsed = time.monotonic()-worker_started if worker_started else None
                    ids = ','.join(str(item['id']) for item in items)
                    shard_rows = self.sql("SELECT COALESCE(JSON_EXTRACT(body,'$.shard'),0),COUNT(*),SUM(JSON_LENGTH(body,'$.recipients')) "
                                          "FROM feed_tasks WHERE kind='feed.chunk' AND JSON_EXTRACT(body,'$.post_id') IN (" + ids + ') GROUP BY 1')
                    self.verify()
                    self.verify_indexes()
                    self.record('performance', group='expanded' if expanded else 'fixed', round=round_number,
                                budget=budget, posts=len(items), recipients=len(items)*1001, elapsed_s_batch=elapsed,
                                posts_per_s=len(items)/elapsed, recipient_effects_per_s=len(items)*1001/elapsed,
                                worker_drain_s=worker_elapsed,
                                publish_p99_ms=percentile([item['publish_ms'] for item in items], 99),
                                feed_p99_ms=percentile(read_latency,99), visibility_p99_ms=percentile(visibility,99),
                                shards=shard_rows, samples=len(read_latency))

    def pause_container(self, name):
        self.paused.add(name)
        command(['docker', 'compose', 'pause', name])

    def resume_container(self, name):
        command(['docker', 'compose', 'unpause', name])
        self.paused.discard(name)

    def capacity(self):
        budget = self.configure(3, expanded=True)
        self.warm()
        for rate in (1,2,4):
            for number in range(1,4):
                self.phase = f'capacity-{rate}-{number}'
                latencies, futures = [], []
                start = time.monotonic()
                with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
                    for index in range(rate*15):
                        deadline = start+index/rate
                        while time.monotonic()<deadline:
                            before = time.monotonic()
                            self.api('query','/api/v1/feed?limit=50',4)
                            latencies.append((time.monotonic()-before)*1000)
                            time.sleep(min(.1,max(0,deadline-time.monotonic())))
                        futures.append(pool.submit(self.api,'core','/api/v1/posts',1,{'content':f's5 capacity {rate} {number} {index}'}))
                    items = [future.result() for future in futures]
                while time.monotonic() < start+15:
                    before = time.monotonic()
                    self.api('query','/api/v1/feed?limit=50',4)
                    latencies.append((time.monotonic()-before)*1000)
                    time.sleep(.1)
                load_elapsed = time.monotonic()-start
                pending_jobs = int(self.sql("SELECT COUNT(*) FROM consumption_jobs WHERE status='pending'"))
                unpublished = int(self.sql('SELECT COUNT(*) FROM outbox_events WHERE published_on IS NULL'))
                queued = sum(queue.get('messages_ready',0)+queue.get('messages_unacknowledged',0)
                             for queue in self.queues() if not queue['name'].endswith('.search'))
                before = time.monotonic()
                self.drain()
                drain_s = time.monotonic()-before
                self.verify_indexes()
                self.record('capacity',rate=rate,round=number,budget=budget,posts=len(items),load_elapsed_s=load_elapsed,
                            pending_jobs_at_stop=pending_jobs,unpublished_at_stop=unpublished,drain_s=drain_s,
                            broker_backlog_at_stop=queued,
                            feed_p99_ms=percentile(latencies,99),read_samples=len(latencies))

    def business_checks(self, number):
        key = secrets.token_hex(16)
        body = {'content': f's5 business {number}'}
        post = self.api('core','/api/v1/posts',3,body,key=key)
        assert self.api('core','/api/v1/posts',3,body,key=key)['id'] == post['id']
        path = '/api/v1/posts/' + str(post['id'])
        assert self.api('core',path,4)['content'] == body['content']
        assert self.api('core',path,4)['content'] == body['content']
        try:
            self.api('core',path,4,method='DELETE')
            raise AssertionError('unauthorized deletion succeeded')
        except urllib.error.HTTPError as exc:
            assert exc.code == 403, exc.code
        self.api('core',path,3,method='DELETE')
        try:
            self.api('core',path,4)
            raise AssertionError('deleted cached body returned')
        except urllib.error.HTTPError as exc:
            assert exc.code == 404, exc.code
        self.api('core','/api/v1/users/1/follow',4,method='DELETE')
        self.verify(readers=(4,))
        self.api('core','/api/v1/users/1/follow',4,method='PUT')
        self.drain()
        self.verify()
        self.verify_indexes()
        # Expire only eight test reader caches, then request concurrent rebuilds.
        for reader in range(4,12):
            for suffix in ('cache','posts','deleted','relations'):
                self.redis('PEXPIRE',f'{self.prefix}:reader:{{{reader}}}:{suffix}',1)
        with concurrent.futures.ThreadPoolExecutor(max_workers=16) as pool:
            pages = list(pool.map(lambda reader: self.api('query','/api/v1/feed?limit=50',reader), list(range(4,12))*2))
        assert all(page['items'] for page in pages)
        self.verify(readers=range(4,12))
        self.verify_indexes()
        self.record('business_and_concurrent_rebuild',round=number,readers=8,requests=16,
                    checks=['idempotency','hot_body_delete','forbidden_delete','unfollow','refollow','pagination','1000_index_digests'])

    def faults(self):
        self.configure(2)
        self.warm()
        for number in range(1, 6):
            self.phase = f'fault-{number}'
            # Wait for real sharded work, then stall Core so an external claim
            # stays in flight while the active worker is killed.
            items = self.batch(24)
            ids = ','.join(str(item['id']) for item in items)
            self.wait(lambda: int(self.sql("SELECT COUNT(*) FROM feed_tasks WHERE kind='feed.chunk' AND JSON_EXTRACT(body,'$.post_id') IN ("+ids+')')) > 0, 'new chunk tasks')
            self.processes['core'].send_signal(signal.SIGSTOP)
            pending_query = ("SELECT id FROM consumption_jobs WHERE status='pending' AND next_attempt_on>UNIX_TIMESTAMP() "
                             "AND attempts>0 AND consumer IN ('"+self.prefix+".feed.chunk.s000','"+self.prefix+".feed.chunk.s002')")
            self.wait(lambda: self.sql(pending_query), 'claimed external work on A')
            self.processes['worker-a'].send_signal(signal.SIGSTOP)
            claimed_ids = self.sql(pending_query).splitlines()
            assert claimed_ids, 'claim finished before freeze'
            self.wait(lambda: any(q.get('messages_unacknowledged',0) > 0 for q in self.queues()
                                  if '.feed.chunk.s' in q['name']), 'unacknowledged shard delivery')
            before = self.queues()
            start = time.monotonic()
            self.stop_process('worker-a', hard=True)
            self.processes['core'].send_signal(signal.SIGCONT)
            self.wait(lambda: self.ownership(1, preferred=False), 'standby takeover')
            takeover = time.monotonic()-start
            self.drain()
            recovery = time.monotonic()-start
            retried = self.sql('SELECT id,status,attempts FROM consumption_jobs WHERE id IN ('+','.join(claimed_ids)+')')
            assert all(row.split('\t')[1] == 'completed' and int(row.split('\t')[2]) >= 2 for row in retried.splitlines()), retried
            self.verify()
            self.verify_indexes()
            self.wait(lambda: not self.etcd('kv/range', {'key': b64('/notex/'+self.prefix+'/services/feed-worker/worker-a')}).get('kvs'), 'dead lease expiry', 30)
            self.launch('worker-a')
            _, revision = self.runtime()
            self.wait(lambda: self.applied(revision), 'restarted A applied')
            self.wait(lambda: self.ownership(2), 'preferred owners return')
            self.record('sigkill', round=number, takeover_s=takeover, recovery_s=recovery, claimed_jobs=retried,
                        before=[{k:q.get(k) for k in ('name','messages_unacknowledged','messages_ready')} for q in before])

            # Lose A's lease while its data-plane connections keep processing.
            config, old_revision = self.runtime()
            self.proxy.blocked.set()
            self.wait(lambda: not self.etcd('kv/range', {'key': b64('/notex/'+self.prefix+'/services/feed-worker/worker-a')}).get('kvs'), 'partitioned lease expiry', 35)
            duplicate_env = dict(self.base_env, NOTEX_ROLE='feed-worker', INSTANCE_ID='worker-a', HTTP_ADDR='127.0.0.1:18107')
            duplicate = subprocess.run([self.args.binary], env=duplicate_env, capture_output=True, text=True, timeout=10)
            assert duplicate.returncode != 0 and 'slot' in duplicate.stderr, duplicate.stderr
            self.batch(4)
            self.drain()
            assert self.api('worker-a','/controlz')['config_revision'] == old_revision
            # Reject malformed update on live B, then advance beyond A's Watch.
            invalid = dict(config, concurrency=0)
            self.put_runtime(invalid)
            self.wait(lambda: self.api('worker-b','/controlz')['last_control_error'], 'invalid config rejection')
            assert self.api('worker-b','/controlz')['config_revision'] == old_revision
            revision = self.put_runtime(config)
            self.etcd('kv/compaction', {'revision': str(revision), 'physical': True})
            start = time.monotonic()
            self.proxy.blocked.clear()
            self.wait(lambda: self.applied(revision), 'compacted Watch resnapshot', 45)
            self.wait(lambda: self.etcd('kv/range', {'key': b64('/notex/'+self.prefix+'/services/feed-worker/worker-a')}).get('kvs'), 'lease reregister', 45)
            self.verify()
            self.verify_indexes()
            self.record('lease_watch_compaction_invalid_config', round=number, old_revision=old_revision,
                        revision=revision, recovery_s=time.monotonic()-start, duplicate_slot_rejected=True)

            self.pause_container('etcd')
            start = time.monotonic()
            try:
                self.batch(4)
                self.drain()
                self.verify()
                while time.monotonic()-start < 17:
                    self.check()
                    time.sleep(.2)
            finally:
                self.resume_container('etcd')
            self.wait(lambda: self.registrations_applied(revision,WORKERS[:2]),'directory recovery',45)
            self.record('etcd_outage',round=number,outage_s=time.monotonic()-start)

            self.processes['core'].send_signal(signal.SIGSTOP)
            start = time.monotonic()
            try:
                try:
                    self.api('query','/api/v1/feed?limit=10',4)
                    raise AssertionError('stalled Core unexpectedly served Feed')
                except urllib.error.HTTPError as exc:
                    assert exc.code == 503, exc.code
                deadline_elapsed = time.monotonic()-start
                assert deadline_elapsed < 7, deadline_elapsed
            finally:
                self.processes['core'].send_signal(signal.SIGCONT)
            self.verify()
            self.record('rpc_deadline',round=number,elapsed_s_call=deadline_elapsed,status=503)

            # Brief Redis outage exercises RPC fallback and durable retries.
            self.pause_container('redis')
            try:
                items = self.batch(4)
                start = time.monotonic()
                page = self.api('query','/api/v1/feed?limit=10',4)
                assert page['items']
                fallback = time.monotonic()-start
                time.sleep(3)
            finally:
                self.resume_container('redis')
            self.drain()
            self.verify()
            self.verify_indexes()
            self.record('redis_outage',round=number,fallback_s=fallback,posts=[item['id'] for item in items])

            # Expand under load, lower per-instance slots, and remove C again.
            self.launch('worker-c')
            self.batch(8)
            budget = self.configure(3, expanded=True, restart=False)
            self.drain()
            self.verify()
            self.configure(2, restart=False)
            self.stop_process('worker-c')
            self.wait(lambda: self.ownership(2), 'scale-in ownership')
            self.record('scale_drain', round=number, budget=budget)
            self.business_checks(number)

    def run(self):
        monitor = None
        status, error = 'failed', None
        try:
            self.setup()
            monitor = threading.Thread(target=self.monitor, daemon=True)
            monitor.start()
            for name in (() if self.args.phase == 'guards' else ('core','rebuilder','query','relay')):
                self.launch(name)
            if self.args.phase in ('all','guards'):
                self.phase='guards'
                env = dict(self.base_env, GOCACHE='/tmp/notex-grpc-build-cache', GOMEMLIMIT='256MiB',
                           NOTEX_DISTRIBUTED_DSN=self.base_env['MYSQL_DSN'],
                           NOTEX_DISTRIBUTED_REDIS=self.base_env['REDIS_URL'],
                           NOTEX_DISTRIBUTED_MQ=self.base_env['RABBITMQ_URL'])
                output = command(['go','test','-p','1','-count=1','-run','^TestDistributed','-v',
                                  './src/internal/feed','./src/internal/outbox'],env=env)
                (self.output/(self.run_id+'-guards.log')).write_text(output+'\n')
                self.record('guards',status='passed')
            if self.args.phase in ('all','smoke'):
                self.phase='smoke'
                self.configure(2, expanded=True)
                self.warm()
                self.batch(2)
                self.batch(2, author=2)
                self.drain()
                self.verify()
                self.record('smoke', status='passed', ownership=self.ownership(2))
            if self.args.phase in ('all','performance'):
                self.performance()
            if self.args.phase in ('all','faults'):
                self.faults()
            if self.args.phase in ('all','capacity'):
                self.capacity()
            status='passed'
        except BaseException as exc:
            error=f'{type(exc).__name__}: {exc}'
            print(error,flush=True)
        finally:
            self.proxy.blocked.clear()
            for name in list(self.paused):
                self.resume_container(name)
            for name in reversed(list(self.processes)):
                self.stop_process(name)
            self.stop.set()
            if monitor:
                monitor.join(timeout=15)
            self.proxy.close()
            for log in self.logs:
                log.close()
            report=dict(status=status,error=error,database=self.database,phase=self.args.phase,results=self.results,
                        budget=dict(memory_gib=4,disk_gib=2,max_workers=3),resource_error=self.resource_error,
                        peak_memory_bytes=max((row['total_bytes'] for row in self.samples), default=0),
                        peak_disk_growth_upper_bytes=max((row['disk_growth_upper_bytes'] for row in self.samples),default=0),
                        peak_db_connections=max((row['db_connections'] for row in self.samples),default=0),
                        binary_sha256=hashlib.sha256(Path(self.args.binary).read_bytes()).hexdigest(),
                        elapsed_s=time.monotonic()-self.started)
            (self.output/(self.run_id+'-report.json')).write_text(json.dumps(report,indent=2)+'\n')
            print(json.dumps({key:value for key,value in report.items() if key!='results'}),flush=True)
        return status=='passed' and not self.resource_error


if __name__ == '__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary',required=True)
    parser.add_argument('--control',required=True)
    parser.add_argument('--output',required=True)
    parser.add_argument('--phase',choices=['smoke','performance','faults','capacity','guards','all'],default='all')
    parser.add_argument('--cases',nargs='+',choices=[f'{group}-{count}' for group in ('fixed','expanded') for count in (1,2,3)],
                        help='rerun selected performance groups against the retained fixture')
    args=parser.parse_args()
    def terminate(signum, frame):
        raise KeyboardInterrupt('termination requested')
    signal.signal(signal.SIGTERM,terminate)
    raise SystemExit(0 if Regression(args).run() else 1)
