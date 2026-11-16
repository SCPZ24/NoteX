#!/usr/bin/env python3
"""Isolated Sprint 3 baseline. Requires mysql CLI and prebuilt API/events binaries.

Admin connection uses TEST_MYSQL_HOST/PORT/USER/PASSWORD, as verify_api.py does.
Run C and Q separately. Output includes raw HTTP samples and EXPLAIN ANALYZE.
"""
import argparse
import base64
import collections
import hashlib
import json
import math
import os
from pathlib import Path
import secrets
import signal
import socket
import subprocess
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
EPOCH = 1700000000
SEED = 3101


def reference_feed(reader, relationships, posts):
    """Independent set membership + Python sort over raw authoritative rows."""
    authors = {target for _, source, target, _ in relationships if source == reader}
    authors.add(reader)
    return sorted((p for p in posts if p[1] in authors and p[3] is None),
                  key=lambda p: (p[2], p[0]), reverse=True)


def fixture(dataset):
    """Deterministic unique relations, tied timestamps and deleted posts."""
    users, edges, count = (200, 5000, 10000) if dataset == 'C' else (120000, 1000000, 200000)
    pairs = []
    if dataset == 'C':
        pairs = [(u, (u - 1 + d) % users + 1) for u in range(1, users + 1) for d in range(1, 26)]
    else:
        # Two hubs permit >100,000-position paging in each direction.
        pairs.extend((1, u) for u in range(2, 100102))
        pairs.extend((u, 2) for u in range(3, 100103))
        for reader, degree in [(3, 100), (4, 500), (5, 1000)]:
            pairs.extend((reader, u) for u in range(10, 10 + degree - 1))
        for source in range(6, users + 1):
            for distance in range(1, 9):
                target = (source - 1 + distance * SEED) % users + 1
                if target == 2:
                    continue
                pairs.append((source, target))
                if len(pairs) == edges:
                    break
            if len(pairs) == edges:
                break
    assert len(pairs) == edges and len(set(pairs)) == edges
    relationships = [(i, u, v, EPOCH + i // 7) for i, (u, v) in enumerate(pairs, 1)]
    posts = [(i, (i * SEED) % users + 1, EPOCH + i // 11,
              EPOCH + count if i % 17 == 0 else None) for i in range(1, count + 1)]
    return users, relationships, posts


def digest(rows):
    value = hashlib.sha256()
    for row in rows:
        value.update((json.dumps(row, separators=(',', ':')) + '\n').encode())
    return value.hexdigest()


def percentiles(samples):
    values = sorted(samples)
    return {f'p{p}_ms': values[min(len(values) - 1, math.ceil(len(values) * p / 100) - 1)]
            for p in (50, 95, 99)} if values else {}


class Baseline:
    def __init__(self, args):
        self.args = args
        self.output = Path(args.output).resolve()
        self.output.mkdir(parents=True, exist_ok=False)
        self.database = 'notex_fanout_' + secrets.token_hex(6)
        self.password = secrets.token_hex(16)
        self.host = os.getenv('TEST_MYSQL_HOST', '127.0.0.1')
        self.port = os.getenv('TEST_MYSQL_PORT', '3306')
        self.admin_env = dict(os.environ, MYSQL_PWD=os.getenv('TEST_MYSQL_PASSWORD', ''))
        self.connection = ['mysql', '--protocol=tcp', '--host=' + self.host, '--port=' + self.port,
                           '--user=' + os.getenv('TEST_MYSQL_USER', 'root'), '--connect-timeout=5',
                           '--batch', '--raw', '--skip-column-names']
        self.processes = []
        self.tokens = {}
        self.created = self.account_created = False
        self.stop = threading.Event()
        self.resource_error = None
        self.started = time.monotonic()
        self.phase = 'setup'
        self.report = {'dataset': args.dataset, 'seed': SEED, 'status': 'running',
                       'baseline_commit': self.command(['git', 'rev-parse', 'HEAD']).strip(),
                       'kind': 'correctness_and_exploratory_baseline', 'cleanup': False,
                       'budget': {'own_rss_mib': 2048, 'mysql_rss_mib': 4096,
                                  'new_disk_mib': 2048, 'seconds': 1800, 'http_concurrency': 1,
                                  'db_user_connections': 9, 'db_connections_including_admin': 10, 'gomaxprocs': 2},
                       'versions': {}, 'checks': [], 'peak_own_rss_kib': 0, 'peak_mysql_rss_kib': 0}
        self.samples = (self.output / 'http.jsonl').open('w')
        self.logs = (self.output / 'process.log').open('w')
        self.datadir = None
        self.command_metadata()

    def command(self, command, **kwargs):
        result = subprocess.run(command, text=True, capture_output=True, timeout=60, **kwargs)
        if result.returncode:
            raise RuntimeError(result.stderr.strip())
        return result.stdout

    def command_metadata(self):
        self.report['command'] = ['python3', 'scripts/fanout_baseline.py', '--dataset', self.args.dataset,
                                  '--binary', self.args.binary, '--events-binary', self.args.events_binary,
                                  '--output', self.args.output]
        for name, command in [('go', ['go', 'version']), ('python', ['python3', '--version'])]:
            self.report['versions'][name] = self.command(command).strip()
        (self.output / 'source.diff').write_text(self.command(['git', 'diff', 'HEAD', '--', 'src', 'migrations', 'go.mod', 'go.sum']))
        self.report['runner_sha256'] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
        (self.output / 'runner.py').write_bytes(Path(__file__).read_bytes())
        self.report['binary_sha256'] = hashlib.sha256(Path(self.args.binary).read_bytes()).hexdigest()
        self.report['events_binary_sha256'] = hashlib.sha256(Path(self.args.events_binary).read_bytes()).hexdigest()
        self.report['rabbitmq_container'] = self.command(['docker', 'compose', 'ps', '-q', 'rabbitmq']).strip()
        if not self.report['rabbitmq_container']:
            raise RuntimeError('the existing Compose RabbitMQ must be running')
        self.report['versions']['rabbitmq'] = self.command(
            ['docker', 'compose', 'exec', '-T', 'rabbitmq', 'rabbitmq-diagnostics', 'server_version']).strip()
        (self.output / 'git-status.txt').write_text(self.command(['git', 'status', '--short']))

    def sql(self, statement, selected=True):
        return self.command(self.connection + ([self.database] if selected else []),
                            input=statement, env=self.admin_env).strip()

    def check_budget(self):
        if self.resource_error:
            raise RuntimeError(self.resource_error)
        if time.monotonic() - self.started > 1800:
            raise RuntimeError('30-minute run budget exceeded')

    def monitor(self):
        rabbit_sample = {}
        next_rabbit_sample = 0
        with (self.output / 'resources.jsonl').open('w') as stream:
            while not self.stop.is_set():
                try:
                    rows = [line.split(None, 4) for line in self.command(['ps', '-axo', 'pid=,ppid=,rss=,%cpu=,comm=']).splitlines()]
                    children = {os.getpid()}
                    for _ in range(6):
                        children.update(int(r[0]) for r in rows if len(r) == 5 and int(r[1]) in children)
                    own_rows = [r for r in rows if len(r) == 5 and int(r[0]) in children]
                    mysql_rows = [r for r in rows if len(r) == 5 and Path(r[4]).name == 'mysqld']
                    own = sum(int(r[2]) for r in own_rows)
                    mysql = sum(int(r[2]) for r in mysql_rows)
                    if time.monotonic() >= next_rabbit_sample:
                        rabbit_sample = json.loads(self.command(['docker', 'stats', '--no-stream', '--format', '{{json .}}',
                                                                 self.report['rabbitmq_container']]))
                        rabbit_sample['sample_elapsed_s'] = time.monotonic() - self.started
                        next_rabbit_sample = time.monotonic() + 10
                    paths = [self.output]
                    if self.datadir:
                        paths.append(self.datadir)
                    disk = sum(p.stat().st_size for root in paths for p in root.rglob('*') if p.is_file())
                    self.report['peak_own_rss_kib'] = max(own, self.report['peak_own_rss_kib'])
                    self.report['peak_mysql_rss_kib'] = max(mysql, self.report['peak_mysql_rss_kib'])
                    stream.write(json.dumps({'elapsed_s': time.monotonic() - self.started,
                                             'own_rss_kib': own, 'mysql_rss_kib': mysql,
                                             'own_cpu_percent': sum(float(r[3]) for r in own_rows),
                                             'mysql_cpu_percent': sum(float(r[3]) for r in mysql_rows),
                                             'rabbitmq': rabbit_sample,
                                             'test_disk_bytes': disk}) + '\n')
                    stream.flush()
                    if own > 2 * 1024**2 or mysql > 4 * 1024**2 or disk > 2 * 1024**3:
                        raise RuntimeError('agreed RSS or disk budget exceeded')
                    self.check_budget()
                except Exception as exc:
                    self.resource_error = str(exc)
                    for process in self.processes:
                        if process.poll() is None:
                            process.terminate()
                    return
                self.stop.wait(2)

    def insert(self, table, columns, rows):
        start = time.monotonic()
        batch = []
        for row in rows:
            batch.append('(' + ','.join('NULL' if x is None else str(x) for x in row) + ')')
            if len(batch) == 1000:
                self.check_budget()
                self.sql(f'INSERT INTO {table} ({columns}) VALUES ' + ','.join(batch))
                batch = []
        if batch:
            self.sql(f'INSERT INTO {table} ({columns}) VALUES ' + ','.join(batch))
        return time.monotonic() - start

    def setup(self):
        self.report['versions']['mysql'] = self.sql('SELECT VERSION(), @@transaction_isolation, @@innodb_buffer_pool_size;', False)
        self.datadir = Path(self.sql('SELECT @@datadir;', False)) / self.database
        self.sql(f'CREATE DATABASE `{self.database}`;', False)
        self.created = True
        for migration in sorted((ROOT / 'migrations').glob('*.sql')):
            self.sql(migration.read_text())
        self.sql(f"CREATE USER '{self.database}'@'127.0.0.1' IDENTIFIED BY '{self.password}' WITH MAX_USER_CONNECTIONS 9;", False)
        self.account_created = True
        self.sql(f"GRANT SELECT, INSERT, UPDATE, DELETE ON `{self.database}`.* TO '{self.database}'@'127.0.0.1';", False)
        users, relationships, posts = fixture(self.args.dataset)
        self.report['counts'] = {'users': users, 'relationships': len(relationships), 'posts': len(posts),
                                 'active_posts': sum(p[3] is None for p in posts)}
        self.report['fixture_hashes'] = {'relationships': digest(relationships), 'posts': digest(posts)}
        self.insert('users', 'id,username,password_hash,created_on',
                    ((u, f"'user_{u}'", "'unused-fixture-password'", EPOCH) for u in range(1, users + 1)))
        self.report['load_seconds'] = {
            'relationships': self.insert('following', 'id,user_id,follow_id,created_on', relationships),
            'posts': self.insert('posts', 'id,author_id,created_on,deleted_on,content',
                                 ((*p, f"'post_{p[0]}'") for p in posts))}
        self.sql('ANALYZE TABLE following, posts, users;')
        # Raw row export deliberately avoids the production joins, EXISTS and cursor predicates.
        raw_edges = [tuple(map(int, row.split('\t'))) for row in self.sql(
            'SELECT id,user_id,follow_id,created_on FROM following ORDER BY id;').splitlines()]
        raw_posts = [tuple(None if v == 'NULL' else int(v) for v in row.split('\t')) for row in self.sql(
            'SELECT id,author_id,created_on,deleted_on FROM posts ORDER BY id;').splitlines()]
        assert raw_edges == relationships and raw_posts == posts, 'fixture round-trip mismatch'
        self.report['checks'].append('all raw relationships/posts match deterministic fixture')
        return users, raw_edges, raw_posts

    def launch(self, mq=False, dummy=False):
        with socket.socket() as probe:
            probe.bind(('127.0.0.1', 0))
            port = probe.getsockname()[1]
        self.base = f'http://127.0.0.1:{port}'
        env = dict(os.environ, GOMAXPROCS='2', GOMEMLIMIT='512MiB', GIN_MODE='release',
                   HTTP_ADDR=f'127.0.0.1:{port}', MYSQL_DSN=f'{self.database}:{self.password}@tcp({self.host}:{self.port})/{self.database}',
                   REDIS_URL='', FEED_MODE='sql', RABBITMQ_QUEUE_PREFIX='notex_dummy_' + self.database,
                   RABBITMQ_URL=os.getenv('TEST_RABBITMQ_URL', 'amqp://notex:notex-local@127.0.0.1:5673/') if mq else '')
        command = [str(Path(self.args.events_binary).resolve()), 'dummy'] if dummy else [str(Path(self.args.binary).resolve())]
        process = subprocess.Popen(command, env=env, stdout=self.logs, stderr=self.logs)
        self.processes.append(process)
        if dummy:
            return
        for _ in range(100):
            if process.poll() is not None:
                raise RuntimeError('API exited at startup')
            try:
                self.request('/readyz', record=False)
                return
            except urllib.error.URLError:
                time.sleep(.1)
        raise RuntimeError('API startup deadline exceeded')

    def stop_processes(self):
        for process in self.processes:
            if process.poll() is None:
                process.send_signal(signal.SIGTERM)
                try:
                    process.wait(timeout=12)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
                    raise RuntimeError('process shutdown exceeded 12 seconds')
        self.processes = []

    def token(self, reader):
        if reader not in self.tokens:
            token = secrets.token_hex(32)
            key = hashlib.sha256(token.encode()).hexdigest()
            self.sql(f"INSERT INTO sessions VALUES (UNHEX('{key}'),{reader},{int(time.time()) + 3600});")
            self.tokens[reader] = token
        return self.tokens[reader]

    def request(self, path, reader=None, data=None, record=True):
        self.check_budget()
        headers = {'Content-Type': 'application/json'}
        if reader:
            headers['Authorization'] = 'Bearer ' + self.token(reader)
        if data is not None:
            headers['Idempotency-Key'] = secrets.token_hex(16)
        req = urllib.request.Request(self.base + path, headers=headers,
                                     data=None if data is None else json.dumps(data).encode())
        start = time.monotonic()
        status = 0
        try:
            with urllib.request.urlopen(req, timeout=20) as response:
                status = response.status
                return json.load(response)
        except urllib.error.HTTPError as exc:
            status = exc.code
            raise
        finally:
            if record:
                self.samples.write(json.dumps({'elapsed_s': start - self.started, 'phase': self.phase, 'path': path,
                                               'reader': reader, 'status': status,
                                               'latency_ms': (time.monotonic() - start) * 1000}) + '\n')

    def verify_pages(self, path, reader, expected, projection, limit):
        offset, cursor, seen = 0, '', set()
        while True:
            query = urllib.parse.urlencode({'limit': limit, 'cursor': cursor})
            page = self.request(path + '?' + query, reader)
            actual = [projection(item) for item in page['items']]
            assert actual == expected[offset:offset + limit], f'page mismatch {path} reader={reader} offset={offset}'
            offset += len(actual)
            assert page['has_more'] == (offset < len(expected)), 'has_more mismatch'
            if not page['has_more']:
                assert page['next_cursor'] == '' and offset == len(expected)
                return offset
            cursor = page['next_cursor']
            assert cursor and cursor not in seen, 'cursor failed to advance'
            seen.add(cursor)

    def verify(self, users, relationships, posts):
        readers = range(1, users + 1) if self.args.dataset == 'C' else [3, 4, 5]
        checked = 0
        refs = self.output / 'reference.jsonl'
        with refs.open('w') as output:
            for reader in readers:
                expected = reference_feed(reader, relationships, posts)
                output.write(json.dumps({'reader': reader, 'post_ids': [p[0] for p in expected], 'sha256': digest(expected)}) + '\n')
                checked += self.verify_pages('/api/v1/feed', reader,
                    [(p[0], p[1], p[2], f'post_{p[0]}', f'user_{p[1]}') for p in expected],
                    lambda p: (p['id'], p['author']['id'], p['created_on'], p['content'], p['author']['username']), 50)
        self.report['feed_correctness'] = {'readers': len(readers), 'returned_posts': checked,
                                            'scope': 'all C readers' if self.args.dataset == 'C' else 'Q readers with 100/500/1000 followees'}
        for direction, column, other, owners in [
            ('following', 1, 2, range(1, users + 1) if self.args.dataset == 'C' else [1, 3, 4, 5]),
            ('followers', 2, 1, range(1, users + 1) if self.args.dataset == 'C' else [2])]:
            for owner in owners:
                expected = sorted((r for r in relationships if r[column] == owner), key=lambda r: (r[3], r[0]), reverse=True)
                self.verify_pages(f'/api/v1/users/{owner}/{direction}', owner,
                                  [(r[0], r[other], r[3]) for r in expected],
                                  lambda r: (r['id'], r['user_id'], r['created_on']), 50)
        self.report['checks'].append('Feed and bidirectional relationships: full selected pagination, identity/order/body/cursors')
        print('PASS', self.args.dataset, self.report['feed_correctness'], flush=True)

    def plans(self, relationships):
        records = []
        for direction, column, other, owner in [('following', 'user_id', 'follow_id', 1), ('followers', 'follow_id', 'user_id', 2)]:
            index = 1 if direction == 'following' else 2
            expected = sorted((r for r in relationships if r[index] == owner), key=lambda r: (r[3], r[0]), reverse=True)
            for offset in (0, 1000, 10000, 100000):
                if offset >= len(expected):
                    continue
                select = f'SELECT f.id,u.id,u.username,f.created_on FROM following f JOIN users u ON u.id=f.{other} WHERE f.{column}={owner}'
                order = ' ORDER BY f.created_on DESC,f.id DESC LIMIT 50'
                boundary = ''
                cursor = ''
                if offset:
                    previous = expected[offset - 1]
                    boundary = f' AND (f.created_on<{previous[3]} OR (f.created_on={previous[3]} AND f.id<{previous[0]}))'
                    cursor = base64.urlsafe_b64encode(json.dumps({'owner': owner, 'direction': direction,
                        'created_on': previous[3], 'id': previous[0]}).encode()).decode().rstrip('=')
                queries = {'offset': select + order + f' OFFSET {offset}', 'cursor': select + boundary + order}
                for kind, query in queries.items():
                    result = self.sql(query)
                    ids = [int(row.split('\t')[0]) for row in result.splitlines()]
                    assert ids == [r[0] for r in expected[offset:offset + 50]]
                    plan = self.sql('EXPLAIN ANALYZE ' + query)
                    records.append({'direction': direction, 'offset': offset, 'kind': kind,
                                    'boundary_source': 'previous row in independently verified full traversal',
                                    'query': query, 'plan': plan})
                self.phase = f'relationship_probe/{direction}/{offset}'
                for _ in range(10):
                    self.request(f'/api/v1/users/{owner}/{direction}?' + urllib.parse.urlencode({'limit': 50, 'cursor': cursor}), owner)
        for reader in (3, 4, 5):
            query = f'''SELECT p.id,u.id,u.username,p.content,p.created_on FROM posts p JOIN users u ON u.id=p.author_id
                WHERE p.deleted_on IS NULL AND (p.author_id={reader} OR EXISTS
                (SELECT 1 FROM following f WHERE f.user_id={reader} AND f.follow_id=p.author_id))
                ORDER BY p.created_on DESC,p.id DESC LIMIT 51'''
            records.append({'kind': 'feed', 'reader': reader, 'query': query, 'plan': self.sql('EXPLAIN ANALYZE ' + query)})
            self.phase = f'feed_probe/{reader}'
            for _ in range(30):
                self.request('/api/v1/feed?limit=50', reader)
        (self.output / 'plans.json').write_text(json.dumps(records, indent=2) + '\n')

    def storage(self):
        self.sql('ANALYZE TABLE following,posts,post_requests,outbox_events,consumption_jobs,dummy_effects,event_replays;')
        return {'tables_tsv': self.sql('SELECT table_name,table_rows,data_length,index_length FROM information_schema.tables WHERE table_schema=DATABASE() ORDER BY table_name;'),
                'indexes_tsv': self.sql("SELECT table_name,index_name,stat_value*@@innodb_page_size FROM mysql.innodb_index_stats WHERE database_name=DATABASE() AND stat_name='size' ORDER BY table_name,index_name;")}

    def write_cost(self):
        # Copies exist only in this isolated DB. Both retain uniqueness and FK-compatible indexes.
        self.sql('CREATE TABLE following_full LIKE following; CREATE TABLE following_min LIKE following; '
                 'ALTER TABLE following_min ADD KEY idx_target(follow_id), DROP KEY idx_follow_created, DROP KEY idx_user_created;')
        timings = []
        row_count = min(10000, self.report['counts']['relationships'])
        for round_number in range(3):
            order = ['following_full', 'following_min'] if round_number % 2 == 0 else ['following_min', 'following_full']
            for table in order:
                self.sql(f'TRUNCATE TABLE {table};')
                start = time.monotonic()
                self.sql(f'INSERT INTO {table} SELECT * FROM following ORDER BY id LIMIT 10000;')
                timings.append({'round': round_number + 1, 'table': table, 'rows': row_count,
                                'client_elapsed_ms': (time.monotonic() - start) * 1000})
        self.report['index_write_probe'] = timings
        self.sql('DROP TABLE following_full,following_min;')

    def statement_counts(self):
        rows = self.sql("SELECT DIGEST_TEXT,COUNT_STAR FROM performance_schema.events_statements_summary_by_digest "
                        f"WHERE SCHEMA_NAME='{self.database}' AND DIGEST_TEXT IS NOT NULL;", False)
        return {row.rsplit('\t', 1)[0]: int(row.rsplit('\t', 1)[1]) for row in rows.splitlines()}

    def publication_cost(self):
        self.token(3)
        before = self.statement_counts()
        post = self.request('/api/v1/posts', 3, {'content': 'publication_cost_probe'})
        after = self.statement_counts()
        self.report['publication_sql_counts'] = {query: count - before.get(query, 0)
                                                 for query, count in after.items() if count > before.get(query, 0)}
        self.report['publication_payload_bytes'] = self.sql(f"SELECT OCTET_LENGTH(p.content),"
            "OCTET_LENGTH(r.content)+OCTET_LENGTH(r.request_key)+JSON_STORAGE_SIZE(r.response_json),"
            "JSON_STORAGE_SIZE(e.payload) FROM posts p JOIN post_requests r ON r.post_id=p.id "
            f"JOIN outbox_events e ON e.payload->>'$.post_id'=p.id WHERE p.id={post['id']};")

    def progress(self):
        row = self.sql("SELECT (SELECT COUNT(*) FROM outbox_events WHERE published_on IS NULL AND failed_on IS NULL),"
            "(SELECT COALESCE(MAX(UNIX_TIMESTAMP()-created_on),0) FROM outbox_events WHERE published_on IS NULL AND failed_on IS NULL),"
            "(SELECT COUNT(*) FROM consumption_jobs WHERE status='completed'),"
            "(SELECT COUNT(*) FROM dummy_effects),"
            "(SELECT COALESCE(MAX(UNIX_TIMESTAMP()-created_on),0) FROM consumption_jobs WHERE status='pending');")
        pending, age, completed, effects, consumer_age = map(int, row.split('\t'))
        return {'elapsed_s': time.monotonic() - self.started, 'outbox_pending': pending,
                'outbox_oldest_age_s': age, 'consumer_oldest_age_s': consumer_age,
                'completed': completed, 'dummy_effects': effects}

    def pipeline(self):
        expected_effects = (int(self.sql('SELECT COUNT(*) FROM outbox_events;')) + 20) * 2
        self.stop_processes()
        # The C run exercises publisher/dummy/replay through their normal entry points.
        prefix = 'notex_dummy_' + self.database
        self.report['queue_prefix'] = prefix
        self.launch(mq=True, dummy=True)
        self.launch(mq=True)
        samples = []
        progress = [self.progress()]
        for sequence in range(20):
            start = time.monotonic()
            post = self.request('/api/v1/posts', 3, {'content': f'visibility_{sequence}'})
            accepted = time.monotonic()
            deadline = start + 15
            while True:
                page = self.request('/api/v1/feed?limit=50', 2)
                if any(p['id'] == post['id'] for p in page['items']):
                    break
                if time.monotonic() > deadline:
                    raise RuntimeError('Feed visibility deadline exceeded')
                time.sleep(.02)
            visible = time.monotonic()
            samples.append({'sequence': sequence, 'post_id': post['id'], 'reader': 2,
                            'request_to_visible_ms': (visible - start) * 1000,
                            'response_to_visible_ms': (visible - accepted) * 1000})
            progress.append(self.progress())
        deadline = time.monotonic() + 30
        while True:
            current = self.progress()
            progress.append(current)
            if current['outbox_pending'] == 0 and current['completed'] == expected_effects and current['dummy_effects'] == expected_effects:
                break
            if time.monotonic() > deadline:
                raise RuntimeError('publisher/dummy did not catch up')
            time.sleep(.2)
        # Fail only this isolated database's dummy effects, then exercise explicit replay.
        self.sql("CREATE TRIGGER fail_dummy BEFORE INSERT ON dummy_effects FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='baseline replay probe';")
        post = self.request('/api/v1/posts', 3, {'content': 'replay_probe'})
        deadline = time.monotonic() + 50
        while True:
            failed = self.sql("SELECT id FROM consumption_jobs WHERE status='failed' ORDER BY id;")
            if len(failed.splitlines()) == 2:
                break
            if time.monotonic() > deadline:
                raise RuntimeError('retry exhaustion deadline exceeded')
            time.sleep(.5)
        self.sql('DROP TRIGGER fail_dummy;')
        env = dict(os.environ, MYSQL_DSN=f'{self.database}:{self.password}@tcp({self.host}:{self.port})/{self.database}',
                   RABBITMQ_URL='', GOMAXPROCS='2', GOMEMLIMIT='512MiB')
        (self.output / 'failed-jobs.json').write_text(self.command(
            [str(Path(self.args.events_binary).resolve()), 'list', '--scope', 'consumer', '--status', 'failed'], env=env))
        for job in failed.splitlines():
            self.command([str(Path(self.args.events_binary).resolve()), 'replay', '--scope', 'consumer', '--id', job,
                          '--reason', 'Sprint 3 baseline dependency restored'], env=env)
        deadline = time.monotonic() + 15
        while int(self.sql("SELECT COUNT(*) FROM consumption_jobs WHERE status='completed';")) != expected_effects + 2:
            if time.monotonic() > deadline:
                raise RuntimeError('explicit replay deadline exceeded')
            time.sleep(.2)
        assert self.sql('SELECT COUNT(*) FROM event_replays;') == '2'
        assert int(self.sql('SELECT COUNT(*) FROM dummy_effects;')) == expected_effects + 2
        self.report['replay_results_tsv'] = self.sql('SELECT id,event_id,status,attempts,cycle_attempts FROM consumption_jobs '
            'WHERE id IN (' + ','.join(failed.splitlines()) + ') ORDER BY id;')
        assert self.sql("SELECT COUNT(*) FROM outbox_events e CROSS JOIN "
            f"(SELECT '{prefix}.feed' AS consumer UNION ALL SELECT '{prefix}.search') c "
            "LEFT JOIN dummy_effects d ON d.event_id=e.id AND d.consumer=c.consumer "
            "WHERE d.event_id IS NULL OR d.event_type<>e.event_type OR d.payload<>e.payload;") == '0'
        self.report['pipeline'] = {'visibility_samples': samples, 'progress': progress,
                                  'replayed_jobs': 2, 'published_posts': 21,
                                  'logical_reader_index_writes_per_post': 0,
                                  'scope': 'SQL Feed visibility; SQL Dummy progress',
                                  'outbox_attempts': self.sql('SELECT SUM(attempts) FROM outbox_events;')}
        self.report['checks'].append('real HTTP publication -> SQL Feed visible; MQ -> SQL Dummy; retry exhaustion -> CLI replay')

    def cleanup(self):
        errors = []
        try:
            self.stop_processes()
        except Exception as exc:
            errors.append(str(exc))
        if self.args.dataset == 'C' and 'queue_prefix' in self.report:
            for suffix in ('feed', 'search'):
                try:
                    self.command(['docker', 'compose', 'exec', '-T', 'rabbitmq', 'rabbitmqctl', 'delete_queue',
                                  self.report['queue_prefix'] + '.' + suffix])
                except Exception as exc:
                    errors.append(str(exc))
        if self.created:
            try:
                self.sql(f'DROP DATABASE `{self.database}`;', False)
            except Exception as exc:
                errors.append(str(exc))
        if self.account_created:
            try:
                self.sql(f"DROP USER '{self.database}'@'127.0.0.1';", False)
            except Exception as exc:
                errors.append(str(exc))
        self.report['cleanup'] = not errors
        if errors:
            raise RuntimeError('cleanup: ' + '; '.join(errors))

    def run(self):
        thread = threading.Thread(target=self.monitor, daemon=True)
        thread.start()
        try:
            users, relationships, posts = self.setup()
            self.launch()
            self.phase = 'correctness'
            self.verify(users, relationships, posts)
            self.plans(relationships)
            self.write_cost()
            self.report['storage_before_pipeline'] = self.storage()
            if self.args.dataset == 'C':
                self.phase = 'publication_cost'
                self.publication_cost()
                self.phase = 'pipeline'
                self.pipeline()
            self.report['storage_after_pipeline'] = self.storage()
            self.check_budget()
            self.report['status'] = 'passed'
        except BaseException as exc:
            self.report['status'] = 'failed'
            self.report['error'] = str(exc).replace(self.password, '<redacted>')
        finally:
            self.stop.set()
            thread.join(timeout=5)
            try:
                self.cleanup()
            except Exception as exc:
                self.report['status'] = 'failed'
                self.report['cleanup_error'] = str(exc).replace(self.password, '<redacted>')
            self.samples.close()
            self.logs.close()
            self.report['elapsed_s'] = time.monotonic() - self.started
            grouped = collections.defaultdict(list)
            for line in (self.output / 'http.jsonl').read_text().splitlines():
                row = json.loads(line)
                grouped[row['path'].split('?')[0]].append(row['latency_ms'])
            self.report['http_summary'] = {path: {'count': len(values), **percentiles(values)} for path, values in grouped.items()}
            (self.output / 'report.json').write_text(json.dumps(self.report, ensure_ascii=False, indent=2) + '\n')
        print(json.dumps({k: self.report.get(k) for k in ['dataset', 'status', 'error', 'cleanup', 'elapsed_s']}, ensure_ascii=False), flush=True)
        return 0 if self.report['status'] == 'passed' else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dataset', choices=['C', 'Q'], required=True)
    parser.add_argument('--binary', required=True)
    parser.add_argument('--events-binary', required=True)
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    def terminate(signum, frame):
        raise KeyboardInterrupt('termination requested')
    signal.signal(signal.SIGTERM, terminate)
    raise SystemExit(Baseline(args).run())


if __name__ == '__main__':
    main()
