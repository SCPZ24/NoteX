"""Run an isolated hot-like staircase, steady window, or ordered DML replay."""
import argparse
import base64
from contextlib import ExitStack
import hashlib
import json
from pathlib import Path
import resource
import secrets
import subprocess
import sys
import tempfile
import time
from urllib.error import HTTPError
from urllib.request import Request

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import run as ladder


def queue_depth(namespace):
    req = Request(f'http://127.0.0.1:15673/api/queues/{namespace}/{namespace}.likes')
    req.add_header('Authorization', 'Basic ' + base64.b64encode(b'notex:notex-local').decode())
    with ladder.HTTP.open(req, timeout=3) as response:
        state = json.load(response)
    return [int(ladder.redis('XLEN', namespace + ':like:stream')),
            state.get('messages_ready'), state.get('messages_unacknowledged')]


def overlay(work):
    replacements = {}
    for pkg in ('post', 'outbox'):
        rel = Path('src/internal')/pkg/'likes.go'
        copy = work/rel
        copy.parent.mkdir(parents=True)
        copy.write_bytes((ladder.ROOT/rel).read_bytes())
        replacements[str(ladder.ROOT/rel)] = str(copy)
    patch = Path(__file__).resolve().with_name('counts.diff')
    subprocess.run(['patch', '-p0', '-i', str(patch)], cwd=work, check=True)
    descriptor = work/'overlay.json'
    ladder.write_json(descriptor, {'Replace': replacements})
    return descriptor


def measure(work, output, namespace, check, stack):
    def window(rate, seconds):
        check()
        with (output/f'{rate}.json').open('w') as log:
            proc = subprocess.Popen([work/'load', work/'fixture.json', str(rate), str(seconds), args.phase], stdout=log)
            stack.callback(ladder.terminate, proc)
            next_sample = 0
            while proc.poll() is None:
                check()
                if args.mode == 'adaptive' and time.monotonic() >= next_sample:
                    with (output/'queue.jsonl').open('a') as q:
                        print(json.dumps([time.time(), rate, queue_depth(namespace)]), file=q)
                    next_sample = time.monotonic() + 5
                time.sleep(.15)
            if proc.returncode:
                raise RuntimeError('load generator failed')
        check()
        row = json.loads((output/f'{rate}.json').read_text())
        ok = (row['write_success'] > 0 and row['write_p95_ms'] < 100
              and (row['write_failed'] + row['write_unsent']) < .001 * row['write_planned'])
        print(args.mode, args.phase, row, 'PASS' if ok else 'FAIL', flush=True)
        return ok

    if args.phase.startswith('warm-'):
        import warm
        report['initial_likes'] = warm.prepare(work, namespace, args.mode == 'adaptive', check, ladder)
        if args.phase == 'warm-confirm':
            window(args.rate, 60)
        else:
            rate = 1024
            while window(rate, 30):
                rate += 512
    elif args.phase == 'ladder':
        rate = 512
        if window(rate, 30):
            while window(rate * 2, 30):
                rate *= 2
        else:
            while rate > 1:
                rate //= 2
                if window(rate, 30):
                    break
    else:
        window(args.rate, 300 if args.phase == 'steady' else 30)
    if args.mode == 'adaptive':
        start = time.monotonic()
        empty = 0
        while empty < 3:
            check()
            depth = queue_depth(namespace)
            empty = empty + 1 if depth == [0, 0, 0] else 0
            if time.monotonic()-start > 180:
                raise RuntimeError(f'queues did not drain: {depth}')
            time.sleep(2)
        report['empty_confirmed_s'] = time.monotonic()-start
    if args.phase.startswith('warm-'):
        return
    state = ladder.sql('SELECT post_id,user_id FROM post_likes ORDER BY post_id,user_id', namespace)
    report['rows'] = len(state.splitlines())
    report['state_hash'] = hashlib.sha256(state.encode()).hexdigest()
    if args.phase == 'replay':
        row = json.loads((output/f'{args.rate}.json').read_text())
        for role in ('core', 'like-worker'):
            log = work/(role+'.log')
            if log.exists():
                records = [line.split() for line in log.read_text().splitlines() if line.startswith('REPLAY_DML ')]
                report.setdefault('committed_post_likes', {})[role] = {
                    'calls': sum(int(line[1]) for line in records), 'rows': sum(int(line[2]) for line in records)}
        report['matches_reference'] = row['expected_state_hash'] == report['state_hash']


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('mode', choices=['sql', 'adaptive'])
    parser.add_argument('phase', choices=['ladder', 'probe', 'steady', 'replay', 'warm-ladder', 'warm-confirm'])
    parser.add_argument('rate', type=int)
    parser.add_argument('output', type=Path)
    args = parser.parse_args()
    if args.phase != 'ladder' and args.rate <= 0:
        parser.error('probe, steady and replay require a positive scenario QPS')
    args.fixture = ladder.ROOT/'notes/fanout/runs/sprint5/fixture.json'
    args.core_conns, args.core_read_conns = 32, 4
    args.measure = measure
    args.overlay = overlay
    args.output.mkdir(parents=True, exist_ok=False)
    report = dict(namespace='notex_l_'+secrets.token_hex(8), mode=args.mode, phase=args.phase)
    try:
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        if soft < 4096:
            resource.setrlimit(resource.RLIMIT_NOFILE, (min(4096, hard) if hard > 0 else 4096, hard))
        with tempfile.TemporaryDirectory() as tmp, ExitStack() as stack:
            ladder.run(args, args.output, Path(tmp), report, stack, likes=args)
    except BaseException as exc:
        report['error'] = repr(exc)
        raise
    finally:
        ladder.write_json(args.output/'run.json', report)
