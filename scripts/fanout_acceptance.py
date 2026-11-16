#!/usr/bin/env python3
"""One isolated C/Q correctness and timed read round; reuse baseline fixtures."""
import argparse
import collections
import json
import os
from pathlib import Path
import signal
import sys
import threading
import time
sys.dont_write_bytecode = True
from fanout_baseline import Baseline, percentiles, reference_feed, digest


class Acceptance(Baseline):
    def cleanup(self):
        self.stop_processes()
        prefix = 'notex_dummy_' + self.database
        if self.args.mode != 'sql':
            for suffix in ('feed', 'search', 'feed.plan', 'feed.chunk'):
                self.command(['docker', 'compose', 'exec', '-T', 'rabbitmq', 'rabbitmqctl', 'delete_queue', prefix + '.' + suffix])
            keys = self.command(['docker', 'compose', 'exec', '-T', 'redis', 'redis-cli', '--scan', '--pattern', prefix + ':*']).splitlines()
            for start in range(0, len(keys), 100):
                self.command(['docker', 'compose', 'exec', '-T', 'redis', 'redis-cli', 'UNLINK'] + keys[start:start+100])
        super().cleanup()

    def verify(self, users, relationships, posts):
        # Relationship pagination is covered by the retained baseline and API
        # regression. These rounds compare the changed Feed entry point.
        readers = range(1, users + 1) if self.args.dataset == 'C' else [3, 4, 5]
        checked = 0
        with (self.output/'reference.jsonl').open('w') as output:
            for reader in readers:
                expected = reference_feed(reader, relationships, posts)
                output.write(json.dumps({'reader':reader,'post_ids':[p[0] for p in expected],'sha256':digest(expected)})+'\n')
                checked += self.verify_pages('/api/v1/feed',reader,
                    [(p[0],p[1],p[2],f'post_{p[0]}',f'user_{p[1]}') for p in expected],
                    lambda p:(p['id'],p['author']['id'],p['created_on'],p['content'],p['author']['username']),50)
        self.report['feed_correctness']={'readers':len(readers),'returned_posts':checked}
        self.report['checks'].append('full Feed pagination equals independent reference')
        print('PASS',self.args.dataset,self.report['feed_correctness'],flush=True)

    def statements(self):
        return self.sql("SELECT EVENT_NAME,COUNT_STAR,SUM_ROWS_EXAMINED,SUM_ROWS_SENT FROM performance_schema.events_statements_summary_by_account_by_event_name WHERE USER='" + self.database + "' AND COUNT_STAR>0;", False)

    def run(self):
        monitor = threading.Thread(target=self.monitor, daemon=True)
        monitor.start()
        self.report['command'] = ['python3'] + sys.argv
        self.report.update(kind='three_strategy_read_acceptance', mode=self.args.mode, round=self.args.round,
                           load_model='sequential closed-loop HTTP, one outstanding request', warmup_seconds=self.args.warmup, sample_seconds=self.args.seconds)
        (self.output/'acceptance.py').write_bytes(Path(__file__).read_bytes())
        try:
            users, relationships, posts = self.setup()
            self.launch()
            self.phase='correctness'
            self.verify(users, relationships, posts)
            readers = [1, 2, 3] if self.args.dataset == 'C' else [3, 4, 5]
            for reader in readers:
                self.token(reader)
            for phase, duration in [('warmup', self.args.warmup), ('sample', self.args.seconds)]:
                self.phase=phase
                before=time.monotonic()
                count=0
                if phase=='sample':
                    self.report['sql_before']=self.statements()
                while time.monotonic()-before < duration:
                    self.request('/api/v1/feed?limit=50',readers[count % len(readers)])
                    count+=1
                self.report[phase]={'requests':count,'elapsed_s':time.monotonic()-before}
            self.stop_processes()
            self.report['sql_after']=self.statements()
            self.report['sql_after_capture']='after graceful API shutdown'
            counts = []
            for snapshot in ('sql_before', 'sql_after'):
                counts.append({fields[0]: int(fields[1]) for line in self.report[snapshot].splitlines()
                               if (fields := line.split('\t'))})
            executions = counts[1].get('statement/com/Execute', 0) - counts[0].get('statement/com/Execute', 0)
            prepares = counts[1].get('statement/com/Prepare', 0) - counts[0].get('statement/com/Prepare', 0)
            self.report['sql_counter_consistent'] = abs(executions - prepares) <= 10 and executions >= self.report['sample']['requests'] * 2.9

            self.report['redis_memory']=self.command(['docker','compose','exec','-T','redis','redis-cli','INFO','memory'])
            self.report['status']='passed'
        except BaseException as exc:
            self.report.update(status='failed',error=str(exc).replace(self.password,'<redacted>'))
        finally:
            self.stop.set()
            monitor.join(timeout=5)
            try:
                self.cleanup()
            except Exception as exc:
                self.report.update(status='failed',cleanup_error=str(exc).replace(self.password,'<redacted>'))
            self.samples.close()
            self.logs.close()
            groups=collections.defaultdict(list)
            with (self.output/'http.jsonl').open() as samples:
                for line in samples:
                    row=json.loads(line)
                    if row['phase']=='sample':
                        groups[row['reader']].append(row['latency_ms'])
            self.report['latency_by_reader']={reader:{'count':len(values),**percentiles(values)} for reader,values in groups.items()}
            self.report['elapsed_s']=time.monotonic()-self.started
            (self.output/'report.json').write_text(json.dumps(self.report,ensure_ascii=False,indent=2)+'\n')
        print(json.dumps({k:self.report.get(k) for k in ('dataset','mode','round','status','error','elapsed_s')},ensure_ascii=False),flush=True)
        return self.report['status']=='passed'


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dataset',choices=['C','Q'],required=True)
    parser.add_argument('--mode',choices=['sql','push','hybrid'],required=True)
    parser.add_argument('--round',type=int,required=True)
    parser.add_argument('--binary',required=True)
    parser.add_argument('--events-binary',required=True)
    parser.add_argument('--output',required=True)
    parser.add_argument('--warmup',type=int,default=60)
    parser.add_argument('--seconds',type=int,default=300)
    args=parser.parse_args()
    def terminate(signum,frame):
        raise KeyboardInterrupt('termination requested')
    signal.signal(signal.SIGTERM,terminate)
    raise SystemExit(0 if Acceptance(args).run() else 1)
