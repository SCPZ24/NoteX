#!/usr/bin/env python3
"""Report Performance Schema statement deltas for one workload schema."""
import argparse
import json
import os
import subprocess
import time


def query(sql):
    command = [
        "mysql", "--protocol=tcp", "--connect-timeout=5", "--batch", "--raw",
        "--skip-column-names", "--host=" + os.getenv("TEST_MYSQL_HOST", "127.0.0.1"),
        "--port=" + os.getenv("TEST_MYSQL_PORT", "3306"),
        "--user=" + os.getenv("TEST_MYSQL_USER", "root"),
        "--database=performance_schema",
    ]
    env = os.environ.copy()
    if "TEST_MYSQL_PASSWORD" in env:
        env["MYSQL_PWD"] = env["TEST_MYSQL_PASSWORD"]
    result = subprocess.run(command, input=sql, env=env, text=True,
                            stdout=subprocess.PIPE, check=True)
    return result.stdout.splitlines()


def snapshot(schema):
    # Hex encoding keeps the name a SQL literal, including for unusual schema names.
    schema_hex = schema.encode().hex()
    rows = query(f"""
        SELECT JSON_ARRAY(DIGEST, DIGEST_TEXT, COUNT_STAR, SUM_TIMER_WAIT,
                          SUM_ROWS_EXAMINED, SUM_ROWS_SENT)
        FROM events_statements_summary_by_digest
        WHERE SCHEMA_NAME = CONVERT(X'{schema_hex}' USING utf8mb4) AND DIGEST IS NOT NULL
    """)
    records = [json.loads(row) for row in rows]
    return {digest: (sql, counters) for digest, sql, *counters in records}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("schema", help="default database of the workload connections")
    parser.add_argument("--seconds", type=int, default=30)
    args = parser.parse_args()
    if args.seconds <= 0 or not args.schema or args.schema == "performance_schema":
        parser.error("supply a workload schema and a positive interval")

    enabled, = query("""SELECT COUNT(*) FROM setup_consumers WHERE ENABLED='YES'
        AND NAME IN ('global_instrumentation','thread_instrumentation','statements_digest')""")
    if enabled != "3":
        raise SystemExit("Performance Schema statement digest consumers must be enabled")
    start = snapshot(args.schema)
    started = time.monotonic()
    time.sleep(args.seconds)
    end = snapshot(args.schema)
    if start.keys() - end.keys():
        raise SystemExit("digest entries disappeared during sampling; rerun the window")

    rows = []
    for digest, (sql, counters) in end.items():
        previous = start.get(digest, (None, [0] * len(counters)))[1]
        count, picoseconds, examined, sent = delta = [v - p for v, p in zip(counters, previous)]
        if min(delta) < 0:
            raise SystemExit("digest counters decreased during sampling; rerun the window")
        if count:
            rows.append(dict(digest=digest, sql=sql, calls=count, total_ms=picoseconds / 1e9,
                             mean_ms=picoseconds / 1e9 / count, rows_examined=examined,
                             rows_sent=sent))
    print(json.dumps(dict(schema=args.schema, seconds=time.monotonic() - started,
                          statements=sorted(rows, key=lambda r: -r["total_ms"])), indent=2))


if __name__ == "__main__":
    main()
