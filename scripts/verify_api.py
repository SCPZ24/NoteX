#!/usr/bin/env python3
"""Functional API checks against an isolated database on an existing MySQL server.

Build first: GOMAXPROCS=2 go build -p 2 -o /tmp/notex-api-verify ./src
Run: python3 scripts/verify_api.py --binary /tmp/notex-api-verify
Admin connection: TEST_MYSQL_HOST/PORT/USER/PASSWORD (defaults 127.0.0.1/3306/root/empty).
Creates and removes only its own random database and database account.
"""

import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import os
from pathlib import Path
import secrets
import signal
import socket
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True)
    parser.add_argument("--report", default="/tmp/notex-api-verification.json")
    args = parser.parse_args()
    binary = str(Path(args.binary).resolve())
    host = os.getenv("TEST_MYSQL_HOST", "127.0.0.1")
    port = os.getenv("TEST_MYSQL_PORT", "3306")
    admin = os.getenv("TEST_MYSQL_USER", "root")
    admin_env = dict(os.environ, MYSQL_PWD=os.getenv("TEST_MYSQL_PASSWORD", ""))
    database = "notex_verify_" + secrets.token_hex(6)
    db_password = secrets.token_hex(16)
    database_created = account_created = False
    process = None
    log = tempfile.TemporaryFile(mode="w+")
    report = {"kind": "functional", "cases": [], "database": database,
              "concurrency": 8, "cleanup": False, "peak_new_process_rss_kib": 0,
              "peak_mysql_rss_kib": 0}
    stop_monitor = threading.Event()
    resource_error = []
    connection = ["mysql", "--protocol=tcp", f"--host={host}", f"--port={port}",
                  f"--user={admin}", "--connect-timeout=5", "--batch", "--skip-column-names"]

    def sql(statement, selected=True):
        command = connection + ([database] if selected else [])
        result = subprocess.run(command, input=statement, text=True, capture_output=True,
                                env=admin_env, timeout=15)
        if result.returncode:
            raise RuntimeError("MySQL command failed: " + result.stderr.strip())
        return result.stdout.strip()

    def check(condition, name):
        if not condition:
            raise AssertionError(name)

    def passed(name):
        if resource_error:
            raise RuntimeError(resource_error[0])
        report["cases"].append(name)
        print("PASS", name, flush=True)

    def monitor():
        # RSS includes shared pages and is sampled, making this a conservative
        # observational budget rather than an OS-enforced memory limit.
        while not stop_monitor.is_set():
            try:
                output = subprocess.check_output(
                    ["ps", "-axo", "pid=,ppid=,rss=,comm="], text=True)
                rows = [line.strip().split(None, 3) for line in output.splitlines()]
                children = {os.getpid()}
                for _ in range(5):
                    children.update(int(r[0]) for r in rows if len(r) == 4 and int(r[1]) in children)
                own = sum(int(r[2]) for r in rows if len(r) == 4 and int(r[0]) in children)
                mysql = sum(int(r[2]) for r in rows if len(r) == 4 and Path(r[3]).name == "mysqld")
                report["peak_new_process_rss_kib"] = max(report["peak_new_process_rss_kib"], own)
                report["peak_mysql_rss_kib"] = max(report["peak_mysql_rss_kib"], mysql)
                if own > 2 * 1024 * 1024:
                    resource_error.append("new process RSS exceeded the agreed 2 GiB budget")
                    if process and process.poll() is None:
                        process.terminate()
                    return
            except Exception as exc:
                resource_error.append("resource monitoring failed: " + str(exc))
                return
            stop_monitor.wait(0.2)

    def stop_server():
        if process and process.poll() is None:
            process.send_signal(signal.SIGTERM)
            try:
                process.wait(timeout=12)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
                raise RuntimeError("server failed to shut down within 12 seconds")

    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        http_port = probe.getsockname()[1]
    base = f"http://127.0.0.1:{http_port}"

    def request(method, path, token=None, data=None, status=200, code=None):
        headers = {"Content-Type": "application/json"}
        if token:
            headers["Authorization"] = "Bearer " + token
        body = None if data is None else json.dumps(data).encode()
        req = urllib.request.Request(base + path, data=body, headers=headers, method=method)
        try:
            response = urllib.request.urlopen(req, timeout=8)
        except urllib.error.HTTPError as exc:
            response = exc
        with response:
            actual = response.status
            result = json.loads(response.read())
        check(actual == status, f"{method} {path.split('?')[0]}: HTTP {actual}, expected {status}")
        if code:
            check(result == {"error": code}, f"unexpected error code for {path.split('?')[0]}")
        return result

    def start_server():
        nonlocal process
        env = dict(os.environ, GOMAXPROCS="2", GOMEMLIMIT="512MiB", GIN_MODE="release",
                   HTTP_ADDR=f"127.0.0.1:{http_port}",
                   MYSQL_DSN=f"{database}:{db_password}@tcp({host}:{port})/{database}")
        process = subprocess.Popen([binary], env=env, stdout=log, stderr=log)
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if process.poll() is not None:
                raise RuntimeError("API exited during startup; inspect database connectivity")
            try:
                request("GET", "/readyz")
                return
            except urllib.error.URLError:
                time.sleep(0.1)
        raise RuntimeError("API readiness deadline exceeded")

    thread = threading.Thread(target=monitor, daemon=True)
    thread.start()
    error = None
    try:
        report["mysql"] = sql("SELECT VERSION(), @@transaction_isolation;", False)
        sql(f"CREATE DATABASE `{database}`;", False)
        database_created = True
        sql((ROOT / "migrations/0001_accounts_follow.sql").read_text())
        sql(f"CREATE USER '{database}'@'127.0.0.1' IDENTIFIED BY '{db_password}';", False)
        account_created = True
        sql(f"GRANT SELECT, INSERT, UPDATE, DELETE ON `{database}`.* TO '{database}'@'127.0.0.1';", False)
        start_server()
        check(request("GET", "/healthz") == {"status": "ok"}, "liveness")
        check(request("GET", "/readyz") == {"status": "ready"}, "readiness")
        passed("health and database readiness")
        api = "/api/v1"
        password = "verification-password"
        users = []
        for name in ["alice", "bob", "carol", "david", "emily", "frank", "grace"]:
            users.append(request("POST", api + "/register", data={"username": name, "password": password}, status=201))
        request("POST", api + "/register", data={"username": "alice", "password": password}, status=409, code="username_taken")
        for name, pwd in [("ab", password), ("中文用户名", password), ("a b", password), ("valid", "short"), ("valid", "x" * 73)]:
            request("POST", api + "/register", data={"username": name, "password": pwd}, status=400, code="invalid_input")
        hashes = sql("SELECT password_hash FROM users;").splitlines()
        check(all(h.startswith("$2") and len(h) == 60 and h != password for h in hashes), "password hashes")
        passed("registration, unique username, validation and bcrypt storage")
        request("POST", api + "/login", data={"username": "alice", "password": "wrong-password"}, status=401, code="unauthorized")
        request("POST", api + "/login", data={"username": "Alice", "password": password}, status=401, code="unauthorized")
        tokens = [request("POST", api + "/login", data={"username": u["username"], "password": password})["token"] for u in users]
        actor = users[0]["id"]
        token = tokens[0]
        check(request("GET", api + "/me", token) == users[0], "current identity")
        request("GET", api + "/me", status=401, code="unauthorized")
        request("GET", api + "/me", "z" * 64, status=401, code="unauthorized")
        request("GET", api + "/me", "0" * 64, status=401, code="unauthorized")
        check(request("GET", api + "/users?username=bob", token) == users[1], "exact user lookup")
        request("GET", api + "/users?username=Bob", token, status=404, code="user_not_found")
        token_hash = hashlib.sha256(token.encode()).hexdigest()
        check(sql(f"SELECT COUNT(*) FROM sessions WHERE token_hash=UNHEX('{token_hash}') AND user_id={actor};") == "1", "stored token digest")
        passed("login, token digest, authentication and exact username lookup")

        def relation(target):
            return api + f"/users/{target}/follow"

        target = users[1]["id"]
        check(request("GET", relation(target), token) == {"is_following": False}, "initial state")
        request("PUT", relation(actor), token, status=400, code="invalid_input")
        request("PUT", relation(999999999), token, status=404, code="user_not_found")
        request("PUT", relation(target), status=401, code="unauthorized")
        request("PUT", relation(target), token, data={"user_id": users[2]["id"]})
        snapshot = sql(f"SELECT id, created_on FROM following WHERE user_id={actor} AND follow_id={target};")
        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(lambda _: request("PUT", relation(target), token), range(16)))
        check(all(r == {"is_following": True} for r in results), "duplicate follow responses")
        check(sql(f"SELECT id, created_on FROM following WHERE user_id={actor} AND follow_id={target};") == snapshot, "duplicate preserved ID and time")
        check(sql("SELECT COUNT(*) FROM following;") == "1", "only authenticated actor writes")
        with ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(lambda _: request("DELETE", relation(target), token), range(16)))
        check(request("GET", relation(target), token) == {"is_following": False}, "duplicate unfollow")
        request("PUT", relation(target), token)
        check(sql(f"SELECT id FROM following WHERE user_id={actor} AND follow_id={target};") != snapshot.split()[0], "refollow new ID")
        passed("self/missing user checks, actor identity, concurrent idempotency and refollow")

        # Asymmetric graph; expected members originate in this fixture.
        for u in users[2:6]:
            request("PUT", relation(u["id"]), token)
        for i in [2, 4, 6]:
            request("PUT", relation(actor), tokens[i])
        sql("UPDATE following SET created_on=1003;")
        raw_edges = [list(map(int, row.split())) for row in sql("SELECT id, user_id, follow_id FROM following;").splitlines()]
        edge_ids = {(a, b): edge for edge, a, b in raw_edges}
        expected_out = [u["id"] for u in reversed(users[1:6])]
        expected_in = [users[i]["id"] for i in [6, 4, 2]]

        def page(direction, cursor="", limit=2, owner=actor):
            query = urllib.parse.urlencode({"limit": limit, "cursor": cursor})
            return request("GET", api + f"/users/{owner}/{direction}?" + query, token)

        def collect(direction):
            items, cursor, seen = [], "", set()
            for _ in range(20):
                result = page(direction, cursor)
                check(len(result["items"]) <= 2, "page limit")
                items.extend(result["items"])
                if not result["has_more"]:
                    check(result["next_cursor"] == "", "terminal cursor")
                    return items
                cursor = result["next_cursor"]
                check(cursor and cursor not in seen, "cursor progress")
                seen.add(cursor)
            raise AssertionError("pagination did not terminate")

        for direction, expected in [("following", expected_out), ("followers", expected_in)]:
            items = collect(direction)
            check([i["user_id"] for i in items] == expected, "exact directional pagination")
            pairs = [(actor, i["user_id"]) if direction == "following" else (i["user_id"], actor) for i in items]
            check([i["id"] for i in items] == [edge_ids[p] for p in pairs], "relationship IDs")
            check(all(i["created_on"] == 1003 for i in items), "relationship timestamps")
        check(page("following", owner=users[1]["id"])["items"] == [], "empty JSON array")
        check(len(page("following", limit=100)["items"]) == 5, "maximum page size")
        passed("asymmetric directions, tied timestamps, exact page boundary and empty list")
        first = page("following")
        cursor = first["next_cursor"]
        for path in [f"/users/{actor}/followers?cursor={cursor}",
                     f"/users/{target}/following?cursor={cursor}",
                     f"/users/{actor}/following?cursor=invalid!",
                     f"/users/{actor}/following?cursor=" + "a" * 513]:
            request("GET", api + path, token, status=400, code="invalid_cursor")
        for value in ["0", "101", "-1", "abc"]:
            request("GET", api + f"/users/{actor}/following?limit={value}", token, status=400, code="invalid_input")
        request("GET", api + "/users/999999999/following", token, status=404, code="user_not_found")
        request("GET", api + "/users/0/following", token, status=400, code="invalid_input")
        passed("cursor ownership, malformed cursors, page limits and missing users")
        request("DELETE", relation(first["items"][-1]["user_id"]), token)
        request("PUT", relation(users[6]["id"]), token)
        second = page("following", cursor)
        check([i["user_id"] for i in second["items"]] == expected_out[2:4], "deleted boundary continuation")
        check(page("following")["items"][0]["user_id"] == users[6]["id"], "refresh finds new relation")
        passed("deleted cursor boundary and insertion between pages")
        with ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(lambda i: request("PUT" if i % 2 else "DELETE", relation(target), token), range(16)))
        state = int(sql(f"SELECT COUNT(*) FROM following WHERE user_id={actor} AND follow_id={target};"))
        check(state in (0, 1), "mixed operations unique state")
        check(request("GET", relation(target), token)["is_following"] == bool(state), "state matches database")
        check((target in [i["user_id"] for i in collect("following")]) == bool(state), "forward matches state")
        reverse = page("followers", limit=100, owner=target)
        check((actor in [i["user_id"] for i in reverse["items"]]) == bool(state), "reverse matches state")
        passed("mixed concurrent follow/unfollow converges in both directions")
        stop_server()
        check(process.returncode == 0, "graceful shutdown")
        start_server()
        check(request("GET", api + "/me", token)["id"] == actor, "session survives restart")
        passed("graceful shutdown and session persistence across restart")
        second_token = request("POST", api + "/login", data={"username": "alice", "password": password})["token"]
        request("POST", api + "/logout", token)
        request("GET", api + "/me", token, status=401, code="unauthorized")
        request("GET", api + "/me", second_token)
        sql(f"UPDATE sessions SET expires_on=1 WHERE user_id={actor};")
        request("GET", api + "/me", second_token, status=401, code="unauthorized")
        passed("logout revokes only its session and expired sessions are rejected")
        # Test dependency errors without stopping the shared MySQL instance.
        sql("RENAME TABLE following TO following_unavailable;")
        try:
            request("GET", api + f"/users/{target}/following", tokens[1], status=500, code="internal_error")
        finally:
            sql("RENAME TABLE following_unavailable TO following;")
        passed("database query errors remain errors rather than empty lists")
        report["status"] = "passed"
    except Exception as exc:
        error = exc
        report["status"] = "failed"
        report["error"] = str(exc).replace(db_password, "<redacted>")
    finally:
        try:
            stop_server()
            if database_created:
                report["data_bytes"] = int(sql("SELECT COALESCE(SUM(data_length+index_length),0) FROM information_schema.tables WHERE table_schema=DATABASE();") or 0)
                sql(f"DROP DATABASE `{database}`;", False)
            if account_created:
                sql(f"DROP USER '{database}'@'127.0.0.1';", False)
            report["cleanup"] = True
        except Exception as exc:
            error = error or exc
            report["cleanup_error"] = str(exc).replace(db_password, "<redacted>")
            report["status"] = "failed"
        stop_monitor.set()
        thread.join(timeout=2)
        if resource_error:
            report["resource_error"] = resource_error
            report["status"] = "failed"
            error = error or RuntimeError(resource_error[0])
        log.seek(0)
        report["server_errors"] = [line.strip() for line in log if "API operation failed" in line]
        log.close()
        Path(args.report).write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
        print("Report:", args.report)
    if error:
        raise SystemExit(report.get("error", str(error)))


if __name__ == "__main__":
    main()
