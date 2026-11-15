#!/usr/bin/env python3
"""Functional API checks against an isolated database on an existing MySQL server.

Build first: GOMAXPROCS=2 go build -p 2 -o /tmp/notex-api-verify ./src
Run: python3 scripts/verify_api.py --binary /tmp/notex-api-verify
Admin connection: TEST_MYSQL_HOST/PORT/USER/PASSWORD (defaults 127.0.0.1/3306/root/empty).
Creates and removes only its own random database and database account.
"""

import argparse
import base64
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

    def request(method, path, token=None, data=None, status=200, code=None, request_key="auto"):
        headers = {"Content-Type": "application/json"}
        if method == "POST" and path == "/api/v1/posts" and request_key is not None:
            headers["Idempotency-Key"] = secrets.token_hex(16) if request_key == "auto" else request_key
        if token:
            headers["Authorization"] = "Bearer " + token
        body = None if data is None else json.dumps(data, ensure_ascii=False).encode()
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
                   RABBITMQ_URL="",
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
        sql((ROOT / "migrations/0002_posts.sql").read_text())
        sql((ROOT / "migrations/0003_post_requests.sql").read_text())
        sql((ROOT / "migrations/0004_outbox_events.sql").read_text())
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

        post_path = api + "/posts"
        author_posts = api + f"/users/{actor}/posts"
        for method, path in [("POST", post_path), ("GET", post_path + "/1"),
                             ("DELETE", post_path + "/1"), ("GET", author_posts)]:
            request(method, path, status=401, code="unauthorized")
        created = request("POST", post_path, token,
                          data={"content": " \n中文正文 🐈\t ", "author_id": users[1]["id"]}, status=201)
        check(created["content"] == "中文正文 🐈", "trimmed post content")
        check(created["author"] == {"id": actor, "username": "alice"}, "authenticated post author")
        check(created["id"] > 0 and created["created_on"] > 0, "post identity and timestamp")
        check(request("GET", post_path + f"/{created['id']}", tokens[1]) == created, "public post detail")
        for content in ["", " \t\n\u3000 ", "x" * 501, "🐈" * 501]:
            request("POST", post_path, token, data={"content": content}, status=400, code="invalid_input")
        request("POST", post_path, token, data={}, status=400, code="invalid_input")
        unicode_post = request("POST", post_path, tokens[1], data={"content": "🐈" * 500}, status=201)
        check(request("GET", post_path + f"/{unicode_post['id']}", token) == unicode_post,
              "500 Unicode code points round trip")
        request("DELETE", post_path + f"/{created['id']}", tokens[1], status=403, code="forbidden")
        check(request("GET", post_path + f"/{created['id']}", token) == created, "forbidden deletion preserves post")
        for method in ["GET", "DELETE"]:
            request(method, post_path + "/999999999", token, status=404, code="post_not_found")
            for value in ["0", "-1", "bad", "9223372036854775808"]:
                request(method, post_path + "/" + value, token, status=400, code="invalid_input")
        passed("post authentication, author identity, Unicode validation and public detail")

        deleted_path = post_path + f"/{created['id']}"
        check(request("DELETE", deleted_path, token) == {"status": "ok"}, "delete response")
        deletion = sql(f"SELECT deleted_on FROM posts WHERE id={created['id']};")
        check(deletion != "NULL", "durable deletion marker")
        # A fixed old value makes timestamp rewrites detectable even within one second.
        sql(f"UPDATE posts SET deleted_on=1000 WHERE id={created['id']};")
        with ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(lambda _: request("DELETE", deleted_path, token), range(16)))
        check(sql(f"SELECT deleted_on FROM posts WHERE id={created['id']};") == "1000",
              "repeated deletion preserves timestamp")
        check(sql(f"SELECT created_on FROM posts WHERE id={created['id']};") == str(created["created_on"]),
              "deletion preserves publication time")
        request("GET", deleted_path, token, status=404, code="post_not_found")
        request("DELETE", deleted_path, tokens[1], status=403, code="forbidden")
        check(request("GET", author_posts, token) == {"items": [], "next_cursor": "", "has_more": False},
              "deleted posts excluded from empty author list")
        passed("author-only soft deletion, concurrent repeated deletion and read filtering")

        published = []
        for i in range(23):
            item = request("POST", post_path, token, data={"content": f"post {i}"}, status=201)
            published.append(item)
            sql(f"UPDATE posts SET created_on={1000 + i // 3} WHERE id={item['id']};")
        expected_posts = [item["id"] for item in reversed(published)]
        default_page = request("GET", author_posts, tokens[1])
        check(len(default_page["items"]) == 20 and default_page["has_more"], "default post page size")
        check(request("GET", author_posts + "?limit=", token) == default_page, "empty limit default")
        all_posts = request("GET", author_posts + "?limit=100", token)
        check([item["id"] for item in all_posts["items"]] == expected_posts, "author membership and ordering")

        def post_page(cursor=""):
            query = urllib.parse.urlencode({"limit": 2, "cursor": cursor})
            return request("GET", author_posts + "?" + query, tokens[1])

        seen_posts, seen_cursors, after = [], set(), ""
        for _ in range(20):
            result = post_page(after)
            check(len(result["items"]) <= 2, "post page limit")
            seen_posts.extend(item["id"] for item in result["items"])
            if not result["has_more"]:
                check(result["next_cursor"] == "", "terminal post cursor")
                break
            after = result["next_cursor"]
            check(after and after not in seen_cursors, "post cursor progress")
            seen_cursors.add(after)
        check(seen_posts == expected_posts, "complete post pagination without duplicates")
        first_posts = post_page()
        post_cursor = first_posts["next_cursor"]
        request("GET", api + f"/users/{users[1]['id']}/posts?cursor={post_cursor}", token,
                status=400, code="invalid_cursor")
        request("GET", api + f"/users/{actor}/following?cursor={post_cursor}", token,
                status=400, code="invalid_cursor")
        for cursor_value in ["invalid!", "a" * 513]:
            request("GET", author_posts + "?cursor=" + cursor_value, token, status=400, code="invalid_cursor")
        for value in ["0", "101", "-1", "abc"]:
            request("GET", author_posts + "?limit=" + value, token, status=400, code="invalid_input")
        request("GET", api + "/users/999999999/posts", token, status=404, code="user_not_found")
        request("GET", api + "/users/0/posts", token, status=400, code="invalid_input")
        request("DELETE", post_path + f"/{first_posts['items'][-1]['id']}", token)
        new_post = request("POST", post_path, token, data={"content": "new after page one"}, status=201)
        check([item["id"] for item in post_page(post_cursor)["items"]] == expected_posts[2:4],
              "post continuation after boundary deletion and new publication")
        check(post_page()["items"][0]["id"] == new_post["id"], "refresh includes new post")
        passed("author post pagination, tied timestamps, cursor scope and changes between pages")

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
        for direction in ["following", "followers"]:
            check(request("GET", api + f"/users/{actor}/{direction}?limit=", token) == page(direction, limit=100),
                  "empty relationship limit uses default")
        passed("asymmetric directions, tied timestamps, exact page boundary and empty list")
        first = page("following")
        cursor = first["next_cursor"]
        request("GET", author_posts + "?cursor=" + cursor, token, status=400, code="invalid_cursor")
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

        # Independent Feed fixture. Expected results come from the posts created
        # here and explicit author sets, without reusing the API's SQL query.
        feed_users, feed_tokens = [], []
        for name in ["feed_reader", "feed_writer_one", "feed_writer_two", "feed_outsider"]:
            feed_users.append(request("POST", api + "/register",
                                      data={"username": name, "password": password}, status=201))
            feed_tokens.append(request("POST", api + "/login",
                                       data={"username": name, "password": password})["token"])
        reader = feed_users[0]["id"]
        feed_path = api + "/feed"
        request("GET", feed_path, status=401, code="unauthorized")
        empty_feed = {"items": [], "next_cursor": "", "has_more": False}
        check(request("GET", feed_path, feed_tokens[0]) == empty_feed, "empty Feed")

        def publish_feed(index, content, timestamp):
            item = request("POST", post_path, feed_tokens[index], data={"content": content}, status=201)
            sql(f"UPDATE posts SET created_on={timestamp} WHERE id={item['id']};")
            item["created_on"] = timestamp
            return item

        feed_posts = []
        for i in range(34):
            author_index = 1 if i < 25 else 2 if i < 30 else 0
            feed_posts.append(publish_feed(author_index, f"Feed fixture {i}", 1000 + (i * 7) % 11))
        for i in range(22):
            publish_feed(3, f"unfollowed newer post {i}", 3000 + i)
            removed = publish_feed(2, f"deleted newer post {i}", 2000 + i)
            request("DELETE", post_path + f"/{removed['id']}", feed_tokens[2])

        def expected_feed(authors):
            return sorted((p for p in feed_posts if p["author"]["id"] in authors),
                          key=lambda p: (p["created_on"], p["id"]), reverse=True)

        def feed_page(cursor="", limit=3, reader_token=feed_tokens[0]):
            query = urllib.parse.urlencode({"limit": limit, "cursor": cursor})
            return request("GET", feed_path + "?" + query, reader_token)

        def collect_feed():
            items, after, seen = [], "", set()
            for _ in range(100):
                result = feed_page(after)
                check(len(result["items"]) <= 3, "Feed page size")
                items.extend(result["items"])
                if not result["has_more"]:
                    check(result["next_cursor"] == "", "terminal Feed cursor")
                    return items
                after = result["next_cursor"]
                check(after and after not in seen, "Feed cursor progress")
                seen.add(after)
            raise AssertionError("Feed pagination did not terminate")

        check(collect_feed() == expected_feed({reader}), "self posts without following anyone")
        # A follower's posts must not leak into the reader's Feed.
        request("PUT", relation(reader), feed_tokens[3])
        check(collect_feed() == expected_feed({reader}), "incoming follows do not add authors")
        request("PUT", relation(feed_users[1]["id"]), feed_tokens[0])
        check(collect_feed() == expected_feed({reader, feed_users[1]["id"]}),
              "new follow includes author history beyond twenty posts")
        request("PUT", relation(feed_users[2]["id"]), feed_tokens[0])
        authors = {u["id"] for u in feed_users[:3]}
        expected = expected_feed(authors)
        check(collect_feed() == expected, "exact merged Feed with ties, deletion and author filtering")
        default_feed = request("GET", feed_path, feed_tokens[0])
        check(default_feed["items"] == expected[:20] and default_feed["has_more"], "default Feed limit")
        check(request("GET", feed_path + "?limit=", feed_tokens[0]) == default_feed, "empty Feed limit")
        check(request("GET", feed_path + f"?user_id={feed_users[3]['id']}", feed_tokens[0]) == default_feed,
              "Feed identity comes from the session")
        check(feed_page(limit=100) == {"items": expected, "next_cursor": "", "has_more": False},
              "maximum Feed limit and terminal state")
        exact = feed_page(limit=len(expected))
        check(exact["items"] == expected and not exact["has_more"], "exactly one full final page")
        passed("Feed identity, self posts, current follows, historical posts and global pagination")

        first_feed = feed_page()
        feed_cursor = first_feed["next_cursor"]
        request("GET", feed_path + "?cursor=" + feed_cursor, feed_tokens[1], status=400, code="invalid_cursor")
        request("GET", api + f"/users/{reader}/posts?cursor={feed_cursor}", feed_tokens[0],
                status=400, code="invalid_cursor")
        reader_posts = request("GET", api + f"/users/{reader}/posts?limit=1", feed_tokens[0])
        for wrong_cursor in [reader_posts["next_cursor"], cursor, "invalid!", "a" * 513]:
            request("GET", feed_path + "?cursor=" + wrong_cursor, feed_tokens[0], status=400, code="invalid_cursor")
        invalid_boundary = base64.urlsafe_b64encode(json.dumps(
            {"owner": reader, "kind": "feed", "created_on": 0, "id": 1}).encode()).decode().rstrip("=")
        request("GET", feed_path + "?cursor=" + invalid_boundary, feed_tokens[0], status=400, code="invalid_cursor")
        for value in ["0", "101", "-1", "abc"]:
            request("GET", feed_path + "?limit=" + value, feed_tokens[0], status=400, code="invalid_input")
        passed("Feed cursor ownership and type, invalid boundaries and page limits")

        boundary = first_feed["items"][-1]
        author_token = feed_tokens[next(i for i, u in enumerate(feed_users) if u["id"] == boundary["author"]["id"])]
        request("DELETE", post_path + f"/{boundary['id']}", author_token)
        feed_posts.remove(boundary)
        fresh_post = request("POST", post_path, feed_tokens[1], data={"content": "fresh Feed post"}, status=201)
        feed_posts.append(fresh_post)
        check(feed_page(feed_cursor)["items"] == expected[3:6], "Feed continuation after deleted boundary")
        check(feed_page()["items"][0] == fresh_post, "refresh finds newly published Feed post")
        request("DELETE", relation(feed_users[1]["id"]), feed_tokens[0])
        remaining_authors = {reader, feed_users[2]["id"]}
        check(collect_feed() == expected_feed(remaining_authors), "unfollow immediately filters new Feed reads")
        remaining_after = [p for p in expected_feed(remaining_authors)
                           if (p["created_on"], p["id"]) < (boundary["created_on"], boundary["id"])]
        check(feed_page(feed_cursor)["items"] == remaining_after[:3], "old cursor applies current relationships")
        check(request("GET", post_path + f"/{fresh_post['id']}", feed_tokens[0]) == fresh_post,
              "unfollow preserves public detail access")
        request("PUT", relation(feed_users[1]["id"]), feed_tokens[0])
        check(collect_feed() == expected_feed(authors), "refollow restores effective history")
        last = expected_feed(authors)[-1]
        tail_cursor = base64.urlsafe_b64encode(json.dumps(
            {"owner": reader, "kind": "feed", "created_on": last["created_on"], "id": last["id"]}
        ).encode()).decode().rstrip("=")
        check(feed_page(tail_cursor) == empty_feed, "Feed cursor beyond last result")
        passed("Feed deletion, publication between pages, unfollow, refollow and exhausted cursor")

        # Account-only operations must not create events.
        account_events_before = sql("SELECT COUNT(*) FROM outbox_events;")
        # Keep publication retry fixtures independent of pagination expectations.
        retry_user = request("POST", api + "/register",
                             data={"username": "retry_writer", "password": password}, status=201)
        retry_token = request("POST", api + "/login",
                              data={"username": "retry_writer", "password": password})["token"]
        check(sql("SELECT COUNT(*) FROM outbox_events;") == account_events_before,
              "registration and login create no events")
        retry_content = {"content": "并发发布 🌱"}
        for invalid_key in [None, "", "a" * 129, "bad key", "bad,key", "bad/key"]:
            request("POST", post_path, retry_token, data=retry_content,
                    request_key=invalid_key, status=400, code="invalid_input")
        check(sql(f"SELECT COUNT(*) FROM post_requests WHERE user_id={retry_user['id']};") == "0",
              "invalid keys create no request records")
        with ThreadPoolExecutor(max_workers=8) as pool:
            retry_results = list(pool.map(lambda _: request(
                "POST", post_path, retry_token, data=retry_content, status=201,
                request_key="Concurrent-Key"), range(16)))
        original_publication = retry_results[0]
        check(all(item == original_publication for item in retry_results), "concurrent retries return identical results")
        check(sql(f"SELECT COUNT(*) FROM posts WHERE author_id={retry_user['id']};") == "1",
              "concurrent retries create exactly one post")
        check(sql(f"SELECT COUNT(*) FROM post_requests WHERE user_id={retry_user['id']};") == "1",
              "concurrent retries create exactly one request record")
        publication_event = sql("SELECT id, event_type, payload->>'$.actor_id', payload->>'$.post_id' "
                                f"FROM outbox_events WHERE payload->>'$.post_id'={original_publication['id']};")
        event_fields = publication_event.split()
        check(len(event_fields) == 4 and event_fields[1:] == [
            "post.created", str(retry_user["id"]), str(original_publication["id"])],
            "concurrent publication creates one identifiable event")
        check(request("POST", post_path, retry_token, data={"content": "  并发发布 🌱\n"}, status=201,
                      request_key="Concurrent-Key") == original_publication, "normalized content replays")
        request("POST", post_path, retry_token, data={"content": "different"},
                request_key="Concurrent-Key", status=409, code="idempotency_conflict")
        other_key = request("POST", post_path, retry_token, data=retry_content, status=201,
                            request_key="concurrent-key")
        check(other_key["id"] != original_publication["id"], "keys are case sensitive")
        other_user = request("POST", post_path, tokens[1], data=retry_content, status=201,
                             request_key="Concurrent-Key")
        check(other_user["author"]["id"] == users[1]["id"] and other_user["id"] != original_publication["id"],
              "publication key is scoped to authenticated user")
        request("POST", post_path, retry_token, data=retry_content, status=201, request_key="k" * 128)
        passed("publication key validation, concurrent retries, content conflicts and user scope")

        # Fail after inserting the post, before saving the response. MySQL must
        # roll back both the post and the request reservation.
        before_failure = sql("SELECT COUNT(*) FROM posts;")
        sql("CREATE TRIGGER fail_publication_result BEFORE UPDATE ON post_requests "
            "FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='injected result write failure';")
        try:
            request("POST", post_path, retry_token, data=retry_content, request_key="rollback-key",
                    status=500, code="internal_error")
        finally:
            sql("DROP TRIGGER fail_publication_result;")
        check(sql("SELECT COUNT(*) FROM posts;") == before_failure, "failed result write rolls back post")
        check(sql("SELECT COUNT(*) FROM post_requests WHERE request_key='rollback-key';") == "0",
              "failed result write rolls back reservation")
        recovered_publication = request("POST", post_path, retry_token, data=retry_content,
                                        request_key="rollback-key", status=201)
        check(request("POST", post_path, retry_token, data=retry_content,
                      request_key="rollback-key", status=201) == recovered_publication,
              "rolled back key can be retried successfully")
        passed("publication transaction rollback after post insertion and safe retry")

        outbox_post = request("POST", post_path, retry_token, data={"content": "outbox rollback"}, status=201)
        present_target, absent_target = users[1]["id"], users[2]["id"]
        request("PUT", relation(present_target), retry_token)

        def durable_snapshot():
            return {table: sql(f"SELECT * FROM {table} ORDER BY 1, 2;")
                    for table in ["posts", "post_requests", "following", "outbox_events"]}

        before_outbox_failure = durable_snapshot()
        sql("CREATE TRIGGER fail_outbox BEFORE INSERT ON outbox_events "
            "FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='injected outbox write failure';")
        try:
            request("POST", post_path, retry_token, data=retry_content, request_key="outbox-rollback",
                    status=500, code="internal_error")
            request("DELETE", post_path + f"/{outbox_post['id']}", retry_token,
                    status=500, code="internal_error")
            request("PUT", relation(absent_target), retry_token, status=500, code="internal_error")
            request("DELETE", relation(present_target), retry_token, status=500, code="internal_error")
            # No-op operations do not attempt to insert an event.
            request("PUT", relation(present_target), retry_token)
            request("DELETE", relation(absent_target), retry_token)
            check(request("POST", post_path, retry_token, data=retry_content, status=201,
                          request_key="Concurrent-Key") == original_publication,
                  "publication replay requires no new event")
        finally:
            sql("DROP TRIGGER fail_outbox;")
        check(durable_snapshot() == before_outbox_failure,
              "all four business mutations roll back when event insertion fails")
        passed("outbox failure rolls back publication, deletion, follow and unfollow")

        # Exercise the existing bounded deadlock retry at the event-write stage.
        sql("CREATE TRIGGER fail_outbox BEFORE INSERT ON outbox_events "
            "FOR EACH ROW SIGNAL SQLSTATE '40001' SET MYSQL_ERRNO=1213, MESSAGE_TEXT='injected deadlock';")
        try:
            request("PUT", relation(absent_target), retry_token, status=503, code="relationship_busy")
            request("DELETE", relation(present_target), retry_token, status=503, code="relationship_busy")
        finally:
            sql("DROP TRIGGER fail_outbox;")
        check(durable_snapshot() == before_outbox_failure, "exhausted deadlock retries leave no partial writes")
        passed("relationship deadlock retry includes business change and event")

        request("POST", post_path, retry_token, data=retry_content, request_key="outbox-rollback", status=201)
        with ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(lambda _: request("DELETE", post_path + f"/{outbox_post['id']}", retry_token), range(16)))
            list(pool.map(lambda _: request("PUT", relation(absent_target), retry_token), range(16)))
            list(pool.map(lambda _: request("DELETE", relation(present_target), retry_token), range(16)))
        check(sql(f"SELECT COUNT(*) FROM outbox_events WHERE event_type='post.deleted' "
                  f"AND payload->>'$.post_id'={outbox_post['id']};") == "1", "concurrent deletion records one event")
        check(sql(f"SELECT COUNT(*) FROM outbox_events WHERE payload->>'$.actor_id'={retry_user['id']} "
                  f"AND payload->>'$.target_user_id'={absent_target} AND event_type='follow.created';") == "1",
              "concurrent follow records one event")
        check(sql(f"SELECT COUNT(*) FROM outbox_events WHERE payload->>'$.actor_id'={retry_user['id']} "
                  f"AND payload->>'$.target_user_id'={present_target} AND event_type='follow.deleted';") == "1",
              "concurrent unfollow records one event")
        request("PUT", relation(present_target), retry_token)
        check(sql(f"SELECT COUNT(*) FROM outbox_events WHERE payload->>'$.actor_id'={retry_user['id']} "
                  f"AND payload->>'$.target_user_id'={present_target} AND event_type='follow.created';") == "2",
              "refollow creates a new event")
        passed("outbox recovery and concurrent no-op operations preserve event cardinality")

        request("DELETE", post_path + f"/{original_publication['id']}", retry_token)
        sql(f"UPDATE post_requests SET created_on=created_on-90000 WHERE user_id={retry_user['id']};")
        before_replay = sql("SELECT COUNT(*) FROM posts;")
        check(request("POST", post_path, retry_token, data=retry_content, status=201,
                      request_key="Concurrent-Key") == original_publication, "deleted post replays original response after 25h")
        request("GET", post_path + f"/{original_publication['id']}", retry_token,
                status=404, code="post_not_found")
        request("POST", post_path, retry_token, data={"content": "different"},
                request_key="Concurrent-Key", status=409, code="idempotency_conflict")
        check(sql("SELECT COUNT(*) FROM posts;") == before_replay, "replay never recreates deleted post")
        passed("deleted publication identity and retention beyond 24 hours using aged records")

        # Reconcile identities and effects, including earlier mixed concurrent
        # follow/unfollow requests. Fixture timestamp updates do not change IDs.
        for row in sql("SELECT event_type, payload FROM outbox_events;").splitlines():
            kind, raw_payload = row.split("\t", 1)
            payload = json.loads(raw_payload)
            subject = "post_id" if kind.startswith("post.") else "target_user_id"
            check(set(payload) == {"actor_id", subject}, "payload contains exactly the event's fields")
            check(all(type(value) is int and value > 0 for value in payload.values()), "payload contains positive integer IDs")
        events = [row.split() for row in sql(
            "SELECT id, event_type, payload->>'$.actor_id', "
            "COALESCE(payload->>'$.post_id',0), COALESCE(payload->>'$.target_user_id',0) "
            "FROM outbox_events ORDER BY id;").splitlines()]
        expected_post_events = []
        for row in sql("SELECT id, author_id, deleted_on IS NOT NULL FROM posts;").splitlines():
            post_id, author_id, deleted = map(int, row.split())
            expected_post_events.append(("post.created", author_id, post_id))
            if deleted:
                expected_post_events.append(("post.deleted", author_id, post_id))
        actual_post_events = [(kind, int(actor_id), int(post_id))
                              for _, kind, actor_id, post_id, _ in events if kind.startswith("post.")]
        check(sorted(actual_post_events) == sorted(expected_post_events), "every post effect has exactly one event")
        relation_balances = {}
        for _, kind, actor_id, _, target_id in events:
            if kind.startswith("follow."):
                pair = (int(actor_id), int(target_id))
                relation_balances[pair] = relation_balances.get(pair, 0) + (1 if kind == "follow.created" else -1)
        current_pairs = {tuple(map(int, row.split())) for row in sql(
            "SELECT user_id, follow_id FROM following;").splitlines()}
        check(all(balance in (0, 1) for balance in relation_balances.values()), "relationship event balance")
        check({pair for pair, balance in relation_balances.items() if balance == 1} == current_pairs,
              "relationship event effects match authoritative relationships")
        check(sql("SELECT id, event_type, payload->>'$.actor_id', payload->>'$.post_id' FROM outbox_events "
                  f"WHERE payload->>'$.post_id'={original_publication['id']} AND event_type='post.created';") == publication_event,
              "publication retries and deletion retain original event identity")
        saved_events = sql("SELECT * FROM outbox_events ORDER BY id;")
        passed("outbox identities reconcile with posts and current relationships")

        process.kill()
        process.wait(timeout=5)
        start_server()
        check(request("POST", post_path, retry_token, data=retry_content, status=201,
                      request_key="Concurrent-Key") == original_publication, "deleted result survives forced restart")
        check(request("POST", post_path, retry_token, data=retry_content, status=201,
                      request_key="rollback-key") == recovered_publication, "active result survives forced restart")
        check(sql("SELECT COUNT(*) FROM posts;") == before_replay, "forced restart retries create no new posts")
        check(sql("SELECT * FROM outbox_events ORDER BY id;") == saved_events, "events survive forced restart and replay")
        passed("publication results survive forced application restart")
        stop_server()
        check(process.returncode == 0, "graceful shutdown")
        start_server()
        check(sql("SELECT * FROM outbox_events ORDER BY id;") == saved_events, "events survive graceful restart")
        check(request("POST", post_path, retry_token, data=retry_content, status=201,
                      request_key="Concurrent-Key") == original_publication, "publication result survives graceful restart")
        check(request("GET", api + "/me", token)["id"] == actor, "session survives restart")
        check(request("GET", post_path + f"/{new_post['id']}", token) == new_post, "post survives restart")
        request("GET", deleted_path, token, status=404, code="post_not_found")
        check(collect_feed() == expected_feed(authors), "Feed survives application restart")
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
            request("GET", feed_path, feed_tokens[0], status=500, code="internal_error")
        finally:
            sql("RENAME TABLE following_unavailable TO following;")
        passed("database query errors remain errors rather than empty lists")
        sql("RENAME TABLE posts TO posts_unavailable;")
        try:
            request("GET", api + f"/users/{actor}/posts", tokens[1], status=500, code="internal_error")
            request("GET", feed_path, feed_tokens[0], status=500, code="internal_error")
        finally:
            sql("RENAME TABLE posts_unavailable TO posts;")
        passed("post query errors are reported as internal errors")
        passed("Feed dependency failures return errors")
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
