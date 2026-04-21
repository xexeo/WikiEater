from __future__ import annotations

import hashlib
import os
import signal
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from .config import DEFAULT_REQUEST_TIMEOUT, DEFAULT_SLEEP_SECONDS, FILTERED_GAMES, USER_AGENT
from .db import CrawlDB
from .extractor import clean_and_extract, normalize_internal_url


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def _slug(value: str) -> str:
    safe = "".join(c if c.isalnum() else "_" for c in value).strip("_")
    while "__" in safe:
        safe = safe.replace("__", "_")
    return safe.lower()[:120] or "page"


def _build_file_path(base_dir: str, game_name: str, url: str) -> str:
    game_dir = Path(base_dir) / _slug(game_name)
    game_dir.mkdir(parents=True, exist_ok=True)
    parsed = urlparse(url)
    path_part = _slug(parsed.path or "index")
    digest = hashlib.sha1(url.encode("utf-8")).hexdigest()[:10]
    return str(game_dir / f"{path_part}_{digest}.html")


class RoundRobinScheduler:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.lock = threading.Lock()
        self.game_ids: list[int] = []
        self.pos = 0
        self._refresh_game_ids()

    def _refresh_game_ids(self):
        import sqlite3

        conn = sqlite3.connect(self.db_path, timeout=30, isolation_level=None)
        conn.row_factory = sqlite3.Row
        try:
            rows = conn.execute("SELECT id FROM games WHERE active=1 ORDER BY id").fetchall()
            self.game_ids = [r["id"] for r in rows]
            self.pos = 0
        finally:
            conn.close()

    def claim_next(self) -> dict[str, Any] | None:
        import sqlite3

        with self.lock:
            if not self.game_ids:
                return None

            conn = sqlite3.connect(self.db_path, timeout=30, isolation_level=None)
            conn.row_factory = sqlite3.Row
            try:
                tries = 0
                while tries < len(self.game_ids):
                    gid = self.game_ids[self.pos]
                    self.pos = (self.pos + 1) % len(self.game_ids)
                    tries += 1

                    conn.execute("BEGIN IMMEDIATE")
                    row = conn.execute(
                        """
                        SELECT p.id, p.url, g.name AS game_name, g.wiki_url
                        FROM pages p
                        JOIN games g ON g.id=p.game_id
                        WHERE p.game_id=? AND p.status='pending'
                        ORDER BY p.id
                        LIMIT 1
                        """,
                        (gid,),
                    ).fetchone()
                    if row is None:
                        conn.execute("COMMIT")
                        continue

                    conn.execute(
                        "UPDATE pages SET status='in_progress', attempts=attempts+1, updated_at=? WHERE id=?",
                        (_utcnow(), row["id"]),
                    )
                    conn.execute("COMMIT")
                    return dict(row)
                return None
            finally:
                conn.close()


def _fetch_url(url: str, timeout: int) -> str:
    req = Request(url, headers={"User-Agent": USER_AGENT})
    with urlopen(req, timeout=timeout) as resp:
        charset = resp.headers.get_content_charset() or "utf-8"
        return resp.read().decode(charset, errors="replace")


def run_crawler(db_path: str, wiki_dir: str, threads: int = 2, timeout: int = DEFAULT_REQUEST_TIMEOUT, sleep_seconds: float = DEFAULT_SLEEP_SECONDS):
    db = CrawlDB(db_path)
    db.seed_games(FILTERED_GAMES)
    db.recover_in_progress()

    stop_event = threading.Event()

    def handle_stop(signum, _frame):
        db.log("INFO", f"Received signal {signum}; stopping gracefully")
        stop_event.set()

    signal.signal(signal.SIGINT, handle_stop)
    signal.signal(signal.SIGTERM, handle_stop)

    scheduler = RoundRobinScheduler(db_path)

    def worker(worker_id: int):
        import sqlite3

        conn = sqlite3.connect(db_path, timeout=30, isolation_level=None)
        conn.row_factory = sqlite3.Row
        try:
            while not stop_event.is_set():
                task = scheduler.claim_next()
                if task is None:
                    time.sleep(1.0)
                    continue

                page_id = task["id"]
                page_url = task["url"]
                game_name = task["game_name"]
                wiki_root = task["wiki_url"]

                try:
                    raw_html = _fetch_url(page_url, timeout)
                    cleaned_html, discovered_links = clean_and_extract(raw_html, page_url)
                    file_path = _build_file_path(wiki_dir, game_name, page_url)
                    Path(file_path).write_text(cleaned_html, encoding="utf-8")

                    now = _utcnow()
                    conn.execute(
                        "UPDATE pages SET status='done', saved_path=?, updated_at=?, last_error=NULL WHERE id=?",
                        (file_path, now, page_id),
                    )

                    normalized_links = []
                    for link in discovered_links:
                        normalized = normalize_internal_url(wiki_root, link)
                        if normalized:
                            normalized_links.append(normalized)

                    for link in normalized_links:
                        conn.execute(
                            """
                            INSERT INTO pages(game_id, url, discovered_from, status, attempts, discovered_at, updated_at)
                            SELECT p.game_id, ?, ?, 'pending', 0, ?, ?
                            FROM pages p
                            WHERE p.id=?
                            ON CONFLICT(game_id, url) DO NOTHING
                            """,
                            (link, page_url, now, now, page_id),
                        )

                    conn.execute(
                        "INSERT INTO crawl_log(ts, level, game_id, url, message) "
                        "SELECT ?, 'INFO', p.game_id, p.url, ? FROM pages p WHERE p.id=?",
                        (now, f"worker-{worker_id}: downloaded and parsed", page_id),
                    )

                except (URLError, TimeoutError, OSError, ValueError) as exc:
                    conn.execute(
                        "UPDATE pages SET status='failed', updated_at=?, last_error=? WHERE id=?",
                        (_utcnow(), str(exc)[:1000], page_id),
                    )
                    conn.execute(
                        "INSERT INTO crawl_log(ts, level, game_id, url, message) "
                        "SELECT ?, 'ERROR', p.game_id, p.url, ? FROM pages p WHERE p.id=?",
                        (_utcnow(), f"worker-{worker_id}: {exc}", page_id),
                    )

                time.sleep(max(0.0, sleep_seconds))
        finally:
            conn.close()

    db.log("INFO", f"Crawler started with {threads} threads")
    worker_threads = [threading.Thread(target=worker, args=(idx,), daemon=True) for idx in range(threads)]
    for t in worker_threads:
        t.start()

    try:
        while not stop_event.is_set():
            pending = sum(int(r["pending"] or 0) for r in db.stats())
            in_progress = _in_progress_count(db_path)
            if pending == 0 and in_progress == 0:
                db.log("INFO", "Crawler completed all discovered pages")
                stop_event.set()
                break
            time.sleep(2.0)
    finally:
        for t in worker_threads:
            t.join(timeout=10)
        if stop_event.is_set():
            _reset_in_progress(db_path)
            db.log("INFO", "Crawler stopped and state persisted")


def _in_progress_count(db_path: str) -> int:
    import sqlite3

    conn = sqlite3.connect(db_path)
    try:
        return conn.execute("SELECT COUNT(*) FROM pages WHERE status='in_progress'").fetchone()[0]
    finally:
        conn.close()


def _reset_in_progress(db_path: str):
    import sqlite3

    conn = sqlite3.connect(db_path, isolation_level=None)
    try:
        conn.execute("UPDATE pages SET status='pending' WHERE status='in_progress'")
    finally:
        conn.close()
