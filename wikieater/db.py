from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from .config import GameSeed


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


SCHEMA = """
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS games (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    genre TEXT NOT NULL,
    wiki_url TEXT NOT NULL,
    perspective TEXT NOT NULL,
    active INTEGER NOT NULL DEFAULT 1
);
CREATE TABLE IF NOT EXISTS pages (
    id INTEGER PRIMARY KEY,
    game_id INTEGER NOT NULL,
    url TEXT NOT NULL,
    discovered_from TEXT,
    status TEXT NOT NULL CHECK(status IN ('pending', 'in_progress', 'done', 'failed')),
    attempts INTEGER NOT NULL DEFAULT 0,
    saved_path TEXT,
    last_error TEXT,
    discovered_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(game_id, url),
    FOREIGN KEY(game_id) REFERENCES games(id)
);
CREATE INDEX IF NOT EXISTS idx_pages_game_status ON pages(game_id, status);
CREATE TABLE IF NOT EXISTS crawl_log (
    id INTEGER PRIMARY KEY,
    ts TEXT NOT NULL,
    level TEXT NOT NULL,
    game_id INTEGER,
    url TEXT,
    message TEXT NOT NULL,
    FOREIGN KEY(game_id) REFERENCES games(id)
);
"""


class CrawlDB:
    def __init__(self, db_path: str):
        self.db_path = db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.init_db()

    @contextmanager
    def connect(self):
        conn = sqlite3.connect(self.db_path, timeout=30, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        try:
            yield conn
        finally:
            conn.close()

    def init_db(self):
        with self.connect() as conn:
            conn.executescript(SCHEMA)

    def seed_games(self, games: Iterable[GameSeed]):
        now = _utcnow()
        with self.connect() as conn:
            for g in games:
                conn.execute(
                    """
                    INSERT INTO games(name, genre, wiki_url, perspective, active)
                    VALUES(?, ?, ?, ?, 1)
                    ON CONFLICT(name) DO UPDATE SET
                        genre=excluded.genre,
                        wiki_url=excluded.wiki_url,
                        perspective=excluded.perspective,
                        active=1
                    """,
                    (g.name, g.genre, g.wiki_url, g.perspective),
                )

            rows = conn.execute("SELECT id, wiki_url FROM games WHERE active=1").fetchall()
            for row in rows:
                conn.execute(
                    """
                    INSERT INTO pages(game_id, url, discovered_from, status, attempts, discovered_at, updated_at)
                    VALUES(?, ?, NULL, 'pending', 0, ?, ?)
                    ON CONFLICT(game_id, url) DO NOTHING
                    """,
                    (row["id"], row["wiki_url"], now, now),
                )

    def recover_in_progress(self):
        now = _utcnow()
        with self.connect() as conn:
            conn.execute(
                "UPDATE pages SET status='pending', updated_at=? WHERE status='in_progress'",
                (now,),
            )

    def log(self, level: str, message: str, game_id: int | None = None, url: str | None = None):
        with self.connect() as conn:
            conn.execute(
                "INSERT INTO crawl_log(ts, level, game_id, url, message) VALUES(?, ?, ?, ?, ?)",
                (_utcnow(), level, game_id, url, message),
            )

    def stats(self):
        with self.connect() as conn:
            return conn.execute(
                """
                SELECT g.name, g.wiki_url,
                       COUNT(p.id) AS identified,
                       SUM(CASE WHEN p.status='done' THEN 1 ELSE 0 END) AS downloaded,
                       SUM(CASE WHEN p.status='pending' THEN 1 ELSE 0 END) AS pending,
                       SUM(CASE WHEN p.status='failed' THEN 1 ELSE 0 END) AS failed
                FROM games g
                LEFT JOIN pages p ON p.game_id = g.id
                WHERE g.active=1
                GROUP BY g.id
                ORDER BY g.name
                """
            ).fetchall()

    def tail_logs(self, limit: int = 50):
        with self.connect() as conn:
            return conn.execute(
                "SELECT ts, level, game_id, url, message FROM crawl_log ORDER BY id DESC LIMIT ?",
                (limit,),
            ).fetchall()

    def remaining_count(self) -> int:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT COUNT(*) AS total FROM pages WHERE status IN ('pending', 'in_progress')"
            ).fetchone()
            return int(row["total"] if row else 0)
