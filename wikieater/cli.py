from __future__ import annotations

import argparse
import os
from pathlib import Path

from .config import DEFAULT_DB_PATH, DEFAULT_THREADS, DEFAULT_WIKI_DIR, FILTERED_GAMES
from .crawler import run_crawler
from .db import CrawlDB


def _print_status(db: CrawlDB):
    rows = db.stats()
    print("game | wiki | identified | downloaded | pending | failed | completion")
    for r in rows:
        identified = int(r["identified"] or 0)
        downloaded = int(r["downloaded"] or 0)
        completion = f"{(downloaded / identified * 100):.2f}%" if identified else "0.00%"
        print(
            f"{r['name']} | {r['wiki_url']} | {identified} | {downloaded} | "
            f"{int(r['pending'] or 0)} | {int(r['failed'] or 0)} | {completion}"
        )


def _print_logs(db: CrawlDB, limit: int):
    rows = db.tail_logs(limit)
    for row in reversed(rows):
        print(f"[{row['ts']}] {row['level']} game_id={row['game_id']} url={row['url']} {row['message']}")


def main():
    parser = argparse.ArgumentParser(description="WikiEater - polite wiki backup crawler")
    parser.add_argument("--db", default=DEFAULT_DB_PATH, help="SQLite file path")
    parser.add_argument("--wiki-dir", default=DEFAULT_WIKI_DIR, help="Directory where cleaned HTML backup files are written")

    sub = parser.add_subparsers(dest="cmd", required=True)

    run_parser = sub.add_parser("run", help="Run crawler")
    run_parser.add_argument("--threads", type=int, default=DEFAULT_THREADS, help="Number of worker threads (default: 2)")
    run_parser.add_argument("--timeout", type=int, default=20, help="Request timeout in seconds")
    run_parser.add_argument("--sleep", type=float, default=1.0, help="Pause between requests per worker")

    sub.add_parser("status", help="Show crawl status by game")

    logs_parser = sub.add_parser("logs", help="Show crawl logs")
    logs_parser.add_argument("--limit", type=int, default=50)

    args = parser.parse_args()
    Path(os.path.dirname(os.path.abspath(args.db)) or ".").mkdir(parents=True, exist_ok=True)
    db = CrawlDB(args.db)
    db.seed_games(FILTERED_GAMES)

    if args.cmd == "run":
        run_crawler(db_path=args.db, wiki_dir=args.wiki_dir, threads=max(1, args.threads), timeout=args.timeout, sleep_seconds=args.sleep)
    elif args.cmd == "status":
        _print_status(db)
    elif args.cmd == "logs":
        _print_logs(db, args.limit)


if __name__ == "__main__":
    main()
