import os
import tempfile
import unittest

from wikieater.config import FILTERED_GAMES
from wikieater.db import CrawlDB


class DBSeedTests(unittest.TestCase):
    def test_seed_games_creates_initial_pending_pages(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = os.path.join(tmp, "crawler.sqlite")
            db = CrawlDB(db_path)
            db.seed_games(FILTERED_GAMES)

            rows = db.stats()
            self.assertEqual(len(rows), len(FILTERED_GAMES))
            for row in rows:
                self.assertEqual(int(row["identified"]), 1)
                self.assertEqual(int(row["pending"]), 1)


if __name__ == "__main__":
    unittest.main()
