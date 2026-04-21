"""WikiEater crawler package."""

from .config import FILTERED_GAMES
from .crawler import run_crawler

__all__ = ["FILTERED_GAMES", "run_crawler"]
