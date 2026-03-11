"""
config.py
Central configuration loader for the tmdb-pipeline.
All environment variables are read here — never import os.environ directly elsewhere.
"""

import os
from dotenv import load_dotenv

load_dotenv()


# ── AWS ────────────────────────────────────────────────────────────────────
AWS_ACCESS_KEY_ID     = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_DEFAULT_REGION    = os.getenv("AWS_DEFAULT_REGION", "us-east-2")
S3_BUCKET             = os.getenv("S3_BUCKET")

# ── Snowflake ──────────────────────────────────────────────────────────────
SF_ACCOUNT   = os.getenv("SF_ACCOUNT")
SF_USER      = os.getenv("SF_USER")
SF_PASSWORD  = os.getenv("SF_PASSWORD")
SF_DATABASE  = os.getenv("SF_DATABASE", "tmdb")
SF_SCHEMA    = os.getenv("SF_SCHEMA", "raw")
SF_WAREHOUSE = os.getenv("SF_WAREHOUSE", "COMPUTE_WH")
SF_ROLE      = os.getenv("SF_ROLE", "SYSADMIN")

# ── TMDB API ───────────────────────────────────────────────────────────────
TMDB_BASE_URL  = "https://api.themoviedb.org/3"
TMDB_API_KEY   = os.getenv("TMDB_API_KEY")
TMDB_PAGE_SIZE = 20  # TMDB returns 20 results per page (fixed by API)

# Entity types to ingest
TMDB_ENTITIES = ["movies", "tv_shows", "people"]

# ── S3 Path Helpers ────────────────────────────────────────────────────────
def s3_raw_prefix(entity: str, date_str: str) -> str:
    """e.g. raw/movies/2024-11-01/"""
    return f"raw/{entity}/{date_str}/"

def s3_staged_prefix(entity: str, date_str: str) -> str:
    """e.g. staged/movies/2024-11-01/"""
    return f"staged/{entity}/{date_str}/"

def s3_processed_prefix(entity: str, date_str: str) -> str:
    """e.g. processed/movies/2024-11-01/"""
    return f"processed/{entity}/{date_str}/"

# ── Pipeline ───────────────────────────────────────────────────────────────
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")