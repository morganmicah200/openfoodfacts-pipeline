"""
config.py
Central configuration loader for the openfoodfacts-pipeline.
All environment variables are read here — never import os.environ directly elsewhere.
"""

import os
from dotenv import load_dotenv

load_dotenv()


# ── AWS ────────────────────────────────────────────────────────────────────
AWS_ACCESS_KEY_ID     = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_DEFAULT_REGION    = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
S3_BUCKET             = os.getenv("S3_BUCKET")

# ── Snowflake ──────────────────────────────────────────────────────────────
SF_ACCOUNT   = os.getenv("SF_ACCOUNT")
SF_USER      = os.getenv("SF_USER")
SF_PASSWORD  = os.getenv("SF_PASSWORD")
SF_DATABASE  = os.getenv("SF_DATABASE", "OFF_ANALYTICS")
SF_SCHEMA    = os.getenv("SF_SCHEMA", "PUBLIC")
SF_WAREHOUSE = os.getenv("SF_WAREHOUSE", "COMPUTE_WH")
SF_ROLE      = os.getenv("SF_ROLE", "SYSADMIN")

# ── Open Food Facts API ────────────────────────────────────────────────────
OFF_BASE_URL   = "https://world.openfoodfacts.org/api/v2"
OFF_USER_AGENT = os.getenv("OFF_USER_AGENT", "openfoodfacts-pipeline/1.0")
OFF_PAGE_SIZE  = 1000   # max allowed by the API

# Categories to ingest — expand this list as the project grows
OFF_CATEGORIES = [
    "beverages",
    "snacks",
    "dairy-products",
    "cereals-and-their-products",
    "meats",
    "frozen-foods",
    "sauces",
    "breads",
]

# ── S3 Path Helpers ────────────────────────────────────────────────────────
def s3_raw_prefix(date_str: str) -> str:
    """e.g. raw/products/2024-11-01/"""
    return f"raw/products/{date_str}/"

def s3_staged_prefix(date_str: str) -> str:
    """e.g. staged/products/2024-11-01/"""
    return f"staged/products/{date_str}/"

def s3_processed_prefix(date_str: str) -> str:
    """e.g. processed/products/2024-11-01/"""
    return f"processed/products/{date_str}/"

# ── Pipeline ───────────────────────────────────────────────────────────────
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")