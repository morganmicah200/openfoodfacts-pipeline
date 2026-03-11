import gzip
import json
import logging
from datetime import datetime, timedelta

import requests
import boto3
from botocore.exceptions import ClientError

from config import (
    TMDB_BASE_URL,
    TMDB_API_KEY,
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    AWS_DEFAULT_REGION,
    S3_BUCKET,
    s3_raw_prefix,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

EXPORT_BASE_URL = "https://files.tmdb.org/p/exports"
CHECKPOINT_KEY = "checkpoints/movies_checkpoint.json"
BATCH_SIZE = 10000


def get_s3_client():
    return boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_DEFAULT_REGION,
    )


def load_checkpoint() -> dict:
    """Load checkpoint from S3 if it exists."""
    s3 = get_s3_client()
    try:
        response = s3.get_object(Bucket=S3_BUCKET, Key=CHECKPOINT_KEY)
        checkpoint = json.loads(response["Body"].read().decode("utf-8"))
        logger.info(f"Resuming from checkpoint: {checkpoint['movies_fetched']} movies already fetched")
        return checkpoint
    except s3.exceptions.NoSuchKey:
        logger.info("No checkpoint found, starting fresh")
        return {"movies_fetched": 0, "completed_batches": 0, "source_date": None}
    except ClientError:
        logger.info("No checkpoint found, starting fresh")
        return {"movies_fetched": 0, "completed_batches": 0, "source_date": None}


def save_checkpoint(movies_fetched: int, completed_batches: int, source_date: str):
    """Save checkpoint to S3."""
    s3 = get_s3_client()
    checkpoint = {
        "movies_fetched": movies_fetched,
        "completed_batches": completed_batches,
        "source_date": source_date,
    }
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=CHECKPOINT_KEY,
        Body=json.dumps(checkpoint).encode("utf-8"),
        ContentType="application/json",
    )


def save_batch_to_s3(movies: list[dict], batch_num: int, source_date: str):
    """Save a batch of movies to S3."""
    s3 = get_s3_client()
    prefix = s3_raw_prefix("movies", source_date)
    key = f"{prefix}batch_{batch_num:04d}.json"
    payload = json.dumps(movies, ensure_ascii=False)
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=payload.encode("utf-8"),
        ContentType="application/json",
    )
    logger.info(f"Saved batch {batch_num} ({len(movies)} movies) to s3://{S3_BUCKET}/{key}")


def get_movie_ids(date_str: str) -> list[int]:
    """Download TMDB daily movie ID export and return list of valid IDs."""
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    file_date = dt.strftime("%m_%d_%Y")
    url = f"{EXPORT_BASE_URL}/movie_ids_{file_date}.json.gz"

    logger.info(f"Downloading movie ID export from {url}")
    headers = {"User-Agent": "Mozilla/5.0"}
    response = requests.get(url, timeout=60, headers=headers)

    if response.status_code == 404:
        yesterday = (dt - timedelta(days=1)).strftime("%m_%d_%Y")
        url = f"{EXPORT_BASE_URL}/movie_ids_{yesterday}.json.gz"
        logger.info(f"Today's export not found, trying {url}")
        response = requests.get(url, timeout=60, headers=headers)

    response.raise_for_status()

    ids = []
    content = gzip.decompress(response.content).decode("utf-8")
    for line in content.strip().split("\n"):
        try:
            record = json.loads(line)
            if record.get("adult") is False and record.get("id"):
                ids.append(record["id"])
        except json.JSONDecodeError:
            continue

    logger.info(f"Found {len(ids)} movie IDs")
    return ids


def fetch_movie_detail(movie_id: int) -> dict | None:
    """Fetch full detail record for a single movie."""
    url = f"{TMDB_BASE_URL}/movie/{movie_id}"
    params = {"api_key": TMDB_API_KEY, "language": "en-US"}
    try:
        response = requests.get(url, params=params, timeout=15)
        if response.status_code == 404:
            return None
        response.raise_for_status()
        return response.json()
    except requests.RequestException:
        return None


def run_extract(source_date: str = None) -> None:
    """Main entry point for the extract step with checkpointing."""
    if source_date is None:
        source_date = datetime.utcnow().strftime("%Y-%m-%d")

    logger.info(f"Starting TMDB movie extraction for date: {source_date}")

    checkpoint = load_checkpoint()
    completed_batches = checkpoint["completed_batches"]
    total_fetched = checkpoint["movies_fetched"]
    skip_count = completed_batches * BATCH_SIZE

    ids = get_movie_ids(source_date)

    # Skip already processed IDs
    remaining_ids = ids[skip_count:]
    logger.info(f"Skipping {skip_count} already processed IDs, {len(remaining_ids)} remaining")

    current_batch = []
    batch_num = completed_batches

    for i, movie_id in enumerate(remaining_ids):
        movie = fetch_movie_detail(movie_id)
        if movie:
            current_batch.append(movie)
            total_fetched += 1

        if len(current_batch) >= BATCH_SIZE:
            batch_num += 1
            save_batch_to_s3(current_batch, batch_num, source_date)
            save_checkpoint(total_fetched, batch_num, source_date)
            logger.info(f"Checkpoint saved: {total_fetched} total movies fetched")
            current_batch = []

        if (i + 1) % 1000 == 0:
            logger.info(f"Progress: {i + 1}/{len(remaining_ids)} IDs processed, "
                       f"{total_fetched} movies fetched")

    # Save any remaining movies in the last partial batch
    if current_batch:
        batch_num += 1
        save_batch_to_s3(current_batch, batch_num, source_date)
        save_checkpoint(total_fetched, batch_num, source_date)

    logger.info(f"Extract complete. {total_fetched} total movies saved in {batch_num} batches.")


if __name__ == "__main__":
    run_extract("2026-03-09")