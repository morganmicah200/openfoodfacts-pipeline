import json
import logging
from datetime import datetime

import boto3
from botocore.exceptions import ClientError

from config import (
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    AWS_DEFAULT_REGION,
    S3_BUCKET,
    s3_raw_prefix,
    s3_staged_prefix,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Fields that must be present and non-null for a record to pass
REQUIRED_FIELDS = ["id", "title", "release_date"]

# Numeric fields that must be a number if present
NUMERIC_FIELDS = ["budget", "revenue", "runtime", "vote_average", "vote_count", "popularity"]


def get_s3_client():
    """Create and return a boto3 S3 client using credentials from config."""
    return boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_DEFAULT_REGION,
    )


def list_raw_batches(source_date: str) -> list[str]:
    """
    List all batch files in the raw S3 prefix for the given date.
    Returns a sorted list of S3 keys.
    """
    s3 = get_s3_client()
    prefix = s3_raw_prefix("movies", source_date)
    response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
    keys = [obj["Key"] for obj in response.get("Contents", [])]
    logger.info(f"Found {len(keys)} raw batch files to validate")
    return sorted(keys)


def load_batch_from_s3(key: str) -> list[dict]:
    """Load and parse a JSON batch file from S3."""
    s3 = get_s3_client()
    response = s3.get_object(Bucket=S3_BUCKET, Key=key)
    return json.loads(response["Body"].read().decode("utf-8"))


def save_staged_batch(movies: list[dict], batch_num: int, source_date: str):
    """
    Write a validated batch to the staged S3 prefix.
    Files are stored under staged/movies/{date}/batch_XXXX.json.
    """
    s3 = get_s3_client()
    prefix = s3_staged_prefix("movies", source_date)
    key = f"{prefix}batch_{batch_num:04d}.json"
    payload = json.dumps(movies, ensure_ascii=False)
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=payload.encode("utf-8"),
        ContentType="application/json",
    )
    logger.info(f"Saved staged batch {batch_num} ({len(movies)} movies) to s3://{S3_BUCKET}/{key}")


def validate_record(movie: dict, seen_ids: set) -> tuple[bool, str]:
    """
    Run all validation checks on a single movie record.
    Returns (True, "") if valid, or (False, reason) if invalid.

    Checks:
    - Required fields are present and non-empty
    - No duplicate movie IDs across all batches
    - Numeric fields are actually numeric if present
    - vote_average is between 0 and 10 if present
    - runtime is positive if present
    - release_date matches YYYY-MM-DD format if present
    """
    # Check required fields
    for field in REQUIRED_FIELDS:
        if not movie.get(field):
            return False, f"Missing required field: {field}"

    # Check for duplicate IDs
    movie_id = movie["id"]
    if movie_id in seen_ids:
        return False, f"Duplicate movie ID: {movie_id}"
    seen_ids.add(movie_id)

    # Check numeric fields
    for field in NUMERIC_FIELDS:
        value = movie.get(field)
        if value is not None and not isinstance(value, (int, float)):
            return False, f"Field {field} is not numeric: {value}"

    # Check vote_average range
    vote_avg = movie.get("vote_average")
    if vote_avg is not None and not (0 <= vote_avg <= 10):
        return False, f"vote_average out of range: {vote_avg}"

    # Check runtime is positive
    runtime = movie.get("runtime")
    if runtime is not None and runtime < 0:
        return False, f"Negative runtime: {runtime}"

    # Check release_date format
    release_date = movie.get("release_date")
    if release_date:
        try:
            datetime.strptime(release_date, "%Y-%m-%d")
        except ValueError:
            return False, f"Invalid release_date format: {release_date}"

    return True, ""


def run_validate(source_date: str = None) -> None:
    """
    Main validation function. For each raw batch:
    1. Load the batch from S3
    2. Validate each record
    3. Write passing records to staged/
    4. Log summary of passed vs failed records
    """
    if source_date is None:
        source_date = datetime.utcnow().strftime("%Y-%m-%d")

    logger.info(f"Starting validation for date: {source_date}")

    raw_keys = list_raw_batches(source_date)
    seen_ids = set()  # tracks all movie IDs across all batches for duplicate detection

    total_passed = 0
    total_failed = 0
    failure_reasons = {}  # tracks counts of each failure reason for summary

    for batch_num, key in enumerate(raw_keys, start=1):
        movies = load_batch_from_s3(key)
        passed = []
        failed = 0

        for movie in movies:
            valid, reason = validate_record(movie, seen_ids)
            if valid:
                passed.append(movie)
            else:
                failed += 1
                failure_reasons[reason] = failure_reasons.get(reason, 0) + 1

        save_staged_batch(passed, batch_num, source_date)

        total_passed += len(passed)
        total_failed += failed
        logger.info(f"Batch {batch_num}: {len(passed)} passed, {failed} failed")

    # Summary
    logger.info(f"Validation complete.")
    logger.info(f"Total passed: {total_passed}")
    logger.info(f"Total failed: {total_failed}")
    if failure_reasons:
        logger.info("Failure breakdown:")
        for reason, count in sorted(failure_reasons.items(), key=lambda x: -x[1]):
            logger.info(f"  {reason}: {count}")


if __name__ == "__main__":
    run_validate("2026-03-09")