import logging
from datetime import datetime

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO

from config import (
    S3_BUCKET,
    s3_staged_prefix,
    s3_processed_prefix,
)
from pipeline.utils import get_s3_client, load_batch_from_s3

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def list_staged_batches(source_date: str) -> list[str]:
    """List all staged batch files for the given date, sorted."""
    s3 = get_s3_client()
    prefix = s3_staged_prefix("movies", source_date)
    response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
    keys = [obj["Key"] for obj in response.get("Contents", [])]
    logger.info(f"Found {len(keys)} staged batch files to transform")
    return sorted(keys)



def save_parquet_to_s3(df: pd.DataFrame, source_date: str):
    """
    Write the flattened movies DataFrame to S3 as a single Parquet file.
    Stored under processed/movies/{date}/movies.parquet.
    """
    s3 = get_s3_client()
    prefix = s3_processed_prefix("movies", source_date)
    key = f"{prefix}movies.parquet"

    buffer = BytesIO()
    table = pa.Table.from_pandas(df)
    pq.write_table(table, buffer)
    buffer.seek(0)

    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=buffer.read(),
        ContentType="application/octet-stream",
    )
    logger.info(f"Saved movies.parquet ({len(df)} rows) to s3://{S3_BUCKET}/{key}")


def flatten_movie(movie: dict) -> dict:
    """
    Flatten a single TMDB movie record from nested JSON into a flat row.

    Nested arrays (genres, spoken_languages, production_companies) are
    collapsed into pipe-delimited strings so they can be loaded into
    Snowflake as a single staging table. dbt will handle the dimensional
    modeling from there.
    """
    # Extract genre names as pipe-delimited string e.g. "Action|Drama"
    genres = movie.get("genres") or []
    genre_names = "|".join([genre.get("name", "") for genre in genres if genre.get("name")])
    genre_ids = "|".join([str(genre.get("id", "")) for genre in genres if genre.get("id")])

    # Extract spoken language codes e.g. "en|fr|es"
    languages = movie.get("spoken_languages") or []
    language_codes = "|".join([lang.get("iso_639_1", "") for lang in languages if lang.get("iso_639_1")])
    language_names = "|".join([lang.get("name", "") for lang in languages if lang.get("name")])

    # Extract production company names and IDs
    companies = movie.get("production_companies") or []
    company_names = "|".join([company.get("name", "") for company in companies if company.get("name")])
    company_ids = "|".join([str(company.get("id", "")) for company in companies if company.get("id")])

    # Extract production country codes e.g. "US|GB"
    countries = movie.get("production_countries") or []
    country_codes = "|".join([country.get("iso_3166_1", "") for country in countries if country.get("iso_3166_1")])

    return {
        # Core identifiers
        "movie_id": movie.get("id"),
        "imdb_id": movie.get("imdb_id"),
        "title": movie.get("title"),
        "original_title": movie.get("original_title"),
        "original_language": movie.get("original_language"),

        # Release info
        "release_date": movie.get("release_date") or None,
        "status": movie.get("status"),

        # Metrics
        "budget": movie.get("budget"),
        "revenue": movie.get("revenue"),
        "runtime": movie.get("runtime"),
        "vote_average": movie.get("vote_average"),
        "vote_count": movie.get("vote_count"),
        "popularity": movie.get("popularity"),

        # Flattened arrays
        "genre_ids": genre_ids,
        "genre_names": genre_names,
        "language_codes": language_codes,
        "language_names": language_names,
        "company_ids": company_ids,
        "company_names": company_names,
        "country_codes": country_codes,

        # Text
        "overview": movie.get("overview"),
        "tagline": movie.get("tagline"),
        "homepage": movie.get("homepage"),
    }


def run_transform(source_date: str = None) -> None:
    """
    Main transform function:
    1. Load all staged batches from S3
    2. Flatten each movie record from nested JSON to a flat row
    3. Write a single Parquet file to processed/
    """
    if source_date is None:
        source_date = datetime.utcnow().strftime("%Y-%m-%d")

    logger.info(f"Starting transform for date: {source_date}")

    staged_keys = list_staged_batches(source_date)
    rows = []

    for i, key in enumerate(staged_keys, start=1):
        batch = load_batch_from_s3(key)
        for movie in batch:
            rows.append(flatten_movie(movie))
        if i % 10 == 0:
            logger.info(f"Flattened {i}/{len(staged_keys)} batches ({len(rows)} rows so far)")

    logger.info(f"All batches flattened. Building DataFrame from {len(rows)} rows...")
    df = pd.DataFrame(rows)

    # Cast types explicitly
    df["movie_id"] = df["movie_id"].astype("Int64")
    df["budget"] = pd.to_numeric(df["budget"], errors="coerce").astype("Int64")
    df["revenue"] = pd.to_numeric(df["revenue"], errors="coerce").astype("Int64")
    df["runtime"] = pd.to_numeric(df["runtime"], errors="coerce").astype("Int64")
    df["vote_average"] = pd.to_numeric(df["vote_average"], errors="coerce")
    df["vote_count"] = pd.to_numeric(df["vote_count"], errors="coerce").astype("Int64")
    df["popularity"] = pd.to_numeric(df["popularity"], errors="coerce")
    df["release_date"] = df["release_date"].astype(str).replace("None", None)

    logger.info(f"DataFrame built. Shape: {df.shape}")
    logger.info(f"Columns: {list(df.columns)}")

    save_parquet_to_s3(df, source_date)
    logger.info("Transform complete.")


if __name__ == "__main__":
    run_transform("2026-03-09")