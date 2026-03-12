import logging
from datetime import datetime

import boto3
import snowflake.connector

from config import (
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    AWS_DEFAULT_REGION,
    S3_BUCKET,
    SF_ACCOUNT,
    SF_USER,
    SF_PASSWORD,
    SF_DATABASE,
    SF_SCHEMA,
    SF_WAREHOUSE,
    SF_ROLE,
    s3_processed_prefix,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_snowflake_connection():
    """Create and return a Snowflake connection using credentials from config."""
    return snowflake.connector.connect(
        account=SF_ACCOUNT,
        user=SF_USER,
        password=SF_PASSWORD,
        database=SF_DATABASE,
        schema=SF_SCHEMA,
        warehouse=SF_WAREHOUSE,
        role=SF_ROLE,
    )


def create_stage(cursor, stage_name: str):
    """
    Create an external S3 stage in Snowflake if it doesn't already exist.
    The stage points to the processed/ prefix in S3 and uses AWS credentials
    so Snowflake can read Parquet files directly via COPY INTO.
    """
    cursor.execute(f"""
        CREATE STAGE IF NOT EXISTS {stage_name}
        URL = 's3://{S3_BUCKET}/'
        CREDENTIALS = (
            AWS_KEY_ID = '{AWS_ACCESS_KEY_ID}'
            AWS_SECRET_KEY = '{AWS_SECRET_ACCESS_KEY}'
        )
        FILE_FORMAT = (TYPE = PARQUET);
    """)
    logger.info(f"Stage {stage_name} ready")


def create_stg_movies_table(cursor):
    """
    Create the stg_movies staging table in Snowflake if it doesn't exist.
    Column types match the Parquet schema produced by transform.py.
    This is the raw staging table — dbt will build the star schema on top of it.
    """
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS stg_movies (
            movie_id        INTEGER,
            imdb_id         VARCHAR,
            title           VARCHAR,
            original_title  VARCHAR,
            original_language VARCHAR,
            release_date    DATE,
            status          VARCHAR,
            budget          INTEGER,
            revenue         INTEGER,
            runtime         INTEGER,
            vote_average    FLOAT,
            vote_count      INTEGER,
            popularity      FLOAT,
            genre_ids       VARCHAR,
            genre_names     VARCHAR,
            language_codes  VARCHAR,
            language_names  VARCHAR,
            company_ids     VARCHAR,
            company_names   VARCHAR,
            country_codes   VARCHAR,
            overview        VARCHAR,
            tagline         VARCHAR,
            homepage        VARCHAR
        );
    """)
    logger.info("stg_movies table ready")


def load_parquet_to_snowflake(cursor, stage_name: str, source_date: str):
    """
    Use Snowflake's COPY INTO to bulk load the Parquet file from S3
    into stg_movies. This is the fastest way to load data into Snowflake
    and is optimized for Parquet format natively.
    """
    prefix = s3_processed_prefix("movies", source_date)
    parquet_path = f"@{stage_name}/{prefix}movies.parquet"

    logger.info(f"Loading {parquet_path} into stg_movies...")

    cursor.execute(f"""
        COPY INTO stg_movies
        FROM (
            SELECT
                $1:movie_id::INTEGER,
                $1:imdb_id::VARCHAR,
                $1:title::VARCHAR,
                $1:original_title::VARCHAR,
                $1:original_language::VARCHAR,
                $1:release_date::DATE,
                $1:status::VARCHAR,
                $1:budget::INTEGER,
                $1:revenue::INTEGER,
                $1:runtime::INTEGER,
                $1:vote_average::FLOAT,
                $1:vote_count::INTEGER,
                $1:popularity::FLOAT,
                $1:genre_ids::VARCHAR,
                $1:genre_names::VARCHAR,
                $1:language_codes::VARCHAR,
                $1:language_names::VARCHAR,
                $1:company_ids::VARCHAR,
                $1:company_names::VARCHAR,
                $1:country_codes::VARCHAR,
                $1:overview::VARCHAR,
                $1:tagline::VARCHAR,
                $1:homepage::VARCHAR
            FROM {parquet_path}
        )
        FILE_FORMAT = (TYPE = PARQUET)
        ON_ERROR = CONTINUE
        FORCE = TRUE;
    """)

    results = cursor.fetchall()
    for row in results:
        logger.info(f"COPY INTO result: {row}")


def run_load(source_date: str = None) -> None:
    """
    Main load function:
    1. Connect to Snowflake
    2. Create S3 external stage if not exists
    3. Create stg_movies table if not exists
    4. COPY INTO stg_movies from Parquet file in S3
    """
    if source_date is None:
        source_date = datetime.utcnow().strftime("%Y-%m-%d")

    logger.info(f"Starting load for date: {source_date}")

    conn = get_snowflake_connection()
    cursor = conn.cursor()

    try:
        cursor.execute(f"USE DATABASE {SF_DATABASE}")
        cursor.execute(f"USE SCHEMA {SF_SCHEMA}")
        stage_name = "tmdb_s3_stage"
        create_stage(cursor, stage_name)
        create_stg_movies_table(cursor)
        cursor.execute("TRUNCATE TABLE stg_movies")
        load_parquet_to_snowflake(cursor, stage_name, source_date)
        conn.commit()
        logger.info("Load complete.")
    except Exception as e:
        logger.error(f"Load failed: {e}")
        raise
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    run_load("2026-03-09")