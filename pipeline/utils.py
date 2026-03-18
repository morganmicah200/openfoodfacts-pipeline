import json
import logging

import boto3

from config import (
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    AWS_DEFAULT_REGION,
    S3_BUCKET,
)

logger = logging.getLogger(__name__)


def get_s3_client():
    """Create and return a boto3 S3 client using credentials from config."""
    return boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_DEFAULT_REGION,
    )


def load_batch_from_s3(key: str) -> list[dict]:
    """Load and parse a JSON batch file from S3."""
    s3 = get_s3_client()
    response = s3.get_object(Bucket=S3_BUCKET, Key=key)
    return json.loads(response["Body"].read().decode("utf-8"))