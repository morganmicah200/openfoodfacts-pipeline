"""
pipeline/extract.py
Fetches product data from the Open Food Facts API and writes raw JSON to S3.

Responsibilities:
- Paginate through each category in config.OFF_CATEGORIES
- Respect rate limits with a small sleep between requests
- Write one JSON file per category/page to S3 raw/products/YYYY-MM-DD/
- Return a manifest of files written (for validate.py to consume)
"""

import json
import logging
from datetime import datetime

import requests
import boto3
from botocore.exceptions import ClientError

from config import (
    OFF_BASE_URL,
    OFF_PAGE_SIZE,
    OFF_CATEGORIES,
    OFF_USER_AGENT,
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    AWS_DEFAULT_REGION,
    S3_BUCKET,
    s3_raw_prefix,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def fetch_products_by_category(category: str, page: int) -> dict:
    """Fetch one page of products for a given category from the OFF API."""
    url = f"{OFF_BASE_URL}/search"
    params = {
        "categories_tags": category,
        "fields": (
            "code,product_name,brands,categories,categories_tags,"
            "countries,countries_tags,nutriscore_grade,ecoscore_grade,"
            "energy-kcal_100g,fat_100g,saturated-fat_100g,carbohydrates_100g,"
            "sugars_100g,proteins_100g,salt_100g,fiber_100g"
        ),
        "page_size": OFF_PAGE_SIZE,
        "page": page,
        "json": 1,
    }
    headers = {"User-Agent": OFF_USER_AGENT}

    response = requests.get(url, params=params, headers=headers, timeout=60)
    response.raise_for_status()
    return response.json()


def extract_all_products() -> list[dict]:
    """Fetch all products across all configured categories."""
    all_products = []
    seen_ids = set()

    for category in OFF_CATEGORIES:
        logger.info(f"Extracting category: {category}")
        page = 1

        while True:
            try:
                data = fetch_products_by_category(category, page)
                products = data.get("products", [])

                if not products:
                    logger.info(f"  {category}: no more products at page {page}")
                    break

                new_products = []
                for p in products:
                    product_id = p.get("code")
                    if product_id and product_id not in seen_ids:
                        seen_ids.add(product_id)
                        new_products.append(p)

                all_products.extend(new_products)
                logger.info(
                    f"  {category} page {page}: {len(new_products)} new products "
                    f"(total: {len(all_products)})"
                )

                if len(products) < OFF_PAGE_SIZE:
                    break

                page += 1

            except requests.RequestException as e:
                logger.error(f"  {category} page {page} failed: {e}")
                break

    return all_products


def upload_to_s3(products: list[dict], source_date: str) -> str:
    """Upload extracted products as JSON to S3 raw layer."""
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_DEFAULT_REGION,
    )

    prefix = s3_raw_prefix(source_date)
    key = f"{prefix}products.json"

    payload = json.dumps(products, ensure_ascii=False)

    try:
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=key,
            Body=payload.encode("utf-8"),
            ContentType="application/json",
        )
        s3_path = f"s3://{S3_BUCKET}/{key}"
        logger.info(f"Uploaded {len(products)} products to {s3_path}")
        return s3_path

    except ClientError as e:
        logger.error(f"S3 upload failed: {e}")
        raise


def run_extract(source_date: str = None) -> str:
    """Main entry point for the extract step."""
    if source_date is None:
        source_date = datetime.utcnow().strftime("%Y-%m-%d")

    logger.info(f"Starting extraction for date: {source_date}")
    products = extract_all_products()

    if not products:
        raise ValueError("No products extracted — aborting upload")

    s3_path = upload_to_s3(products, source_date)
    logger.info(f"Extract complete. {len(products)} products at {s3_path}")
    return s3_path


if __name__ == "__main__":
    run_extract()