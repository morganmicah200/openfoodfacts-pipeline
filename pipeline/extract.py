"""
pipeline/extract.py
Fetches product data from the Open Food Facts API and writes raw JSON to S3.

Responsibilities:
- Paginate through each category in config.OFF_CATEGORIES
- Respect rate limits with a small sleep between requests
- Write one JSON file per category/page to S3 raw/products/YYYY-MM-DD/
- Return a manifest of files written (for validate.py to consume)
"""

# TODO: implement extract()