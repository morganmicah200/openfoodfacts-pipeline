"""
pipeline/load.py
Copies processed Parquet files from S3 into Snowflake stg_products table.

Responsibilities:
- Use Snowflake external stage pointing at S3 processed/ prefix
- Run COPY INTO stg_products from the stage for today's date partition
- Upsert logic: merge on product_id, update all columns, insert new rows
- Log row counts before and after load for auditing
"""

# TODO: implement load()