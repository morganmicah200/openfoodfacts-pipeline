"""
pipeline/transform.py
Reads staged JSON from S3, casts types, normalizes fields, writes Parquet to S3 processed layer.

Responsibilities:
- Cast nutriment strings to float (using pd.to_numeric with errors='coerce')
- Extract first value from comma-separated brand / category / country strings
- Normalize nutriscore_grade and ecoscore_grade to lowercase
- Add extracted_at and source_date audit columns
- Write to S3 processed/products/YYYY-MM-DD/ as Parquet (snappy compression)
"""

# TODO: implement transform()