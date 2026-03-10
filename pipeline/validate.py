"""
pipeline/validate.py
Reads raw JSON from S3, runs validation checks, writes passing records to S3 staged layer.

Responsibilities:
- Required fields present: product_id (barcode), product_name
- Nutriscore grade is one of: a, b, c, d, e, or null (never an unexpected value)
- Numeric fields (energy, fat, etc.) are castable to float or null — reject strings like "N/A"
- Write valid records to S3 staged/products/YYYY-MM-DD/
- Log rejection counts per category for data quality tracking
"""

# TODO: implement validate()