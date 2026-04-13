
from google.cloud import bigquery
import pandas as pd

client = bigquery.Client()
project = "music-sales-project"
dataset = "sales_aggregator_dataset"

# Check unified_sales_data schema
table_ref = f"{project}.{dataset}.unified_sales_data"
table = client.get_table(table_ref)
print(f"Table: {table_ref}")
print("Columns in unified_sales_data:")
for field in table.schema:
    print(f"  - {field.name} ({field.field_type})")

# Check unified_columns mappings
mappings_ref = f"{project}.{dataset}.unified_columns"
mappings = client.query(f"SELECT unified_name FROM `{mappings_ref}`").to_dataframe()
print(f"\nMappings from {mappings_ref}:")
print(mappings['unified_name'].tolist())
