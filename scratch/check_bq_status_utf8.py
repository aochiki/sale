
from google.cloud import bigquery
import pandas as pd
import sys

# Set stdout to UTF-8
sys.stdout.reconfigure(encoding='utf-8')

client = bigquery.Client()
project = "music-sales-project"
dataset = "sales_aggregator_dataset"

# Check unified_sales_data schema
table_ref = f"{project}.{dataset}.unified_sales_data"
table = client.get_table(table_ref)
print(f"Table: {table_ref}")
print("Columns in unified_sales_data:")
col_names = [field.name for field in table.schema]
for name in col_names:
    print(f"  - {name}")

# Check unified_columns mappings
mappings_ref = f"{project}.{dataset}.unified_columns"
mappings = client.query(f"SELECT unified_name FROM `{mappings_ref}`").to_dataframe()
names = mappings['unified_name'].tolist()
print(f"\nMappings from {mappings_ref}:")
for n in names:
    print(f"  - {n}")

# Check for specific terminology mismatch
has_inzei = "印税額" in col_names or "印税額(JPY)" in col_names
has_shueki = "収益" in col_names or "収益(JPY)" in col_names

print(f"\nResult: inzei={has_inzei}, shueki={has_shueki}")
