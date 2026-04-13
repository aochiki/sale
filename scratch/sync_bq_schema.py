
from google.cloud import bigquery
import pandas as pd

client = bigquery.Client()
project = "music-sales-project"
dataset = "sales_aggregator_dataset"

# 1. Get current mappings (unified names)
mappings_ref = f"{project}.{dataset}.unified_columns"
mappings = client.query(f"SELECT unified_name, is_numeric FROM `{mappings_ref}`").to_dataframe()
desired_cols = mappings.to_dict('records')

# 2. Get current table schema
table_ref = f"{project}.{dataset}.unified_sales_data"
table = client.get_table(table_ref)
existing_cols = [field.name for field in table.schema]

# 3. Find missing columns
missing = []
for d in desired_cols:
    name = d['unified_name']
    if name not in existing_cols:
        col_type = "FLOAT64" if d['is_numeric'] else "STRING"
        missing.append((name, col_type))

# Required system columns if not present
for sys_col in ["FILE_NAME", "SOURCE", "uploaded_at", "備考"]:
    if sys_col not in existing_cols:
        missing.append((sys_col, "STRING"))

if not missing:
    print("No missing columns found.")
else:
    for name, col_type in missing:
        sql = f"ALTER TABLE `{project}.{dataset}.unified_sales_data` ADD COLUMN `{name}` {col_type}"
        print(f"Executing: {sql}")
        try:
            client.query(sql).result()
            print("Success.")
        except Exception as e:
            print(f"Failed to add {name}: {e}")
