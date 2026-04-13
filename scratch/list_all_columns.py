
from google.cloud import bigquery
import sys
sys.stdout.reconfigure(encoding='utf-8')

client = bigquery.Client()
project = "music-sales-project"
dataset = "sales_aggregator_dataset"
table_ref = f"{project}.{dataset}.unified_sales_data"
table = client.get_table(table_ref)

print("ALL COLUMNS in unified_sales_data:")
for field in table.schema:
    print(field.name)
