from google.cloud import bigquery
import os

project_id = "music-sales-project"
dataset_id = "sales_aggregator_dataset"
table_id = f"{project_id}.{dataset_id}.raw_sales_data"

client = bigquery.Client(project=project_id, location="asia-northeast1")
try:
    client.delete_table(table_id, not_found_ok=True)
    print(f"Table {table_id} deleted successfully.")
except Exception as e:
    print(f"Error deleting table: {e}")
