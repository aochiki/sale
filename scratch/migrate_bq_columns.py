
from google.cloud import bigquery

client = bigquery.Client()
project = "music-sales-project"
dataset = "sales_aggregator_dataset"

queries = [
    f"ALTER TABLE `{project}.{dataset}.unified_sales_data` RENAME COLUMN `印税額` TO `収益` ;",
    f"ALTER TABLE `{project}.{dataset}.unified_sales_data` RENAME COLUMN `印税額(JPY)` TO `収益(JPY)` ;"
]

for q in queries:
    print(f"Executing: {q}")
    try:
        query_job = client.query(q)
        query_job.result()
        print("Success.")
    except Exception as e:
        print(f"Failed: {e}")
