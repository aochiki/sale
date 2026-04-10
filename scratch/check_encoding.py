from google.cloud import bigquery
import json

client = bigquery.Client(project='music-sales-project')
dataset_id = 'sales_aggregator_dataset'
table_id = f'music-sales-project.{dataset_id}.unified_columns'

rows = client.list_rows(table_id)
for row in rows:
    print(f"Original: {row.unified_name}")
    try:
        # CP932で化けている可能性を考慮
        decoded = row.unified_name.encode('latin1').decode('cp932')
        print(f"Decoded (latin1 -> cp932): {decoded}")
    except:
        pass
    try:
        decoded = row.unified_name.encode('utf-8').decode('utf-8')
        print(f"Decoded (utf-8): {decoded}")
    except:
        pass
