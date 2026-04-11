import os
from google.cloud import bigquery
import json
from dotenv import load_dotenv

load_dotenv()
project_id = os.getenv('GOOGLE_CLOUD_PROJECT', 'nippo-app-491512')
client = bigquery.Client(project=project_id)

query = f"""
    SELECT * 
    FROM `{project_id}.sales_aggregator_dataset.unified_sales_data` 
    WHERE SOURCE = 'ORCHARD' 
    LIMIT 1
"""

try:
    results = client.query(query).to_dataframe()
    with open('scratch/orchard_headers_result.txt', 'w', encoding='utf-8') as f:
        if not results.empty:
            row = results.iloc[0]
            f.write("--- Orchard Data Sample ---\n")
            # 備考以外の列も何が入っているか確認
            f.write(json.dumps(row.to_dict(), indent=2, ensure_ascii=False, default=str))
        else:
            f.write("Orchardのデータが見つかりませんでした。")
except Exception as e:
    with open('scratch/orchard_headers_result.txt', 'w', encoding='utf-8') as f:
        f.write(f"Error: {e}")
