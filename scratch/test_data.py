import os
from dotenv import load_dotenv
import pandas as pd
from aggregator.processor import SalesAggregator
from aggregator.database_bq import DatabaseManager

load_dotenv()
project_id = os.getenv('GOOGLE_CLOUD_PROJECT', 'music-sales-project')

db = DatabaseManager(project_id=project_id, dataset_id="sales_aggregator_dataset")
processor = SalesAggregator()

print("Fetching raw data & mappings...")
raw_df = db.get_raw_data()
mappings = db.get_unified_columns()

print("Unifying...")
unified_df = processor.unify_raw_records(raw_df, mappings)

if 'アーティスト名' in unified_df.columns:
    artists = unified_df['アーティスト名'].dropna().unique()
    print("Unique artists in unified_df:")
    for a in artists[:20]:
        print(f" - {a}")
else:
    print("Column 'アーティスト名' not found!")
    print(f"Columns: {unified_df.columns}")
