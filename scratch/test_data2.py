import os
from dotenv import load_dotenv
import pandas as pd
from aggregator.processor import SalesAggregator
from aggregator.database_bq import DatabaseManager

load_dotenv()
project_id = os.getenv('GOOGLE_CLOUD_PROJECT', 'music-sales-project')

db = DatabaseManager(project_id=project_id, dataset_id="sales_aggregator_dataset")
processor = SalesAggregator()

# Bypass Service Account issues in DB? Actually DB gets ADC by default
print("Fetching raw data count...")
df = db.get_raw_data()
print("Data loaded. Count:", len(df))
