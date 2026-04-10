import os
from dotenv import load_dotenv
from aggregator.ai_query import parse_natural_language_query
import time
import logging

logging.basicConfig(level=logging.INFO)

load_dotenv()
project_id = os.getenv('GOOGLE_CLOUD_PROJECT', '')
api_key = os.getenv('GEMINI_API_KEY', '')

unified_columns = ['アーティスト名', '曲名', 'ISRC', '数量']
num_cols = ['数量']
user_text = "かりゆし58のISRCと数量"

print("Calling AI...")
t0 = time.time()
result = parse_natural_language_query(project_id, user_text, unified_columns, num_cols, api_key=api_key)
print(f"Time taken: {time.time() - t0:.2f}s")
print(f"Result: {result}")
