import os
from dotenv import load_dotenv
from aggregator.ai_query import parse_natural_language_query

load_dotenv()
project_id = os.getenv('GOOGLE_CLOUD_PROJECT', '')
api_key = os.getenv('GEMINI_API_KEY', '')

print(f"Project ID: {project_id}")
print(f"API Key start: {api_key[:5] if api_key else 'None'}")

unified_columns = ['アーティスト名', '曲名', 'ISRC', '数量']
num_cols = ['数量']
user_text = "かりゆし58のISRCと数量"

print("Calling AI...")
result = parse_natural_language_query(project_id, user_text, unified_columns, num_cols, api_key=api_key)
print(f"Result: {result}")
