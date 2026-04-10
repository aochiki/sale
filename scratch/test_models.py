import os
from dotenv import load_dotenv
from google import genai
from google.genai import types

load_dotenv()
api_key = os.getenv('GEMINI_API_KEY', '')

client = genai.Client(api_key=api_key)

print("Listing models:")
for m in client.models.list():
    if "flash" in m.name:
        print(m.name)
