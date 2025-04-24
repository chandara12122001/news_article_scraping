import requests
import os
from dotenv import load_dotenv
import pandas as pd

load_dotenv()
api_key = os.getenv("API_KEY")

url = f"https://newsapi.org/v2/sources?apiKey={api_key}"
response = requests.get(url)
data = response.json()

sources = data.get("sources", [])
df = pd.json_normalize(sources)
df.to_excel('sources.xlsx', index=False)
# Print distinct source names
print(df["name"].unique())