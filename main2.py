import os
import requests
import pandas as pd
from newspaper import Article
from dotenv import load_dotenv
from datetime import datetime, timedelta
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# Load environment variables
load_dotenv()
api_key = os.getenv("API_KEY")
sf_account = os.getenv("SF_ACCOUNT")
sf_user = os.getenv("SF_USER")
sf_password = os.getenv("SF_PASSWORD")
sf_warehouse = os.getenv("SF_WAREHOUSE")
sf_database = os.getenv("SF_DATABASE")
sf_schema = os.getenv("SF_SCHEMA", "PUBLIC")
sf_table = os.getenv("SF_TABLE")
sources = 'the-wall-street-journal,bloomberg,business-insider,fortune'

# Extract full article text
def extract_full_article(article_url):
    try:
        article = Article(article_url)
        article.download()
        article.parse()
        return article.text
    except Exception as e:
        print(f"❌ Failed to parse {article_url}: {e}")
        return None

# Prepare date loop
start_date = datetime.strptime("2025-04-01", "%Y-%m-%d")
end_date = datetime.strptime("2025-04-22", "%Y-%m-%d")

# Prepare list to collect all articles
all_articles = []

# Loop day by day
current_date = start_date
while current_date <= end_date:
    from_date = current_date.strftime("%Y-%m-%d")
    to_date = (current_date + timedelta(days=1)).strftime("%Y-%m-%d")

    url = (
        f"https://newsapi.org/v2/everything?"
        f"sources={sources}&from={from_date}&to={to_date}"
        f"&language=en&apiKey={api_key}"
    )
    print(f"📥 Fetching: {url}")
    response = requests.get(url)
    data = response.json()

    if response.status_code != 200 or "articles" not in data:
        print(f"❌ Error: {data}")
        break

    articles = data["articles"]
    if not articles:
        break

    for art in articles:
        full_text = extract_full_article(art["url"])
        all_articles.append({
            "source": art["source"]["name"],
            "author": art.get("author"),
            "title": art.get("title"),
            "description": art.get("description"),
            "url": art.get("url"),
            "publishedAt": art.get("publishedAt"),
            "content_newsapi": art.get("content"),
            "content_scraped": full_text
        })

    current_date += timedelta(days=1)

# Convert to DataFrame and save
df = pd.DataFrame(all_articles)
df.to_csv("articles.csv", index=False)
print(df[["title", "url", "content_scraped"]].head())

# Connect to Snowflake
conn = snowflake.connector.connect(
    account=sf_account,
    user=sf_user,
    password=sf_password,
    warehouse=sf_warehouse,
    database=sf_database,
    schema=sf_schema
)

# Create table if not exists
columns_sql = ", ".join([f'"{col}" STRING' for col in df.columns])
create_table_sql = f'CREATE TABLE IF NOT EXISTS "{sf_schema}"."{sf_table}" ({columns_sql});'

with conn.cursor() as cur:
    cur.execute(f'USE DATABASE {sf_database}')
    cur.execute(f'USE WAREHOUSE {sf_warehouse}')
    cur.execute(f'USE SCHEMA {sf_schema}')
    cur.execute(create_table_sql)

# Write data
success, nchunks, nrows, _ = write_pandas(conn, df, sf_table, schema=sf_schema)
print(f"✅ Inserted {nrows} rows into {sf_table}. Success: {success}")
conn.close()
