import os
import requests
import pandas as pd
from newspaper import Article
from dotenv import load_dotenv
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

sources= 'the-wall-street-journal,bloomberg,business-insider,fortune'


from_date = "2025-04-01"
to_date = "2025-04-22"
# loop this from the date range
url = f"https://newsapi.org/v2/everything?sources={sources}&from={from_date}&to={to_date}&?source&language=en&apiKey={api_key}"
print(url)
response = requests.get(url)
data = response.json()
articles = data.get("articles", [])

# Step 2: Extract full content using newspaper3k
def extract_full_article(article_url):
    try:
        article = Article(article_url)
        article.download()
        article.parse()
        return article.text
    except Exception as e:
        print(f"❌ Failed to parse {article_url}: {e}")
        return None

enriched_articles = []
for art in articles:
    full_text = extract_full_article(art["url"])
    enriched_articles.append({
        "source": art["source"]["name"],
        "author": art.get("author"),
        "title": art.get("title"),
        "description": art.get("description"),
        "url": art.get("url"),
        "publishedAt": art.get("publishedAt"),
        "content_newsapi": art.get("content"),
        "content_scraped": full_text
    })

# Step 3: Convert to DataFrame and save to CSV
df = pd.DataFrame(enriched_articles)
df.to_csv("articles.csv", index=False)
print(df[["title", "url", "content_scraped"]].head())

# Step 4: Write to Snowflake
conn = snowflake.connector.connect(
    account=sf_account,
    user=sf_user,
    password=sf_password,
    warehouse=sf_warehouse,
    database=sf_database,
    schema=sf_schema
)

# Create table if not exists (optional)
columns_sql = ", ".join([f'"{col}" STRING' for col in df.columns])
create_table_sql = f'CREATE TABLE IF NOT EXISTS "{sf_schema}"."{sf_table}" ({columns_sql});'

with conn.cursor() as cur:
    cur.execute(f'USE DATABASE {sf_database}')
    cur.execute(f'USE WAREHOUSE {sf_warehouse}')
    cur.execute(f'USE SCHEMA {sf_schema}')
    cur.execute(create_table_sql)

# Insert into Snowflake
success, nchunks, nrows, _ = write_pandas(conn, df, sf_table, schema=sf_schema)
print(f"✅ Inserted {nrows} rows into {sf_table}. Success: {success}")
conn.close()
