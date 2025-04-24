# FWDDemo

This project fetches articles from the NewsAPI, extracts full article content using the `newspaper` library, and stores the data in a Snowflake database. The project also saves the articles in a CSV file for local storage.

## Features

- Fetches articles from multiple sources using the NewsAPI.
- Extracts full article content from URLs.
- Saves articles to a CSV file.
- Inserts articles into a Snowflake database.

## Prerequisites

- Python 3.8 or higher
- A Snowflake account
- A NewsAPI key

## Installation

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd FWDDemo
   ```

2. Install the required Python packages:
   ```bash
   pip install -r requirements.txt
   ```

3. Create a `.env` file in the project directory with the following variables:
   ```env
   SF_ACCOUNT=<your_snowflake_account>
   SF_USER=<your_snowflake_username>
   SF_PASSWORD=<your_snowflake_password>
   SF_WAREHOUSE=<your_snowflake_warehouse>
   SF_DATABASE=<your_snowflake_database>
   SF_SCHEMA=<your_snowflake_schema>
   SF_TABLE=<your_snowflake_table>
   API_KEY=<your_newsapi_key>
   ```

## Usage

1. Run the script:
   ```bash
   python main2.py
   ```

2. The script will:
   - Fetch articles from the NewsAPI for the specified date range.
   - Extract full article content.
   - Save the articles to `articles.csv`.
   - Insert the articles into the specified Snowflake table.

## Requirements

The following Python packages are required and listed in `requirements.txt`:

- `snowflake-connector-python`
- `snowflake-connector-python[pandas]`
- `newspaper`
- `lxml_html_clean`
- `openpyxl`

## File Structure

- `main2.py`: The main script for fetching, processing, and storing articles.
- `articles.csv`: The CSV file where articles are saved locally.
- `.env`: Environment variables for API keys and Snowflake credentials.
- `requirements.txt`: List of required Python packages.

## License

This project is licensed under the MIT License. See the `LICENSE` file for details.

## Acknowledgments

- [NewsAPI](https://newsapi.org/) for providing the article data.
- [Snowflake](https://www.snowflake.com/) for cloud data storage.
- [Newspaper](https://newspaper.readthedocs.io/) for article content extraction.