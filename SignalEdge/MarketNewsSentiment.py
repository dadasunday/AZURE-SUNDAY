
import pyodbc
import json
import logging
import requests
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("script_debug.log"),
        logging.StreamHandler()
    ]
)

# SQL Server connection details
server = 'hypv8669.hostedbyappliedi.net'
database = 'TradingForeignCurrency'
username = 'ali_muwwakkil'
password = 'ali00250025'
connection_string = (
    f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};'
    f'DATABASE={database};UID={username};PWD={password}'
)



# API Endpoint
api_endpoint = "https://www.alphavantage.co/query?function=NEWS_SENTIMENT&tickers={tickers}&sort=LATEST&limit=2000&apikey=IOX4MQY1X8GSVZ81"

def fetch_and_store_news_sentiment():
    try:
        # Step 1: Connect to SQL Server
        logging.info("Attempting to connect to SQL Server...")
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        logging.info("Connected to SQL Server successfully!")


        # Step 2: Fetch currency pairs from SQL Server
        cursor.execute("""
            SELECT DISTINCT BaseCurrency, QuoteCurrency
            FROM CurrencyPairs
            WHERE BaseCurrency IS NOT NULL AND QuoteCurrency IS NOT NULL
        """)
        currency_pairs = cursor.fetchall()

        if not currency_pairs:
            logging.warning("No currency pairs found in the database!")

        logging.info(f"Found {len(currency_pairs)} currency pairs for sentiment processing.")

        processed_data = []

        # Step 3: Fetch Market News & Sentiment Data from API
        for base_currency, quote_currency in currency_pairs:
            tickers = [f"FOREX:{base_currency}", f"FOREX:{quote_currency}"]
            tickers = list(set(tickers))

            for ticker in tickers:
                try:
                    formatted_api_endpoint = api_endpoint.format(tickers=ticker)

                    logging.debug(f"Fetching data from API: {formatted_api_endpoint}")
                    response = requests.get(formatted_api_endpoint)

                    if response.status_code != 200:
                        logging.error(f"API request failed with status code {response.status_code}")
                        continue

                    api_data = response.json()

                    if "feed" not in api_data or not api_data["feed"]:
                        logging.warning(f"No news data found for ticker: {ticker}")
                        continue

                    for record in api_data.get("feed", []):
                        #article_id = record.get("title", "N/A")  # Assuming title as unique ID (Modify as needed)
                        published_at = record.get("time_published")
                        sentiment_score = record.get("overall_sentiment_score")
                        sentiment_label = record.get("overall_sentiment_label")
                        relevance_score = record.get("relevance_score", 0)  # Default to 0 if not available
                        source = record.get("source")
                        article_url = record.get("url")
                        summary = record.get("summary")

                        if not published_at or sentiment_score is None or not sentiment_label:
                            continue  # Skip incomplete records

                        try:
                            published_at = datetime.strptime(published_at, "%Y%m%dT%H%M%S")
                        except ValueError:
                            logging.error(f"Invalid date format in response: {published_at}")
                            continue

                        # Extract topics
                        topics = ", ".join([topic["topic"] for topic in record.get("topics", [])])

                        # Extract ticker sentiment
                        for ticker_info in record.get("ticker_sentiment", []):
                            ticker_name = ticker_info.get("ticker", "N/A")
                            if not ticker_name.startswith("FOREX:"):
                                continue  # Skip non-FOREX tickers like CRYPTO:BTC or NASDAQ:AAPL
                            ticker_sentiment_score = float(ticker_info.get("ticker_sentiment_score", 0))
                            ticker_sentiment_label = ticker_info.get("ticker_sentiment_label", "N/A")
                            relevance = float(ticker_info.get("relevance_score", 0))

                            processed_data.append((
                                published_at,
                                ticker_name,
                                topics,
                                ticker_sentiment_score,
                                ticker_sentiment_label,
                                relevance,
                                source,
                                article_url,
                                summary
                            ))


                    logging.info(f"Successfully processed {len(processed_data)} records for {ticker}")

                except requests.exceptions.RequestException as e:
                    logging.error(f"API request error for ticker {ticker}: {e}")
                except Exception as e:
                    logging.error(f"Unexpected error while processing ticker {ticker}: {e}")

        logging.info(f"Total processed records: {len(processed_data)}")

        # Step 4: Insert data into SQL Server
        if processed_data:
            insert_query = """
            INSERT INTO [dbo].[Staging_NewsSentiment] 
            (PublishedAt, Ticker, Topics, SentimentScore, SentimentLabel, 
            RelevanceScore, Source, ArticleURL, Summary)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            inserted_count = 0  # Track how many were inserted

            for record in processed_data:
                published_at = record[0]  # Extract PublishedAt (first item in tuple)
                ticker_name = record[1]
                

                # Skip if already exists
                cursor.execute("""
                    SELECT 1 FROM [dbo].[Staging_NewsSentiment]
                    WHERE PublishedAt = ? AND Ticker = ? 
                """, published_at, ticker_name)

                if not cursor.fetchone():
                    try:
                        cursor.execute(insert_query, record)
                        inserted_count += 1
                    except Exception as e:
                        logging.error(f"Error inserting record at {published_at}for {ticker_name}: {e}")


            conn.commit()
            logging.info(f"{inserted_count} new records inserted into Market News & Sentiment.")

        else:
            logging.warning("No data to insert into Market News & Sentiment.")

    except Exception as e:
        logging.error(f"Unhandled exception: {e}")

    finally:
        if 'conn' in locals() and conn:
            conn.close()
            logging.info("SQL Server connection closed.")
    

import schedule
import time

def run_scheduled_task():
    logging.info("Running scheduled forex data fetch...")
    fetch_and_store_news_sentiment()

if __name__ == "__main__":
    run_scheduled_task()  # Run once immediately
    schedule.every(2).minutes.do(run_scheduled_task)
    logging.info("Scheduler started. Running every 2 minutes.")
    while True:
        schedule.run_pending()
        time.sleep(10)
    