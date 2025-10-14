import azure.functions as func
import logging
import os
import pyodbc  # kept for type hints; not used after switching to pytds
import requests
from datetime import datetime
import json
import pytds
import certifi


app = func.FunctionApp()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
# Quiet verbose driver logs in production
logging.getLogger('pytds').setLevel(logging.WARNING)


def get_db_params():
    """Get SQL Server connection params from environment variables."""
    server = os.environ.get('SQL_SERVER')
    database = os.environ.get('SQL_DATABASE')
    username = os.environ.get('SQL_USERNAME')
    password = os.environ.get('SQL_PASSWORD')

    if not all([server, database, username, password]):
        raise ValueError("Missing required database credentials in environment variables")

    return server, database, username, password


def fetch_and_store_news_sentiment():
    """Fetch Alpha Vantage NEWS_SENTIMENT and store into SQL Server staging table."""
    conn = None
    try:
        logging.info("Connecting to SQL Server via pytds (no ODBC dependency)...")
        server, database, username, password = get_db_params()
        conn = pytds.connect(
            server=server,
            database=database,
            user=username,
            password=password,
            port=1433,
            autocommit=False,
            timeout=30,
            cafile=certifi.where(),
        )
        cursor = conn.cursor()
        logging.info("Connected to SQL Server.")

        # Fetch currency pairs
        cursor.execute(
            """
            SELECT DISTINCT BaseCurrency, QuoteCurrency
            FROM CurrencyPairs
            WHERE BaseCurrency IS NOT NULL AND QuoteCurrency IS NOT NULL
            """
        )
        rows = cursor.fetchall()
        if not rows:
            logging.warning("No currency pairs found.")
            return

        currency_pairs = rows
        logging.info(f"Found {len(currency_pairs)} currency pairs for sentiment processing.")

        # API key
        api_key = os.environ.get('ALPHAVANTAGE_API_KEY')
        if not api_key:
            raise ValueError("ALPHAVANTAGE_API_KEY environment variable is not set.")

        api_template = (
            "https://www.alphavantage.co/query?function=NEWS_SENTIMENT&"
            "tickers={ticker}&sort=LATEST&limit=2000&apikey={apikey}"
        )

        processed_data = []

        # Step 3: Fetch Market News & Sentiment Data from API
        for base_currency, quote_currency in currency_pairs:
            tickers = [f"FOREX:{base_currency}", f"FOREX:{quote_currency}"]
            tickers = list(set(tickers))

            for ticker in tickers:
                try:
                    url = api_template.format(ticker=ticker, apikey=api_key)
                    logging.debug(f"Fetching NEWS_SENTIMENT for {ticker}")
                    resp = requests.get(url, timeout=15)
                    if resp.status_code != 200:
                        logging.error(f"AlphaVantage API error {resp.status_code} for {ticker}")
                        continue

                    payload = resp.json()
                    feed = payload.get("feed", [])
                    if not feed:
                        logging.info(f"No news feed items for {ticker}")
                        continue

                    for item in feed:
                        published_at = item.get("time_published")
                        sentiment_score = item.get("overall_sentiment_score")
                        sentiment_label = item.get("overall_sentiment_label")
                        relevance_score = item.get("relevance_score", 0)
                        source = item.get("source")
                        article_url = item.get("url")
                        summary = item.get("summary")

                        if not published_at or sentiment_score is None or not sentiment_label:
                            continue

                        try:
                            published_dt = datetime.strptime(published_at, "%Y%m%dT%H%M%S")
                        except ValueError:
                            logging.error(f"Invalid time_published format: {published_at}")
                            continue

                        # Extract topics
                        topics = ", ".join([t.get("topic", "") for t in item.get("topics", []) if t.get("topic")])

                        # Extract ticker sentiment - loop through all ticker_sentiment items
                        for ticker_info in item.get("ticker_sentiment", []):
                            ticker_name = ticker_info.get("ticker", "N/A")
                            if not ticker_name.startswith("FOREX:"):
                                continue  # Skip non-FOREX tickers like CRYPTO:BTC or NASDAQ:AAPL
                            ticker_sentiment_score = float(ticker_info.get("ticker_sentiment_score", 0))
                            ticker_sentiment_label = ticker_info.get("ticker_sentiment_label", "N/A")
                            relevance = float(ticker_info.get("relevance_score", 0))

                            processed_data.append(
                                (
                                    published_dt,
                                    ticker_name,
                                    topics,
                                    ticker_sentiment_score,
                                    ticker_sentiment_label,
                                    relevance,
                                    source,
                                    article_url,
                                    summary,
                                )
                            )

                    logging.info(f"Successfully processed {len(processed_data)} records for {ticker}")

                except requests.RequestException as e:
                    logging.error(f"Request error for {ticker}: {e}")
                except Exception as e:
                    logging.error(f"Unexpected error for {ticker}: {e}")

        logging.info(f"Total processed records: {len(processed_data)}")

        if not processed_data:
            logging.warning("No data to insert.")
            return

        # Insert into staging table, skipping existing PublishedAt+Ticker
        insert_sql = (
            """
            INSERT INTO [dbo].[Staging_NewsSentiment]
            (PublishedAt, Ticker, Topics, SentimentScore, SentimentLabel,
             RelevanceScore, Source, ArticleURL, Summary)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
        )

        inserted_count = 0  # Track how many were inserted

        # Prepare existence check using both PublishedAt and Ticker to avoid collisions
        for rec in processed_data:
            published_dt, ticker_name = rec[0], rec[1]
            try:
                cursor.execute(
                    """
                    SELECT 1 FROM [dbo].[Staging_NewsSentiment]
                    WHERE PublishedAt = %s AND Ticker = %s
                    """,
                    (published_dt, ticker_name),
                )
                if cursor.fetchone():
                    continue
                cursor.execute(insert_sql, rec)
                inserted_count += 1
            except Exception as e:
                logging.error(f"Insert error for {published_dt} {ticker_name}: {e}")

        conn.commit()
        logging.info(f"{inserted_count} new records inserted into Market News & Sentiment.")

    except Exception as e:
        logging.error(f"Unhandled exception: {e}")
        raise
    finally:
        if conn:
            conn.close()
            logging.info("SQL connection closed.")


# Timer trigger: hourly
@app.timer_trigger(
    schedule="0 0 * * * *",
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=False,
)
def NewsSentimentFetcherTimer(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('Timer is past due!')
    logging.info('NewsSentimentFetcher timer trigger started.')
    fetch_and_store_news_sentiment()
    logging.info('NewsSentimentFetcher timer trigger completed.')


# HTTP trigger for manual execution
@app.route(route="NewsSentimentFetcher", auth_level=func.AuthLevel.FUNCTION)
def NewsSentimentFetcherHttp(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('NewsSentimentFetcher HTTP trigger started.')
    try:
        fetch_and_store_news_sentiment()
        return func.HttpResponse("News sentiment fetch completed successfully!", status_code=200)
    except Exception as e:
        logging.error(f"HTTP trigger error: {e}")
        return func.HttpResponse(f"Error occurred: {str(e)}", status_code=500)
