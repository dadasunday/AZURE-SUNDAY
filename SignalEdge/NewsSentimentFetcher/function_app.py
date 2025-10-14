import azure.functions as func
import logging
import os
import pyodbc  # kept for type hints; not used after switching to pytds
import requests
from datetime import datetime
import json
import pytds


app = func.FunctionApp()

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


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
            validate_host=False,
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

        # Build a unique set of tickers like FOREX:USD, FOREX:EUR, etc.
        unique_tickers = set()
        for base_currency, quote_currency in rows:
            if base_currency:
                unique_tickers.add(f"FOREX:{base_currency}")
            if quote_currency:
                unique_tickers.add(f"FOREX:{quote_currency}")

        logging.info(f"Processing {len(unique_tickers)} unique tickers for news sentiment.")

        # API key
        api_key = os.environ.get('ALPHAVANTAGE_API_KEY')
        if not api_key:
            raise ValueError("ALPHAVANTAGE_API_KEY environment variable is not set.")

        api_template = (
            "https://www.alphavantage.co/query?function=NEWS_SENTIMENT&"
            "tickers={ticker}&sort=LATEST&limit=2000&apikey={apikey}"
        )

        processed_data = []

        for ticker in unique_tickers:
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

                    topics = ", ".join([t.get("topic", "") for t in item.get("topics", []) if t.get("topic")])

                    # If ticker_sentiment exists, use first ticker; else default to current ticker
                    ts = item.get("ticker_sentiment") or []
                    ticker_name = ts[0].get("ticker") if ts and ts[0].get("ticker") else ticker

                    processed_data.append(
                        (
                            published_dt,
                            ticker_name,
                            topics,
                            float(sentiment_score),
                            sentiment_label,
                            float(relevance_score) if relevance_score is not None else 0.0,
                            source,
                            article_url,
                            summary,
                        )
                    )

                logging.info(f"Processed {len(processed_data)} cumulative items (latest ticker {ticker}).")

            except requests.RequestException as e:
                logging.error(f"Request error for {ticker}: {e}")
            except Exception as e:
                logging.error(f"Unexpected error for {ticker}: {e}")

        if not processed_data:
            logging.warning("No data collected to insert.")
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
            except Exception as e:
                logging.error(f"Insert error for {published_dt} {ticker_name}: {e}")

        conn.commit()
        logging.info("News sentiment data inserted successfully.")

    except Exception as e:
        logging.error(f"Unhandled exception: {e}")
        raise
    finally:
        if conn:
            conn.close()
            logging.info("SQL connection closed.")


# Timer trigger: every 3 minutes
@app.timer_trigger(
    schedule="0 */3 * * * *",
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
