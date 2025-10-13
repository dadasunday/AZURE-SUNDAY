import azure.functions as func
import logging
import pyodbc
import requests
import json
import os
import concurrent.futures
from datetime import datetime
from pytz import timezone, utc

app = func.FunctionApp()

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def get_connection_string():
    """Get SQL Server connection string from environment variables"""
    server = os.environ.get('SQL_SERVER', 'hypv8669.hostedbyappliedi.net')
    database = os.environ.get('SQL_DATABASE', 'TradingForeignCurrency')
    username = os.environ.get('SQL_USERNAME', 'ali_muwwakkil')
    password = os.environ.get('SQL_PASSWORD', 'ali00250025')

    return (
        f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};'
        f'DATABASE={database};UID={username};PWD={password}'
    )

def fetch_and_store_market_data():
    """Main function to fetch and store forex market data"""
    try:
        # Step 1: Connect to SQL Server
        logging.info("Attempting to connect to SQL Server...")
        connection_string = get_connection_string()
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        logging.info("Connected to SQL Server successfully!")

        # Step 2: Fetch active resources
        cursor.execute("""
            SELECT *
            FROM ResourceRegistry
            WHERE IsActive = 1
        """)
        resources = cursor.fetchall()
        logging.info(f"Fetched {len(resources)} active resources.")

        if not resources:
            logging.warning("No active resources found. Exiting...")
            return

        # Step 3: Process each resource
        for resource in resources:
            try:
                # Unpack the entire resource tuple
                (
                    resource_id,
                    resource_name,
                    type_id,
                    description,
                    source,
                    impact_description,
                    registered_date,
                    is_active,
                    target_table,
                    api_function,
                    api_interval,
                    api_endpoint,
                    create_table_sql,
                    requires_looping,
                    cte_query,
                    primary_currency,
                    secondary_currency,
                    impact_weight,
                ) = resource
                logging.info(f"Processing Resource: {resource_name} (ResourceID: {resource_id})")

                # Ensure the table exists
                try:
                    logging.info(f"Creating or verifying table {target_table}...")
                    cursor.execute(create_table_sql)
                    conn.commit()
                    logging.info(f"Table {target_table} verified or created.")
                except Exception as e:
                    logging.error(f"Error creating/verifying table {target_table}: {e}")
                    continue

                # Get the latest timestamp from the target table
                try:
                    # Function to determine the correct timestamp column
                    def get_timestamp_column(cursor, table_name):
                        query = f"""
                        SELECT COLUMN_NAME
                        FROM INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_NAME = '{table_name}'
                        AND COLUMN_NAME IN ('Timestamp', 'PublishedAt')
                        """
                        cursor.execute(query)
                        result = cursor.fetchone()
                        return result[0] if result else None

                    # Get the correct column name
                    timestamp_column = get_timestamp_column(cursor, target_table)

                    cursor.execute(f"SELECT MAX({timestamp_column}) FROM {target_table}")
                    latest_timestamp = cursor.fetchone()[0]
                    latest_timestamp = latest_timestamp.strftime('%Y-%m-%d %H:%M:%S') if latest_timestamp else "1900-01-01 00:00:00"
                    logging.info(f"Latest Timestamp in {target_table}: {latest_timestamp}")
                except Exception as e:
                    logging.error(f"Error fetching latest timestamp from {target_table}: {e}")
                    continue

                processed_data = []

                # TypeID 2: Technical Indicators
                if type_id == 2:
                    try:
                        logging.info(f"Processing {resource_name} data for ResourceID: {resource_id}...")

                        # Split ApiInterval into interval and time_period
                        if api_interval and ':' in api_interval:
                            interval, time_period = api_interval.split(':')
                        else:
                            interval, time_period = api_interval or "daily", "20"

                        series_type = "close"

                        # Fetch currency pairs
                        try:
                            cursor.execute("""
                                SELECT BaseCurrency, QuoteCurrency
                                FROM CurrencyPairs
                                WHERE BaseCurrency IS NOT NULL AND QuoteCurrency IS NOT NULL
                            """)
                            currency_pairs = cursor.fetchall()
                            logging.info(f"Found {len(currency_pairs)} currency pairs for {resource_name} processing.")
                        except Exception as e:
                            logging.error(f"Error fetching currency pairs: {e}")
                            continue

                        # Initialize counters using a dictionary
                        counters = {"processed_records": 0, "merged_records": 0}

                        # Function to process each currency pair
                        def process_currency_pair(pair):
                            base_currency, quote_currency = pair
                            processed_count = 0
                            merged_count = 0

                            try:
                                # Get API key from environment
                                api_key = os.environ.get('ALPHAVANTAGE_API_KEY', 'IOX4MQY1X8GSVZ81')

                                # Format API endpoint
                                formatted_api_endpoint = api_endpoint.format(
                                    symbol=f"{base_currency}{quote_currency}",
                                    interval=interval,
                                    time_period=time_period,
                                    series_type=series_type,
                                    apikey=api_key
                                )
                                logging.debug(f"Formatted API Endpoint: {formatted_api_endpoint}")

                                # Fetch data from API
                                response = requests.get(formatted_api_endpoint, timeout=10)
                                response.raise_for_status()
                                api_data = response.json()

                                # Parse technical analysis data
                                technical_analysis_key = f"Technical Analysis: {api_function.upper()}"
                                technical_analysis = api_data.get(technical_analysis_key, {})

                                if not technical_analysis:
                                    logging.warning(f"No data found for {api_function} with {base_currency}/{quote_currency}")
                                    return 0

                                processed_data = []
                                for date, values in technical_analysis.items():
                                    if date <= latest_timestamp:
                                        continue

                                    try:
                                        indicator_value = values.get(api_function.upper())
                                        if not indicator_value:
                                            continue
                                        processed_data.append((
                                            date,
                                            f"{base_currency}/{quote_currency}",
                                            float(indicator_value),
                                            interval,
                                            int(time_period),
                                            series_type
                                        ))
                                    except Exception as e:
                                        logging.error(f"Error parsing data for {date}: {e}")
                                        continue

                                # Update processed records
                                counters["processed_records"] += len(processed_data)

                                if processed_data:
                                    try:
                                        cursor.executemany(f"""
                                            MERGE INTO {target_table} AS target
                                            USING (VALUES (?, ?, ?, ?, ?, ?)) AS source
                                            (Timestamp, Symbol, {api_function}_Value, Interval, TimePeriod, SeriesType)
                                            ON target.Timestamp = source.Timestamp AND target.Symbol = source.Symbol
                                            WHEN MATCHED THEN
                                                UPDATE SET
                                                    {api_function}_Value = source.{api_function}_Value,
                                                    Interval = source.Interval,
                                                    TimePeriod = source.TimePeriod,
                                                    SeriesType = source.SeriesType
                                            WHEN NOT MATCHED THEN
                                                INSERT (Timestamp, Symbol, {api_function}_Value, Interval, TimePeriod, SeriesType)
                                                VALUES (source.Timestamp, source.Symbol, source.{api_function}_Value, source.Interval, source.TimePeriod, source.SeriesType);
                                        """, processed_data)
                                        conn.commit()

                                        # Update merged records
                                        merged_count = len(processed_data)
                                        counters["merged_records"] += merged_count
                                    except Exception as e:
                                        logging.error(f"Error merging data for {base_currency}/{quote_currency}: {e}")
                                        conn.rollback()

                                return merged_count

                            except requests.exceptions.RequestException as e:
                                logging.error(f"API call failed for {base_currency}/{quote_currency}: {e}")
                                return 0
                            except Exception as e:
                                logging.error(f"Error processing API data for {base_currency}/{quote_currency}: {e}")
                                return 0

                        # Process all currency pairs
                        logging.info("Starting parallel processing of currency pairs...")
                        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                            list(executor.map(process_currency_pair, currency_pairs))

                        # Log final results
                        logging.info(f"Total records processed for {resource_name} with ResourceID {resource_id}: {counters['processed_records']}")
                        logging.info(f"Total records merged for {resource_name} with ResourceID {resource_id}: {counters['merged_records']}")

                    except Exception as e:
                        logging.error(f"Error processing {resource_name} data for ResourceID {resource_id}: {e}")

                # TypeID 1: Real GDP
                if type_id == 1:
                    try:
                        # Fetch column names dynamically
                        cursor.execute(f"SELECT * FROM {target_table} WHERE 1=0")
                        columns = [desc[0] for desc in cursor.description]

                        if len(columns) != 2 or columns[0] != "Timestamp":
                            raise ValueError("Unexpected table structure. The table must have 'Timestamp' as the first column and one other column.")

                        second_column = columns[1]
                    except Exception as e:
                        logging.error(f"Error fetching second column name: {e}")
                        continue
                    try:
                        logging.info(f"Processing {resource_name} data...")
                        response = requests.get(api_endpoint)
                        response.raise_for_status()
                        api_data = response.json()

                        for record in api_data.get("data", []):
                            date = record.get("date")
                            value = record.get("value")
                            if not date or not value or value == ".":
                                logging.warning(f"Skipping {resource_name} invalid record: {record}")
                                continue

                            timestamp = f"{date} 00:00:00"
                            if latest_timestamp is None or timestamp > latest_timestamp:
                                processed_data.append({
                                    "Timestamp": timestamp,
                                    second_column: float(value)
                                })
                        logging.info(f"Processed {len(processed_data)} new {resource_name} records.")
                    except Exception as e:
                        logging.error(f"Error processing {resource_name} data: {e}")

                # TypeID 4: WTI Crude Oil
                if type_id == 4:
                    try:
                        logging.info(f"Processing {resource_name} data...")
                        response = requests.get(api_endpoint)
                        response.raise_for_status()
                        api_data = response.json()
                        for record in api_data.get("data", []):
                            date = record.get("date")
                            value = record.get("value")
                            if not date or not value or value == ".":
                                continue
                            timestamp = f"{date} 00:00:00"
                            if latest_timestamp is None or timestamp > latest_timestamp:
                                processed_data.append({
                                    "Timestamp": timestamp,
                                    "ClosePrice": float(value)
                                })
                        logging.info(f"Processed {len(processed_data)} {resource_name} records.")
                    except Exception as e:
                        logging.error(f"Error processing {resource_name} data: {e}")

                # TypeID 5: FOREX Intraday Exchange Data
                if type_id == 5:
                    try:
                        logging.info(f"Processing {resource_name} ...")
                        cursor.execute("""
                            SELECT DISTINCT BaseCurrency, QuoteCurrency
                            FROM CurrencyPairs
                            WHERE BaseCurrency IS NOT NULL AND QuoteCurrency IS NOT NULL
                        """)
                        currency_pairs = cursor.fetchall()
                        logging.info(f"Found {len(currency_pairs)} currency pairs.")

                        for from_symbol, to_symbol in currency_pairs:
                            try:
                                formatted_api_endpoint = api_endpoint.format(
                                    from_symbol=from_symbol,
                                    to_symbol=to_symbol
                                )
                                logging.info(f"Calling API for {from_symbol}/{to_symbol}: {formatted_api_endpoint}")
                                response = requests.get(formatted_api_endpoint)
                                response.raise_for_status()
                                api_data = response.json()

                                if "Time Series FX (5min)" not in api_data:
                                    logging.warning(f"No Time Series data for {from_symbol}/{to_symbol}.")
                                    continue

                                time_series = api_data["Time Series FX (5min)"]
                                for timestamp, values in time_series.items():
                                    try:
                                        utc_time = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
                                        if latest_timestamp and utc_time <= datetime.strptime(latest_timestamp, "%Y-%m-%d %H:%M:%S"):
                                            continue

                                        cst_time = utc.localize(utc_time).astimezone(timezone('US/Central')).strftime("%Y-%m-%d %H:%M:%S")
                                        processed_data.append({
                                            "Timestamp": cst_time,
                                            "FromSymbol": from_symbol,
                                            "ToSymbol": to_symbol,
                                            "OpenPrice": float(values.get("1. open", 0)),
                                            "HighPrice": float(values.get("2. high", 0)),
                                            "LowPrice": float(values.get("3. low", 0)),
                                            "ClosePrice": float(values.get("4. close", 0))
                                        })
                                    except Exception as e:
                                        logging.error(f"Error processing record for timestamp {timestamp}: {e}")
                            except requests.exceptions.RequestException as e:
                                logging.error(f"API call failed for {from_symbol}/{to_symbol}: {e}")
                                continue
                    except Exception as e:
                        logging.error(f"Error processing {resource_name} : {e}")

                # TypeID 6: FOREX Daily Exchange Data
                if type_id == 6:
                    try:
                        logging.info(f"Processing {resource_name} ...")
                        cursor.execute("""
                            SELECT DISTINCT BaseCurrency, QuoteCurrency
                            FROM CurrencyPairs
                            WHERE BaseCurrency IS NOT NULL AND QuoteCurrency IS NOT NULL
                        """)
                        currency_pairs = cursor.fetchall()
                        logging.info(f"Found {len(currency_pairs)} currency pairs.")

                        for from_symbol, to_symbol in currency_pairs:
                            try:
                                formatted_api_endpoint = api_endpoint.format(
                                    from_symbol=from_symbol,
                                    to_symbol=to_symbol
                                )
                                logging.info(f"Calling API for {from_symbol}/{to_symbol}: {formatted_api_endpoint}")
                                response = requests.get(formatted_api_endpoint)
                                response.raise_for_status()
                                api_data = response.json()

                                if "Time Series FX (Daily)" not in api_data:
                                    logging.warning(f"No Time Series data for {from_symbol}/{to_symbol}.")
                                    continue

                                time_series = api_data["Time Series FX (Daily)"]
                                for date, values in time_series.items():
                                    try:
                                        utc_time = datetime.strptime(date, "%Y-%m-%d")
                                        if latest_timestamp and utc_time <= datetime.strptime(latest_timestamp, "%Y-%m-%d %H:%M:%S"):
                                            continue

                                        cst_time = utc.localize(utc_time).astimezone(timezone('US/Central')).strftime("%Y-%m-%d %H:%M:%S")
                                        processed_data.append({
                                            "Timestamp": cst_time,
                                            "FromSymbol": from_symbol,
                                            "ToSymbol": to_symbol,
                                            "OpenPrice": float(values.get("1. open", 0)),
                                            "HighPrice": float(values.get("2. high", 0)),
                                            "LowPrice": float(values.get("3. low", 0)),
                                            "ClosePrice": float(values.get("4. close", 0))
                                        })
                                    except Exception as e:
                                        logging.error(f"Error processing record for date {date}: {e}")
                            except requests.exceptions.RequestException as e:
                                logging.error(f"API call failed for {from_symbol}/{to_symbol}: {e}")
                                continue

                    except Exception as e:
                        logging.error(f"Error processing {resource_name}: {e}")

                # Handle data merging and logging
                if processed_data:
                    try:
                        logging.info(f"Merging data into {target_table}...")

                        # Fetch the column names dynamically
                        column_query = f"""
                            SELECT COLUMN_NAME
                            FROM INFORMATION_SCHEMA.COLUMNS
                            WHERE TABLE_NAME = '{target_table}'
                            AND TABLE_SCHEMA = 'dbo'
                            ORDER BY ORDINAL_POSITION;
                        """

                        cursor.execute(column_query)
                        columns = [row.COLUMN_NAME for row in cursor.fetchall()]

                        # Exclude "ID" or auto-increment columns if needed
                        columns = [col for col in columns if col.lower() != "id"]

                        # Insert data using the CTE query
                        logging.info("Executing CTE query to insert data...")
                        json_data = json.dumps(processed_data)

                        # Safely execute the stored CTE query with JSON parameter
                        safe_cte_query = cte_query.replace("'", "''")
                        sql = f"EXEC sp_executesql N'{safe_cte_query}', N'@Json NVARCHAR(MAX)', @Json = ?"
                        cursor.execute(sql, (json_data,))
                        conn.commit()
                        logging.info(f"Inserted {len(processed_data)} records into {target_table}.")
                        conn.commit()
                        logging.info("Database commit successful.")

                    except Exception as e:
                        logging.error(f"Error during merge operation for {target_table}: {e}")
                        conn.rollback()

            except Exception as e:
                logging.error(f"Error processing resource {resource_name}: {e}")

    except Exception as e:
        logging.error(f"Unhandled exception: {e}")

    finally:
        if 'conn' in locals() and conn:
            conn.close()
            logging.info("SQL Server connection closed.")


# Timer trigger: runs every 5 minutes
@app.timer_trigger(schedule="0 */5 * * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=False)
def ForexDataFetcherTimer(myTimer: func.TimerRequest) -> None:
    """Timer-triggered function that runs every 5 minutes"""
    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('ForexDataFetcher timer trigger function started.')
    fetch_and_store_market_data()
    logging.info('ForexDataFetcher timer trigger function completed.')


# HTTP trigger for manual execution
@app.route(route="ForexDataFetcher", auth_level=func.AuthLevel.FUNCTION)
def ForexDataFetcherHttp(req: func.HttpRequest) -> func.HttpResponse:
    """HTTP-triggered function for manual execution"""
    logging.info('ForexDataFetcher HTTP trigger function started.')

    try:
        fetch_and_store_market_data()
        return func.HttpResponse(
            "Forex data fetch completed successfully!",
            status_code=200
        )
    except Exception as e:
        logging.error(f"Error in HTTP trigger: {e}")
        return func.HttpResponse(
            f"Error occurred: {str(e)}",
            status_code=500
        )
