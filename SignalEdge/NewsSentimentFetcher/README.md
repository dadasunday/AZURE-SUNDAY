# NewsSentimentFetcher Azure Function

Fetches market news and sentiment from Alpha Vantage and stores it into SQL Server staging table `dbo.Staging_NewsSentiment`.

## Triggers

- Timer Trigger: hourly (`0 0 * * * *`)
- HTTP Trigger: manual execution at route `NewsSentimentFetcher`

## Configuration

Set these environment variables (locally in `local.settings.json` and as App Settings in Azure):

```
{
  "Values": {
    "SQL_SERVER": "<server>",
    "SQL_DATABASE": "TradingForeignCurrency",
    "SQL_USERNAME": "<username>",
    "SQL_PASSWORD": "<password>",
    "ALPHAVANTAGE_API_KEY": "<api-key>"
  }
}
```

## Running Locally

1. Install dependencies:
   - `pip install -r requirements.txt`
2. Install Azure Functions Core Tools
3. Start function host:
   - `func start`

## Deployment

1. Create an Azure Function App (Python runtime)
2. Publish:
   - `func azure functionapp publish <your-function-app-name>`
3. Configure Application Settings in Azure Portal

## Notes

- Requires `ODBC Driver 18 for SQL Server`.
- The function deduplicates inserts by `(PublishedAt, Ticker)`.

