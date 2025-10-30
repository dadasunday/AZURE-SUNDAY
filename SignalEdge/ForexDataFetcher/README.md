# ForexDataFetcher Azure Function

This Azure Function fetches and stores forex market data from various sources into a SQL Server database.

## Features

- **Timer Trigger**: Automatically runs every 5 minutes
- **HTTP Trigger**: Manual execution via HTTP endpoint
- **Multi-source Data**: Supports 5 different data types:
  - Type 1: Real GDP data
  - Type 2: Technical Indicators (RSI, MACD, etc.)
  - Type 4: WTI Crude Oil prices
  - Type 5: FOREX Intraday (5-minute) Exchange Data
  - Type 6: FOREX Daily Exchange Data
- **Parallel Processing**: Efficiently processes multiple currency pairs
- **Incremental Loading**: Only fetches new data based on latest timestamps

## Configuration

### Local Development

Edit `local.settings.json` with your credentials:

```json
{
  "Values": {
    "SQL_SERVER": "your-server.database.windows.net",
    "SQL_DATABASE": "TradingForeignCurrency",
    "SQL_USERNAME": "your-username",
    "SQL_PASSWORD": "your-password",
    "ALPHAVANTAGE_API_KEY": "your-api-key"
  }
}
```

### Azure Deployment

Set the following Application Settings in Azure Portal:

- `SQL_SERVER`
- `SQL_DATABASE`
- `SQL_USERNAME`
- `SQL_PASSWORD`
- `ALPHAVANTAGE_API_KEY`

**IMPORTANT**: For production, use Azure Key Vault to store sensitive credentials.

## Timer Schedule

The function runs on this CRON schedule: `0 */5 * * * *`
- This means: Every 5 minutes

To change the schedule, edit the `schedule` parameter in the `@app.timer_trigger` decorator in `function_app.py`.

### CRON Schedule Examples

- Every minute: `0 * * * * *`
- Every 10 minutes: `0 */10 * * * *`
- Every hour: `0 0 * * * *`
- Every day at midnight: `0 0 0 * * *`

## HTTP Endpoint

You can manually trigger the function by calling:

**Local**: `http://localhost:7071/api/ForexDataFetcher`

**Azure**: `https://<your-function-app>.azurewebsites.net/api/ForexDataFetcher?code=<function-key>`

## Running Locally

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Install Azure Functions Core Tools

3. Run the function:
   ```bash
   func start
   ```

## Deploying to Azure

1. Create an Azure Function App (Python runtime)

2. Deploy using Azure Functions Core Tools:
   ```bash
   func azure functionapp publish <your-function-app-name>
   ```

3. Or use VS Code Azure Functions extension

4. Configure Application Settings in Azure Portal

## Database Requirements

The function expects these tables in your SQL Server database:

- `ResourceRegistry` - Configuration table for data sources
- `CurrencyPairs` - List of currency pairs to process
- Various target tables defined in ResourceRegistry

## Logging

- Logs are written to Azure Application Insights (if configured)
- Local logs appear in the console when running locally
- Log level can be adjusted in `host.json`

## Troubleshooting

1. **Connection Issues**: Verify SQL Server firewall rules allow Azure services
2. **API Rate Limits**: Alpha Vantage has rate limits - consider upgrading or throttling
3. **Timeout**: Adjust `functionTimeout` in `host.json` if processing takes longer

## Security Best Practices

- Never commit `local.settings.json` to source control (already in .gitignore)
- Use Azure Key Vault for production credentials
- Use Managed Identity for Azure SQL Database connections
- Rotate API keys regularly
