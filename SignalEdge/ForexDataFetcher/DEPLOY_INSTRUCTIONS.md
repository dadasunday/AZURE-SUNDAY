# Deployment Instructions for ForexDataFetcher

## Quick Deploy

Open a **new PowerShell or Command Prompt** window (not in bash) and run:

```cmd
cd C:\Users\Dell\OneDrive\AZURE+SUNDAY\SignalEdge\ForexDataFetcher
func azure functionapp publish forexdatafetcher --python
```

## Available Function Apps

Based on your Azure subscription, you have these Function Apps:

| Name                   | Resource Group          |
|------------------------|-------------------------|
| forexdatafetcher       | ForexDataRG             |
| NewsSentimentFetcher   | rg-signaledge-functions |

## After Deployment

Once deployed, your function will be available at:
- **HTTP Trigger**: `https://forexdatafetcher.azurewebsites.net/api/ForexDataFetcher`
- **Timer Trigger**: Runs automatically every hour (configured in function_app.py)

## Verify Deployment

After deployment, you can:

1. Test the HTTP endpoint:
   ```
   curl https://forexdatafetcher.azurewebsites.net/api/ForexDataFetcher
   ```

2. Check logs in Azure Portal:
   - Go to Azure Portal → Function Apps → forexdatafetcher
   - Click on "Functions" → "ForexDataFetcherTimer" or "ForexDataFetcherHttp"
   - View "Monitor" tab for execution logs

3. Verify Application Settings:
   Make sure these environment variables are set in Azure Portal:
   - SQL_SERVER
   - SQL_DATABASE
   - SQL_USERNAME
   - SQL_PASSWORD
   - ALPHAVANTAGE_API_KEY

## Troubleshooting

If deployment fails from bash/terminal, open a fresh Command Prompt or PowerShell window directly from Windows Start menu and run the deployment command there.
