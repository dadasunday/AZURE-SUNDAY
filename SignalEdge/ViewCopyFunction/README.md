# ViewCopyFunction - Azure Function

Automated Azure Function to copy SQL Server views from one database to another.

## Features

- **Automated Sync**: Timer-triggered to run on schedule (default: daily at 2 AM UTC)
- **Manual Trigger**: HTTP endpoint for on-demand execution
- **Smart Copying**:
  - Automatically creates schemas if missing
  - Drops existing views before recreating
  - Tracks dependencies
  - Comprehensive error handling and logging
- **Flexible Configuration**: Copy all views or specific views
- **Status Endpoint**: Check configuration and function status

## Environment Variables

### Required

| Variable | Description | Example |
|----------|-------------|---------|
| `SOURCE_SQL_SERVER` | Source SQL Server hostname | `myserver.database.windows.net` |
| `SOURCE_SQL_DATABASE` | Source database name | `SourceDB` |
| `SOURCE_SQL_USERNAME` | Source database username | `sqluser` |
| `SOURCE_SQL_PASSWORD` | Source database password | `P@ssw0rd!` |
| `TARGET_SQL_SERVER` | Target SQL Server hostname | `targetserver.database.windows.net` |
| `TARGET_SQL_DATABASE` | Target database name | `TargetDB` |
| `TARGET_SQL_USERNAME` | Target database username | `sqluser` |
| `TARGET_SQL_PASSWORD` | Target database password | `P@ssw0rd!` |

### Optional

| Variable | Description | Default |
|----------|-------------|---------|
| `SPECIFIC_VIEWS` | Comma-separated list of view names to copy | (all views) |
| `DROP_EXISTING_VIEWS` | Drop existing views before creating | `true` |
| `CREATE_SCHEMAS` | Create missing schemas automatically | `true` |
| `SYNC_SCHEDULE` | Cron expression for timer trigger | `0 0 2 * * *` |

## Function Endpoints

### 1. ViewCopy (HTTP Trigger)

**URL**: `https://<function-app>.azurewebsites.net/api/ViewCopy`

**Method**: GET or POST

**Parameters**:
- `views` (optional): Comma-separated list of view names to copy

**Example**:
```bash
# Copy all views
curl "https://your-function-app.azurewebsites.net/api/ViewCopy?code=YOUR_FUNCTION_KEY"

# Copy specific views
curl "https://your-function-app.azurewebsites.net/api/ViewCopy?views=View1,View2,View3&code=YOUR_FUNCTION_KEY"
```

**Response**:
```json
{
  "status": "success",
  "summary": {
    "start_time": "2025-10-13T10:30:00.000Z",
    "end_time": "2025-10-13T10:30:45.000Z",
    "source_database": "sourceserver/SourceDB",
    "target_database": "targetserver/TargetDB",
    "total_views": 10,
    "successful": 9,
    "failed": 1,
    "view_details": [...]
  }
}
```

### 2. ViewCopy/status (Status Endpoint)

**URL**: `https://<function-app>.azurewebsites.net/api/ViewCopy/status`

**Method**: GET

**Example**:
```bash
curl "https://your-function-app.azurewebsites.net/api/ViewCopy/status?code=YOUR_FUNCTION_KEY"
```

**Response**:
```json
{
  "status": "operational",
  "configuration": {
    "source_server": "sourceserver.database.windows.net",
    "source_database": "SourceDB",
    "target_server": "targetserver.database.windows.net",
    "target_database": "TargetDB",
    "specific_views": "ALL",
    "drop_existing_views": "true",
    "create_schemas": "true",
    "sync_schedule": "0 0 2 * * *"
  }
}
```

### 3. ViewCopyTimer (Timer Trigger)

Runs automatically on schedule. Default: Daily at 2 AM UTC (`0 0 2 * * *`)

To change schedule, update `SYNC_SCHEDULE` environment variable with a cron expression.

**Cron Expression Examples**:
- Every hour: `0 0 * * * *`
- Every 6 hours: `0 0 */6 * * *`
- Daily at 3 AM: `0 0 3 * * *`
- Weekly on Sunday at 2 AM: `0 0 2 * * 0`

## Local Development

### Prerequisites

1. Python 3.9 or higher
2. Azure Functions Core Tools
3. ODBC Driver 18 for SQL Server

### Setup

1. **Clone and navigate to directory**:
   ```bash
   cd ViewCopyFunction
   ```

2. **Create virtual environment**:
   ```bash
   python -m venv .venv
   .venv\Scripts\activate  # Windows
   source .venv/bin/activate  # Linux/Mac
   ```

3. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure local.settings.json**:
   ```json
   {
     "Values": {
       "SOURCE_SQL_SERVER": "your-source-server.database.windows.net",
       "SOURCE_SQL_DATABASE": "SourceDB",
       "SOURCE_SQL_USERNAME": "username",
       "SOURCE_SQL_PASSWORD": "password",
       "TARGET_SQL_SERVER": "your-target-server.database.windows.net",
       "TARGET_SQL_DATABASE": "TargetDB",
       "TARGET_SQL_USERNAME": "username",
       "TARGET_SQL_PASSWORD": "password"
     }
   }
   ```

5. **Run locally**:
   ```bash
   func start
   ```

6. **Test HTTP trigger**:
   ```bash
   curl http://localhost:7071/api/ViewCopy
   ```

## Deployment to Azure

### Method 1: VS Code Extension

1. Install "Azure Functions" extension in VS Code
2. Right-click on `ViewCopyFunction` folder
3. Select "Deploy to Function App..."
4. Follow prompts

### Method 2: Azure CLI

```bash
# Login to Azure
az login

# Create resource group (if needed)
az group create --name MyResourceGroup --location eastus

# Create storage account (if needed)
az storage account create --name mystorageaccount --resource-group MyResourceGroup --location eastus

# Create Function App
az functionapp create \
  --resource-group MyResourceGroup \
  --consumption-plan-location eastus \
  --runtime python \
  --runtime-version 3.9 \
  --functions-version 4 \
  --name MyViewCopyFunction \
  --storage-account mystorageaccount \
  --os-type Linux

# Deploy function
cd ViewCopyFunction
func azure functionapp publish MyViewCopyFunction

# Configure environment variables
az functionapp config appsettings set \
  --name MyViewCopyFunction \
  --resource-group MyResourceGroup \
  --settings \
    SOURCE_SQL_SERVER="source-server.database.windows.net" \
    SOURCE_SQL_DATABASE="SourceDB" \
    SOURCE_SQL_USERNAME="username" \
    SOURCE_SQL_PASSWORD="password" \
    TARGET_SQL_SERVER="target-server.database.windows.net" \
    TARGET_SQL_DATABASE="TargetDB" \
    TARGET_SQL_USERNAME="username" \
    TARGET_SQL_PASSWORD="password"
```

### Method 3: Azure Portal

1. Go to Azure Portal
2. Create new Function App
3. Choose Python runtime
4. Deploy code via ZIP deployment or GitHub Actions
5. Configure Application Settings with environment variables

## Post-Deployment Configuration

### 1. Set Environment Variables

In Azure Portal:
1. Go to your Function App
2. Settings → Configuration
3. Add all required environment variables
4. Click "Save"

### 2. Enable System Assigned Identity (Optional)

For better security using Azure AD authentication:
1. Go to Identity → System assigned
2. Turn on
3. Grant SQL Server permissions to the managed identity

### 3. Configure Firewall Rules

Ensure your Function App can access both SQL Servers:
1. Add Function App's outbound IPs to SQL Server firewall rules
2. Or enable "Allow Azure services and resources to access this server"

### 4. Monitor Execution

1. Go to Function App → Functions → ViewCopyTimer or ViewCopyHttp
2. Click "Monitor" to view execution history
3. Configure Application Insights for detailed logging

## Monitoring and Logs

### View Logs in Azure Portal

1. Go to Function App → Monitor → Logs
2. Or use Application Insights → Logs

### Query Example (Application Insights)

```kusto
traces
| where message contains "ViewCopyFunction"
| order by timestamp desc
| project timestamp, message, severityLevel
```

### Log Levels

- **INFO**: Normal operations (connections, view processing)
- **WARNING**: Non-critical issues (missing dependencies)
- **ERROR**: Failed operations (view creation errors)

## Troubleshooting

### Issue: Connection timeout

**Solution**:
- Check firewall rules on both SQL Servers
- Verify network connectivity from Function App
- Increase connection timeout in connection string

### Issue: View creation fails

**Solution**:
- Check if dependent objects exist in target database
- Verify schema exists or enable `CREATE_SCHEMAS=true`
- Check for cross-database references in view definition

### Issue: Authentication failed

**Solution**:
- Verify credentials in environment variables
- Check SQL Server authentication mode (SQL Auth vs Azure AD)
- Ensure user has proper permissions (CREATE VIEW, CREATE SCHEMA)

### Issue: Timer trigger not firing

**Solution**:
- Check cron expression syntax
- Verify timer trigger is enabled in Azure Portal
- Check Function App is running (not stopped)
- Review execution history in Monitor tab

## Required SQL Permissions

### Source Database
- `SELECT` on `sys.views`
- `SELECT` on `sys.sql_modules`
- `SELECT` on `sys.sql_expression_dependencies`

### Target Database
- `CREATE VIEW`
- `CREATE SCHEMA`
- `DROP VIEW`
- `ALTER VIEW` (if modifying existing views)

## Security Best Practices

1. **Use Key Vault**: Store passwords in Azure Key Vault
2. **Use Managed Identity**: Enable Azure AD authentication for SQL
3. **Restrict Access**: Use function key authentication for HTTP triggers
4. **Monitor Access**: Enable diagnostic logging
5. **Limit Permissions**: Grant minimum required SQL permissions

## Cost Optimization

- **Consumption Plan**: Pay only when function executes
- **Reduce Frequency**: Adjust schedule if views don't change often
- **Use Specific Views**: Set `SPECIFIC_VIEWS` to copy only what's needed
- **Monitor Execution Time**: Optimize for functions < 5 minutes

## Related Files

- [function_app.py](function_app.py) - Main function code
- [requirements.txt](requirements.txt) - Python dependencies
- [host.json](host.json) - Function host configuration
- [local.settings.json](local.settings.json) - Local development settings

## Support

For issues or questions:
1. Check logs in Azure Portal
2. Review Application Insights
3. Enable DEBUG logging temporarily for more details
