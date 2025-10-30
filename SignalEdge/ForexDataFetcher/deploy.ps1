# Azure Function Deployment Script for ForexDataFetcher
# Run this script in PowerShell

Write-Host "=== Deploying ForexDataFetcher to Azure ===" -ForegroundColor Green

# Change to the ForexDataFetcher directory
Set-Location $PSScriptRoot

# Check if Azure CLI is installed
try {
    az --version | Out-Null
    Write-Host "Azure CLI found" -ForegroundColor Green
} catch {
    Write-Host "ERROR: Azure CLI not found. Please install it first." -ForegroundColor Red
    exit 1
}

# Check if logged in
Write-Host "`nChecking Azure login status..."
$account = az account show 2>$null
if ($LASTEXITCODE -ne 0) {
    Write-Host "Not logged in. Logging in to Azure..." -ForegroundColor Yellow
    az login
} else {
    Write-Host "Already logged in to Azure" -ForegroundColor Green
}

# List available function apps
Write-Host "`nAvailable Function Apps:" -ForegroundColor Cyan
az functionapp list --query "[].{Name:name, ResourceGroup:resourceGroup, State:state}" --output table

# Prompt for function app name
Write-Host "`nEnter the name of your Function App (or press Enter to try 'signaledge-forex'):" -ForegroundColor Yellow
$functionAppName = Read-Host
if ([string]::IsNullOrWhiteSpace($functionAppName)) {
    $functionAppName = "signaledge-forex"
}

Write-Host "`nDeploying to: $functionAppName" -ForegroundColor Cyan

# Deploy using func
Write-Host "`nStarting deployment..." -ForegroundColor Green
func azure functionapp publish $functionAppName --python

if ($LASTEXITCODE -eq 0) {
    Write-Host "`n=== Deployment Successful ===" -ForegroundColor Green
    Write-Host "`nFunction URL: https://$functionAppName.azurewebsites.net/api/ForexDataFetcher" -ForegroundColor Cyan
} else {
    Write-Host "`n=== Deployment Failed ===" -ForegroundColor Red
    Write-Host "Please check the error messages above" -ForegroundColor Yellow
}
