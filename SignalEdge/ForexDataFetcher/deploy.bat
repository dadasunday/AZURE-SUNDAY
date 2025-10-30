@echo off
REM Azure Function Deployment Script for ForexDataFetcher
REM Run this script in Command Prompt or PowerShell

echo === Deploying ForexDataFetcher to Azure ===
echo.

cd /d "%~dp0"

REM Check if logged in to Azure
echo Checking Azure login status...
"%ProgramFiles%\Microsoft SDKs\Azure\CLI2\wbin\az.cmd" account show >nul 2>&1
if errorlevel 1 (
    echo Not logged in. Opening Azure login...
    "%ProgramFiles%\Microsoft SDKs\Azure\CLI2\wbin\az.cmd" login
)

echo.
echo Available Function Apps:
"%ProgramFiles%\Microsoft SDKs\Azure\CLI2\wbin\az.cmd" functionapp list --query "[].{Name:name, ResourceGroup:resourceGroup}" --output table

echo.
echo Enter the Function App name (or press Enter for 'signaledge-forex'):
set /p FUNCTION_APP_NAME=
if "%FUNCTION_APP_NAME%"=="" set FUNCTION_APP_NAME=signaledge-forex

echo.
echo Deploying to: %FUNCTION_APP_NAME%
echo.

func azure functionapp publish %FUNCTION_APP_NAME% --python

if errorlevel 1 (
    echo.
    echo === Deployment Failed ===
    echo Please check the error messages above
    pause
    exit /b 1
)

echo.
echo === Deployment Successful ===
echo Function URL: https://%FUNCTION_APP_NAME%.azurewebsites.net/api/ForexDataFetcher
echo.
pause
