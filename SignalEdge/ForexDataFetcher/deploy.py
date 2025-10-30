import subprocess
import sys
import os

def run_command(cmd, shell=True):
    """Run a command and return the result"""
    print(f"Running: {cmd}")
    result = subprocess.run(cmd, shell=shell, capture_output=True, text=True)
    print(result.stdout)
    if result.stderr:
        print(result.stderr, file=sys.stderr)
    return result.returncode

def main():
    print("=== Deploying ForexDataFetcher to Azure ===\n")

    # Set the Azure CLI path
    az_path = r'"C:\Program Files\Microsoft SDKs\Azure\CLI2\wbin\az.cmd"'

    # Check if logged in
    print("Checking Azure login status...")
    result = run_command(f"{az_path} account show")

    if result != 0:
        print("\nNot logged in. Please log in to Azure...")
        run_command(f"{az_path} login")

    # List function apps
    print("\nFetching available Function Apps...")
    run_command(f'{az_path} functionapp list --query "[].{{Name:name, ResourceGroup:resourceGroup}}" --output table')

    # Get function app name
    function_app_name = input("\nEnter the Function App name (or press Enter for 'signaledge-forex'): ").strip()
    if not function_app_name:
        function_app_name = "signaledge-forex"

    print(f"\nDeploying to: {function_app_name}")
    print("This may take a few minutes...\n")

    # Deploy
    result = run_command(f"func azure functionapp publish {function_app_name} --python")

    if result == 0:
        print(f"\n=== Deployment Successful ===")
        print(f"Function URL: https://{function_app_name}.azurewebsites.net/api/ForexDataFetcher")
    else:
        print(f"\n=== Deployment Failed ===")
        print("Please check the error messages above")
        sys.exit(1)

if __name__ == "__main__":
    main()
