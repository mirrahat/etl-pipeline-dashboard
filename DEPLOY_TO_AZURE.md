# Quick Azure Deployment for Mir Hasibul Hasan Rahat
# ETL Pipeline Dashboard - Azure Cloud Migration

# Step 1: Login to Azure
az login

# Step 2: Set your subscription
az account set --subscription "b6a34c6e-3b3b-4cf4-85dc-279699bca03c"

# Step 3: Create Resource Group
az group create --name "rg-mirrahat-dataeng" --location "East US"

# Step 4: Deploy using PowerShell script
.\deploy-azure.ps1 -SubscriptionId "b6a34c6e-3b3b-4cf4-85dc-279699bca03c" -ResourceGroupName "rg-mirrahat-dataeng" -Location "East US" -UserEmail "your-email@domain.com"

# Alternative: Manual deployment commands
# Step 5a: Create Storage Account for Data Lake
az storage account create --name "stmirrahatdataeng2025" --resource-group "rg-mirrahat-dataeng" --location "East US" --sku Standard_LRS --kind StorageV2 --enable-hierarchical-namespace true

# Step 5b: Create Data Factory
az datafactory create --resource-group "rg-mirrahat-dataeng" --factory-name "adf-mirrahat-etl" --location "East US"

# Step 5c: Create Databricks Workspace (Premium tier for advanced features)
az databricks workspace create --resource-group "rg-mirrahat-dataeng" --name "databricks-mirrahat-etl" --location "East US" --sku premium

# Your Azure Resources will be:
# - Resource Group: rg-mirrahat-dataeng
# - Storage Account: stmirrahatdataeng2025
# - Data Factory: adf-mirrahat-etl  
# - Databricks: databricks-mirrahat-etl
# - Region: East US