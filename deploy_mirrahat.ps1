# Azure Deployment Command for Mir Hasibul Hasan Rahat
# Complete ETL Pipeline Migration to Azure Cloud
# Email: mirrahat0081@outlook.com
# Subscription: b6a34c6e-3b3b-4cf4-85dc-279699bca03c

Write-Host "🚀 Azure ETL Pipeline Deployment" -ForegroundColor Green
Write-Host "Developer: Mir Hasibul Hasan Rahat" -ForegroundColor Cyan
Write-Host "Email: mirrahat0081@outlook.com" -ForegroundColor Cyan
Write-Host ""

# STEP 1: Login to Azure
Write-Host "Step 1: Azure Login..." -ForegroundColor Yellow
az login

# STEP 2: Set Subscription
Write-Host "Step 2: Setting subscription..." -ForegroundColor Yellow
az account set --subscription "b6a34c6e-3b3b-4cf4-85dc-279699bca03c"

# STEP 3: Run Complete Deployment
Write-Host "Step 3: Deploying ETL Pipeline..." -ForegroundColor Yellow
.\deploy-azure.ps1 -SubscriptionId "b6a34c6e-3b3b-4cf4-85dc-279699bca03c" -ResourceGroupName "rg-mirrahat-dataeng" -Location "East US" -UserEmail "mirrahat0081@outlook.com"

Write-Host ""
Write-Host "✅ Deployment Complete!" -ForegroundColor Green
Write-Host "Your Azure Resources:" -ForegroundColor Cyan
Write-Host "• Resource Group: rg-mirrahat-dataeng" -ForegroundColor White
Write-Host "• Storage Account: stmirrahatdataeng2025" -ForegroundColor White  
Write-Host "• Data Factory: adf-mirrahat-etl" -ForegroundColor White
Write-Host "• Databricks: databricks-mirrahat-etl" -ForegroundColor White
Write-Host "• Region: East US" -ForegroundColor White
Write-Host ""
Write-Host "Access your resources at: https://portal.azure.com" -ForegroundColor Green