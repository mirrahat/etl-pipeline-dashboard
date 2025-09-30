# Azure Deployment Automation Script
# Developed by: Mir Hasibul Hasan Rahat
# This script automates the deployment of your local data engineering platform to Azure

param(
    [Parameter(Mandatory=$true)]
    [string]$SubscriptionId,
    
    [Parameter(Mandatory=$true)]
    [string]$ResourceGroupName = "rg-dataeng-platform",
    
    [Parameter(Mandatory=$true)]
    [string]$Location = "East US",
    
    [Parameter(Mandatory=$true)]
    [string]$UserEmail,
    
    [Parameter(Mandatory=$false)]
    [string]$StorageAccountName = "stdataengplatform$(Get-Random -Maximum 9999)"
)

Write-Host "=== Azure Data Engineering Platform Deployment ===" -ForegroundColor Green
Write-Host "Developed by: Mir Hasibul Hasan Rahat" -ForegroundColor Cyan
Write-Host ""

# Function to check if Azure CLI is installed
function Test-AzureCLI {
    try {
        $azVersion = az --version 2>$null
        if ($azVersion) {
            Write-Host "✓ Azure CLI is installed" -ForegroundColor Green
            return $true
        }
    }
    catch {
        Write-Host "✗ Azure CLI is not installed" -ForegroundColor Red
        Write-Host "Please install Azure CLI from: https://docs.microsoft.com/en-us/cli/azure/install-azure-cli"
        return $false
    }
}

# Function to login to Azure
function Connect-Azure {
    Write-Host "Logging into Azure..." -ForegroundColor Yellow
    
    try {
        $loginResult = az login --output table
        if ($LASTEXITCODE -eq 0) {
            Write-Host "✓ Successfully logged into Azure" -ForegroundColor Green
            
            # Set subscription
            az account set --subscription $SubscriptionId
            Write-Host "✓ Subscription set to: $SubscriptionId" -ForegroundColor Green
            return $true
        }
    }
    catch {
        Write-Host "✗ Failed to login to Azure" -ForegroundColor Red
        return $false
    }
}

# Function to create resource group
function New-ResourceGroup {
    Write-Host "Creating resource group: $ResourceGroupName..." -ForegroundColor Yellow
    
    $result = az group create --name $ResourceGroupName --location $Location --output json | ConvertFrom-Json
    
    if ($result.properties.provisioningState -eq "Succeeded") {
        Write-Host "✓ Resource group created successfully" -ForegroundColor Green
        return $true
    } else {
        Write-Host "✗ Failed to create resource group" -ForegroundColor Red
        return $false
    }
}

# Function to create storage account
function New-StorageAccount {
    Write-Host "Creating storage account: $StorageAccountName..." -ForegroundColor Yellow
    
    $result = az storage account create `
        --name $StorageAccountName `
        --resource-group $ResourceGroupName `
        --location $Location `
        --sku "Standard_LRS" `
        --kind "StorageV2" `
        --hierarchical-namespace true `
        --output json | ConvertFrom-Json
    
    if ($result.provisioningState -eq "Succeeded") {
        Write-Host "✓ Storage account created successfully" -ForegroundColor Green
        
        # Create containers for data lake
        $storageKey = (az storage account keys list --resource-group $ResourceGroupName --account-name $StorageAccountName --query "[0].value" --output tsv)
        
        $containers = @("raw", "bronze", "silver", "gold", "archive")
        foreach ($container in $containers) {
            az storage container create --name $container --account-name $StorageAccountName --account-key $storageKey --output none
            Write-Host "  ✓ Created container: $container" -ForegroundColor Cyan
        }
        
        return $true
    } else {
        Write-Host "✗ Failed to create storage account" -ForegroundColor Red
        return $false
    }
}

# Function to create Databricks workspace
function New-DatabricksWorkspace {
    Write-Host "Creating Databricks workspace..." -ForegroundColor Yellow
    
    $workspaceName = "databricks-dataeng-platform"
    
    $result = az databricks workspace create `
        --resource-group $ResourceGroupName `
        --name $workspaceName `
        --location $Location `
        --sku "standard" `
        --output json | ConvertFrom-Json
    
    if ($result.provisioningState -eq "Succeeded") {
        Write-Host "✓ Databricks workspace created successfully" -ForegroundColor Green
        Write-Host "  Workspace URL: $($result.workspaceUrl)" -ForegroundColor Cyan
        return $true
    } else {
        Write-Host "✗ Failed to create Databricks workspace" -ForegroundColor Red
        return $false
    }
}

# Function to deploy Data Factory
function New-DataFactory {
    Write-Host "Deploying Azure Data Factory..." -ForegroundColor Yellow
    
    $templatePath = "Databricks-ETL-Pipeline-using-PySpark-and-Delta-Lake-Architecture-main\ARM templates\ADF Template"
    
    if (Test-Path $templatePath) {
        # Update parameters file
        $parametersFile = Join-Path $templatePath "ARMTemplateParametersForFactory.json"
        $parameters = Get-Content $parametersFile -Raw | ConvertFrom-Json
        
        $parameters.parameters.factoryName.value = "adf-dataeng-platform"
        $parameters.parameters.userEmail.value = $UserEmail
        
        $parameters | ConvertTo-Json -Depth 10 | Set-Content $parametersFile
        
        # Deploy ARM template
        $deployment = az deployment group create `
            --resource-group $ResourceGroupName `
            --template-file (Join-Path $templatePath "ARMTemplateForFactory.json") `
            --parameters (Join-Path $templatePath "ARMTemplateParametersForFactory.json") `
            --output json | ConvertFrom-Json
        
        if ($deployment.properties.provisioningState -eq "Succeeded") {
            Write-Host "✓ Data Factory deployed successfully" -ForegroundColor Green
            return $true
        }
    }
    
    Write-Host "✗ Failed to deploy Data Factory" -ForegroundColor Red
    return $false
}

# Function to upload sample data
function Copy-SampleData {
    Write-Host "Uploading sample data..." -ForegroundColor Yellow
    
    if (Test-Path "data\sample_movies.json") {
        $storageKey = (az storage account keys list --resource-group $ResourceGroupName --account-name $StorageAccountName --query "[0].value" --output tsv)
        
        az storage blob upload `
            --account-name $StorageAccountName `
            --account-key $storageKey `
            --container-name "raw" `
            --name "movies.json" `
            --file "data\sample_movies.json" `
            --output none
        
        Write-Host "✓ Sample data uploaded successfully" -ForegroundColor Green
        return $true
    } else {
        Write-Host "⚠ Sample data file not found, skipping upload" -ForegroundColor Yellow
        return $true
    }
}

# Function to create Key Vault
function New-KeyVault {
    Write-Host "Creating Key Vault..." -ForegroundColor Yellow
    
    $vaultName = "kv-dataeng-platform-$(Get-Random -Maximum 9999)"
    
    $result = az keyvault create `
        --name $vaultName `
        --resource-group $ResourceGroupName `
        --location $Location `
        --output json | ConvertFrom-Json
    
    if ($result.properties.provisioningState -eq "Succeeded") {
        Write-Host "✓ Key Vault created successfully" -ForegroundColor Green
        Write-Host "  Vault Name: $vaultName" -ForegroundColor Cyan
        return $true
    } else {
        Write-Host "✗ Failed to create Key Vault" -ForegroundColor Red
        return $false
    }
}

# Main deployment function
function Start-Deployment {
    Write-Host "Starting Azure deployment process..." -ForegroundColor Green
    Write-Host "Parameters:" -ForegroundColor Cyan
    Write-Host "  Subscription: $SubscriptionId"
    Write-Host "  Resource Group: $ResourceGroupName"
    Write-Host "  Location: $Location"
    Write-Host "  User Email: $UserEmail"
    Write-Host "  Storage Account: $StorageAccountName"
    Write-Host ""
    
    $steps = @(
        @{ Name = "Azure CLI Check"; Function = { Test-AzureCLI } },
        @{ Name = "Azure Login"; Function = { Connect-Azure } },
        @{ Name = "Resource Group"; Function = { New-ResourceGroup } },
        @{ Name = "Storage Account"; Function = { New-StorageAccount } },
        @{ Name = "Databricks Workspace"; Function = { New-DatabricksWorkspace } },
        @{ Name = "Data Factory"; Function = { New-DataFactory } },
        @{ Name = "Sample Data Upload"; Function = { Copy-SampleData } },
        @{ Name = "Key Vault"; Function = { New-KeyVault } }
    )
    
    $successCount = 0
    $totalSteps = $steps.Count
    
    foreach ($step in $steps) {
        Write-Host ""
        Write-Host "Step: $($step.Name)" -ForegroundColor Yellow
        Write-Host "----------------------------------------"
        
        if (& $step.Function) {
            $successCount++
        } else {
            Write-Host "Deployment failed at step: $($step.Name)" -ForegroundColor Red
            break
        }
    }
    
    Write-Host ""
    Write-Host "=== Deployment Summary ===" -ForegroundColor Green
    Write-Host "Completed: $successCount/$totalSteps steps" -ForegroundColor Cyan
    
    if ($successCount -eq $totalSteps) {
        Write-Host "🎉 Azure deployment completed successfully!" -ForegroundColor Green
        Write-Host ""
        Write-Host "Next Steps:" -ForegroundColor Yellow
        Write-Host "1. Configure Databricks notebooks"
        Write-Host "2. Set up Data Factory triggers"
        Write-Host "3. Configure monitoring and alerts"
        Write-Host "4. Test end-to-end pipeline"
        Write-Host ""
        Write-Host "Access your resources in Azure portal:" -ForegroundColor Cyan
        Write-Host "https://portal.azure.com/#@/resource/subscriptions/$SubscriptionId/resourceGroups/$ResourceGroupName"
    } else {
        Write-Host "❌ Deployment failed. Please check the errors above." -ForegroundColor Red
    }
}

# Run the deployment
Start-Deployment