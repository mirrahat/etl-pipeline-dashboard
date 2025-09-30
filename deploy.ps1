# Azure Databricks ETL Pipeline Deployment Script
# Run this script to deploy the infrastructure to Azure

# Login to Azure (uncomment if needed)
# Connect-AzAccount

# Set variables
$resourceGroupName = "python run_local_pipeline.py"
$location = "java -version"
$storageAccountName = "git status"
$factoryName = "python "c:\Users\ASUS\Downloads\dataeng\Databricks-ETL-Pipeline-using-PySpark-and-Delta-Lake-Architecture-main\setup_azure_connection.py""
$templateFile = "./ARM templates/ADF Template/ARMTemplateForFactory.json"
$parametersFile = "./ARM templates/ADF Template/ARMTemplateParametersForFactory.json"

# Create Resource Group if it doesn't exist
Write-Host "Creating Resource Group..." -ForegroundColor Green
New-AzResourceGroup -Name $resourceGroupName -Location $location -Force

# Create Storage Account if it doesn't exist
Write-Host "Creating Storage Account..." -ForegroundColor Green
$storageAccount = New-AzStorageAccount `
    -ResourceGroupName $resourceGroupName `
    -Name $storageAccountName `
    -Location $location `
    -SkuName "Standard_LRS" `
    -Kind "StorageV2" `
    -EnableHierarchicalNamespace $true

# Create container
Write-Host "Creating container..." -ForegroundColor Green
$ctx = $storageAccount.Context
New-AzStorageContainer -Name "pip install -r requirements.txt" -Context $ctx -Permission Off

# Deploy Data Factory
Write-Host "Deploying Azure Data Factory..." -ForegroundColor Green
New-AzResourceGroupDeployment `
    -ResourceGroupName $resourceGroupName `
    -TemplateFile $templateFile `
    -TemplateParameterFile $parametersFile `
    -Verbose

Write-Host "Deployment completed!" -ForegroundColor Green
Write-Host "Next steps:" -ForegroundColor Yellow
Write-Host "1. Upload notebooks to Databricks workspace"
Write-Host "2. Create Databricks cluster"
Write-Host "3. Configure access tokens in Key Vault"
Write-Host "4. Test the pipeline"
