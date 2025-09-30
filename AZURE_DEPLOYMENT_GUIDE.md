# Azure Deployment Guide
## Complete Cloud Migration for Data Engineering Platform

**Developed by: Mir Hasibul Hasan Rahat**

This guide will help you deploy your local data engineering platform to Azure cloud infrastructure.

## Prerequisites

### 1. Azure Account Setup
- Active Azure subscription
- Azure CLI installed (`az --version` to check)
- PowerShell execution policy set (`Set-ExecutionPolicy -ExecutionPolicy RemoteSigned`)

### 2. Required Azure Services
- **Azure Data Factory (ADF)**: ETL orchestration
- **Azure Databricks**: Spark processing
- **Azure Storage Account**: Data lake storage
- **Azure Key Vault**: Secure credential management

## Architecture Overview

```
Local Platform  →  Azure Cloud Platform
=================  ====================
Simple Pipeline  →  Azure Data Factory
Data Lake       →  Azure Data Lake Storage Gen2
Streamlit UI    →  Azure Web Apps
Analytics       →  Power BI / Azure Synapse
```

## Step-by-Step Deployment

### Phase 1: Azure Infrastructure Setup

#### 1.1 Login to Azure
```powershell
# Login to Azure
az login

# Set your subscription (if you have multiple)
az account set --subscription "your-subscription-id"

# Create resource group
az group create --name "rg-dataeng-platform" --location "East US"
```

#### 1.2 Deploy Storage Account
```powershell
# Create storage account for data lake
az storage account create \
  --name "stdataengplatform" \
  --resource-group "rg-dataeng-platform" \
  --location "East US" \
  --sku "Standard_LRS" \
  --kind "StorageV2" \
  --hierarchical-namespace true
```

#### 1.3 Create Databricks Workspace
```powershell
# Create Databricks workspace
az databricks workspace create \
  --resource-group "rg-dataeng-platform" \
  --name "databricks-dataeng-platform" \
  --location "East US" \
  --sku "standard"
```

### Phase 2: Data Factory Deployment

#### 2.1 Deploy ARM Template
```powershell
# Navigate to ARM templates directory
cd "Databricks-ETL-Pipeline-using-PySpark-and-Delta-Lake-Architecture-main\ARM templates\ADF Template"

# Deploy Data Factory
az deployment group create \
  --resource-group "rg-dataeng-platform" \
  --template-file "ARMTemplateForFactory.json" \
  --parameters "ARMTemplateParametersForFactory.json" \
  --parameters factoryName="adf-dataeng-platform"
```

#### 2.2 Configure Parameters
Update `ARMTemplateParametersForFactory.json`:
```json
{
  "factoryName": {
    "value": "adf-dataeng-platform"
  },
  "databricksWorkspaceUrl": {
    "value": "https://adb-xxxxxxxxx.xx.azuredatabricks.net"
  },
  "userEmail": {
    "value": "your-email@domain.com"
  }
}
```

### Phase 3: Data Migration

#### 3.1 Upload Sample Data
```powershell
# Upload your local data to Azure Storage
az storage blob upload-batch \
  --account-name "stdataengplatform" \
  --destination "raw-data" \
  --source "data/sample_movies.json"
```

#### 3.2 Configure Data Lake Structure
```
Azure Storage Container Structure:
├── raw/                    # Landing zone
├── bronze/                 # Standardized data
├── silver/                 # Cleaned data
└── gold/                   # Analytics-ready data
```

### Phase 4: Databricks Configuration

#### 4.1 Create Databricks Notebooks
Upload Python notebooks equivalent to your local scripts:

**Bronze Layer Notebook:**
```python
# Databricks notebook: bronze_processing.py
import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("BronzeLayer").getOrCreate()

# Read from Azure Storage
df = spark.read.json("abfss://raw@stdataengplatform.dfs.core.windows.net/movies.json")

# Write to Bronze layer
df.write.mode("overwrite").parquet("abfss://bronze@stdataengplatform.dfs.core.windows.net/movies/")
```

#### 4.2 Create Cluster Configuration
```json
{
  "cluster_name": "data-eng-cluster",
  "spark_version": "11.3.x-scala2.12",
  "node_type_id": "Standard_DS3_v2",
  "num_workers": 2,
  "autotermination_minutes": 30
}
```

### Phase 5: Pipeline Orchestration

#### 5.1 Data Factory Pipeline Structure
```
Pipeline: ETL-DataEngineering-Pipeline
├── Activity 1: Bronze Layer Processing (Databricks)
├── Activity 2: Silver Layer Cleaning (Databricks)
├── Activity 3: Gold Layer Analytics (Databricks)
└── Activity 4: Data Quality Checks
```

#### 5.2 Schedule Configuration
- **Trigger Type**: Schedule
- **Frequency**: Daily at 2:00 AM UTC
- **Time Zone**: Your local timezone

## Monitoring and Management

### Azure Monitor Integration
```powershell
# Enable diagnostic logging
az monitor diagnostic-settings create \
  --name "adf-diagnostics" \
  --resource "/subscriptions/{subscription-id}/resourceGroups/rg-dataeng-platform/providers/Microsoft.DataFactory/factories/adf-dataeng-platform" \
  --logs '[{"category":"PipelineRuns","enabled":true},{"category":"ActivityRuns","enabled":true}]'
```

### Cost Optimization
- **Databricks**: Use spot instances for non-critical workloads
- **Storage**: Configure lifecycle policies for old data
- **Data Factory**: Use scheduled triggers instead of tumbling windows

## Security Configuration

### 1. Key Vault Setup
```powershell
# Create Key Vault
az keyvault create \
  --name "kv-dataeng-platform" \
  --resource-group "rg-dataeng-platform" \
  --location "East US"

# Store Databricks token
az keyvault secret set \
  --vault-name "kv-dataeng-platform" \
  --name "databricks-token" \
  --value "your-databricks-access-token"
```

### 2. Service Principal Authentication
```powershell
# Create service principal
az ad sp create-for-rbac \
  --name "sp-dataeng-platform" \
  --role "Contributor" \
  --scopes "/subscriptions/{subscription-id}/resourceGroups/rg-dataeng-platform"
```

## Testing and Validation

### 1. Pipeline Testing
- Run test data through the complete pipeline
- Validate data quality at each layer
- Check performance metrics

### 2. Performance Benchmarks
- **Local Pipeline**: ~0.04 seconds (10 records)
- **Azure Pipeline**: ~2-5 minutes (same data + cloud overhead)
- **Scalability**: Can handle TB-scale datasets

## Migration Checklist

- [ ] Azure subscription activated
- [ ] Resource group created
- [ ] Storage account deployed
- [ ] Databricks workspace configured
- [ ] Data Factory deployed
- [ ] ARM templates customized
- [ ] Sample data uploaded
- [ ] Notebooks created and tested
- [ ] Pipeline scheduled and validated
- [ ] Monitoring configured
- [ ] Security implemented

## Cost Estimation (Monthly)

| Service | Configuration | Estimated Cost |
|---------|---------------|----------------|
| Data Factory | 10 pipeline runs/day | $50-100 |
| Databricks | Standard, 2 nodes, 8 hours/day | $200-400 |
| Storage Account | 100GB with transactions | $20-50 |
| **Total** | | **$270-550/month** |

## Troubleshooting

### Common Issues:
1. **Authentication Errors**: Check service principal permissions
2. **Storage Access**: Verify storage account keys and connection strings  
3. **Databricks Connection**: Confirm workspace URL and access tokens
4. **Pipeline Failures**: Check activity logs in Data Factory monitor

### Support Resources:
- Azure Documentation: https://docs.microsoft.com/en-us/azure/
- Databricks Documentation: https://docs.databricks.com/
- Community Forums: Stack Overflow, Azure Community

---

**Ready to deploy your Local Data Engineering Platform to Azure Cloud!**

**Developed by: Mir Hasibul Hasan Rahat**  
*Your local innovation, now enterprise-ready in the cloud*