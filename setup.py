#!/usr/bin/env python3
"""
Setup script for Databricks ETL Pipeline
This script helps you configure the pipeline for your Azure environment
"""

import json
import os
import sys
from pathlib import Path

class PipelineSetup:
    def __init__(self):
        self.base_path = Path(__file__).parent
        self.config_file = self.base_path / "config" / "pipeline_config.py"
        
    def welcome(self):
        print("=" * 60)
        print("🚀 DATABRICKS ETL PIPELINE SETUP")
        print("=" * 60)
        print()
        print("This setup will help you configure the pipeline for your Azure environment.")
        print("You'll need the following information ready:")
        print("  • Azure Storage Account name")
        print("  • Databricks workspace URL")
        print("  • Databricks cluster ID") 
        print("  • Your email address")
        print("  • Azure Data Factory name")
        print()
        
    def get_user_input(self):
        """Collect configuration from user"""
        print("📝 Please provide the following information:")
        print()
        
        config = {}
        
        # Storage Account
        config['storage_account'] = input("Enter your Azure Storage Account name: ").strip()
        
        # Container name
        config['container_name'] = input("Enter container name [datalake]: ").strip() or "datalake"
        
        # Resource Group
        config['resource_group'] = input("Enter Resource Group name: ").strip()
        
        # Databricks Workspace URL
        config['databricks_url'] = input("Enter Databricks workspace URL: ").strip()
        
        # Cluster ID
        config['cluster_id'] = input("Enter Databricks cluster ID: ").strip()
        
        # User Email
        config['user_email'] = input("Enter your email address: ").strip()
        
        # ADF Factory Name
        config['adf_factory'] = input("Enter Azure Data Factory name: ").strip()
        
        # Location
        config['location'] = input("Enter Azure region [East US]: ").strip() or "East US"
        
        # Timezone
        config['timezone'] = input("Enter timezone [UTC]: ").strip() or "UTC"
        
        return config
        
    def update_config_file(self, config):
        """Update the configuration file with user inputs"""
        print("\n🔧 Updating configuration files...")
        
        # Read current config file
        with open(self.config_file, 'r') as f:
            content = f.read()
        
        # Replace placeholder values
        replacements = {
            'yourstorageaccount': config['storage_account'],
            'datalake': config['container_name'],
            'your-rg-databricks-etl': config['resource_group'],
            'https://adb-xxxxxxxxx.xx.azuredatabricks.net': config['databricks_url'],
            'your-cluster-id': config['cluster_id'],
            'your-adf-lakehouse-pipeline': config['adf_factory'],
            'East US': config['location'],
            'your-email@domain.com': config['user_email'],
            'UTC': config['timezone']
        }
        
        for old_value, new_value in replacements.items():
            content = content.replace(old_value, new_value)
        
        # Write updated config
        with open(self.config_file, 'w') as f:
            f.write(content)
        
        print("✅ Configuration file updated!")
        
    def update_arm_templates(self, config):
        """Update ARM template parameter files"""
        print("🔧 Updating ARM template parameters...")
        
        # Update ARM template parameters
        arm_params_file = self.base_path / "ARM templates" / "ADF Template" / "ARMTemplateParametersForFactory.json"
        
        if arm_params_file.exists():
            with open(arm_params_file, 'r') as f:
                arm_params = json.load(f)
            
            # Update parameter values
            arm_params['parameters']['factoryName']['value'] = config['adf_factory']
            arm_params['parameters']['databricksWorkspaceUrl']['value'] = config['databricks_url']
            arm_params['parameters']['ADFtoDatabricks_properties_typeProperties_existingClusterId']['value'] = config['cluster_id']
            arm_params['parameters']['userEmail']['value'] = config['user_email']
            arm_params['parameters']['scheduleTimeZone']['value'] = config['timezone']
            
            # Write updated ARM parameters
            with open(arm_params_file, 'w') as f:
                json.dump(arm_params, f, indent=4)
            
            print("✅ ARM template parameters updated!")
        
    def create_deployment_script(self, config):
        """Create Azure deployment script"""
        print("🔧 Creating deployment scripts...")
        
        # PowerShell deployment script
        ps_script = f"""# Azure Databricks ETL Pipeline Deployment Script
# Run this script to deploy the infrastructure to Azure

# Login to Azure (uncomment if needed)
# Connect-AzAccount

# Set variables
$resourceGroupName = "{config['resource_group']}"
$location = "{config['location']}"
$storageAccountName = "{config['storage_account']}"
$factoryName = "{config['adf_factory']}"
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
New-AzStorageContainer -Name "{config['container_name']}" -Context $ctx -Permission Off

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
"""
        
        # Save PowerShell script
        with open(self.base_path / "deploy.ps1", 'w') as f:
            f.write(ps_script)
        
        print("✅ Deployment script created!")
        
    def generate_readme_section(self, config):
        """Generate customized README section"""
        readme_section = f"""
## Your Configuration

**Azure Resources:**
- Storage Account: `{config['storage_account']}`
- Container: `{config['container_name']}`
- Resource Group: `{config['resource_group']}`
- Data Factory: `{config['adf_factory']}`
- Region: `{config['location']}`

**Databricks Configuration:**
- Workspace URL: `{config['databricks_url']}`
- Cluster ID: `{config['cluster_id']}`
- User Email: `{config['user_email']}`

**Data Lake Paths:**
- Bronze: `abfss://{config['container_name']}@{config['storage_account']}.dfs.core.windows.net/bronze/`
- Silver: `abfss://{config['container_name']}@{config['storage_account']}.dfs.core.windows.net/silver/`
- Gold: `abfss://{config['container_name']}@{config['storage_account']}.dfs.core.windows.net/gold/`

## Quick Deployment

1. Run the deployment script:
   ```powershell
   ./deploy.ps1
   ```

2. Upload notebooks to Databricks:
   - Upload all files from `notebooks/` folder to `/Workspace/Users/{config['user_email']}/notebooks/`

3. Configure secrets in Databricks:
   ```python
   # Create secret scope
   databricks secrets create-scope --scope databricks-etl-secrets
   
   # Add storage account key
   databricks secrets put --scope databricks-etl-secrets --key storage-account-key
   ```

4. Test the pipeline by running notebooks manually, then trigger ADF pipeline.
"""
        return readme_section
        
    def run_setup(self):
        """Run the complete setup process"""
        self.welcome()
        
        # Get user configuration
        config = self.get_user_input()
        
        # Update files
        self.update_config_file(config)
        self.update_arm_templates(config)
        self.create_deployment_script(config)
        
        # Generate README section
        readme_section = self.generate_readme_section(config)
        
        # Save configuration summary
        with open(self.base_path / "YOUR_CONFIGURATION.md", 'w') as f:
            f.write(readme_section)
        
        print("\n" + "=" * 60)
        print("🎉 SETUP COMPLETED SUCCESSFULLY!")
        print("=" * 60)
        print()
        print("Files updated:")
        print("  ✅ config/pipeline_config.py")
        print("  ✅ ARM templates/ADF Template/ARMTemplateParametersForFactory.json")
        print("  ✅ deploy.ps1 (deployment script)")
        print("  ✅ YOUR_CONFIGURATION.md (your settings)")
        print()
        print("Next steps:")
        print("  1. Review YOUR_CONFIGURATION.md")
        print("  2. Run ./deploy.ps1 to deploy Azure resources")
        print("  3. Upload notebooks to Databricks")
        print("  4. Configure secrets and test pipeline")
        print()
        print("🚀 Ready to deploy your ETL pipeline!")

if __name__ == "__main__":
    setup = PipelineSetup()
    setup.run_setup()