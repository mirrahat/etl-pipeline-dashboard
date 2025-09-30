"""
Azure Integration Setup for Local Development
This script helps connect your local pipeline to Azure resources
"""

import os
import json
from pathlib import Path

class AzureConnector:
    def __init__(self):
        self.config_file = "azure_config.json"
        self.config = {}
        
    def setup_azure_connection(self):
        """Interactive setup for Azure connection"""
        print("🔗 AZURE CONNECTION SETUP")
        print("=" * 50)
        print("This will help you connect your local pipeline to Azure resources.")
        print()
        
        # Collect Azure configuration
        azure_config = {
            "subscription_id": input("Enter your Azure Subscription ID: ").strip(),
            "resource_group": input("Enter Resource Group name: ").strip(),
            "storage_account": input("Enter Storage Account name: ").strip(),
            "container_name": input("Enter Container name [datalake]: ").strip() or "datalake",
            "databricks_workspace": input("Enter Databricks workspace URL: ").strip(),
            "databricks_cluster_id": input("Enter Databricks cluster ID: ").strip(),
            "tenant_id": input("Enter Azure Tenant ID (optional): ").strip(),
            "region": input("Enter Azure region [East US]: ").strip() or "East US"
        }
        
        # Save configuration
        with open(self.config_file, 'w') as f:
            json.dump(azure_config, f, indent=2)
        
        print(f"\n✅ Configuration saved to {self.config_file}")
        
        # Generate connection strings
        self.generate_connection_examples(azure_config)
        
    def generate_connection_examples(self, config):
        """Generate example connection code"""
        
        connection_examples = f"""
# Azure Storage Connection String Example
storage_connection_string = "DefaultEndpointsProtocol=https;AccountName={config['storage_account']};AccountKey=YOUR_ACCOUNT_KEY;EndpointSuffix=core.windows.net"

# Databricks Connection Example  
databricks_host = "{config['databricks_workspace']}"
databricks_token = "YOUR_DATABRICKS_TOKEN"

# Data Lake Path Examples
bronze_path = "abfss://{config['container_name']}@{config['storage_account']}.dfs.core.windows.net/bronze/"
silver_path = "abfss://{config['container_name']}@{config['storage_account']}.dfs.core.windows.net/silver/"
gold_path = "abfss://{config['container_name']}@{config['storage_account']}.dfs.core.windows.net/gold/"

# Spark Configuration for Azure
spark_config = {{
    "spark.hadoop.fs.azure.account.auth.type.{config['storage_account']}.dfs.core.windows.net": "OAuth",
    "spark.hadoop.fs.azure.account.oauth.provider.type.{config['storage_account']}.dfs.core.windows.net": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "spark.hadoop.fs.azure.account.oauth2.client.id.{config['storage_account']}.dfs.core.windows.net": "YOUR_CLIENT_ID",
    "spark.hadoop.fs.azure.account.oauth2.client.secret.{config['storage_account']}.dfs.core.windows.net": "YOUR_CLIENT_SECRET",
    "spark.hadoop.fs.azure.account.oauth2.client.endpoint.{config['storage_account']}.dfs.core.windows.net": "https://login.microsoftonline.com/{config['tenant_id']}/oauth2/token"
}}
"""
        
        # Save examples
        with open("azure_connection_examples.py", 'w') as f:
            f.write(connection_examples)
        
        print(f"📝 Connection examples saved to azure_connection_examples.py")
        
    def create_hybrid_config(self):
        """Create configuration that works both locally and with Azure"""
        
        if not os.path.exists(self.config_file):
            print("❌ Azure config not found. Run setup_azure_connection() first.")
            return
            
        with open(self.config_file, 'r') as f:
            azure_config = json.load(f)
        
        hybrid_config = f'''
"""
Hybrid Configuration - Works locally and with Azure
"""
import os
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

class HybridConfig:
    def __init__(self, use_azure=False):
        self.use_azure = use_azure
        self.azure_config = {azure_config}
        
        if use_azure:
            self.setup_azure_paths()
        else:
            self.setup_local_paths()
    
    def setup_local_paths(self):
        """Setup for local development"""
        base_path = os.getcwd()
        self.bronze_path = os.path.join(base_path, "data", "bronze")
        self.silver_path = os.path.join(base_path, "data", "silver") 
        self.gold_path = os.path.join(base_path, "data", "gold")
        
        # Create directories
        for path in [self.bronze_path, self.silver_path, self.gold_path]:
            os.makedirs(path, exist_ok=True)
    
    def setup_azure_paths(self):
        """Setup for Azure integration"""
        storage_account = self.azure_config["storage_account"]
        container = self.azure_config["container_name"]
        
        base_path = f"abfss://{{container}}@{{storage_account}}.dfs.core.windows.net"
        self.bronze_path = f"{{base_path}}/bronze/"
        self.silver_path = f"{{base_path}}/silver/"
        self.gold_path = f"{{base_path}}/gold/"
    
    def get_spark_session(self):
        """Get Spark session configured for local or Azure"""
        builder = SparkSession.builder.appName("HybridETLPipeline")
        
        if self.use_azure:
            # Add Azure configurations
            for key, value in self.get_azure_spark_config().items():
                builder = builder.config(key, value)
        else:
            # Local configuration
            builder = builder.master("local[*]")
        
        builder = builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \\
                        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        
        return configure_spark_with_delta_pip(builder).getOrCreate()
    
    def get_azure_spark_config(self):
        """Get Azure-specific Spark configuration"""
        storage_account = self.azure_config["storage_account"]
        return {{
            "spark.hadoop.fs.azure.account.auth.type.{{storage_account}}.dfs.core.windows.net": "OAuth",
            # Add other Azure configs as needed
        }}

# Global instances
local_config = HybridConfig(use_azure=False)
azure_config = HybridConfig(use_azure=True)
'''
        
        with open("config/hybrid_config.py", 'w') as f:
            f.write(hybrid_config)
        
        print("✅ Hybrid configuration created: config/hybrid_config.py")

def main():
    """Main setup function"""
    connector = AzureConnector()
    
    print("Choose an option:")
    print("1. Setup Azure connection")
    print("2. Create hybrid configuration")
    print("3. Both")
    
    choice = input("Enter choice (1-3): ").strip()
    
    if choice in ['1', '3']:
        connector.setup_azure_connection()
    
    if choice in ['2', '3']:
        connector.create_hybrid_config()
    
    print("\n🎉 Setup complete!")
    print("\nNext steps:")
    print("1. Update azure_connection_examples.py with your actual credentials")
    print("2. Test local pipeline first: python run_local_pipeline.py")
    print("3. Then test with Azure: python run_hybrid_pipeline.py")

if __name__ == "__main__":
    main()