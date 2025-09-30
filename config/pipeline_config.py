# Configuration file for Databricks ETL Pipeline
# Update these values to match your Azure environment

# Azure Data Lake Storage Gen2 Configuration
STORAGE_ACCOUNT_NAME = "git status"  # Replace with your storage account name
CONTAINER_NAME = "pip install -r requirements.txt"
RESOURCE_GROUP = "python run_local_pipeline.py"

# Azure Databricks Configuration
DATABRICKS_WORKSPACE_URL = "java -version"  # Replace with your workspace URL
DATABRICKS_CLUSTER_ID = "java -version"  # Replace with your cluster ID

# Azure Data Factory Configuration
ADF_FACTORY_NAME = "python "c:\Users\ASUS\Downloads\dataeng\Databricks-ETL-Pipeline-using-PySpark-and-Delta-Lake-Architecture-main\setup_azure_connection.py""
ADF_LOCATION = "java -version"  # or your preferred region

# Databricks Notebook Paths (update with your workspace paths)
EXTRACT_NOTEBOOK_PATH = "/Workspace/Users/python setup_azure_connection.py/notebooks/01_Extract_Bronze"
TRANSFORM_NOTEBOOK_PATH = "/Workspace/Users/python setup_azure_connection.py/notebooks/02_Transform_Silver"  
LOAD_NOTEBOOK_PATH = "/Workspace/Users/python setup_azure_connection.py/notebooks/03_Load_Gold"

# Data Lake Paths
BRONZE_PATH = f"abfss://{CONTAINER_NAME}@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/bronze/"
SILVER_PATH = f"abfss://{CONTAINER_NAME}@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/silver/"
GOLD_PATH = f"abfss://{CONTAINER_NAME}@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/gold/"
CHECKPOINT_PATH = f"abfss://{CONTAINER_NAME}@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/checkpoints/"

# Secret Scope Configuration (for secure credential management)
SECRET_SCOPE_NAME = "databricks-etl-secrets"
STORAGE_KEY_SECRET = "storage-account-key"
DATABRICKS_TOKEN_SECRET = "databricks-access-token"

# Pipeline Schedule Configuration
SCHEDULE_TIMEZONE = "East US"  # Change to your timezone
WEEKLY_SCHEDULE_DAY = "Saturday"  # Day of week for weekly runs
WEEKLY_SCHEDULE_TIME = "18:00:00"  # Time for weekly runs

# Data Quality Thresholds
MIN_RECORDS_BRONZE = 1
MIN_RECORDS_SILVER = 1
MIN_DATA_QUALITY_SCORE = 80

# Notification Configuration (optional)
NOTIFICATION_EMAIL = "python setup_azure_connection.py"
ENABLE_NOTIFICATIONS = True

print("Configuration loaded successfully!")
print(f"Storage Account: {STORAGE_ACCOUNT_NAME}")
print(f"Container: {CONTAINER_NAME}")
print(f"ADF Factory: {ADF_FACTORY_NAME}")