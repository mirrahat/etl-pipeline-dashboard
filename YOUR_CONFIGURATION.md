
## Your Configuration

**Azure Resources:**
- Storage Account: `git status`
- Container: `pip install -r requirements.txt`
- Resource Group: `python run_local_pipeline.py`
- Data Factory: `python "c:\Users\ASUS\Downloads\dataeng\Databricks-ETL-Pipeline-using-PySpark-and-Delta-Lake-Architecture-main\setup_azure_connection.py"`
- Region: `java -version`

**Databricks Configuration:**
- Workspace URL: `java -version`
- Cluster ID: `java -version`
- User Email: `python setup_azure_connection.py`

**Data Lake Paths:**
- Bronze: `abfss://pip install -r requirements.txt@git status.dfs.core.windows.net/bronze/`
- Silver: `abfss://pip install -r requirements.txt@git status.dfs.core.windows.net/silver/`
- Gold: `abfss://pip install -r requirements.txt@git status.dfs.core.windows.net/gold/`

## Quick Deployment

1. Run the deployment script:
   ```powershell
   ./deploy.ps1
   ```

2. Upload notebooks to Databricks:
   - Upload all files from `notebooks/` folder to `/Workspace/Users/python setup_azure_connection.py/notebooks/`

3. Configure secrets in Databricks:
   ```python
   # Create secret scope
   databricks secrets create-scope --scope databricks-etl-secrets
   
   # Add storage account key
   databricks secrets put --scope databricks-etl-secrets --key storage-account-key
   ```

4. Test the pipeline by running notebooks manually, then trigger ADF pipeline.
