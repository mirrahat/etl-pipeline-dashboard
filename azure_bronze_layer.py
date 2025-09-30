# Databricks Notebook: Azure ETL Pipeline - Bronze Layer
# Developed by: Mir Hasibul Hasan Rahat
# This notebook processes data in Azure Data Lake Storage

# COMMAND ----------

# MAGIC %md
# MAGIC # Azure ETL Pipeline - Bronze Layer Processing
# MAGIC **Developed by: Mir Hasibul Hasan Rahat**
# MAGIC 
# MAGIC This notebook implements the Bronze layer processing for the Azure cloud version of our local data engineering platform.

# COMMAND ----------

# Import required libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
from datetime import datetime

print("=== AZURE ETL PIPELINE - BRONZE LAYER ===")
print("Developed by: Mir Hasibul Hasan Rahat")
print(f"Started at: {datetime.now()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC Set up Azure Storage connection and paths

# COMMAND ----------

# Configuration - Update these values for your environment
storage_account_name = "stdataengplatform"
container_raw = "raw"
container_bronze = "bronze"

# Construct Azure Data Lake paths
raw_path = f"abfss://{container_raw}@{storage_account_name}.dfs.core.windows.net/"
bronze_path = f"abfss://{container_bronze}@{storage_account_name}.dfs.core.windows.net/"

print(f"Raw Data Path: {raw_path}")
print(f"Bronze Data Path: {bronze_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer Processing
# MAGIC Read raw data and standardize format

# COMMAND ----------

try:
    # Read raw JSON data from Azure Storage
    print("[BRONZE LAYER] - Reading raw data from Azure Storage...")
    
    raw_df = spark.read.option("multiline", "true").json(f"{raw_path}movies.json")
    
    # Add metadata columns for data lineage
    bronze_df = raw_df.select(
        "*",
        current_timestamp().alias("ingestion_timestamp"),
        lit("azure_etl_pipeline").alias("source_system"),
        lit("bronze").alias("processing_layer")
    )
    
    # Show sample data
    print(f"Records processed: {bronze_df.count()}")
    print("Sample Bronze data:")
    bronze_df.show(3, truncate=False)
    
    # Write to Bronze layer in Parquet format
    print(f"Writing to Bronze layer: {bronze_path}movies/")
    bronze_df.write.mode("overwrite").parquet(f"{bronze_path}movies/")
    
    print("✓ Bronze layer processing completed successfully!")
    
except Exception as e:
    print(f"✗ Error in Bronze layer processing: {str(e)}")
    raise e

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Checks
# MAGIC Validate data quality and completeness

# COMMAND ----------

# Data quality validation
print("[DATA QUALITY] - Running validation checks...")

try:
    # Check for null values in critical columns
    null_check = bronze_df.select([count(when(col(c).isNull(), c)).alias(c) for c in bronze_df.columns])
    null_check.show()
    
    # Check for duplicate records
    total_records = bronze_df.count()
    distinct_records = bronze_df.distinct().count()
    duplicates = total_records - distinct_records
    
    print(f"Total records: {total_records}")
    print(f"Distinct records: {distinct_records}")
    print(f"Duplicate records: {duplicates}")
    
    # Schema validation
    print("Bronze layer schema:")
    bronze_df.printSchema()
    
    if duplicates == 0:
        print("✓ Data quality checks passed!")
    else:
        print(f"⚠ Warning: {duplicates} duplicate records found")
        
except Exception as e:
    print(f"✗ Data quality check failed: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary and Next Steps
# MAGIC Bronze layer processing completed

# COMMAND ----------

print("=== BRONZE LAYER SUMMARY ===")
print(f"✓ Raw data ingested from: {raw_path}")
print(f"✓ Bronze data written to: {bronze_path}")
print(f"✓ Records processed: {bronze_df.count()}")
print(f"✓ Processing completed at: {datetime.now()}")
print("")
print("Next: Run Silver layer processing notebook")
print("Developed by: Mir Hasibul Hasan Rahat")