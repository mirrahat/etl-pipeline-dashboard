# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer - Data Extraction
# MAGIC 
# MAGIC This notebook handles the extraction and initial loading of raw data into the Bronze layer.
# MAGIC 
# MAGIC **Layer Purpose:** Raw data ingestion with minimal processing
# MAGIC - Ingest data from various sources (APIs, files, databases)
# MAGIC - Apply basic schema but preserve original structure
# MAGIC - Store in Delta format for versioning and reliability

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Configuration

# COMMAND ----------

# Import required libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *
import requests
import json
from datetime import datetime, date

# Initialize Spark session
spark = SparkSession.builder \
    .appName("BronzeLayer-DataExtraction") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration Parameters

# COMMAND ----------

# Storage configuration
STORAGE_ACCOUNT = "your_storage_account"  # Replace with your storage account name
CONTAINER_NAME = "datalake"
BRONZE_PATH = f"abfss://{CONTAINER_NAME}@{STORAGE_ACCOUNT}.dfs.core.windows.net/bronze/"
CHECKPOINT_PATH = f"abfss://{CONTAINER_NAME}@{STORAGE_ACCOUNT}.dfs.core.windows.net/checkpoints/bronze/"

# Data source configuration
KAGGLE_API_KEY = dbutils.secrets.get(scope="kaggle-secrets", key="api-key")  # Store in Databricks secrets
KAGGLE_USERNAME = dbutils.secrets.get(scope="kaggle-secrets", key="username")

print(f"Bronze layer path: {BRONZE_PATH}")
print(f"Checkpoint path: {CHECKPOINT_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Extraction Functions

# COMMAND ----------

def extract_movies_data():
    """
    Extract movies data from various sources
    For demo purposes, we'll create sample data and also show Kaggle API integration
    """
    
    # Sample movie data structure (replace with actual API calls)
    sample_data = [
        ("The Matrix", 1999, 8.7, "Sci-Fi", "Keanu Reeves", 136000000, 463517383),
        ("Inception", 2010, 8.8, "Sci-Fi", "Leonardo DiCaprio", 160000000, 836836967),
        ("The Dark Knight", 2008, 9.0, "Action", "Christian Bale", 185000000, 1004558444),
        ("Pulp Fiction", 1994, 8.9, "Crime", "John Travolta", 8000000, 214179088),
        ("The Shawshank Redemption", 1994, 9.3, "Drama", "Tim Robbins", 25000000, 16000000),
        ("Forrest Gump", 1994, 8.8, "Drama", "Tom Hanks", 55000000, 677387716),
        ("Fight Club", 1999, 8.8, "Drama", "Brad Pitt", 63000000, 100853753),
        ("The Lord of the Rings: The Fellowship", 2001, 8.8, "Fantasy", "Elijah Wood", 93000000, 871368364),
        ("Star Wars: Episode IV", 1977, 8.6, "Sci-Fi", "Mark Hamill", 11000000, 775398007),
        ("Goodfellas", 1990, 8.7, "Crime", "Robert De Niro", 25000000, 46842311)
    ]
    
    # Define schema
    schema = StructType([
        StructField("title", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("rating", DoubleType(), True),
        StructField("genre", StringType(), True),
        StructField("main_actor", StringType(), True),
        StructField("budget", LongType(), True),
        StructField("box_office", LongType(), True)
    ])
    
    # Create DataFrame
    df = spark.createDataFrame(sample_data, schema)
    
    # Add extraction metadata
    df = df.withColumn("extraction_date", current_timestamp()) \
           .withColumn("source", lit("sample_data")) \
           .withColumn("file_name", lit("movies_sample.json"))
    
    return df

def extract_kaggle_dataset(dataset_name, file_name):
    """
    Extract data from Kaggle API (placeholder - implement based on your dataset)
    """
    try:
        print(f"Extracting {dataset_name} from Kaggle...")
        
        # Kaggle API integration would go here
        # This is a placeholder for actual implementation
        
        # For now, return None to indicate external data source
        return None
        
    except Exception as e:
        print(f"Error extracting from Kaggle: {str(e)}")
        return None

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Extraction Execution

# COMMAND ----------

# Extract movies data
print("Starting data extraction...")
movies_df = extract_movies_data()

# Display sample data
print("Sample extracted data:")
movies_df.show(5, truncate=False)
print(f"Total records extracted: {movies_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save to Bronze Layer

# COMMAND ----------

try:
    # Write to Bronze layer as Delta table
    bronze_table_path = f"{BRONZE_PATH}movies_raw"
    
    movies_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .save(bronze_table_path)
    
    # Create/replace Delta table
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS bronze.movies_raw
        USING DELTA
        LOCATION '{bronze_table_path}'
    """)
    
    print(f"Successfully saved {movies_df.count()} records to Bronze layer!")
    print(f"Table location: {bronze_table_path}")
    
    # Show table info
    spark.sql("DESCRIBE EXTENDED bronze.movies_raw").show(truncate=False)
    
except Exception as e:
    print(f"Error saving to Bronze layer: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Checks

# COMMAND ----------

# Basic data quality checks
print("=== BRONZE LAYER DATA QUALITY REPORT ===")
print(f"Total records: {movies_df.count()}")
print(f"Unique titles: {movies_df.select('title').distinct().count()}")
print(f"Year range: {movies_df.agg(min('year'), max('year')).collect()[0]}")
print(f"Null values per column:")

# Check for nulls in each column
for col_name in movies_df.columns:
    null_count = movies_df.filter(col(col_name).isNull()).count()
    print(f"  {col_name}: {null_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup and Logging

# COMMAND ----------

# Log completion
extraction_time = datetime.now()
print(f"Bronze layer extraction completed at: {extraction_time}")
print("Ready for Silver layer transformation!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC 
# MAGIC The Bronze layer extraction is complete. The raw data is now stored in Delta format with:
# MAGIC - Original data structure preserved
# MAGIC - Extraction metadata added
# MAGIC - Delta versioning enabled
# MAGIC 
# MAGIC **Next:** Run the Transform notebook for Silver layer processing