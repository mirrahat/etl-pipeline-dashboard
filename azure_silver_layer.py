# Databricks Notebook: Azure ETL Pipeline - Silver Layer
# Developed by: Mir Hasibul Hasan Rahat
# This notebook implements data cleaning and enrichment for Azure cloud

# COMMAND ----------

# MAGIC %md
# MAGIC # Azure ETL Pipeline - Silver Layer Processing
# MAGIC **Developed by: Mir Hasibul Hasan Rahat**
# MAGIC 
# MAGIC This notebook implements the Silver layer processing with data cleaning and enrichment.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import re

print("=== AZURE ETL PIPELINE - SILVER LAYER ===")
print("Developed by: Mir Hasibul Hasan Rahat")
print(f"Started at: {datetime.now()}")

# COMMAND ----------

# Configuration
storage_account_name = "stdataengplatform"
container_bronze = "bronze"
container_silver = "silver"

bronze_path = f"abfss://{container_bronze}@{storage_account_name}.dfs.core.windows.net/"
silver_path = f"abfss://{container_silver}@{storage_account_name}.dfs.core.windows.net/"

print(f"Bronze Data Path: {bronze_path}")
print(f"Silver Data Path: {silver_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer Processing
# MAGIC Clean and enrich Bronze layer data

# COMMAND ----------

try:
    print("[SILVER LAYER] - Reading Bronze layer data...")
    
    # Read Bronze layer data
    bronze_df = spark.read.parquet(f"{bronze_path}movies/")
    
    print(f"Bronze records loaded: {bronze_df.count()}")
    
    # Data cleaning and enrichment
    silver_df = bronze_df.select(
        col("title").alias("movie_title"),
        col("year").cast("int").alias("release_year"),
        upper(col("genre")).alias("genre_standardized"),
        col("rating").cast("double").alias("imdb_rating"),
        col("budget").cast("long").alias("production_budget"),
        col("revenue").cast("long").alias("box_office_revenue"),
        col("ingestion_timestamp"),
        col("source_system")
    ).filter(
        # Data quality filters
        col("movie_title").isNotNull() &
        col("release_year").between(1900, 2030) &
        col("imdb_rating").between(0, 10) &
        col("production_budget") > 0 &
        col("box_office_revenue") >= 0
    ).withColumn(
        # Add business calculations
        "profit_loss", col("box_office_revenue") - col("production_budget")
    ).withColumn(
        "profit_margin_percent", 
        round((col("profit_loss") / col("production_budget")) * 100, 2)
    ).withColumn(
        # Add era classification
        "movie_era",
        when(col("release_year") < 1980, "Classic")
        .when(col("release_year") < 2000, "Modern")
        .when(col("release_year") < 2010, "Digital")
        .otherwise("Contemporary")
    ).withColumn(
        # Add processing metadata
        "silver_processing_timestamp", current_timestamp()
    ).withColumn(
        "processing_layer", lit("silver")
    )
    
    print(f"Silver records after cleaning: {silver_df.count()}")
    print("Sample Silver data:")
    silver_df.show(3, truncate=False)
    
    # Write to Silver layer with partitioning
    print(f"Writing to Silver layer: {silver_path}movies_clean/")
    silver_df.write.mode("overwrite").partitionBy("movie_era").parquet(f"{silver_path}movies_clean/")
    
    print("✓ Silver layer processing completed successfully!")
    
except Exception as e:
    print(f"✗ Error in Silver layer processing: {str(e)}")
    raise e

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Validation
# MAGIC Validate Silver layer data quality

# COMMAND ----------

print("[DATA QUALITY] - Silver layer validation...")

try:
    # Statistics
    stats_df = silver_df.describe()
    stats_df.show()
    
    # Genre distribution
    print("Genre distribution:")
    silver_df.groupBy("genre_standardized").count().orderBy(desc("count")).show()
    
    # Era distribution
    print("Era distribution:")
    silver_df.groupBy("movie_era").count().orderBy(desc("count")).show()
    
    # Revenue analysis
    print("Revenue statistics by genre:")
    silver_df.groupBy("genre_standardized").agg(
        avg("box_office_revenue").alias("avg_revenue"),
        max("box_office_revenue").alias("max_revenue"),
        min("box_office_revenue").alias("min_revenue")
    ).show()
    
    print("✓ Silver layer validation completed!")
    
except Exception as e:
    print(f"✗ Silver layer validation failed: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC Silver layer processing summary

# COMMAND ----------

print("=== SILVER LAYER SUMMARY ===")
print(f"✓ Bronze data read from: {bronze_path}")
print(f"✓ Silver data written to: {silver_path}")
print(f"✓ Records processed: {silver_df.count()}")
print(f"✓ Data partitioned by: movie_era")
print(f"✓ Processing completed at: {datetime.now()}")
print("")
print("Data enhancements applied:")
print("- Standardized column names and types")
print("- Added profit/loss calculations")
print("- Added profit margin percentages")
print("- Added movie era classification")
print("- Applied data quality filters")
print("")
print("Next: Run Gold layer analytics notebook")
print("Developed by: Mir Hasibul Hasan Rahat")