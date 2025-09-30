# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer - Data Transformation
# MAGIC 
# MAGIC This notebook handles the transformation of Bronze layer data into clean, validated Silver layer data.
# MAGIC 
# MAGIC **Layer Purpose:** Clean and standardize data
# MAGIC - Remove duplicates and handle null values
# MAGIC - Standardize data types and formats
# MAGIC - Apply business rules and data validation
# MAGIC - Create analytics-ready clean datasets

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Configuration

# COMMAND ----------

# Import required libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *
import re
from datetime import datetime

# Initialize Spark session
spark = SparkSession.builder \
    .appName("SilverLayer-DataTransformation") \
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
SILVER_PATH = f"abfss://{CONTAINER_NAME}@{STORAGE_ACCOUNT}.dfs.core.windows.net/silver/"
CHECKPOINT_PATH = f"abfss://{CONTAINER_NAME}@{STORAGE_ACCOUNT}.dfs.core.windows.net/checkpoints/silver/"

print(f"Bronze layer path: {BRONZE_PATH}")
print(f"Silver layer path: {SILVER_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Bronze Layer Data

# COMMAND ----------

# Read from Bronze layer
try:
    bronze_df = spark.read.format("delta").load(f"{BRONZE_PATH}movies_raw")
    print(f"Loaded {bronze_df.count()} records from Bronze layer")
    
    # Show sample data
    print("Sample Bronze data:")
    bronze_df.show(5, truncate=False)
    
except Exception as e:
    print(f"Error reading Bronze layer: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Transformation Functions

# COMMAND ----------

def clean_text_data(df):
    """
    Clean and standardize text fields
    """
    print("Applying text cleaning transformations...")
    
    # Clean title field - remove extra spaces, standardize case
    df = df.withColumn("title_clean", 
                      regexp_replace(trim(col("title")), "\\s+", " "))
    
    # Clean genre field
    df = df.withColumn("genre_clean", 
                      initcap(trim(col("genre"))))
    
    # Clean main_actor field
    df = df.withColumn("main_actor_clean", 
                      initcap(trim(col("main_actor"))))
    
    return df

def handle_missing_values(df):
    """
    Handle null and missing values
    """
    print("Handling missing values...")
    
    # Fill missing ratings with median
    median_rating = df.approxQuantile("rating", [0.5], 0.25)[0]
    df = df.fillna({"rating": median_rating})
    
    # Fill missing budget with 0 (unknown budget)
    df = df.fillna({"budget": 0})
    
    # Fill missing box_office with 0 (unknown revenue)
    df = df.fillna({"box_office": 0})
    
    # Remove records with missing essential fields
    df = df.filter(col("title").isNotNull() & (col("title") != ""))
    df = df.filter(col("year").isNotNull())
    
    return df

def validate_data_types(df):
    """
    Validate and correct data types
    """
    print("Validating data types...")
    
    # Ensure year is within reasonable range
    df = df.filter((col("year") >= 1800) & (col("year") <= year(current_date()) + 2))
    
    # Ensure rating is within valid range (0-10)
    df = df.filter((col("rating") >= 0) & (col("rating") <= 10))
    
    # Ensure budget and box_office are non-negative
    df = df.filter(col("budget") >= 0)
    df = df.filter(col("box_office") >= 0)
    
    return df

def remove_duplicates(df):
    """
    Remove duplicate records
    """
    print("Removing duplicates...")
    
    initial_count = df.count()
    
    # Remove duplicates based on title and year
    df = df.dropDuplicates(["title", "year"])
    
    final_count = df.count()
    print(f"Removed {initial_count - final_count} duplicate records")
    
    return df

def add_calculated_fields(df):
    """
    Add calculated fields for analytics
    """
    print("Adding calculated fields...")
    
    # Calculate profit/loss
    df = df.withColumn("profit", col("box_office") - col("budget"))
    
    # Calculate ROI (Return on Investment)
    df = df.withColumn("roi", 
                      when(col("budget") > 0, 
                           round((col("box_office") - col("budget")) / col("budget") * 100, 2))
                      .otherwise(0))
    
    # Add decade
    df = df.withColumn("decade", (floor(col("year") / 10) * 10).cast("integer"))
    
    # Add rating category
    df = df.withColumn("rating_category",
                      when(col("rating") >= 9.0, "Excellent")
                      .when(col("rating") >= 8.0, "Very Good")
                      .when(col("rating") >= 7.0, "Good")
                      .when(col("rating") >= 6.0, "Average")
                      .otherwise("Below Average"))
    
    # Add budget category
    df = df.withColumn("budget_category",
                      when(col("budget") >= 100000000, "Big Budget")
                      .when(col("budget") >= 50000000, "Medium Budget")
                      .when(col("budget") >= 10000000, "Low Budget")
                      .when(col("budget") > 0, "Micro Budget")
                      .otherwise("Unknown Budget"))
    
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply Transformations

# COMMAND ----------

# Apply all transformations step by step
print("=== STARTING SILVER LAYER TRANSFORMATIONS ===")

# Step 1: Clean text data
silver_df = clean_text_data(bronze_df)
print(f"After text cleaning: {silver_df.count()} records")

# Step 2: Handle missing values
silver_df = handle_missing_values(silver_df)
print(f"After handling missing values: {silver_df.count()} records")

# Step 3: Validate data types
silver_df = validate_data_types(silver_df)
print(f"After data validation: {silver_df.count()} records")

# Step 4: Remove duplicates
silver_df = remove_duplicates(silver_df)
print(f"After removing duplicates: {silver_df.count()} records")

# Step 5: Add calculated fields
silver_df = add_calculated_fields(silver_df)
print(f"After adding calculated fields: {silver_df.count()} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Validation

# COMMAND ----------

print("=== SILVER LAYER DATA QUALITY REPORT ===")

# Basic statistics
silver_df.describe().show()

# Check for nulls in cleaned data
print("Null values in Silver layer:")
for col_name in silver_df.columns:
    null_count = silver_df.filter(col(col_name).isNull()).count()
    if null_count > 0:
        print(f"  {col_name}: {null_count}")

# Show sample of transformed data
print("Sample transformed data:")
silver_df.select("title_clean", "year", "rating", "rating_category", 
                "budget_category", "profit", "roi").show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Final Silver Dataset

# COMMAND ----------

# Select final columns for Silver layer
silver_final = silver_df.select(
    col("title_clean").alias("title"),
    col("year"),
    col("rating"),
    col("genre_clean").alias("genre"),
    col("main_actor_clean").alias("main_actor"),
    col("budget"),
    col("box_office"),
    col("profit"),
    col("roi"),
    col("decade"),
    col("rating_category"),
    col("budget_category"),
    current_timestamp().alias("transformation_date"),
    lit("silver_processed").alias("processing_stage")
)

# Add row quality score
silver_final = silver_final.withColumn("data_quality_score",
    when((col("title").isNotNull()) & 
         (col("year").isNotNull()) & 
         (col("rating").isNotNull()) &
         (col("budget") >= 0) &
         (col("box_office") >= 0), 100)
    .otherwise(80)
)

print(f"Final Silver dataset: {silver_final.count()} records")
silver_final.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save to Silver Layer

# COMMAND ----------

try:
    # Write to Silver layer as Delta table
    silver_table_path = f"{SILVER_PATH}movies_clean"
    
    silver_final.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .save(silver_table_path)
    
    # Create/replace Delta table
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS silver.movies_clean
        USING DELTA
        LOCATION '{silver_table_path}'
    """)
    
    print(f"Successfully saved {silver_final.count()} records to Silver layer!")
    print(f"Table location: {silver_table_path}")
    
    # Show table info
    spark.sql("DESCRIBE EXTENDED silver.movies_clean").show(truncate=False)
    
except Exception as e:
    print(f"Error saving to Silver layer: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer Statistics

# COMMAND ----------

# Generate statistics for Silver layer
print("=== SILVER LAYER STATISTICS ===")

# Rating distribution
print("Rating distribution:")
silver_final.groupBy("rating_category").count().orderBy("count", ascending=False).show()

# Genre distribution
print("Genre distribution:")
silver_final.groupBy("genre").count().orderBy("count", ascending=False).show()

# Decade distribution
print("Movies by decade:")
silver_final.groupBy("decade").count().orderBy("decade").show()

# Budget analysis
print("Average metrics by budget category:")
silver_final.groupBy("budget_category") \
    .agg(avg("rating").alias("avg_rating"),
         avg("roi").alias("avg_roi"),
         count("*").alias("movie_count")) \
    .orderBy("avg_rating", ascending=False).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup and Logging

# COMMAND ----------

# Log completion
transformation_time = datetime.now()
print(f"Silver layer transformation completed at: {transformation_time}")
print("Data is now clean and ready for Gold layer business logic!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC 
# MAGIC The Silver layer transformation is complete. The clean data now includes:
# MAGIC - Standardized text fields
# MAGIC - Validated data types and ranges
# MAGIC - Calculated fields (profit, ROI, categories)
# MAGIC - Quality scores and metadata
# MAGIC 
# MAGIC **Next:** Run the Load notebook for Gold layer business aggregations