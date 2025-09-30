# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Business Analytics Data
# MAGIC 
# MAGIC This notebook creates business-ready analytics datasets from Silver layer data.
# MAGIC 
# MAGIC **Layer Purpose:** Business logic and aggregations
# MAGIC - Create business-specific aggregations and KPIs
# MAGIC - Build dimension and fact tables for analytics
# MAGIC - Prepare data for reporting and dashboards
# MAGIC - Implement complex business rules

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Configuration

# COMMAND ----------

# Import required libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from delta.tables import *
from datetime import datetime

# Initialize Spark session
spark = SparkSession.builder \
    .appName("GoldLayer-BusinessAnalytics") \
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
SILVER_PATH = f"abfss://{CONTAINER_NAME}@{STORAGE_ACCOUNT}.dfs.core.windows.net/silver/"
GOLD_PATH = f"abfss://{CONTAINER_NAME}@{STORAGE_ACCOUNT}.dfs.core.windows.net/gold/"

print(f"Silver layer path: {SILVER_PATH}")
print(f"Gold layer path: {GOLD_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Silver Layer Data

# COMMAND ----------

# Read from Silver layer
try:
    silver_df = spark.read.format("delta").load(f"{SILVER_PATH}movies_clean")
    print(f"Loaded {silver_df.count()} records from Silver layer")
    
    # Show sample data
    print("Sample Silver data:")
    silver_df.show(5, truncate=False)
    
except Exception as e:
    print(f"Error reading Silver layer: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Business Logic Implementation

# COMMAND ----------

def create_movie_era_classification(df):
    """
    Classify movies into different eras based on business rules
    """
    print("Creating movie era classifications...")
    
    df = df.withColumn("movie_era",
                      when(col("year") < 1980, "Classic Era")
                      .when(col("year") < 2000, "Modern Era")
                      .when(col("year") < 2010, "Digital Era")
                      .otherwise("Contemporary Era"))
    
    return df

def create_performance_metrics(df):
    """
    Create business performance metrics
    """
    print("Creating performance metrics...")
    
    # Success metrics
    df = df.withColumn("is_blockbuster", 
                      when(col("box_office") >= 500000000, True).otherwise(False))
    
    df = df.withColumn("is_profitable", 
                      when(col("profit") > 0, True).otherwise(False))
    
    df = df.withColumn("is_highly_rated", 
                      when(col("rating") >= 8.0, True).otherwise(False))
    
    # Performance score (weighted combination of metrics)
    df = df.withColumn("performance_score",
                      round((col("rating") * 0.4 + 
                            when(col("roi") > 0, least(col("roi") / 100, 10), 0) * 0.3 +
                            when(col("box_office") >= 100000000, 5, 0) * 0.3), 2))
    
    return df

def create_popularity_tiers(df):
    """
    Create popularity tiers based on box office performance
    """
    print("Creating popularity tiers...")
    
    # Calculate percentiles for box office
    percentiles = df.approxQuantile("box_office", [0.25, 0.5, 0.75, 0.9], 0.01)
    p25, p50, p75, p90 = percentiles
    
    df = df.withColumn("popularity_tier",
                      when(col("box_office") >= p90, "Most Popular")
                      .when(col("box_office") >= p75, "Popular")
                      .when(col("box_office") >= p50, "Moderate")
                      .when(col("box_office") >= p25, "Low")
                      .otherwise("Niche"))
    
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Gold Layer Datasets

# COMMAND ----------

# Apply business logic transformations
print("=== APPLYING BUSINESS LOGIC TRANSFORMATIONS ===")

gold_movies = silver_df
gold_movies = create_movie_era_classification(gold_movies)
gold_movies = create_performance_metrics(gold_movies)
gold_movies = create_popularity_tiers(gold_movies)

# Add final Gold layer metadata
gold_movies = gold_movies.withColumn("gold_processing_date", current_timestamp()) \
                        .withColumn("business_date", current_date())

print(f"Gold movies dataset: {gold_movies.count()} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Business Analytics Tables

# COMMAND ----------

# 1. Movie Performance Summary
print("Creating Movie Performance Summary...")
movie_performance = gold_movies.select(
    "title", "year", "genre", "main_actor", "movie_era",
    "rating", "rating_category", "budget", "box_office", "profit", "roi",
    "performance_score", "popularity_tier", "is_blockbuster", 
    "is_profitable", "is_highly_rated"
).orderBy(desc("performance_score"))

# 2. Genre Analytics
print("Creating Genre Analytics...")
genre_analytics = gold_movies.groupBy("genre").agg(
    count("*").alias("total_movies"),
    round(avg("rating"), 2).alias("avg_rating"),
    round(avg("budget"), 0).alias("avg_budget"),
    round(avg("box_office"), 0).alias("avg_box_office"),
    round(avg("roi"), 2).alias("avg_roi"),
    sum(col("is_blockbuster").cast("int")).alias("blockbuster_count"),
    round(sum(col("is_blockbuster").cast("int")) / count("*") * 100, 2).alias("blockbuster_percentage")
).orderBy(desc("avg_rating"))

# 3. Era Analytics  
print("Creating Era Analytics...")
era_analytics = gold_movies.groupBy("movie_era").agg(
    count("*").alias("total_movies"),
    round(avg("rating"), 2).alias("avg_rating"),
    round(avg("budget"), 0).alias("avg_budget"),
    round(avg("box_office"), 0).alias("avg_box_office"),
    sum(col("is_profitable").cast("int")).alias("profitable_count"),
    round(sum(col("is_profitable").cast("int")) / count("*") * 100, 2).alias("profitability_rate")
).orderBy("movie_era")

# 4. Top Performers by Category
print("Creating Top Performers...")
window_spec = Window.partitionBy("genre").orderBy(desc("performance_score"))
top_performers = gold_movies.withColumn("rank_in_genre", 
                                       row_number().over(window_spec)) \
                           .filter(col("rank_in_genre") <= 3) \
                           .select("genre", "title", "year", "rating", 
                                  "performance_score", "rank_in_genre")

# 5. Financial Performance KPIs
print("Creating Financial KPIs...")
financial_kpis = gold_movies.agg(
    count("*").alias("total_movies"),
    sum("budget").alias("total_budget"),
    sum("box_office").alias("total_revenue"),
    sum("profit").alias("total_profit"),
    round(avg("roi"), 2).alias("avg_roi"),
    sum(col("is_profitable").cast("int")).alias("profitable_movies"),
    sum(col("is_blockbuster").cast("int")).alias("blockbuster_movies")
).withColumn("profitability_rate", 
            round(col("profitable_movies") / col("total_movies") * 100, 2)) \
 .withColumn("blockbuster_rate", 
            round(col("blockbuster_movies") / col("total_movies") * 100, 2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Display Business Insights

# COMMAND ----------

print("=== BUSINESS ANALYTICS INSIGHTS ===")

print("1. FINANCIAL PERFORMANCE KPIs:")
financial_kpis.show(truncate=False)

print("2. GENRE PERFORMANCE:")
genre_analytics.show(truncate=False)

print("3. ERA ANALYSIS:")
era_analytics.show(truncate=False)

print("4. TOP PERFORMERS BY GENRE:")
top_performers.show(50, truncate=False)

print("5. SAMPLE MOVIE PERFORMANCE DATA:")
movie_performance.show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Gold Layer Tables

# COMMAND ----------

# Save all Gold layer tables
tables_to_save = [
    (movie_performance, "movie_performance_summary", "Comprehensive movie performance data"),
    (genre_analytics, "genre_analytics", "Genre-based analytics and KPIs"),
    (era_analytics, "era_analytics", "Historical era analysis"),
    (top_performers, "top_performers_by_genre", "Top performing movies by genre"),
    (financial_kpis, "financial_kpis", "Overall financial performance metrics")
]

for df, table_name, description in tables_to_save:
    try:
        table_path = f"{GOLD_PATH}{table_name}"
        
        print(f"Saving {description} to {table_name}...")
        
        df.write \
          .format("delta") \
          .mode("overwrite") \
          .option("mergeSchema", "true") \
          .save(table_path)
        
        # Create Delta table
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS gold.{table_name}
            USING DELTA
            LOCATION '{table_path}'
        """)
        
        print(f"✅ Successfully saved {df.count()} records to gold.{table_name}")
        
    except Exception as e:
        print(f"❌ Error saving {table_name}: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Business Report

# COMMAND ----------

print("=== EXECUTIVE BUSINESS REPORT ===")
print(f"Report generated on: {datetime.now()}")
print()

# Get key metrics
kpis = financial_kpis.collect()[0]
top_genre = genre_analytics.orderBy(desc("avg_rating")).first()
most_profitable_era = era_analytics.orderBy(desc("profitability_rate")).first()

print("KEY BUSINESS METRICS:")
print(f"📊 Total Movies Analyzed: {kpis['total_movies']:,}")
print(f"💰 Total Investment: ${kpis['total_budget']:,}")
print(f"💵 Total Revenue: ${kpis['total_revenue']:,}")
print(f"💸 Total Profit: ${kpis['total_profit']:,}")
print(f"📈 Average ROI: {kpis['avg_roi']:.2f}%")
print(f"✅ Profitability Rate: {kpis['profitability_rate']:.1f}%")
print(f"🎬 Blockbuster Rate: {kpis['blockbuster_rate']:.1f}%")
print()

print("TOP INSIGHTS:")
print(f"🏆 Best Performing Genre: {top_genre['genre']} (Avg Rating: {top_genre['avg_rating']})")
print(f"💡 Most Profitable Era: {most_profitable_era['movie_era']} ({most_profitable_era['profitability_rate']:.1f}% success rate)")
print(f"🎯 Total Blockbusters: {kpis['blockbuster_movies']} movies")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Export for BI Tools

# COMMAND ----------

# Export key tables to Parquet for external BI tools
export_tables = ["movie_performance_summary", "genre_analytics", "era_analytics"]

for table_name in export_tables:
    try:
        parquet_path = f"{GOLD_PATH}exports/{table_name}.parquet"
        
        spark.sql(f"SELECT * FROM gold.{table_name}") \
             .coalesce(1) \
             .write \
             .mode("overwrite") \
             .parquet(parquet_path)
        
        print(f"✅ Exported gold.{table_name} to Parquet format")
        
    except Exception as e:
        print(f"❌ Error exporting {table_name}: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup and Final Logging

# COMMAND ----------

# Log completion
processing_time = datetime.now()
print(f"=== GOLD LAYER PROCESSING COMPLETE ===")
print(f"Processing completed at: {processing_time}")
print(f"Total tables created: {len(tables_to_save)}")
print()
print("📋 AVAILABLE GOLD LAYER TABLES:")
for _, table_name, description in tables_to_save:
    print(f"  • gold.{table_name} - {description}")

print()
print("🎯 PIPELINE STATUS: SUCCESS")
print("💾 Data ready for analytics and reporting!")
print("🔄 ETL Pipeline (Bronze → Silver → Gold) completed successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC 
# MAGIC The Gold layer processing is complete! We've created business-ready analytics datasets including:
# MAGIC 
# MAGIC ### 📊 Analytics Tables Created:
# MAGIC - **Movie Performance Summary**: Complete movie dataset with business metrics
# MAGIC - **Genre Analytics**: Genre-based performance and trends
# MAGIC - **Era Analytics**: Historical performance analysis
# MAGIC - **Top Performers**: Best movies by category
# MAGIC - **Financial KPIs**: Overall business performance metrics
# MAGIC 
# MAGIC ### 🎯 Key Business Features:
# MAGIC - Movie era classification (Classic, Modern, Digital, Contemporary)
# MAGIC - Performance scoring algorithm
# MAGIC - Popularity tiers based on box office
# MAGIC - Blockbuster and profitability flags
# MAGIC - ROI and financial metrics
# MAGIC 
# MAGIC ### 📈 Ready for:
# MAGIC - Power BI dashboards
# MAGIC - Business intelligence reporting
# MAGIC - Data science and ML workflows
# MAGIC - Executive reporting and insights
# MAGIC 
# MAGIC **The complete Lakehouse ETL pipeline (Bronze → Silver → Gold) is now operational!** 🚀