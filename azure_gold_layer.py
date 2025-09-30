# Databricks Notebook: Azure ETL Pipeline - Gold Layer
# Developed by: Mir Hasibul Hasan Rahat
# This notebook creates business-ready analytics datasets

# COMMAND ----------

# MAGIC %md
# MAGIC # Azure ETL Pipeline - Gold Layer Analytics
# MAGIC **Developed by: Mir Hasibul Hasan Rahat**
# MAGIC 
# MAGIC This notebook creates business-ready analytics and aggregations for reporting and BI tools.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime

print("=== AZURE ETL PIPELINE - GOLD LAYER ===")
print("Developed by: Mir Hasibul Hasan Rahat")
print(f"Started at: {datetime.now()}")

# COMMAND ----------

# Configuration
storage_account_name = "stdataengplatform"
container_silver = "silver"
container_gold = "gold"

silver_path = f"abfss://{container_silver}@{storage_account_name}.dfs.core.windows.net/"
gold_path = f"abfss://{container_gold}@{storage_account_name}.dfs.core.windows.net/"

print(f"Silver Data Path: {silver_path}")
print(f"Gold Data Path: {gold_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer Analytics
# MAGIC Create business intelligence datasets

# COMMAND ----------

try:
    print("[GOLD LAYER] - Reading Silver layer data...")
    
    # Read Silver layer data
    silver_df = spark.read.parquet(f"{silver_path}movies_clean/")
    print(f"Silver records loaded: {silver_df.count()}")
    
    # === Genre Performance Analysis ===
    print("Creating Genre Performance analysis...")
    
    genre_analysis = silver_df.groupBy("genre_standardized").agg(
        count("*").alias("movie_count"),
        round(avg("imdb_rating"), 2).alias("avg_rating"),
        sum("box_office_revenue").alias("total_revenue"),
        round(avg("profit_margin_percent"), 2).alias("avg_profit_margin"),
        max("box_office_revenue").alias("highest_grossing_movie_revenue"),
        min("box_office_revenue").alias("lowest_grossing_movie_revenue")
    ).orderBy(desc("total_revenue"))
    
    print("Genre Performance Analysis:")
    genre_analysis.show()
    
    # Write Genre Analysis
    genre_analysis.write.mode("overwrite").parquet(f"{gold_path}genre_performance/")
    
    # === Era Performance Analysis ===
    print("Creating Era Performance analysis...")
    
    era_analysis = silver_df.groupBy("movie_era").agg(
        count("*").alias("movie_count"),
        round(avg("imdb_rating"), 2).alias("avg_rating"),
        round(avg("production_budget"), 0).alias("avg_budget"),
        round(avg("box_office_revenue"), 0).alias("avg_revenue"),
        round(avg("profit_margin_percent"), 2).alias("avg_profit_margin")
    ).orderBy("movie_era")
    
    print("Era Performance Analysis:")
    era_analysis.show()
    
    # Write Era Analysis
    era_analysis.write.mode("overwrite").parquet(f"{gold_path}era_performance/")
    
    # === Top Performers Analysis ===
    print("Creating Top Performers analysis...")
    
    # Top movies by revenue
    top_movies_revenue = silver_df.select(
        "movie_title",
        "release_year", 
        "genre_standardized",
        "box_office_revenue",
        "imdb_rating",
        "profit_margin_percent"
    ).orderBy(desc("box_office_revenue")).limit(10)
    
    print("Top 10 Movies by Revenue:")
    top_movies_revenue.show(truncate=False)
    
    # Write Top Movies
    top_movies_revenue.write.mode("overwrite").parquet(f"{gold_path}top_movies_revenue/")
    
    # Top movies by rating
    top_movies_rating = silver_df.select(
        "movie_title",
        "release_year",
        "genre_standardized", 
        "imdb_rating",
        "box_office_revenue"
    ).orderBy(desc("imdb_rating")).limit(10)
    
    top_movies_rating.write.mode("overwrite").parquet(f"{gold_path}top_movies_rating/")
    
    # === Business KPIs ===
    print("Creating Business KPIs...")
    
    business_kpis = silver_df.agg(
        count("*").alias("total_movies"),
        round(avg("imdb_rating"), 2).alias("overall_avg_rating"),
        round(sum("box_office_revenue") / 1000000, 2).alias("total_revenue_millions"),
        round(avg("production_budget") / 1000000, 2).alias("avg_budget_millions"),
        round(avg("profit_margin_percent"), 2).alias("overall_avg_profit_margin"),
        countDistinct("genre_standardized").alias("unique_genres"),
        countDistinct("movie_era").alias("unique_eras")
    )
    
    print("Business KPIs:")
    business_kpis.show()
    
    # Write Business KPIs
    business_kpis.write.mode("overwrite").parquet(f"{gold_path}business_kpis/")
    
    print("✓ Gold layer analytics completed successfully!")
    
except Exception as e:
    print(f"✗ Error in Gold layer processing: {str(e)}")
    raise e

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Export for BI Tools
# MAGIC Export data in formats suitable for Power BI, Tableau, etc.

# COMMAND ----------

try:
    print("[EXPORT] - Preparing data for BI tools...")
    
    # Create a comprehensive business view
    business_view = silver_df.select(
        "movie_title",
        "release_year",
        "genre_standardized",
        "movie_era", 
        "imdb_rating",
        "production_budget",
        "box_office_revenue",
        "profit_loss",
        "profit_margin_percent"
    ).withColumn("revenue_category", 
        when(col("box_office_revenue") >= 1000000000, "Blockbuster (1B+)")
        .when(col("box_office_revenue") >= 500000000, "Hit (500M+)")
        .when(col("box_office_revenue") >= 100000000, "Success (100M+)")
        .otherwise("Standard")
    ).withColumn("rating_category",
        when(col("imdb_rating") >= 9.0, "Exceptional (9+)")
        .when(col("imdb_rating") >= 8.0, "Excellent (8+)")
        .when(col("imdb_rating") >= 7.0, "Good (7+)")
        .otherwise("Average")
    )
    
    print("Business View for BI Tools:")
    business_view.show(5, truncate=False)
    
    # Write business view
    business_view.write.mode("overwrite").parquet(f"{gold_path}business_view/")
    
    # Export as CSV for Excel compatibility
    business_view.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{gold_path}business_view_csv/")
    
    print("✓ BI export completed successfully!")
    
except Exception as e:
    print(f"✗ BI export failed: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary and Insights
# MAGIC Gold layer processing summary

# COMMAND ----------

print("=== GOLD LAYER SUMMARY ===")
print(f"✓ Silver data read from: {silver_path}")
print(f"✓ Gold analytics written to: {gold_path}")
print(f"✓ Records processed: {silver_df.count()}")
print(f"✓ Processing completed at: {datetime.now()}")
print("")
print("Analytics datasets created:")
print("1. Genre Performance Analysis")
print("2. Era Performance Analysis") 
print("3. Top Movies by Revenue")
print("4. Top Movies by Rating")
print("5. Business KPIs Dashboard")
print("6. Business View for BI Tools")
print("")
print("Available formats:")
print("- Parquet (for Spark/Databricks)")
print("- CSV (for Excel/Power BI)")
print("")
print("Ready for:")
print("- Power BI dashboards")
print("- Tableau visualizations") 
print("- Excel reports")
print("- Custom analytics applications")
print("")
print("=== AZURE ETL PIPELINE COMPLETED! ===")
print("Developed by: Mir Hasibul Hasan Rahat")
print("Your local data engineering platform is now running in Azure Cloud! 🚀")