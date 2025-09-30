#!/usr/bin/env python3
"""
🚀 LOCAL ETL PIPELINE RUNNER
=====================================

This script runs the complete Databricks ETL pipeline locally using PySpark.
Perfect for development, testing, and demonstration purposes.

Requirements:
- Python 3.8+
- Java 8 or 11 (for Spark)
- All dependencies from requirements.txt

Usage:
    python run_local_pipeline.py
"""

import sys
import os
import time
from datetime import datetime

# Add current directory to path for imports
sys.path.append(os.getcwd())

def setup_environment():
    """Setup the local environment and check dependencies"""
    print("🔧 Setting up local environment...")
    
    try:
        # Import required modules
        from config.local_config import local_config
        from local_demo_data import create_sample_datasets
        
        print("✅ Configuration loaded successfully")
        return local_config, create_sample_datasets
        
    except ImportError as e:
        print(f"❌ Missing dependency: {e}")
        print("💡 Please install requirements: pip install -r requirements.txt")
        return None, None

def run_bronze_layer(spark, config, data_paths):
    """Execute Bronze layer processing"""
    print("\n🥉 BRONZE LAYER - Raw Data Ingestion")
    print("=" * 50)
    
    # Read the sample data
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(data_paths["movies_csv"])
    
    print(f"📊 Loaded {df.count()} records from sample data")
    
    # Add metadata columns
    from pyspark.sql.functions import current_timestamp, lit
    
    df_bronze = df.withColumn("ingestion_timestamp", current_timestamp()) \
                  .withColumn("source_system", lit("local_demo")) \
                  .withColumn("data_quality_flag", lit("pending"))
    
    # Write to Bronze Delta table
    bronze_table_path = os.path.join(config.bronze_path, "movies")
    
    df_bronze.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(bronze_table_path)
    
    print(f"✅ Bronze data written to: {bronze_table_path}")
    
    # Show sample data
    print("\n📋 Sample Bronze Data:")
    df_bronze.select("title", "year", "genre", "rating", "ingestion_timestamp").show(5, truncate=False)
    
    return bronze_table_path

def run_silver_layer(spark, config, bronze_path):
    """Execute Silver layer processing"""
    print("\n🥈 SILVER LAYER - Data Cleaning & Validation")
    print("=" * 50)
    
    # Read from Bronze
    df_bronze = spark.read.format("delta").load(bronze_path)
    
    # Data cleaning and transformations
    from pyspark.sql.functions import when, col, regexp_replace, upper, coalesce, lit
    from pyspark.sql.types import IntegerType, DoubleType
    
    df_silver = df_bronze.select(
        col("title"),
        col("year").cast(IntegerType()),
        upper(regexp_replace(col("genre"), "\\s+", " ")).alias("genre_clean"),
        col("rating").cast(DoubleType()),
        coalesce(col("budget"), lit(0)).cast(IntegerType()).alias("budget_clean"),
        coalesce(col("revenue"), lit(0)).cast(IntegerType()).alias("revenue_clean"),
        col("director"),
        col("studio"),
        col("runtime").cast(IntegerType()),
        col("votes").cast(IntegerType()),
        col("ingestion_timestamp")
    ).filter(
        (col("year") > 1900) & (col("year") <= 2025) &
        (col("rating") >= 0) & (col("rating") <= 10)
    ).withColumn(
        "profit_margin", 
        when(col("budget_clean") > 0, 
             (col("revenue_clean") - col("budget_clean")) / col("budget_clean") * 100)
        .otherwise(0)
    ).withColumn(
        "era", 
        when(col("year") < 1980, "Classic")
        .when(col("year") < 2000, "Modern")
        .when(col("year") < 2010, "Digital")
        .otherwise("Contemporary")
    )
    
    # Write to Silver Delta table
    silver_table_path = os.path.join(config.silver_path, "movies_clean")
    
    df_silver.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(silver_table_path)
    
    print(f"✅ Silver data written to: {silver_table_path}")
    print(f"📊 Cleaned records: {df_silver.count()}")
    
    # Show sample cleaned data
    print("\n📋 Sample Silver Data:")
    df_silver.select("title", "year", "genre_clean", "era", "profit_margin").show(5, truncate=False)
    
    return silver_table_path

def run_gold_layer(spark, config, silver_path):
    """Execute Gold layer processing"""
    print("\n🥇 GOLD LAYER - Business Analytics")
    print("=" * 50)
    
    # Read from Silver
    df_silver = spark.read.format("delta").load(silver_path)
    
    # Create business aggregations
    from pyspark.sql.functions import avg, sum, count, max, min, round
    
    # Genre analysis
    genre_analysis = df_silver.groupBy("genre_clean") \
        .agg(
            count("*").alias("movie_count"),
            round(avg("rating"), 2).alias("avg_rating"),
            sum("revenue_clean").alias("total_revenue"),
            round(avg("profit_margin"), 2).alias("avg_profit_margin")
        ).orderBy("total_revenue", ascending=False)
    
    # Era analysis
    era_analysis = df_silver.groupBy("era") \
        .agg(
            count("*").alias("movie_count"),
            round(avg("rating"), 2).alias("avg_rating"),
            round(avg("budget_clean"), 0).alias("avg_budget"),
            round(avg("revenue_clean"), 0).alias("avg_revenue")
        ).orderBy("era")
    
    # Top performers
    top_movies = df_silver.select(
        "title", "year", "genre_clean", "rating", 
        "revenue_clean", "profit_margin", "era"
    ).orderBy("revenue_clean", ascending=False)
    
    # Write Gold tables
    genre_path = os.path.join(config.gold_path, "genre_analysis")
    era_path = os.path.join(config.gold_path, "era_analysis") 
    top_movies_path = os.path.join(config.gold_path, "top_movies")
    
    genre_analysis.write.format("delta").mode("overwrite").save(genre_path)
    era_analysis.write.format("delta").mode("overwrite").save(era_path)
    top_movies.write.format("delta").mode("overwrite").save(top_movies_path)
    
    print(f"✅ Gold tables created:")
    print(f"   📁 Genre Analysis: {genre_path}")
    print(f"   📁 Era Analysis: {era_path}")
    print(f"   📁 Top Movies: {top_movies_path}")
    
    # Show business insights
    print("\n📊 BUSINESS INSIGHTS:")
    print("\n🎭 Genre Performance:")
    genre_analysis.show(truncate=False)
    
    print("\n🕰️ Era Analysis:")
    era_analysis.show(truncate=False)
    
    print("\n🏆 Top 5 Movies by Revenue:")
    top_movies.show(5, truncate=False)
    
    return {
        "genre_analysis": genre_path,
        "era_analysis": era_path,
        "top_movies": top_movies_path
    }

def create_visualizations(spark, gold_paths):
    """Create simple visualizations using the processed data"""
    print("\n📈 CREATING VISUALIZATIONS")
    print("=" * 50)
    
    try:
        import matplotlib.pyplot as plt
        import pandas as pd
        
        # Read data as Pandas for visualization
        genre_df = spark.read.format("delta").load(gold_paths["genre_analysis"]).toPandas()
        era_df = spark.read.format("delta").load(gold_paths["era_analysis"]).toPandas()
        
        # Create visualizations directory
        viz_dir = "local_visualizations"
        os.makedirs(viz_dir, exist_ok=True)
        
        # Genre Revenue Chart
        plt.figure(figsize=(12, 6))
        plt.bar(genre_df['genre_clean'], genre_df['total_revenue'])
        plt.title('Total Revenue by Genre')
        plt.xlabel('Genre')
        plt.ylabel('Revenue ($)')
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig(os.path.join(viz_dir, 'genre_revenue.png'))
        plt.close()
        
        # Era Analysis Chart
        plt.figure(figsize=(10, 6))
        plt.plot(era_df['era'], era_df['avg_rating'], marker='o', linewidth=2, markersize=8)
        plt.title('Average Rating by Era')
        plt.xlabel('Era')
        plt.ylabel('Average Rating')
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.savefig(os.path.join(viz_dir, 'era_ratings.png'))
        plt.close()
        
        print(f"✅ Visualizations saved to: {viz_dir}/")
        print(f"   📊 genre_revenue.png")
        print(f"   📊 era_ratings.png")
        
    except ImportError:
        print("⚠️  Matplotlib not available - skipping visualizations")
        print("💡 Install with: pip install matplotlib")

def main():
    """Main execution function"""
    start_time = time.time()
    
    print("🚀 DATABRICKS ETL PIPELINE - LOCAL EXECUTION")
    print("=" * 60)
    print(f"⏰ Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Setup environment
    config, create_datasets = setup_environment()
    if not config:
        return
    
    # Create sample data
    print("📦 Preparing sample data...")
    data_paths = create_datasets()
    
    # Initialize Spark session
    print("\n⚡ Initializing Spark session...")
    spark = config.get_spark_session("LocalETLDemo")
    print("✅ Spark session created successfully")
    
    try:
        # Execute pipeline layers
        bronze_path = run_bronze_layer(spark, config, data_paths)
        silver_path = run_silver_layer(spark, config, bronze_path)
        gold_paths = run_gold_layer(spark, config, silver_path)
        
        # Create visualizations
        create_visualizations(spark, gold_paths)
        
        # Summary
        execution_time = time.time() - start_time
        print(f"\n🎉 PIPELINE COMPLETED SUCCESSFULLY!")
        print("=" * 60)
        print(f"⏱️  Total execution time: {execution_time:.2f} seconds")
        print(f"📁 Data stored in: {config.data_path}")
        print(f"📊 Layers processed: Bronze → Silver → Gold")
        print(f"✨ Ready for analysis and visualization!")
        
    except Exception as e:
        print(f"\n❌ Pipeline failed: {str(e)}")
        raise
    
    finally:
        # Clean up
        spark.stop()
        print("\n🛑 Spark session stopped")

if __name__ == "__main__":
    main()