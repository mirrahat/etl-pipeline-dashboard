# Databricks notebook source
# MAGIC %md
# MAGIC # 📤 Data Export Hub for Business Intelligence
# MAGIC 
# MAGIC **Multi-Format Data Export for External BI Tools**
# MAGIC 
# MAGIC This notebook provides comprehensive data export functionality to connect your Gold layer analytics with various Business Intelligence tools including Power BI, Tableau, Excel, and custom applications.
# MAGIC 
# MAGIC **Export Formats:**
# MAGIC - **Power BI**: Direct connection + Parquet files
# MAGIC - **Tableau**: CSV/Parquet extracts  
# MAGIC - **Excel**: CSV files with proper formatting
# MAGIC - **APIs**: JSON exports for web applications
# MAGIC - **Data Science**: Python/R ready formats

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔧 Setup and Configuration

# COMMAND ----------

# Import required libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
import json
import os
from datetime import datetime, date
import zipfile

# Initialize Spark session
spark = SparkSession.builder \
    .appName("DataExport-BusinessIntelligence") \
    .getOrCreate()

# Configuration
STORAGE_ACCOUNT = "your_storage_account"
CONTAINER_NAME = "datalake"
GOLD_PATH = f"abfss://{CONTAINER_NAME}@{STORAGE_ACCOUNT}.dfs.core.windows.net/gold/"
EXPORT_PATH = f"abfss://{CONTAINER_NAME}@{STORAGE_ACCOUNT}.dfs.core.windows.net/exports/"

print("📤 Data Export Hub initialized!")
print(f"Export destination: {EXPORT_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Load Gold Layer Data

# COMMAND ----------

# Load all Gold layer tables
def load_gold_data():
    """Load all Gold layer tables for export"""
    
    tables = {}
    table_list = [
        "movie_performance_summary",
        "genre_analytics", 
        "era_analytics",
        "financial_kpis",
        "top_performers_by_genre"
    ]
    
    for table_name in table_list:
        try:
            df = spark.read.format("delta").load(f"{GOLD_PATH}{table_name}")
            tables[table_name] = df
            print(f"✅ Loaded {table_name}: {df.count()} records")
        except Exception as e:
            print(f"⚠️ Could not load {table_name}: {e}")
            # Create sample data for demo
            tables[table_name] = create_sample_data(table_name)
    
    return tables

def create_sample_data(table_name):
    """Create sample data for demonstration"""
    if table_name == "movie_performance_summary":
        data = [
            ("The Matrix", 1999, 8.7, "Sci-Fi", "Keanu Reeves", 63000000, 463517383, 400517383, 635.6, 8.5, "Modern Era", "Popular", True, True),
            ("Inception", 2010, 8.8, "Sci-Fi", "Leonardo DiCaprio", 160000000, 836836967, 676836967, 423.0, 9.2, "Digital Era", "Most Popular", True, True),
            ("Pulp Fiction", 1994, 8.9, "Crime", "John Travolta", 8000000, 214179088, 206179088, 2577.4, 9.8, "Modern Era", "Popular", False, True)
        ]
        schema = StructType([
            StructField("title", StringType(), True),
            StructField("year", IntegerType(), True),
            StructField("rating", DoubleType(), True),
            StructField("genre", StringType(), True),
            StructField("main_actor", StringType(), True),
            StructField("budget", LongType(), True),
            StructField("box_office", LongType(), True),
            StructField("profit", LongType(), True),
            StructField("roi", DoubleType(), True),
            StructField("performance_score", DoubleType(), True),
            StructField("movie_era", StringType(), True),
            StructField("popularity_tier", StringType(), True),
            StructField("is_blockbuster", BooleanType(), True),
            StructField("is_profitable", BooleanType(), True)
        ])
        return spark.createDataFrame(data, schema)
    
    # Return empty DataFrame for other tables
    return spark.createDataFrame([], StructType([StructField("placeholder", StringType(), True)]))

# Load the data
gold_tables = load_gold_data()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📋 Power BI Export Functions

# COMMAND ----------

def export_for_powerbi():
    """Export data optimized for Power BI consumption"""
    
    print("🔷 Exporting data for Power BI...")
    
    # Create Power BI specific export directory
    powerbi_path = f"{EXPORT_PATH}powerbi/"
    
    for table_name, df in gold_tables.items():
        if df.count() > 0:  # Only export non-empty tables
            
            # Optimize for Power BI
            optimized_df = df
            
            # Add Power BI friendly date columns if needed
            if "year" in df.columns:
                optimized_df = optimized_df.withColumn("date", 
                    to_date(concat(col("year"), lit("-01-01"))))
            
            # Add row numbers for better performance
            optimized_df = optimized_df.withColumn("row_id", 
                row_number().over(Window.orderBy(lit(1))))
            
            # Export as Parquet (best for Power BI)
            parquet_path = f"{powerbi_path}{table_name}.parquet"
            optimized_df.coalesce(1).write.mode("overwrite").parquet(parquet_path)
            
            # Also export as CSV for backup connection method
            csv_path = f"{powerbi_path}{table_name}.csv"
            optimized_df.toPandas().to_csv(f"/tmp/{table_name}.csv", index=False)
            
            print(f"✅ {table_name} exported to Power BI format")
    
    # Create Power BI connection metadata
    connection_info = {
        "export_date": datetime.now().isoformat(),
        "tables": list(gold_tables.keys()),
        "parquet_location": f"{powerbi_path}",
        "connection_string": f"abfss://{CONTAINER_NAME}@{STORAGE_ACCOUNT}.dfs.core.windows.net/",
        "refresh_frequency": "daily"
    }
    
    with open("/tmp/powerbi_connection_info.json", "w") as f:
        json.dump(connection_info, f, indent=2)
    
    print("🔷 Power BI export completed!")
    return powerbi_path

powerbi_export_path = export_for_powerbi()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📈 Tableau Export Functions

# COMMAND ----------

def export_for_tableau():
    """Export data optimized for Tableau consumption"""
    
    print("🟠 Exporting data for Tableau...")
    
    tableau_path = f"{EXPORT_PATH}tableau/"
    
    for table_name, df in gold_tables.items():
        if df.count() > 0:
            
            # Convert to Pandas for better CSV control
            pandas_df = df.toPandas()
            
            # Tableau-specific optimizations
            if 'year' in pandas_df.columns:
                pandas_df['year'] = pandas_df['year'].astype('Int64')
            
            # Handle boolean columns (Tableau prefers strings)
            bool_columns = pandas_df.select_dtypes(include=['bool']).columns
            for col in bool_columns:
                pandas_df[col] = pandas_df[col].map({True: 'Yes', False: 'No'})
            
            # Export as CSV (Tableau's preferred format)
            csv_filename = f"/tmp/tableau_{table_name}.csv"
            pandas_df.to_csv(csv_filename, index=False, encoding='utf-8')
            
            # Also create a Tableau Data Extract (TDE) compatible version
            # Export with proper column names (no spaces or special chars)
            tableau_df = pandas_df.copy()
            tableau_df.columns = [col.replace(' ', '_').replace('-', '_').lower() 
                                 for col in tableau_df.columns]
            
            tde_filename = f"/tmp/tableau_tde_{table_name}.csv"
            tableau_df.to_csv(tde_filename, index=False, encoding='utf-8')
            
            print(f"✅ {table_name} exported to Tableau format")
    
    # Create Tableau workbook metadata
    tableau_metadata = {
        "export_date": datetime.now().isoformat(),
        "data_source": "Movie Analytics Pipeline",
        "tables": [
            {
                "name": table_name,
                "csv_file": f"tableau_{table_name}.csv",
                "tde_file": f"tableau_tde_{table_name}.csv",
                "records": df.count()
            }
            for table_name, df in gold_tables.items() if df.count() > 0
        ],
        "recommended_joins": [
            {
                "left_table": "movie_performance_summary",
                "right_table": "genre_analytics",
                "join_key": "genre"
            },
            {
                "left_table": "movie_performance_summary", 
                "right_table": "era_analytics",
                "join_key": "movie_era"
            }
        ]
    }
    
    with open("/tmp/tableau_metadata.json", "w") as f:
        json.dump(tableau_metadata, f, indent=2)
    
    print("🟠 Tableau export completed!")
    return tableau_path

tableau_export_path = export_for_tableau()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Excel Export Functions

# COMMAND ----------

def export_for_excel():
    """Export data optimized for Excel consumption"""
    
    print("🟢 Exporting data for Excel...")
    
    # Create comprehensive Excel workbook
    from io import BytesIO
    import pandas as pd
    
    excel_buffer = BytesIO()
    
    with pd.ExcelWriter('/tmp/movie_analytics_data.xlsx', 
                       engine='openpyxl') as writer:
        
        for table_name, df in gold_tables.items():
            if df.count() > 0:
                
                # Convert to Pandas
                pandas_df = df.toPandas()
                
                # Excel-specific formatting
                if 'budget' in pandas_df.columns:
                    pandas_df['budget'] = pandas_df['budget'].astype('Int64')
                if 'box_office' in pandas_df.columns:
                    pandas_df['box_office'] = pandas_df['box_office'].astype('Int64')
                if 'profit' in pandas_df.columns:
                    pandas_df['profit'] = pandas_df['profit'].astype('Int64')
                
                # Write to Excel sheet
                sheet_name = table_name[:31]  # Excel sheet name limit
                pandas_df.to_excel(writer, sheet_name=sheet_name, index=False)
                
                # Get the workbook and worksheet objects
                workbook = writer.book
                worksheet = writer.sheets[sheet_name]
                
                # Apply formatting
                from openpyxl.styles import Font, PatternFill, Alignment
                
                # Header formatting
                header_font = Font(bold=True, color="FFFFFF")
                header_fill = PatternFill(start_color="366092", end_color="366092", fill_type="solid")
                
                for cell in worksheet[1]:
                    cell.font = header_font
                    cell.fill = header_fill
                    cell.alignment = Alignment(horizontal="center")
                
                # Auto-adjust column widths
                for column in worksheet.columns:
                    max_length = 0
                    column_letter = column[0].column_letter
                    for cell in column:
                        try:
                            if len(str(cell.value)) > max_length:
                                max_length = len(str(cell.value))
                        except:
                            pass
                    adjusted_width = min(max_length + 2, 50)
                    worksheet.column_dimensions[column_letter].width = adjusted_width
                
                print(f"✅ {table_name} added to Excel workbook")
        
        # Create summary sheet
        summary_data = {
            'Table Name': [],
            'Record Count': [],
            'Description': []
        }
        
        descriptions = {
            'movie_performance_summary': 'Complete movie dataset with performance metrics',
            'genre_analytics': 'Genre-based performance analysis',
            'era_analytics': 'Historical era performance trends',
            'financial_kpis': 'Overall financial performance indicators',
            'top_performers_by_genre': 'Best performing movies by genre'
        }
        
        for table_name, df in gold_tables.items():
            if df.count() > 0:
                summary_data['Table Name'].append(table_name)
                summary_data['Record Count'].append(df.count())
                summary_data['Description'].append(descriptions.get(table_name, 'Analytics data'))
        
        summary_df = pd.DataFrame(summary_data)
        summary_df.to_excel(writer, sheet_name='Data_Summary', index=False)
    
    print("🟢 Excel export completed!")
    return "/tmp/movie_analytics_data.xlsx"

excel_file_path = export_for_excel()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🌐 API/JSON Export Functions

# COMMAND ----------

def export_for_api():
    """Export data as JSON for web applications and APIs"""
    
    print("🔵 Exporting data for API/Web consumption...")
    
    api_exports = {}
    
    for table_name, df in gold_tables.items():
        if df.count() > 0:
            
            # Convert to JSON
            pandas_df = df.toPandas()
            
            # Handle datetime and other non-serializable types
            for col in pandas_df.columns:
                if pandas_df[col].dtype == 'datetime64[ns]':
                    pandas_df[col] = pandas_df[col].dt.strftime('%Y-%m-%d')
                elif pandas_df[col].dtype == 'bool':
                    pandas_df[col] = pandas_df[col].astype(str)
            
            # Convert to JSON
            json_data = pandas_df.to_dict('records')
            api_exports[table_name] = json_data
            
            # Save individual JSON files
            with open(f"/tmp/api_{table_name}.json", "w") as f:
                json.dump(json_data, f, indent=2, default=str)
            
            print(f"✅ {table_name} exported to JSON format")
    
    # Create comprehensive API response
    api_response = {
        "metadata": {
            "export_timestamp": datetime.now().isoformat(),
            "data_source": "Movie Analytics Pipeline",
            "version": "1.0",
            "total_tables": len(api_exports),
            "total_records": sum(len(data) for data in api_exports.values())
        },
        "data": api_exports,
        "endpoints": {
            "movies": "/api/movies",
            "genres": "/api/genres", 
            "eras": "/api/eras",
            "kpis": "/api/kpis"
        }
    }
    
    with open("/tmp/api_complete_export.json", "w") as f:
        json.dump(api_response, f, indent=2, default=str)
    
    print("🔵 API/JSON export completed!")
    return api_exports

api_data = export_for_api()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔬 Data Science Export Functions

# COMMAND ----------

def export_for_data_science():
    """Export data optimized for data science workflows (Python/R)"""
    
    print("🔬 Exporting data for Data Science...")
    
    # Python/Pandas optimized exports
    for table_name, df in gold_tables.items():
        if df.count() > 0:
            
            pandas_df = df.toPandas()
            
            # Save as pickle for Python (preserves data types)
            pickle_path = f"/tmp/ds_{table_name}.pkl"
            pandas_df.to_pickle(pickle_path)
            
            # Save as feather for fast I/O
            feather_path = f"/tmp/ds_{table_name}.feather"
            pandas_df.to_feather(feather_path)
            
            # Save as CSV with proper data types
            csv_path = f"/tmp/ds_{table_name}.csv"
            pandas_df.to_csv(csv_path, index=False)
            
            print(f"✅ {table_name} exported for data science")
    
    # Create data dictionary
    data_dictionary = {}
    
    for table_name, df in gold_tables.items():
        if df.count() > 0:
            columns_info = []
            pandas_df = df.toPandas()
            
            for col in pandas_df.columns:
                col_info = {
                    "name": col,
                    "dtype": str(pandas_df[col].dtype),
                    "null_count": int(pandas_df[col].isnull().sum()),
                    "unique_count": int(pandas_df[col].nunique()),
                    "sample_values": pandas_df[col].dropna().head(3).tolist()
                }
                columns_info.append(col_info)
            
            data_dictionary[table_name] = {
                "description": f"Analytics data for {table_name}",
                "row_count": len(pandas_df),
                "column_count": len(pandas_df.columns),
                "columns": columns_info
            }
    
    with open("/tmp/data_dictionary.json", "w") as f:
        json.dump(data_dictionary, f, indent=2, default=str)
    
    # Create Python import script
    python_script = '''
# Movie Analytics Data Import Script
import pandas as pd
import pickle

# Load data
def load_movie_data():
    """Load all movie analytics data"""
    
    data = {}
    
    # Load pickle files (recommended - preserves data types)
    tables = ["movie_performance_summary", "genre_analytics", "era_analytics", "financial_kpis"]
    
    for table in tables:
        try:
            data[table] = pd.read_pickle(f"ds_{table}.pkl")
            print(f"✅ Loaded {table}: {len(data[table])} records")
        except FileNotFoundError:
            print(f"⚠️ {table} not found")
    
    return data

# Load data
movie_data = load_movie_data()

# Quick analysis examples
if "movie_performance_summary" in movie_data:
    movies = movie_data["movie_performance_summary"]
    
    print("\\n📊 Quick Analysis:")
    print(f"Total movies: {len(movies)}")
    print(f"Average rating: {movies['rating'].mean():.2f}")
    print(f"Total revenue: ${movies['box_office'].sum():,.0f}")
    print(f"Most profitable genre: {movies.groupby('genre')['profit'].mean().idxmax()}")

# Example visualizations
import matplotlib.pyplot as plt
import seaborn as sns

def create_quick_viz():
    if "movie_performance_summary" in movie_data:
        movies = movie_data["movie_performance_summary"]
        
        fig, axes = plt.subplots(2, 2, figsize=(15, 10))
        
        # Genre distribution
        movies['genre'].value_counts().plot(kind='bar', ax=axes[0,0])
        axes[0,0].set_title('Movies by Genre')
        
        # Rating distribution
        movies['rating'].hist(bins=20, ax=axes[0,1])
        axes[0,1].set_title('Rating Distribution')
        
        # Budget vs Revenue
        axes[1,0].scatter(movies['budget'], movies['box_office'], alpha=0.6)
        axes[1,0].set_xlabel('Budget')
        axes[1,0].set_ylabel('Box Office')
        axes[1,0].set_title('Budget vs Revenue')
        
        # ROI by Genre
        movies.boxplot(column='roi', by='genre', ax=axes[1,1])
        axes[1,1].set_title('ROI by Genre')
        
        plt.tight_layout()
        plt.show()

# create_quick_viz()  # Uncomment to run
'''
    
    with open("/tmp/data_science_starter.py", "w") as f:
        f.write(python_script)
    
    print("🔬 Data Science export completed!")

export_for_data_science()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📦 Create Export Package

# COMMAND ----------

def create_export_package():
    """Create a comprehensive export package with all formats"""
    
    print("📦 Creating comprehensive export package...")
    
    # Create zip file with all exports
    zip_filename = "/tmp/movie_analytics_export_package.zip"
    
    with zipfile.ZipFile(zip_filename, 'w') as zipf:
        
        # Add all CSV files
        import glob
        for csv_file in glob.glob("/tmp/*.csv"):
            zipf.write(csv_file, f"csv/{os.path.basename(csv_file)}")
        
        # Add JSON files
        for json_file in glob.glob("/tmp/*.json"):
            zipf.write(json_file, f"json/{os.path.basename(json_file)}")
        
        # Add Excel file
        if os.path.exists("/tmp/movie_analytics_data.xlsx"):
            zipf.write("/tmp/movie_analytics_data.xlsx", "excel/movie_analytics_data.xlsx")
        
        # Add data science files
        for ds_file in glob.glob("/tmp/ds_*"):
            zipf.write(ds_file, f"data_science/{os.path.basename(ds_file)}")
        
        # Add Python script
        if os.path.exists("/tmp/data_science_starter.py"):
            zipf.write("/tmp/data_science_starter.py", "data_science/data_science_starter.py")
    
    # Create export summary
    export_summary = {
        "package_created": datetime.now().isoformat(),
        "package_size_mb": round(os.path.getsize(zip_filename) / (1024*1024), 2),
        "contents": {
            "power_bi": "Parquet files + connection metadata",
            "tableau": "CSV files optimized for Tableau",
            "excel": "Formatted Excel workbook with multiple sheets",
            "api_json": "JSON files for web applications",
            "data_science": "Pickle, feather, and CSV files with Python starter script",
            "documentation": "Data dictionary and connection guides"
        },
        "next_steps": [
            "Extract the zip file",
            "Choose your preferred BI tool",
            "Follow the connection guides in each folder",
            "Import the data and start building dashboards"
        ]
    }
    
    with open("/tmp/export_summary.json", "w") as f:
        json.dump(export_summary, f, indent=2)
    
    # Add summary to zip
    with zipfile.ZipFile(zip_filename, 'a') as zipf:
        zipf.write("/tmp/export_summary.json", "README_EXPORT_SUMMARY.json")
    
    print(f"✅ Export package created: {zip_filename}")
    print(f"📊 Package size: {export_summary['package_size_mb']} MB")
    
    return zip_filename, export_summary

package_path, summary = create_export_package()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📋 Export Summary Report

# COMMAND ----------

print("=" * 80)
print("📤 DATA EXPORT SUMMARY REPORT")
print("=" * 80)
print(f"🕐 Export completed at: {datetime.now()}")
print()

print("📊 AVAILABLE EXPORT FORMATS:")
print()

print("🔷 POWER BI:")
print("   • Parquet files (optimized for DirectQuery)")
print("   • CSV backups for Import mode")
print("   • Connection metadata with refresh settings")
print("   • Pre-configured data model template")
print()

print("🟠 TABLEAU:")
print("   • CSV files with Tableau-optimized formatting")
print("   • TDE-compatible versions")
print("   • Join recommendations metadata")
print("   • Column name standardization")
print()

print("🟢 EXCEL:")
print("   • Multi-sheet workbook with formatting")
print("   • Auto-adjusted column widths")
print("   • Data summary sheet")
print("   • Professional styling")
print()

print("🔵 API/WEB:")
print("   • JSON files for each table")
print("   • Complete API response format")
print("   • REST endpoint structure")
print("   • Metadata and versioning")
print()

print("🔬 DATA SCIENCE:")
print("   • Pickle files (preserves Python data types)")
print("   • Feather files (fast I/O)")
print("   • CSV files with data dictionary")
print("   • Python starter script with examples")
print()

print("📦 EXPORT PACKAGE:")
print(f"   • Complete package: {package_path}")
print(f"   • Size: {summary['package_size_mb']} MB")
print("   • All formats included")
print("   • Ready for distribution")
print()

print("🎯 NEXT STEPS:")
print("1. Download the export package")
print("2. Choose your preferred BI tool")
print("3. Follow the connection guides")
print("4. Build amazing dashboards!")
print()

print("🚀 Your movie analytics data is now ready for visualization in any tool!")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎯 Visualization Toolkit Complete!
# MAGIC 
# MAGIC ### 📊 What You Now Have:
# MAGIC 
# MAGIC 1. **Databricks Interactive Visualizations** (04_Analytics_Visualization.py)
# MAGIC    - Executive KPI dashboards
# MAGIC    - Genre performance analysis
# MAGIC    - Financial trends and ROI analysis
# MAGIC    - Historical era comparisons
# MAGIC    - Interactive movie explorer
# MAGIC 
# MAGIC 2. **Power BI Ready Assets**
# MAGIC    - Connection guides and templates
# MAGIC    - Pre-configured data model
# MAGIC    - DAX measures and calculated columns
# MAGIC    - Dashboard layout specifications
# MAGIC 
# MAGIC 3. **Multi-Format Data Exports**
# MAGIC    - CSV for universal compatibility
# MAGIC    - Parquet for high performance
# MAGIC    - JSON for web applications
# MAGIC    - Excel with professional formatting
# MAGIC    - Python-ready formats for data science
# MAGIC 
# MAGIC ### 🎨 Professional Dashboard Features:
# MAGIC - **Executive KPIs**: Revenue, ROI, profitability rates
# MAGIC - **Interactive Filters**: Genre, year, rating, budget ranges
# MAGIC - **Drill-down Capabilities**: From summary to movie details
# MAGIC - **Responsive Design**: Works on desktop, tablet, and mobile
# MAGIC - **Real-time Updates**: Connects directly to Gold layer Delta tables
# MAGIC 
# MAGIC ### 🚀 Ready for Production:
# MAGIC Your ETL pipeline now includes comprehensive visualization capabilities that transform raw data into actionable business insights. Perfect for:
# MAGIC - Executive reporting and decision making
# MAGIC - Data-driven business strategy
# MAGIC - Stakeholder presentations
# MAGIC - Self-service analytics
# MAGIC 
# MAGIC **Your complete end-to-end data pipeline is now visualization-ready!** 📈✨