"""
Local Development Configuration for Databricks ETL Pipeline
This configuration allows the pipeline to run locally using PySpark
"""

import os
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

class LocalConfig:
    def __init__(self):
        # Local directories for data storage
        self.base_path = os.getcwd()
        self.data_path = os.path.join(self.base_path, "data")
        self.bronze_path = os.path.join(self.data_path, "bronze")
        self.silver_path = os.path.join(self.data_path, "silver")
        self.gold_path = os.path.join(self.data_path, "gold")
        self.checkpoint_path = os.path.join(self.data_path, "checkpoints")
        
        # Create directories if they don't exist
        for path in [self.data_path, self.bronze_path, self.silver_path, 
                    self.gold_path, self.checkpoint_path]:
            os.makedirs(path, exist_ok=True)
    
    def get_spark_session(self, app_name="LocalETLPipeline"):
        """Create a local Spark session with Delta Lake support"""
        
        builder = SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
            .config("spark.sql.warehouse.dir", os.path.join(self.data_path, "warehouse")) \
            .master("local[*]")
        
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        spark.sparkContext.setLogLevel("WARN")  # Reduce logging verbosity
        
        return spark
    
    def get_paths(self):
        """Return all configured paths"""
        return {
            "base_path": self.base_path,
            "bronze_path": self.bronze_path,
            "silver_path": self.silver_path,
            "gold_path": self.gold_path,
            "checkpoint_path": self.checkpoint_path
        }

# Global configuration instance
local_config = LocalConfig()