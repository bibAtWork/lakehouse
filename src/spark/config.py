"""
Spark configuration module for the Lakehouse project.
Handles environment-based configuration for Spark, Delta Lake, and Unity Catalog.
"""

import os
from typing import Dict, Any
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


class SparkConfig:
    """Configuration class for Spark settings."""
    
    def __init__(self):
        self.master_url = os.getenv("SPARK_MASTER_URL", "spark://localhost:7077")
        self.driver_memory = os.getenv("SPARK_DRIVER_MEMORY", "2g")
        self.executor_memory = os.getenv("SPARK_EXECUTOR_MEMORY", "2g")
        self.executor_cores = int(os.getenv("SPARK_EXECUTOR_CORES", "2"))
        
        # Deployment mode
        self.deployment_mode = os.getenv("DEPLOYMENT_MODE", "local")
        
        # Databricks configuration (for cloud deployment)
        self.databricks_host = os.getenv("DATABRICKS_HOST")
        self.databricks_token = os.getenv("DATABRICKS_TOKEN")
        self.databricks_cluster_id = os.getenv("DATABRICKS_CLUSTER_ID")
        
        # Unity Catalog configuration
        self.unity_catalog_host = os.getenv("UNITY_CATALOG_HOST", "http://localhost:8081")
        self.unity_catalog_catalog = os.getenv("UNITY_CATALOG_CATALOG", "main")
        self.unity_catalog_schema = os.getenv("UNITY_CATALOG_SCHEMA", "default")
        self.unity_catalog_warehouse = os.getenv("UNITY_CATALOG_WAREHOUSE")
        
        # Unity Catalog metadata database (for local deployment)
        self.unity_catalog_metadata_db_host = os.getenv("UNITY_CATALOG_METADATA_DB_HOST", "localhost")
        self.unity_catalog_metadata_db_port = os.getenv("UNITY_CATALOG_METADATA_DB_PORT", "5432")
        self.unity_catalog_metadata_db_name = os.getenv("UNITY_CATALOG_METADATA_DB_NAME", "lakehouse")
        self.unity_catalog_metadata_db_user = os.getenv("UNITY_CATALOG_METADATA_DB_USER", "lakehouse")
        self.unity_catalog_metadata_db_password = os.getenv("UNITY_CATALOG_METADATA_DB_PASSWORD")
        
        # Storage configuration
        self.s3_endpoint = os.getenv("S3_ENDPOINT", "http://seaweedfs-s3.seaweedfs.svc:8333")
        self.s3_access_key = os.getenv("S3_ACCESS_KEY")
        self.s3_secret_key = os.getenv("S3_SECRET_KEY")
        self.s3_bucket = os.getenv("S3_BUCKET", "lakehouse-data")
        
        # Environment
        self.environment = os.getenv("ENVIRONMENT", "development")
        self.log_level = os.getenv("LOG_LEVEL", "INFO")
    
    def get_spark_config(self) -> Dict[str, Any]:
        """Get Spark configuration dictionary."""
        config = {
            "spark.master": self.master_url,
            "spark.driver.memory": self.driver_memory,
            "spark.executor.memory": self.executor_memory,
            "spark.executor.cores": self.executor_cores,
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            "spark.kryoserializer.buffer.max": "1024m",
            "spark.memory.fraction": "0.8",
            "spark.memory.storageFraction": "0.3",
            "spark.databricks.delta.optimizeWrite.enabled": "true",
            "spark.databricks.delta.autoCompact.enabled": "true",
            "spark.databricks.delta.properties.defaults.enableChangeDataFeed": "true",
            "spark.sql.adaptive.skewJoin.enabled": "true",
            "spark.sql.adaptive.localShuffleReader.enabled": "true",
            "spark.sql.adaptive.advisoryPartitionSizeInBytes": "128m",
        }
        
        # Add Unity Catalog configuration
        if self.deployment_mode == "local":
            # Local Unity Catalog configuration
            config.update({
                "spark.databricks.unityCatalog.enabled": "true",
                "spark.databricks.unityCatalog.catalog": self.unity_catalog_catalog,
                "spark.databricks.unityCatalog.schema": self.unity_catalog_schema,
                "spark.databricks.unityCatalog.host": self.unity_catalog_host,
                "spark.databricks.unityCatalog.metadata.db.host": self.unity_catalog_metadata_db_host,
                "spark.databricks.unityCatalog.metadata.db.port": self.unity_catalog_metadata_db_port,
                "spark.databricks.unityCatalog.metadata.db.name": self.unity_catalog_metadata_db_name,
                "spark.databricks.unityCatalog.metadata.db.user": self.unity_catalog_metadata_db_user,
                "spark.databricks.unityCatalog.metadata.db.password": self.unity_catalog_metadata_db_password,
            })
        elif self.deployment_mode == "databricks":
            # Databricks Unity Catalog configuration
            config.update({
                "spark.databricks.unityCatalog.enabled": "true",
                "spark.databricks.unityCatalog.catalog": self.unity_catalog_catalog,
                "spark.databricks.unityCatalog.schema": self.unity_catalog_schema,
            })
        
        # SeaweedFS provides the cluster's S3-compatible endpoint.
        config.update({
            "spark.hadoop.fs.s3a.endpoint": self.s3_endpoint,
            "spark.hadoop.fs.s3a.access.key": self.s3_access_key,
            "spark.hadoop.fs.s3a.secret.key": self.s3_secret_key,
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        })
        
        return config
    
    def get_databricks_config(self) -> Dict[str, Any]:
        """Get Databricks configuration dictionary."""
        if self.deployment_mode != "databricks":
            raise ValueError("Databricks configuration only available in 'databricks' deployment mode")
        
        if not all([self.databricks_host, self.databricks_token, self.databricks_cluster_id]):
            raise ValueError("Databricks configuration incomplete. Please set DATABRICKS_HOST, DATABRICKS_TOKEN, and DATABRICKS_CLUSTER_ID.")
        
        return {
            "host": self.databricks_host,
            "token": self.databricks_token,
            "cluster_id": self.databricks_cluster_id,
            "unity_catalog": {
                "catalog": self.unity_catalog_catalog,
                "schema": self.unity_catalog_schema,
                "warehouse": self.unity_catalog_warehouse,
            }
        }
    
    def get_unity_catalog_config(self) -> Dict[str, Any]:
        """Get Unity Catalog configuration dictionary."""
        return {
            "host": self.unity_catalog_host,
            "catalog": self.unity_catalog_catalog,
            "schema": self.unity_catalog_schema,
            "warehouse": self.unity_catalog_warehouse,
            "metadata_db": {
                "host": self.unity_catalog_metadata_db_host,
                "port": self.unity_catalog_metadata_db_port,
                "name": self.unity_catalog_metadata_db_name,
                "user": self.unity_catalog_metadata_db_user,
                "password": self.unity_catalog_metadata_db_password,
            },
            "deployment_mode": self.deployment_mode,
        }


# Global configuration instance
spark_config = SparkConfig() 
