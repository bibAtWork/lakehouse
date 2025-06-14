"""
Spark session manager for the Lakehouse project.
Handles session creation with Delta Lake and Unity Catalog support.
"""

import logging
from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType

from .config import spark_config

logger = logging.getLogger(__name__)


class SparkSessionManager:
    """Manages Spark session creation and configuration."""
    
    def __init__(self):
        self._spark: Optional[SparkSession] = None
        self.config = spark_config
    
    def get_or_create_spark_session(self, app_name: str = "LakehouseApp") -> SparkSession:
        """Get existing Spark session or create a new one."""
        if self._spark is None:
            self._spark = self._create_spark_session(app_name)
            logger.info(f"Created new Spark session: {app_name}")
        return self._spark
    
    def _create_spark_session(self, app_name: str) -> SparkSession:
        """Create a new Spark session with Delta Lake and Unity Catalog support."""
        builder = SparkSession.builder.appName(app_name)
        
        # Apply configuration
        for key, value in self.config.get_spark_config().items():
            builder = builder.config(key, value)
        
        # Create session
        spark = builder.getOrCreate()
        
        # Configure logging
        spark.sparkContext.setLogLevel(self.config.log_level)
        
        # Initialize Delta Lake
        self._initialize_delta_lake(spark)
        
        # Initialize Unity Catalog if configured
        if self.config.unity_catalog_catalog and self.config.unity_catalog_schema:
            self._initialize_unity_catalog(spark)
        
        return spark
    
    def _initialize_delta_lake(self, spark: SparkSession):
        """Initialize Delta Lake extensions."""
        try:
            # Enable Delta Lake SQL extensions
            spark.sql("SET spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension")
            spark.sql("SET spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog")
            
            # Configure Delta Lake optimizations
            spark.sql("SET spark.databricks.delta.optimizeWrite.enabled=true")
            spark.sql("SET spark.databricks.delta.autoCompact.enabled=true")
            spark.sql("SET spark.databricks.delta.properties.defaults.enableChangeDataFeed=true")
            
            logger.info("Delta Lake initialized successfully")
        except Exception as e:
            logger.warning(f"Failed to initialize Delta Lake: {e}")
    
    def _initialize_unity_catalog(self, spark: SparkSession):
        """Initialize Unity Catalog if configured."""
        try:
            # Set Unity Catalog catalog and schema
            spark.sql(f"USE CATALOG {self.config.unity_catalog_catalog}")
            spark.sql(f"USE SCHEMA {self.config.unity_catalog_schema}")
            
            logger.info(f"Unity Catalog initialized: {self.config.unity_catalog_catalog}.{self.config.unity_catalog_schema}")
        except Exception as e:
            logger.warning(f"Failed to initialize Unity Catalog: {e}")
    
    def stop_session(self):
        """Stop the Spark session."""
        if self._spark is not None:
            self._spark.stop()
            self._spark = None
            logger.info("Spark session stopped")
    
    def get_sample_schema(self) -> StructType:
        """Get a sample schema for testing."""
        return StructType([
            StructField("id", IntegerType(), False),
            StructField("name", StringType(), True),
            StructField("value", DoubleType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("category", StringType(), True)
        ])
    
    def create_sample_data(self, spark: SparkSession, num_rows: int = 100):
        """Create sample data for testing."""
        from pyspark.sql.functions import current_timestamp, rand, when
        
        # Create sample data
        data = spark.range(num_rows).select(
            (rand() * 1000).cast("int").alias("id"),
            when(rand() > 0.5, "Category A").otherwise("Category B").alias("category"),
            (rand() * 100).alias("value"),
            current_timestamp().alias("timestamp")
        )
        
        return data


# Global session manager
session_manager = SparkSessionManager()


def get_spark_session(app_name: str = "LakehouseApp") -> SparkSession:
    """Convenience function to get a Spark session."""
    return session_manager.get_or_create_spark_session(app_name)


def stop_spark_session():
    """Convenience function to stop the Spark session."""
    session_manager.stop_session() 