"""
Sample PySpark ingestion job for the Lakehouse project.
Demonstrates data ingestion with Delta Lake and Unity Catalog.
"""

import logging
from datetime import datetime
from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col, lit
from delta.tables import DeltaTable

from ..session import get_spark_session
from ..config import spark_config

logger = logging.getLogger(__name__)


class DataIngestionJob:
    """Sample data ingestion job using PySpark and Delta Lake."""
    
    def __init__(self, spark: Optional[SparkSession] = None):
        self.spark = spark or get_spark_session("DataIngestion")
        self.config = spark_config
    
    def ingest_sample_data(self, source_path: str, target_table: str, mode: str = "append"):
        """
        Ingest sample data from source to target Delta table.
        
        Args:
            source_path: Path to source data (CSV, JSON, etc.)
            target_table: Target table name
            mode: Write mode (append, overwrite, merge)
        """
        try:
            logger.info(f"Starting data ingestion from {source_path} to {target_table}")
            
            # Read source data
            df = self._read_source_data(source_path)
            
            # Add metadata columns
            df = self._add_metadata_columns(df)
            
            # Write to Delta table
            self._write_to_delta_table(df, target_table, mode)
            
            logger.info(f"Successfully ingested data to {target_table}")
            
        except Exception as e:
            logger.error(f"Error during data ingestion: {e}")
            raise
    
    def _read_source_data(self, source_path: str):
        """Read data from various source formats."""
        if source_path.endswith('.csv'):
            return self.spark.read.csv(source_path, header=True, inferSchema=True)
        elif source_path.endswith('.json'):
            return self.spark.read.json(source_path)
        elif source_path.endswith('.parquet'):
            return self.spark.read.parquet(source_path)
        else:
            # Assume CSV format
            return self.spark.read.csv(source_path, header=True, inferSchema=True)
    
    def _add_metadata_columns(self, df):
        """Add metadata columns to the dataframe."""
        return df.withColumn("ingestion_timestamp", current_timestamp()) \
                .withColumn("batch_id", lit(datetime.now().strftime("%Y%m%d_%H%M%S"))) \
                .withColumn("source_system", lit("lakehouse_sample"))
    
    def _write_to_delta_table(self, df, target_table: str, mode: str):
        """Write dataframe to Delta table."""
        if mode == "merge":
            self._merge_to_delta_table(df, target_table)
        else:
            # Write to Delta format
            df.write \
                .format("delta") \
                .mode(mode) \
                .option("mergeSchema", "true") \
                .saveAsTable(target_table)
    
    def _merge_to_delta_table(self, df, target_table: str):
        """Merge data into existing Delta table."""
        try:
            # Check if table exists
            target_df = self.spark.read.table(target_table)
            
            # Perform merge operation
            delta_table = DeltaTable.forName(self.spark, target_table)
            
            delta_table.alias("target") \
                .merge(
                    df.alias("source"),
                    "target.id = source.id"
                ) \
                .whenMatchedUpdateAll() \
                .whenNotMatchedInsertAll() \
                .execute()
                
        except Exception:
            # Table doesn't exist, create it
            df.write \
                .format("delta") \
                .mode("overwrite") \
                .saveAsTable(target_table)
    
    def create_sample_csv_data(self, output_path: str, num_rows: int = 1000):
        """Create sample CSV data for testing."""
        from pyspark.sql.functions import rand, when, round
        
        logger.info(f"Creating sample CSV data with {num_rows} rows")
        
        # Create sample data
        sample_data = self.spark.range(num_rows).select(
            (rand() * 10000).cast("int").alias("id"),
            when(rand() > 0.5, "Product A").otherwise("Product B").alias("product_name"),
            round(rand() * 1000, 2).alias("price"),
            (rand() * 100).cast("int").alias("quantity"),
            when(rand() > 0.7, "High").when(rand() > 0.4, "Medium").otherwise("Low").alias("priority"),
            current_timestamp().alias("created_at")
        )
        
        # Write to CSV
        sample_data.write \
            .mode("overwrite") \
            .option("header", "true") \
            .csv(output_path)
        
        logger.info(f"Sample CSV data created at {output_path}")
        return output_path
    
    def run_ingestion_pipeline(self):
        """Run the complete ingestion pipeline."""
        try:
            # Create sample data
            sample_path = "data/raw/sample_data"
            self.create_sample_csv_data(sample_path)
            
            # Ingest to bronze layer
            bronze_table = "bronze.sample_data"
            self.ingest_sample_data(sample_path, bronze_table, "overwrite")
            
            # Ingest to silver layer with some transformations
            silver_df = self.spark.read.table(bronze_table)
            silver_df = silver_df.filter(col("price") > 0) \
                               .withColumn("total_value", col("price") * col("quantity"))
            
            silver_df.write \
                .format("delta") \
                .mode("overwrite") \
                .saveAsTable("silver.sample_data")
            
            logger.info("Ingestion pipeline completed successfully")
            
        except Exception as e:
            logger.error(f"Error in ingestion pipeline: {e}")
            raise


def main():
    """Main function to run the ingestion job."""
    logging.basicConfig(level=logging.INFO)
    
    job = DataIngestionJob()
    job.run_ingestion_pipeline()


if __name__ == "__main__":
    main() 