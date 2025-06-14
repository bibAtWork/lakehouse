"""
Bronze layer assets for the Lakehouse project.
"""

from dagster import asset, AssetExecutionContext
import pandas as pd
from datetime import datetime


@asset
def sample_bronze_data(context: AssetExecutionContext) -> pd.DataFrame:
    """Create sample bronze data for testing."""
    context.log.info("Creating sample bronze data")
    
    # Create sample data
    data = {
        'id': range(1, 101),
        'product_name': ['Product A' if i % 2 == 0 else 'Product B' for i in range(1, 101)],
        'price': [100 + i * 0.5 for i in range(1, 101)],
        'quantity': [i % 50 + 1 for i in range(1, 101)],
        'priority': ['High' if i % 3 == 0 else 'Medium' if i % 3 == 1 else 'Low' for i in range(1, 101)],
        'created_at': [datetime.now() for _ in range(1, 101)]
    }
    
    df = pd.DataFrame(data)
    context.log.info(f"Created {len(df)} rows of bronze data")
    
    return df


@asset
def bronze_metadata(context: AssetExecutionContext) -> dict:
    """Create metadata for bronze layer."""
    context.log.info("Creating bronze layer metadata")
    
    metadata = {
        'layer': 'bronze',
        'description': 'Raw data layer',
        'created_at': datetime.now().isoformat(),
        'data_quality': 'raw',
        'schema_version': '1.0'
    }
    
    return metadata 