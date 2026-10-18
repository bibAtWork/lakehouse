"""
Bronze layer assets for the Lakehouse project.
"""

from dagster import asset, AssetExecutionContext, MaterializeResult, MetadataValue
import pandas as pd
from datetime import datetime
import io
import os
import boto3


@asset
def sample_bronze_data(context: AssetExecutionContext) -> MaterializeResult:
    """Write a small sample dataset to the dedicated SeaweedFS bucket."""
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
    
    bucket = os.environ["S3_BUCKET"]
    key = "bronze/sample_data.csv"
    body = io.StringIO()
    df.to_csv(body, index=False)
    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT"],
        aws_access_key_id=os.environ["S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ["S3_SECRET_KEY"],
        region_name="us-east-1",
    )
    s3.put_object(Bucket=bucket, Key=key, Body=body.getvalue().encode("utf-8"))
    return MaterializeResult(metadata={
        "rows": len(df),
        "object": MetadataValue.text(f"s3://{bucket}/{key}"),
    })


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
