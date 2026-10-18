# Dagster assets package

from .bronze import sample_bronze_data, bronze_metadata
from .screener import screener_bronze, screener_silver, screener_gold

# Export assets for each layer
bronze_assets = [sample_bronze_data, bronze_metadata, screener_bronze]
silver_assets = [screener_silver]
gold_assets = [screener_gold]
