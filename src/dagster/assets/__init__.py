# Dagster assets package

from .bronze import sample_bronze_data, bronze_metadata

# Export assets for each layer
bronze_assets = [sample_bronze_data, bronze_metadata]
silver_assets = []  # Will be added later
gold_assets = []    # Will be added later 