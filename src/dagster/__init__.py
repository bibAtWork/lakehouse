"""
Dagster module for the Lakehouse project.
Defines assets, ops, and jobs for data orchestration.
"""

from dagster import Definitions

from .assets import bronze_assets, silver_assets, gold_assets

# Combine all assets
all_assets = [*bronze_assets, *silver_assets, *gold_assets]

# Define the Dagster application
defs = Definitions(
    assets=all_assets,
) 