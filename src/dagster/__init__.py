"""
Dagster module for the Lakehouse project.
Defines assets, ops, and jobs for data orchestration.
"""

from dagster import Definitions, ScheduleDefinition, define_asset_job

from .assets import bronze_assets, silver_assets, gold_assets

# Combine all assets
all_assets = [*bronze_assets, *silver_assets, *gold_assets]

# Define the Dagster application
defs = Definitions(
    assets=all_assets,
    schedules=[ScheduleDefinition(
        job=define_asset_job("screener_bi_refresh", selection=["screener_bronze", "screener_silver", "screener_gold"]),
        cron_schedule="0 4 * * *",
        execution_timezone="UTC",
    )],
)
