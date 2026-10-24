"""
Dagster module for the Lakehouse project.
Defines assets, ops, and jobs for data orchestration.
"""

from dagster import DefaultScheduleStatus, Definitions, ScheduleDefinition, define_asset_job

from .assets import bronze_assets, silver_assets, gold_assets

# Combine all assets
all_assets = [*bronze_assets, *silver_assets, *gold_assets]

# Define the Dagster application
defs = Definitions(
    assets=all_assets,
    schedules=[
        ScheduleDefinition(
            job=define_asset_job("screener_bronze_refresh", selection=["screener_bronze"]),
            cron_schedule="0 3 * * *", execution_timezone="UTC",
            default_status=DefaultScheduleStatus.RUNNING,
        ),
        ScheduleDefinition(
            job=define_asset_job("screener_llm_refresh", selection=["screener_llm_enrichment"]),
            cron_schedule="15 3,6,9,12,15,18,21 * * *", execution_timezone="UTC",
            default_status=DefaultScheduleStatus.RUNNING,
        ),
        ScheduleDefinition(
            job=define_asset_job("screener_bi_refresh", selection=["screener_silver", "screener_gold"]),
            cron_schedule="0 4 * * *", execution_timezone="UTC",
            default_status=DefaultScheduleStatus.RUNNING,
        ),
    ],
)
