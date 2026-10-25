"""Daily, reproducible screener snapshots for the lakehouse."""

import json
import os
from datetime import datetime, timezone
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from dagster import AssetExecutionContext, MaterializeResult, MetadataValue, asset
from lakehouse.dedup import annotate_price_per_sqm, deduplicate, removed_cohorts, split_current_and_history, summary
from lakehouse.enrichment import PropertyEnricher


def _api(path, **params):
    base = os.environ["SCREENER_API_URL"].rstrip("/")
    url = f"{base}{path}"
    if params:
        url += "?" + urlencode(params)
    with urlopen(url, timeout=60) as response:
        return json.load(response)


def _s3():
    return boto3.client(
        "s3", endpoint_url=os.environ["S3_ENDPOINT"],
        aws_access_key_id=os.environ["S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ["S3_SECRET_KEY"],
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}),
    )


def _publish_metrics(summary_data, history_data=None, previous_history=None):
    endpoint = os.environ.get("VM_IMPORT_URL")
    if not endpoint:
        return
    metrics = {
        "source_listings": summary_data["source_listings"],
        "unique_properties": summary_data["unique_properties"],
        "classified_properties": summary_data["classified_properties"],
        "classified_house_properties": summary_data["classified_house_properties"],
        "duplicate_listings": summary_data["duplicate_listings"],
        "duplicate_rate": summary_data["duplicate_rate"],
        "llm_quota_skipped_properties": summary_data["llm_quota_skipped_properties"],
        "llm_budget_skipped_properties": summary_data["llm_budget_skipped_properties"],
        "llm_time_skipped_properties": summary_data["llm_time_skipped_properties"],
        "llm_cache_pending_properties": summary_data["llm_cache_pending_properties"],
        "llm_eligible_properties": summary_data["llm_eligible_properties"],
        "llm_cache_hits": summary_data["llm_cache_hits"],
        "llm_successful_calls": summary_data["llm_successful_calls"],
        "llm_failed_calls": summary_data["llm_failed_calls"],
        "llm_completed_properties": summary_data["llm_completed_properties"],
        "llm_accepted_fact_properties": summary_data["llm_accepted_fact_properties"],
        "priced_properties": summary_data["priced_properties"],
        "median_price_eur": summary_data["median_price_eur"],
        "median_price_per_sqm_eur": summary_data["median_price_per_sqm_eur"],
        "average_price_per_sqm_eur": summary_data["average_price_per_sqm_eur"],
        "price_per_sqm_implausible_area_properties": summary_data["price_per_sqm_implausible_area_properties"],
        "price_per_sqm_excluded_properties": summary_data["price_per_sqm_excluded_properties"],
        "new_properties_24h": summary_data["new_properties_24h"],
        "average_asking_house_price_last_12m_eur": summary_data["average_asking_house_price_last_12m_eur"],
        "asking_house_price_last_12m_properties": summary_data["asking_house_price_last_12m_properties"],
        "snapshot_timestamp_seconds": datetime.now(timezone.utc).timestamp(),
    }
    lines = [f"lakehouse_screener_{name} {value}" for name, value in metrics.items()
             if value is not None]
    for row in summary_data["by_city"]:
        city = str(row["city"]).replace("\\", "\\\\").replace('"', '\\"').replace("\n", " ")
        lines.append(f'lakehouse_screener_properties_by_city{{city="{city}"}} {row["count"]}')
    for row in summary_data["sources_by_provider"]:
        provider = str(row["provider"]).replace("\\", "\\\\").replace('"', '\\"')
        lines.append(f'lakehouse_screener_sources_by_provider{{provider="{provider}"}} {row["count"]}')
    for reason, count in summary_data["matches_by_reason"].items():
        lines.append(f'lakehouse_screener_matches_by_reason{{reason="{reason}"}} {count}')
    def label(value):
        return str(value).replace("\\", "\\\\").replace('"', '\\"').replace("\n", " ")

    for group, label_names in (
        ("average_price_per_sqm_by_plz3", ("plz3",)),
        ("average_price_per_sqm_by_month", ("month",)),
        ("average_price_per_sqm_by_quarter", ("quarter",)),
        ("average_price_per_sqm_by_plz3_quarter", ("plz3", "quarter")),
        ("average_price_per_sqm_by_plz3_month", ("plz3", "month")),
        ("average_asking_price_by_plz3_last_12m", ("plz3",)),
    ):
        for row in summary_data[group]:
            labels = ",".join(f'{name}="{label(row[name])}"' for name in label_names)
            lines.append(f'lakehouse_screener_{group}{{{labels}}} {row["average_eur"]}')
            lines.append(f'lakehouse_screener_{group}_properties{{{labels}}} {row["count"]}')
            if group == "average_price_per_sqm_by_plz3_month":
                lines.append(f'lakehouse_screener_median_price_per_sqm_by_plz3_month{{{labels}}} {row["median_eur"]}')
    for group, label_names in (
        ("price_per_sqm_by_region_month", ("region", "month")),
        ("price_per_sqm_by_region_quarter", ("region", "quarter")),
    ):
        for row in summary_data[group]:
            labels = ",".join(f'{name}="{label(row[name])}"' for name in label_names)
            lines.append(f'lakehouse_screener_{group}_median_eur{{{labels}}} {row["median_eur"]}')
            lines.append(f'lakehouse_screener_{group}_properties{{{labels}}} {row["count"]}')
    if history_data:
        lines.append(f'lakehouse_screener_manually_imported_listings {history_data["manual_imported_listings"]}')
        for name in (
            "source_listings", "unique_properties", "duplicate_listings", "duplicate_rate",
            "priced_properties", "median_price_eur", "median_price_per_sqm_eur",
            "average_price_per_sqm_eur", "price_per_sqm_implausible_area_properties",
            "price_per_sqm_excluded_properties",
        ):
            value = history_data.get(name)
            if value is not None:
                lines.append(f"lakehouse_screener_all_{name} {value}")
        for group, metric, label_name, value_name in (
            ("by_city", "properties_by_city", "city", "count"),
            ("sources_by_provider", "sources_by_provider", "provider", "count"),
            ("average_price_per_sqm_by_plz3", "average_price_per_sqm_by_plz3", "plz3", "average_eur"),
        ):
            for row in history_data[group]:
                lines.append(f'lakehouse_screener_all_{metric}{{{label_name}="{label(row[label_name])}"}} {row[value_name]}')
            for row in removed_cohorts(previous_history, history_data, group, (label_name,)):
                lines.append(f'lakehouse_screener_all_{metric}{{{label_name}="{label(row[label_name])}"}} 0')
        for group, label_names in (
            ("average_price_per_sqm_by_month", ("month",)),
            ("average_price_per_sqm_by_quarter", ("quarter",)),
            ("average_price_per_sqm_by_plz3_quarter", ("plz3", "quarter")),
            ("average_price_per_sqm_by_plz3_month", ("plz3", "month")),
        ):
            for row in history_data[group]:
                labels = ",".join(f'{name}="{label(row[name])}"' for name in label_names)
                lines.append(f'lakehouse_screener_historical_{group}{{{labels}}} {row["average_eur"]}')
                lines.append(f'lakehouse_screener_historical_{group}_properties{{{labels}}} {row["count"]}')
                if group == "average_price_per_sqm_by_plz3_month":
                    lines.append(f'lakehouse_screener_historical_median_price_per_sqm_by_plz3_month{{{labels}}} {row["median_eur"]}')
            for row in removed_cohorts(previous_history, history_data, group, label_names):
                labels = ",".join(f'{name}="{label(row[name])}"' for name in label_names)
                lines.append(f'lakehouse_screener_historical_{group}{{{labels}}} 0')
                lines.append(f'lakehouse_screener_historical_{group}_properties{{{labels}}} 0')
                if group == "average_price_per_sqm_by_plz3_month":
                    lines.append(f'lakehouse_screener_historical_median_price_per_sqm_by_plz3_month{{{labels}}} 0')
        for row in history_data["average_price_per_sqm_by_month"]:
            lines.append(f'lakehouse_screener_historical_median_price_per_sqm_by_month{{month="{label(row["month"])}"}} {row["median_eur"]}')
        for row in removed_cohorts(previous_history, history_data, "average_price_per_sqm_by_month", ("month",)):
            lines.append(f'lakehouse_screener_historical_median_price_per_sqm_by_month{{month="{label(row["month"])}"}} 0')
        for row in history_data["average_price_per_sqm_by_quarter"]:
            lines.append(f'lakehouse_screener_historical_median_price_per_sqm_by_quarter{{quarter="{label(row["quarter"])}"}} {row["median_eur"]}')
        for row in removed_cohorts(previous_history, history_data, "average_price_per_sqm_by_quarter", ("quarter",)):
            lines.append(f'lakehouse_screener_historical_median_price_per_sqm_by_quarter{{quarter="{label(row["quarter"])}"}} 0')
        for group, label_names in (
            ("price_per_sqm_by_region_month", ("region", "month")),
            ("price_per_sqm_by_region_quarter", ("region", "quarter")),
        ):
            for row in history_data[group]:
                labels = ",".join(f'{name}="{label(row[name])}"' for name in label_names)
                lines.append(f'lakehouse_screener_historical_{group}_median_eur{{{labels}}} {row["median_eur"]}')
                lines.append(f'lakehouse_screener_historical_{group}_properties{{{labels}}} {row["count"]}')
            for row in removed_cohorts(previous_history, history_data, group, label_names):
                labels = ",".join(f'{name}="{label(row[name])}"' for name in label_names)
                lines.append(f'lakehouse_screener_historical_{group}_median_eur{{{labels}}} 0')
                lines.append(f'lakehouse_screener_historical_{group}_properties{{{labels}}} 0')
    request = Request(endpoint, data=("\n".join(lines) + "\n").encode("utf-8"),
                      headers={"Content-Type": "text/plain"}, method="POST")
    with urlopen(request, timeout=15) as response:
        response.read()


@asset(group_name="real_estate")
def screener_bronze(context: AssetExecutionContext) -> MaterializeResult:
    """Archive current and manually imported source rows without collapsing identities."""
    rows, after = [], 0
    while True:
        page = _api("/api/bi-export", after=after, limit=500, history="true")
        rows.extend(page["rows"])
        if len(rows) > 10000:
            raise ValueError("Screener export exceeds the 10000-listing safety limit")
        if page["next_after"] is None:
            break
        next_after = int(page["next_after"])
        if next_after <= after:
            raise ValueError("Screener export cursor did not advance")
        after = next_after
    day = datetime.now(timezone.utc).date().isoformat()
    key = f"bronze/screener/date={day}/listings.jsonl"
    body = "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows)
    _s3().put_object(Bucket=os.environ["S3_BUCKET"], Key=key,
                     Body=body.encode("utf-8"), ContentType="application/x-ndjson")
    context.log.info("Archived %s source listing records", len(rows))
    return MaterializeResult(metadata={"rows": len(rows), "object": MetadataValue.text(key)})


@asset(group_name="real_estate", deps=[screener_bronze])
def screener_llm_enrichment(context: AssetExecutionContext) -> MaterializeResult:
    """Fill the evidence cache independently of the daily Silver/Gold build."""
    day = datetime.now(timezone.utc).date().isoformat()
    s3 = _s3()
    bronze = s3.get_object(Bucket=os.environ["S3_BUCKET"],
                           Key=f"bronze/screener/date={day}/listings.jsonl")["Body"].read()
    rows = [json.loads(line) for line in bronze.decode("utf-8").splitlines() if line]
    current_rows, _ = split_current_and_history(rows)
    dataset = deduplicate(current_rows)
    enricher = PropertyEnricher(
        s3, os.environ["S3_BUCKET"],
        max_new_calls=int(os.environ.get("SCREENER_LLM_MAX_NEW_CALLS", "250")),
        max_elapsed_seconds=int(os.environ.get("SCREENER_LLM_MAX_SECONDS", "300")),
    )
    for row in dataset["listings"]:
        enricher.enrich(row)
    context.log.info("LLM cache: %s successes, %s cache hits, %s failures, %s quota skips",
                     enricher.successful_calls, enricher.cache_hits,
                     enricher.failed_calls, enricher.quota_skipped)
    return MaterializeResult(metadata={
        "eligible_properties": enricher.eligible_rows,
        "new_calls": enricher.new_calls,
        "successful_calls": enricher.successful_calls,
        "cache_hits": enricher.cache_hits,
        "failed_calls": enricher.failed_calls,
        "quota_skipped": enricher.quota_skipped,
        "budget_skipped": enricher.budget_skipped,
        "time_skipped": enricher.time_skipped,
    })


@asset(group_name="real_estate", deps=[screener_bronze])
def screener_silver(context: AssetExecutionContext) -> MaterializeResult:
    """Deduplicate verified history separately from the current inventory."""
    day = datetime.now(timezone.utc).date().isoformat()
    s3 = _s3()
    bronze = s3.get_object(Bucket=os.environ["S3_BUCKET"],
                           Key=f"bronze/screener/date={day}/listings.jsonl")["Body"].read()
    rows = [json.loads(line) for line in bronze.decode("utf-8").splitlines() if line]
    current_rows, history_rows = split_current_and_history(rows)
    historical = deduplicate(history_rows)
    dataset = deduplicate(current_rows)
    enricher = PropertyEnricher(s3, os.environ["S3_BUCKET"], cache_only=True)
    dataset["listings"] = [annotate_price_per_sqm(enricher.enrich(row))
                           for row in dataset["listings"]]
    completed = sum((row.get("llm_enrichment") or {}).get("status") == "completed"
                    for row in dataset["listings"])
    accepted = sum(bool((row.get("llm_evidence") or {}).get("quotes"))
                   for row in dataset["listings"])
    dataset = {"algorithm_version": 2, "snapshot_at": datetime.now(timezone.utc).isoformat(),
               "source_count": dataset["source_count"],
               "unique_count": dataset["unique_count"],
               "duplicate_count": dataset["duplicate_count"],
               "llm_quota_skipped_properties": enricher.quota_skipped,
               "llm_budget_skipped_properties": enricher.budget_skipped,
               "llm_time_skipped_properties": enricher.time_skipped,
               "llm_cache_pending_properties": enricher.cache_pending,
               "llm_eligible_properties": enricher.eligible_rows,
               "llm_cache_hits": enricher.cache_hits,
               "llm_successful_calls": enricher.successful_calls,
               "llm_failed_calls": enricher.failed_calls,
               "llm_completed_properties": completed,
               "llm_accepted_fact_properties": accepted,
               "listings": dataset["listings"],
               "match_evidence": dataset["match_evidence"]}
    historical_summary = summary(historical)
    historical_summary["manual_imported_listings"] = len(rows) - len(current_rows)
    dataset["historical_summary"] = historical_summary
    key = f"silver/screener/date={day}/properties.json"
    body = json.dumps(dataset, ensure_ascii=False).encode("utf-8")
    s3.put_object(Bucket=os.environ["S3_BUCKET"], Key=key, Body=body,
                  ContentType="application/json")
    context.log.info("Curated %s unique properties", dataset["unique_count"])
    if enricher.cache_pending:
        context.log.info("%s properties await a completed LLM cache result", enricher.cache_pending)
    return MaterializeResult(metadata={
        "unique_properties": dataset["unique_count"],
        "duplicates": dataset["duplicate_count"],
        "new_llm_calls": enricher.new_calls,
        "llm_successful_calls": enricher.successful_calls,
        "llm_failed_calls": enricher.failed_calls,
        "llm_cache_hits": enricher.cache_hits,
        "llm_eligible_properties": enricher.eligible_rows,
        "llm_completed_properties": completed,
        "llm_accepted_fact_properties": accepted,
        "llm_quota_skipped_properties": enricher.quota_skipped,
        "llm_budget_skipped_properties": enricher.budget_skipped,
        "llm_time_skipped_properties": enricher.time_skipped,
        "llm_cache_pending_properties": enricher.cache_pending,
        "object": MetadataValue.text(key),
    })


@asset(group_name="real_estate", deps=[screener_silver])
def screener_gold(context: AssetExecutionContext) -> MaterializeResult:
    """Publish small KPI objects and time-series samples for Grafana."""
    day = datetime.now(timezone.utc).date().isoformat()
    s3 = _s3()
    silver = json.loads(s3.get_object(Bucket=os.environ["S3_BUCKET"],
        Key=f"silver/screener/date={day}/properties.json")["Body"].read())
    kpis = {"algorithm_version": silver["algorithm_version"],
            "snapshot_at": silver["snapshot_at"], "summary": summary(silver),
            "historical_summary": silver.get("historical_summary")}
    try:
        previous = json.loads(s3.get_object(Bucket=os.environ["S3_BUCKET"],
            Key="gold/screener/current.json")["Body"].read()).get("historical_summary")
    except ClientError as error:
        if error.response["Error"]["Code"] not in ("NoSuchKey", "404"):
            raise
        previous = None
    body = json.dumps(kpis, ensure_ascii=False).encode("utf-8")
    key = f"gold/screener/date={day}/kpis.json"
    s3.put_object(Bucket=os.environ["S3_BUCKET"], Key=key, Body=body,
                  ContentType="application/json")
    _publish_metrics(kpis["summary"], kpis["historical_summary"], previous)
    s3.put_object(Bucket=os.environ["S3_BUCKET"], Key="gold/screener/current.json",
                  Body=body, ContentType="application/json")
    context.log.info("Published KPIs for %s unique properties", kpis["summary"]["unique_properties"])
    return MaterializeResult(metadata={"unique_properties": kpis["summary"]["unique_properties"],
                              "object": MetadataValue.text(key)})
