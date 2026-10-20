"""Daily, reproducible screener snapshots for the lakehouse."""

import json
import os
from datetime import datetime, timezone
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import boto3
from botocore.config import Config
from dagster import AssetExecutionContext, MaterializeResult, MetadataValue, asset
from lakehouse.dedup import deduplicate, summary
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


def _publish_metrics(summary_data):
    endpoint = os.environ.get("VM_IMPORT_URL")
    if not endpoint:
        return
    metrics = {
        "source_listings": summary_data["source_listings"],
        "unique_properties": summary_data["unique_properties"],
        "duplicate_listings": summary_data["duplicate_listings"],
        "duplicate_rate": summary_data["duplicate_rate"],
        "llm_quota_skipped_properties": summary_data["llm_quota_skipped_properties"],
        "priced_properties": summary_data["priced_properties"],
        "median_price_eur": summary_data["median_price_eur"],
        "median_price_per_sqm_eur": summary_data["median_price_per_sqm_eur"],
        "average_price_per_sqm_eur": summary_data["average_price_per_sqm_eur"],
        "new_properties_24h": summary_data["new_properties_24h"],
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
    ):
        for row in summary_data[group]:
            labels = ",".join(f'{name}="{label(row[name])}"' for name in label_names)
            lines.append(f'lakehouse_screener_{group}{{{labels}}} {row["average_eur"]}')
            lines.append(f'lakehouse_screener_{group}_properties{{{labels}}} {row["count"]}')
    request = Request(endpoint, data=("\n".join(lines) + "\n").encode("utf-8"),
                      headers={"Content-Type": "text/plain"}, method="POST")
    with urlopen(request, timeout=15) as response:
        response.read()


@asset(group_name="real_estate")
def screener_bronze(context: AssetExecutionContext) -> MaterializeResult:
    """Copy current source listings without collapsing cross-portal identities."""
    rows, after = [], 0
    while True:
        page = _api("/api/bi-export", after=after, limit=500)
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
def screener_silver(context: AssetExecutionContext) -> MaterializeResult:
    """Clean and deduplicate current source listings with explicit match evidence."""
    day = datetime.now(timezone.utc).date().isoformat()
    s3 = _s3()
    bronze = s3.get_object(Bucket=os.environ["S3_BUCKET"],
                           Key=f"bronze/screener/date={day}/listings.jsonl")["Body"].read()
    rows = [json.loads(line) for line in bronze.decode("utf-8").splitlines() if line]
    dataset = deduplicate(rows)
    enricher = PropertyEnricher(
        s3, os.environ["S3_BUCKET"],
        max_new_calls=int(os.environ.get("SCREENER_LLM_MAX_NEW_CALLS", "250")),
    )
    dataset["listings"] = [enricher.enrich(row) for row in dataset["listings"]]
    dataset = {"algorithm_version": 1, "snapshot_at": datetime.now(timezone.utc).isoformat(),
               "source_count": dataset["source_count"],
               "unique_count": dataset["unique_count"],
               "duplicate_count": dataset["duplicate_count"],
               "llm_quota_skipped_properties": enricher.quota_skipped,
               "listings": dataset["listings"],
               "match_evidence": dataset["match_evidence"]}
    key = f"silver/screener/date={day}/properties.json"
    body = json.dumps(dataset, ensure_ascii=False).encode("utf-8")
    s3.put_object(Bucket=os.environ["S3_BUCKET"], Key=key, Body=body,
                  ContentType="application/json")
    context.log.info("Curated %s unique properties", dataset["unique_count"])
    if enricher.quota_skipped:
        context.log.warning("Gemini quota exhausted; %s properties retained without new enrichment",
                            enricher.quota_skipped)
    return MaterializeResult(metadata={
        "unique_properties": dataset["unique_count"],
        "duplicates": dataset["duplicate_count"],
        "new_llm_calls": enricher.new_calls,
        "llm_quota_skipped_properties": enricher.quota_skipped,
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
            "snapshot_at": silver["snapshot_at"], "summary": summary(silver)}
    body = json.dumps(kpis, ensure_ascii=False).encode("utf-8")
    key = f"gold/screener/date={day}/kpis.json"
    s3.put_object(Bucket=os.environ["S3_BUCKET"], Key=key, Body=body,
                  ContentType="application/json")
    s3.put_object(Bucket=os.environ["S3_BUCKET"], Key="gold/screener/current.json",
                  Body=body, ContentType="application/json")
    _publish_metrics(kpis["summary"])
    context.log.info("Published KPIs for %s unique properties", kpis["summary"]["unique_properties"])
    return MaterializeResult(metadata={"unique_properties": kpis["summary"]["unique_properties"],
                              "object": MetadataValue.text(key)})
