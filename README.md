# Lakehouse

This repository contains the Dagster asset code and configuration for the homelab lakehouse. [Edge_GitOps](https://github.com/bibAtWork/Edge_GitOps) deploys it with Flux into the `lakehouse` Kubernetes namespace. Dagster orchestrates assets, CloudNativePG stores run metadata, and SeaweedFS provides the `lakehouse-data` S3 bucket for datasets. The `sample_bronze_data` asset writes a CSV to `s3://lakehouse-data/bronze/sample_data.csv`.

The daily real-estate pipeline consumes records already produced by the real-estate screener:

- **Bronze**: `screener_bronze` pages the screener's read-only BI export, including its stored descriptions and detail-success timestamps, and archives the source records unchanged in `bronze/screener/date=YYYY-MM-DD/listings.jsonl`.
- **Silver**: `screener_silver` normalizes the five matching attributes and groups listings with exactly equal asking prices when either complete logical addresses match or at least three populated attributes match. It asks Gemini 3.5 Flash-Lite for missing schema fields only when the canonical property's description came from a successful detail fetch; search-only text may be an excerpt. Every accepted LLM value has a verbatim source quote, passes local type/unit checks, and never replaces a portal-derived value. Unknown or conflicting facts remain absent. Silver stores the canonical properties, all source identities, match evidence and accepted LLM evidence in `silver/screener/date=YYYY-MM-DD/properties.json`.
- **Gold**: `screener_gold` stores aggregate KPI objects in `gold/screener/date=YYYY-MM-DD/kpis.json` and `gold/screener/current.json`, and publishes daily aggregate samples to VictoriaMetrics for the Grafana BI dashboard. The BI objects include deduplicated asking-price-per-m² averages by three-digit postcode and by first-seen month/quarter, plus the number of newly seen properties in the last 24 hours. A property's first-seen time is the earliest time across its matched source listings. Historical cohorts cover properties retained by the current screener export; they are not a complete record of removed listings. No property address or URL is used as a metric label.

Price-per-m² metrics require a positive asking price and at least 10 m² of living area. A source listing reporting 1 m² with a million-euro price would otherwise dominate the mean; the listing remains in Silver, and Gold counts properties excluded by this area rule.

The `screener_bi_refresh` schedule runs at 04:00 UTC. The screener remains the sole property-site collector. The lakehouse keeps dated snapshots so BI history does not depend on the metrics retention window.

Silver can fill `property_type`, `build_year`, `living_area_m2`, `plot_area_m2`, `rooms`,
`energy_class`, `end_energy_consumption_kwh_m2a`, `primary_energy_carrier`, and explicit
property-address fields. The property type uses `detached_house`, `semi_detached_house`,
`multi_family_house`, `terraced_house`, or `apartment`. The energy-consumption field is
the stated kWh/(m²·a) figure, not demand or a total annual kWh figure. Title and asking
price remain Screener values. Deduplication runs before LLM enrichment, so uncertain
model claims cannot merge properties.

The fetched detail description is sent to Google's Gemini API when no cached result exists.
The model response is validated against exact quotes in that description; model JSON
shape alone does not establish factual correctness. Successful results are cached by
description SHA-256, model and schema version in SeaweedFS, avoiding repeat API calls
for unchanged text. `SCREENER_LLM_MAX_NEW_CALLS` defaults to 250 per run and
`SCREENER_LLM_MAX_SECONDS` to 300 seconds; an in-flight request may extend the latter
by its timeout. If Gemini returns HTTP 429 after bounded retries or either budget is exhausted, Silver
preserves the source fields for uncached properties and publishes the snapshot; Dagster
metadata and Gold metrics report how many properties skipped enrichment for each reason.
Other API failures or missing credentials fail Silver before publishing a new dataset. The cache and Silver evidence
contain quoted property text and share the `lakehouse-data` bucket's access controls.

The `src/spark` and `src/dbt` directories contain exploratory examples for future pipelines.

## Local testing

Docker Compose starts the upstream Dagster image, PostgreSQL, and SeaweedFS with a local `lakehouse-data` bucket. It mounts the Dagster source and configuration from this checkout. The S3 API and Dagster UI listen on localhost; Compose uses development-only credentials from `env.example` by default. Copy that file to `.env` to override the credentials or host ports. Renovate's Docker Compose manager tracks the upstream image references in `compose.yaml`.

```sh
docker compose up -d --wait
docker compose exec dagster-daemon dagster asset materialize -m lakehouse --select sample_bronze_data
```

Open Dagster at <http://localhost:3333>. Stop the stack with `docker compose down`; use `docker compose down -v` to discard its local database and S3 data.

Set `SCREENER_API_URL` in `.env` when testing the real-estate assets against a local screener. The default is `http://host.docker.internal:3000`. `VM_IMPORT_URL` is optional locally; the cluster sets it to VictoriaMetrics' import endpoint. Run `python -m unittest discover -s tests` to check the matching rules without a running cluster.
For local Silver materialization, set `GEMINI_API_KEY` in `.env`; the cluster reads the
SOPS-encrypted `google-gemini-api-key` Secret from its own namespace. Do not commit
the key or a decrypted Secret. The optional `SCREENER_LLM_MODEL` setting defaults to
`gemini-3.5-flash-lite`.

## Source delivery

The cluster uses the upstream `dagster/dagster-k8s` image. Flux reads this repository at a commit pinned in Edge_GitOps and builds the two ConfigMaps declared by `kustomization.yaml`. Kubernetes mounts those ConfigMaps as the Dagster code location and instance configuration.

To add an asset, edit `src/dagster/assets` and list its file under `lakehouse-code` in `kustomization.yaml`. After merging it here, update the source commit pin in Edge_GitOps. Reloader restarts Dagster when the ConfigMap changes. Renovate tracks the upstream Dagster image tag in Edge_GitOps.

## Operational checks

```sh
kubectl -n lakehouse get pods,svc
kubectl -n lakehouse get cluster lakehouse-pg
kubectl -n lakehouse logs deployment/lakehouse-dagster -c web
kubectl -n lakehouse logs deployment/lakehouse-dagster -c daemon
```

Dagster is served through the cluster Gateway with Keycloak OIDC and OPA authorization. An encrypted Secret in Edge_GitOps supplies the SeaweedFS credential; CloudNativePG manages the PostgreSQL application credential in a Kubernetes Secret.
