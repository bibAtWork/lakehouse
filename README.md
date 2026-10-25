# Lakehouse

This repository contains the Dagster asset code and configuration for the homelab lakehouse. [Edge_GitOps](https://github.com/bibAtWork/Edge_GitOps) deploys it with Flux into the `lakehouse` Kubernetes namespace. Dagster orchestrates assets, CloudNativePG stores run metadata, and SeaweedFS provides the `lakehouse-data` S3 bucket for datasets. The `sample_bronze_data` asset writes a CSV to `s3://lakehouse-data/bronze/sample_data.csv`.

The daily real-estate pipeline consumes records already produced by the real-estate screener:

- **Bronze**: `screener_bronze` pages the screener's read-only BI export, including its stored descriptions and detail-success timestamps, and archives the source records unchanged in `bronze/screener/date=YYYY-MM-DD/listings.jsonl`.
- **LLM enrichment**: `screener_llm_enrichment` deduplicates the current Bronze inventory and fills a validated Gemini 3.5 Flash-Lite result cache for descriptions that are not yet completed; search-only text may be an excerpt. It does not publish Silver or Gold. Failed and rate-limited calls remain eligible for the next LLM run.
- **Silver**: `screener_silver` normalizes the five matching attributes and groups listings with exactly equal asking prices when either complete logical addresses match or at least three populated attributes match. It reads completed enrichment results from the cache without calling Gemini. Every accepted LLM value has a verbatim source quote, passes local type/unit checks, and never replaces a portal-derived value. Unknown or conflicting facts remain absent. Silver stores the canonical properties, all source identities, match evidence and accepted LLM evidence in `silver/screener/date=YYYY-MM-DD/properties.json`.
- **Gold**: `screener_gold` stores aggregate KPI objects in `gold/screener/date=YYYY-MM-DD/kpis.json` and `gold/screener/current.json`, and publishes daily aggregate samples to VictoriaMetrics for Grafana. The BI objects include deduplicated asking-price-per-m² means and medians by first-seen month and three-digit postcode, quarterly postcode cohorts, and the number of newly seen properties in the last 24 hours. A 365-day asking-house-price mean includes only properties with a known house type; its count is published alongside it. Historical cohorts cover properties retained by the current screener export; they are not a complete record of removed listings or completed sales. No property address or URL is used as a metric label.

The regional BI baskets use PLZ3 as an approximation: Reutlingen `727`, Tübingen `720`, and Stuttgarter Speckgürtel `707`–`715`. They are not administrative district boundaries. Each basket publishes monthly and quarterly median asking price per m² and the underlying property count.

Silver keeps the original asking price and living area and adds `price_per_sqm_quality` with a status, reason, and calculated EUR/m² value. For ordinary residential price-per-m² comparisons, a plausible observation needs at least 20 m² of living area and a ratio from €500 to €20,000/m² (inclusive). Smaller areas, nonpositive values, and ratios outside that band are flagged rather than deleted. Missing price or area is marked `insufficient_data`. Gold price-per-m² averages and medians use only plausible observations and publish the excluded count; listing and price totals still include flagged rows. These bounds are data-quality heuristics, not assertions that an unusual property is impossible.

`screener_bronze_refresh` archives the screener at 03:00 UTC. `screener_llm_refresh` fills the enrichment cache at 03:15 and every three hours through 21:15 UTC. `screener_bi_refresh` builds Silver and Gold at 04:00 UTC; later LLM results enter the next daily BI snapshot. The screener remains the sole property-site collector. The lakehouse keeps dated snapshots so BI history does not depend on the metrics retention window.

Silver can fill `property_type`, `build_year`, `living_area_m2`, `plot_area_m2`, `rooms`,
`energy_class`, `end_energy_consumption_kwh_m2a`, `primary_energy_carrier`, and explicit
property-address fields. The property type uses `detached_house`, `semi_detached_house`,
`multi_family_house`, `terraced_house`, or `apartment`. The energy-consumption field is
the stated kWh/(m²·a) figure, not demand or a total annual kWh figure. Title and asking
price remain Screener values. Deduplication runs before LLM enrichment, so uncertain
model claims cannot merge properties.

Any nonempty description is sent to Google's Gemini API when no completed result exists for its text, model and schema version.
The model response is validated against exact quotes in that description; model JSON
shape alone does not establish factual correctness. Successful results are cached by
description SHA-256, model and schema version in SeaweedFS, avoiding repeat API calls
for unchanged text. A completed response with no accepted facts is cached too; a failed or rate-limited response is never cached and is retried on a later LLM run. Silver marks completed rows even when no new fields were accepted. `SCREENER_LLM_MAX_NEW_CALLS` defaults to 250 per LLM run and
`SCREENER_LLM_MAX_SECONDS` to 300 seconds; an in-flight request may extend the latter
by its timeout. If Gemini returns HTTP 429 after bounded retries or either budget is exhausted, the LLM run retains uncached properties for the next attempt. Silver
preserves their source fields and publishes the snapshot. Its `llm_cache_pending_properties` count shows how many canonical descriptions still lack a completed result. The `llm_enrichment.status=completed` marker in Silver records the model, schema version and description SHA-256, including successful responses with zero accepted fields; a changed description becomes eligible again. Dagster LLM-run metadata reports successful calls, cache hits, failed calls and skips.
Other API failures retry on a later LLM run; missing credentials fail that run while Silver remains available. The cache and Silver evidence
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
For local LLM materialization, set `GEMINI_API_KEY` in `.env`; the cluster reads the
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
