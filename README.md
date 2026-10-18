# Lakehouse

The running lakehouse is deployed by Flux from [Edge_GitOps](https://github.com/bibAtWork/Edge_GitOps) into the `lakehouse` Kubernetes namespace. It uses the cluster's existing SeaweedFS S3 endpoint and a dedicated `lakehouse-data` bucket. There is no MinIO instance or Docker Compose deployment.

The first working slice is Dagster with CloudNativePG for run metadata and SeaweedFS for dataset objects. The `sample_bronze_data` asset writes a CSV to `s3://lakehouse-data/bronze/sample_data.csv`. The old Spark, dbt, Delta Lake, and Unity Catalog files are prototypes; they are not connected to Dagster or deployed yet. In particular, the old Databricks-style Spark settings do not provide Unity Catalog integration with open-source Spark.

## Build and verify

```sh
docker build -f Dockerfile.dagster -t lakehouse-dagster:dev .
docker run --rm lakehouse-dagster:dev python -c 'from lakehouse import defs; print([a.key.to_user_string() for a in defs.assets])'
```

The GitHub Actions workflow builds and publishes the Dagster image to GHCR on `main` pushes. The GitOps deployment pins a published digest. To add an asset, edit `src/dagster/assets`, validate the image, then update that digest in Edge_GitOps.

## Operational checks

```sh
kubectl -n lakehouse get pods,svc
kubectl -n lakehouse get cluster lakehouse-pg
kubectl -n lakehouse logs deployment/lakehouse-dagster-web
kubectl -n lakehouse logs deployment/lakehouse-dagster-daemon
```

Dagster is served through the cluster Gateway with Keycloak OIDC and OPA authorization. Keep the SeaweedFS credential and the CloudNativePG application password in Kubernetes Secrets; never put either in this repository.
