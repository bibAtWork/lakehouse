# Lakehouse

The running lakehouse is deployed by Flux from [Edge_GitOps](https://github.com/bibAtWork/Edge_GitOps) into the `lakehouse` Kubernetes namespace. It uses the cluster's existing SeaweedFS S3 endpoint and a dedicated `lakehouse-data` bucket. There is no MinIO instance or Docker Compose deployment.

The first working slice is Dagster with CloudNativePG for run metadata and SeaweedFS for dataset objects. The `sample_bronze_data` asset writes a CSV to `s3://lakehouse-data/bronze/sample_data.csv`. The old Spark, dbt, Delta Lake, and Unity Catalog files are prototypes; they are not connected to Dagster or deployed yet. In particular, the old Databricks-style Spark settings do not provide Unity Catalog integration with open-source Spark.

## Source delivery

The cluster uses the upstream `dagster/dagster-k8s` image. Flux reads this repository at a commit pinned in Edge_GitOps and builds the two ConfigMaps declared by `kustomization.yaml`. Kubernetes mounts those ConfigMaps as the Dagster code location and instance configuration. No project-specific image is built or published.

To add an asset, edit `src/dagster/assets` and list its file under `lakehouse-code` in `kustomization.yaml`. After merging it here, update the source commit pin in Edge_GitOps. Reloader restarts Dagster when the ConfigMap changes. Renovate tracks the upstream Dagster image tag in Edge_GitOps.

## Operational checks

```sh
kubectl -n lakehouse get pods,svc
kubectl -n lakehouse get cluster lakehouse-pg
kubectl -n lakehouse logs deployment/lakehouse-dagster -c web
kubectl -n lakehouse logs deployment/lakehouse-dagster -c daemon
```

Dagster is served through the cluster Gateway with Keycloak OIDC and OPA authorization. Keep the SeaweedFS credential and the CloudNativePG application password in Kubernetes Secrets; never put either in this repository.
