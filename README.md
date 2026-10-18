# Lakehouse

This repository contains the Dagster asset code and configuration for the homelab lakehouse. [Edge_GitOps](https://github.com/bibAtWork/Edge_GitOps) deploys it with Flux into the `lakehouse` Kubernetes namespace. Dagster orchestrates assets, CloudNativePG stores run metadata, and SeaweedFS provides the `lakehouse-data` S3 bucket for datasets. The `sample_bronze_data` asset writes a CSV to `s3://lakehouse-data/bronze/sample_data.csv`.

The `src/spark` and `src/dbt` directories contain exploratory examples for future pipelines.

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
