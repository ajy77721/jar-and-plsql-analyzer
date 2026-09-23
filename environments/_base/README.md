# Kanopy deploy conventions (`_base`)

Shared conventions for the amp-analyzer deployments on Kanopy. The per-environment
values files (`../staging.yml`, `../prod.yml`) are consumed by the Helm steps in
`.drone.yml`; this file explains how that pipeline works and what it needs.

## Stack

- **Chart:** `mongodb/web-app`, pinned to `4.35.0` (the amp-platform pin) in both
  Helm steps in `.drone.yml`. Bump `chart_version` in both `deploy-staging` and
  `deploy-prod` together.
- **Pod shape:** a single container — no sidecars. The Spring Boot `unified-web`
  jar serves the UI/API itself and keeps all artifacts in S3
  (`app.storage.mode=s3` is the default), so the pod is stateless.
- **Namespace:** `amp-delivery` (both clusters), release `amp-analyzer`, with a
  `staging`/`prod` prefix selecting the environment.
- **Registry:** images are published to GHCR as
  `ghcr.io/modernization-factory/amp-analyzer`.

## How the deploy works

`.drone.yml` defines one pipeline with these steps:

| Step | Trigger | What it does |
|---|---|---|
| `test` | every build | `mvn -B -no-transfer-progress package -DskipTests -q` — compile check only. |
| `build+publish-main-branch` | push to `main` | `plugins/docker` builds the repo `Dockerfile` and pushes `git-<sha7>` and `latest` to `ghcr.io/modernization-factory/amp-analyzer`. |
| `build+publish-tag` | tag push | Same build, pushing the tag name and `git-<sha7>`. |
| `deploy-staging` | push to `main` | Helm upgrade/install of `mongodb/web-app` into `amp-delivery` (prefix `staging`, `API_SERVER=https://api.staging.corp.mongodb.com`), with `environments/staging.yml` as the values file and `image.tag=git-<sha7>` overridden from the build. |
| `deploy-prod` | tag push | Same Helm step against `prod` (`API_SERVER=https://api.prod.corp.mongodb.com`) with `environments/prod.yml` and `image.tag=<git tag>`. |
| `slack-start` / `slack-finish` | start / any status | Notifications only. Both carry `failure: ignore`, so a Slack problem can never fail the build. |

The image is built from the repository `Dockerfile` by `plugins/docker`
(the multi-stage Maven build, so the `test` step's output is not what ships).
Only `image.tag` and `image.repository` are overridden per build; everything else
comes from the environment's values file.

## Secrets

Drone repository secrets (Settings → Secrets). Names must match `.drone.yml`
exactly — the steps reference them via `from_secret:`.

| Secret | Required | Purpose |
|---|---|---|
| `ghcr_token` | yes | GitHub PAT with `write:packages` (and `read:packages`), used as the `plugins/docker` password to push to GHCR. |
| `ghcr_user` | yes | GitHub username that owns the PAT (`x-access-token` works); ghcr.io validates it against the token. |
| `staging_kubernetes_token` | yes | Bearer token for `api.staging.corp.mongodb.com`, scoped to namespace `amp-delivery`. |
| `prod_kubernetes_token` | yes | Bearer token for `api.prod.corp.mongodb.com`, scoped to namespace `amp-delivery`. |
| `slack_webhook` | optional | Incoming-webhook URL for the `amp-analyzer` channel. Both Slack steps are `failure: ignore`, so the deploy runs without it. |

Runtime AWS access (for the S3 storage backend) is **not** a Drone secret — the
app pod gets it from the SDK default provider chain (a pod IAM role, or an
`envSecrets` entry). See `../staging.yml` and `../prod.yml`.

## Adding an environment

The two environments differ only in their values files: ingress host
(`staging` vs `prod`), resource sizing, and the `API_SERVER`/secret used by the
matching Helm step. To add a third, copy `../staging.yml`, change the ingress
host and sizing, and add a Helm step to `.drone.yml` that selects it.
