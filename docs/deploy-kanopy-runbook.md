# Kanopy Deploy Runbook — amp-analyzer

Step-by-step guide for deploying amp-analyzer to MongoDB's internal Kubernetes
(Kanopy) via Drone, modeled on the hello-flask template.

Work through the steps **in order** — each one feeds the next.

---

## 3.1 — Kanopy namespace

**Goal:** confirm your team owns a Kanopy namespace (a "folder" in the cluster where your pods run).

You already raised access for the group — when granted, your namespace name will be
confirmed (this runbook assumes `amp-delivery`, from the `10gen-kanopy-amp-delivery`
group).

**You're done when:** you have an email/approval saying you're in the Kanopy group and
the namespace name is confirmed. **If the namespace is different from
`amp-delivery`, tell Claude and the configs will be updated.**

---

## 3.2 — ECR credentials (so Drone can publish the Docker image)

**Goal:** an AWS access key pair that CI uses to push the built image to Amazon's
container registry (ECR) in account `331472312345`.

### Step-by-step

1. **Log in to AWS.** Open your AWS access portal in a browser (the `*.awsapps.com/start`
   URL connected to your Identity Center `ssoins-722332c8fc8beb0d`). Sign in with your
   MongoDB credentials (SSO).

2. **Switch to account 331472312345.** Click the account → pick a role
   (e.g. `PowerUserAccess` / `AdministratorAccess` if available). You land in the AWS
   Console. **Set the region to `us-east-1`** (top-right dropdown).

3. **Create a CI user.** Search for **IAM** → **Users** → **Create user**
   - Name: `drone-amp-analyzer`
   - Permissions: **Attach policies directly** → Create an inline policy with this JSON
     (allows pushing images to ECR only):

     ```json
     {
       "Version": "2012-10-17",
       "Statement": [{
         "Effect": "Allow",
         "Action": [
           "ecr:GetAuthorizationToken",
           "ecr:CreateRepository",
           "ecr:BatchCheckLayerAvailability",
           "ecr:PutImage",
           "ecr:InitiateLayerUpload",
           "ecr:UploadLayerPart",
           "ecr:CompleteLayerUpload",
           "ecr:DescribeRepositories"
         ],
         "Resource": "*"
       }]
     }
     ```

4. **Get the keys.** After the user is created: Users → `drone-amp-analyzer` →
   **Security credentials** tab → **Create access key** → choose
   **Application running outside AWS** → **Download CSV**.
   You now have `Access Key ID` (looks like `AKIA...`) and `Secret Access Key`.

5. **Test locally** (proves the keys work before wiring CI):

   ```bash
   export AWS_ACCESS_KEY_ID=<paste-key-id>
   export AWS_SECRET_ACCESS_KEY=<paste-secret>
   aws ecr get-login-password --region us-east-1 \
     | docker login 331472312345.dkr.ecr.us-east-1.amazonaws.com \
       --username AWS --password-stdin
   ```

   `Login Succeeded` = keys work.

**If you cannot see IAM in the console** (your role lacks permission): email
**aws-services@mongodb.com** — subject: "ECR push access for account 331472312345
(Modernization-Factory CI)" — and ask them to create the user above for you.

**You're done when:** `docker login` succeeds with the keys. Keep the CSV — you need it
in step 3.4.

---

## 3.3 — Kubernetes tokens (so Drone can deploy to staging/prod)

**Goal:** two tokens that authorize a deploy into the Kanopy clusters
(staging + prod), scoped to your namespace.

### Step-by-step

1. **Configure kubectl for staging** — follow
   [`kanopy-docs/docs/kubeconfig.md`](https://github.com/10gen/kanopy-docs/blob/master/docs/kubeconfig.md).
   In short: it has you authenticate against `https://api.staging.corp.mongodb.com`
   and writes a `kubeconfig` with a token. **You need the Kanopy group membership from
   3.1 for this to succeed.**

2. **Extract the token** from the generated kubeconfig:

   ```bash
   kubectl config view --minify -o jsonpath='{.users[0].user.token}'
   ```

   That long string is your **staging token**. Copy it somewhere safe.

3. **Verify it works** (should list resources in the namespace, not a 403):

   ```bash
   kubectl -n amp-delivery get pods
   ```

4. **Repeat 1–3 for prod** — same doc, but against `https://api.prod.corp.mongodb.com`.
   That gives the **prod token**.

**Note:** if the kubeconfig doc's flow differs from what you see (Kanopy evolves), the
underlying need is the same — *a bearer token valid for the `amp-delivery`
namespace on both API servers*. The Kanopy support channel can generate one for a
service account if personal tokens expire too quickly for CI use.

**You're done when:** `kubectl get pods` works in both staging and prod and you have
both token strings saved.

---

## 3.4 — Drone setup (activate repo + secrets)

**Goal:** Drone is MongoDB's internal CI (what hello-flask used). It watches your repo
and runs `.drone.yml` on every push.

### Step-by-step

1. **Find Drone.** Go to MongoDB's Drone instance — `drone.corp.mongodb.com` (if that
   doesn't load, search "drone" on Glean or ask a teammate who has deployed an internal
   app). Sign in with **GitHub OAuth** (same SSO as GitHub).

2. **Activate the repo.** Drone → **Sync** (top-right, refreshes your repo list) →
   find `Modernization-Factory/amp-analyzer` → click **Activate**.
   *If the repo isn't listed: Drone is only enabled for certain orgs — ping the Drone/
   Kanopy support channel to enable it for `Modernization-Factory`.*

3. **Add the 5 secrets.** Repo → **Settings** (gear) → **Secrets** → **New Secret** for
   each:

   | Secret name | Value | Comes from |
   |---|---|---|
   | `ecr_access_key` | the `AKIA...` key ID | Step 3.2 CSV |
   | `ecr_secret_key` | the secret access key | Step 3.2 CSV |
   | `staging_kubernetes_token` | staging token string | Step 3.3 |
   | `prod_kubernetes_token` | prod token string | Step 3.3 |
   | `slack_webhook` | the webhook URL (`https://hooks.slack.com/...`) | Step 3.5 |

   ⚠️ Names must match `.drone.yml` exactly (`from_secret:` names) — copy-paste them.

**You're done when:** all 5 secrets appear in Drone's secret list.

---

## 3.5 — Slack webhook + channel

**Goal:** deploy start/success/failure messages land in a Slack channel.

1. **Create a channel** in Slack, e.g. `#amp-analyzer-deploys` (you + your team).
2. In the channel: click the channel name → **Integrations** tab → **Add apps** →
   search **Incoming WebHooks** → Add → copy the **Webhook URL**
   (`https://hooks.slack.com/services/T.../B.../xxx`).
3. That URL is the `slack_webhook` secret (3.4).
4. **Tell Claude the channel name** (e.g. `amp-analyzer-deploys`) and the two
   `<your-team-channel>` placeholders in `.drone.yml` get filled in.

**You're done when:** the webhook URL is saved as a Drone secret and the channel name
is in `.drone.yml`.

---

## 3.6 — S3 access for the running app

**Goal:** the deployed app must read/write `s3://amp-analyzer-parse-jar` (region
`ap-south-1`) — that's where all analysis artifacts live (`app.storage.mode=s3` is the
default).

**How the app gets AWS credentials:** the AWS SDK's *default provider chain* — it checks
pod IAM role first, then env vars. Pick ONE of:

### Option A — Pod IAM role / IRSA (preferred, no keys to manage)
1. Ask your Kanopy/AWS admin (or aws-services@mongodb.com):
   > "Please create an IAM role for the service account used by pods in Kanopy
   > namespace `amp-delivery` with S3 access (read/write) to bucket
   > `amp-analyzer-parse-jar` in ap-south-1."
2. Once it exists, the app picks it up automatically — **zero config change**.

### Option B — Keys via envSecrets (works today, keys to rotate)
1. Create a least-privileged IAM policy (S3 on that one bucket only):

   ```json
   {
     "Version": "2012-10-17",
     "Statement": [{
       "Effect": "Allow",
       "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"],
       "Resource": [
         "arn:aws:s3:::amp-analyzer-parse-jar",
         "arn:aws:s3:::amp-analyzer-parse-jar/*"
       ]
     }]
   }
   ```

2. Put an IAM user with this policy's keys into your team's **secret store**, named
   e.g. `aws-creds` (per hello-flask's `envSecrets:` mechanism).
3. Uncomment in `environments/staging.yml` and `environments/prod.yml`:

   ```yaml
   envSecrets:
     aws-creds: aws-creds
   ```

**Also set** `AWS_REGION: ap-south-1` in the `env:` block of both environment files
(uncomment the existing line) — the S3 client needs it.

**You're done when:** Option A is confirmed, or Option B's secret exists and the
envSecrets lines are uncommented.

---

## 3.7 — First deploy

Everything above is done → deploy is one push:

```bash
git add -A && git commit -m "Deploy config" && git push origin main
```

Then watch:
1. **Drone dashboard** → your repo → the pipeline runs:
   `test` (compile) → `build+publish-main-branch` (image → ECR) → `deploy-staging` (Helm)
2. When all steps are green, the app is live at:

   **https://amp-analyzer.amp-delivery.staging.corp.mongodb.com**

3. First load takes ~1–2 min (pod image pull + Spring Boot startup).

### If something fails — debug commands

```bash
# What's running?
kubectl -n amp-delivery get pods

# Why is it crash-looping?
kubectl -n amp-delivery logs <pod-name> --tail=100

# Deploy step failed? → the Drone build log shows the exact helm error.
```

Common first-deploy failures:
- **ImagePullBackOff** → ECR repo/permissions wrong (recheck 3.2) or wrong image path in drone.yml
- **CreateContainerConfigError** → a secret referenced in envSecrets doesn't exist (3.6)
- **OOMKilled** → raise memory limits in `environments/staging.yml`

### Production

When staging looks good:

```bash
git tag v1.0.0 && git push origin v1.0.0
```

→ Drone runs the tag pipeline → prod at
**https://amp-analyzer.amp-delivery.prod.corp.mongodb.com**

---

## Quick reference — where every credential ends up

| Credential | Created in | Used by | Drone secret |
|---|---|---|---|
| AWS access keys | IAM, account 331472312345 (3.2) | Drone ECR push | `ecr_access_key` / `ecr_secret_key` |
| K8s staging token | kubeconfig flow (3.3) | Drone Helm deploy | `staging_kubernetes_token` |
| K8s prod token | kubeconfig flow (3.3) | Drone Helm deploy | `prod_kubernetes_token` |
| Slack webhook | Slack channel settings (3.5) | Drone notifications | `slack_webhook` |
| S3 access | IAM role or keys (3.6) | **The running app pod** | none (IRSA) or envSecrets |

---

## Appendix: cross-account S3 access (all-org users)

The app's per-user SSO model means EVERY user connects with THEIR OWN AWS role. For any
user whose role is NOT in the bucket-owning account (331472312345), two one-time grants are
needed — the app's capability probe will report `AccessDenied` and refuse to connect until
they exist (by design — the denial is audited with the exact operation).

**1. Bucket policy (in account 331472312345, on `amp-analyzer-parse-jar`)** — allow the org:

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Sid": "OrgWideAnalyzerAccess",
    "Effect": "Allow",
    "Principal": "*",
    "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"],
    "Resource": [
      "arn:aws:s3:::amp-analyzer-parse-jar",
      "arn:aws:s3:::amp-analyzer-parse-jar/*"
    ],
    "Condition": {
      "StringEquals": { "aws:PrincipalOrgID": "<o-xxxxxxxxxx — the AWS Organization id>" },
      "Bool": { "aws:PrincipalIsAWSService": "false" }
    }
  }]
}
```

(The `PrincipalOrgID` condition restricts the grant to the MongoDB AWS Organization only —
not the public. Get the org id: `aws organizations describe-organization`.)

**2. Identity permission (in each user's account, e.g. 579766882180 / Kernel-Test.User)** —
the role's own policy must also allow those four S3 actions on the bucket ARNs.

**Who applies them:** the AWS account admins (aws-services@mongodb.com for the bucket side;
each account's owners for their roles). One time only — after that every org user can
connect with their own identity, fully attributed.

**Email-ready request line:**
> Please apply the attached bucket policy to s3://amp-analyzer-parse-jar (account 331472312345)
> granting S3 read/write to principals in our AWS Organization (org-scoped, service-account
> excluded), and enable CloudTrail S3 data events for per-user attribution.

---

## DB-impact safety statement (the analyzed database is sacred)

An analysis only ever **reads** the customer database. The guarantees below are enforced in code,
not just by convention, and are configurable per deployment.

**Read-only.** Every catalog/source query is built through `DbConnectionManager.prepareStatement` /
`createStatement`, which assert the SQL starts with `SELECT` or `WITH` and throw otherwise. There is
no DDL/DML path on the analyzed connection. (The one exception, Oracle's
`DBMS_METADATA.SET_TRANSFORM_PARAM` session setup used only as a read-side DDL formatter, is a
`prepareCall` outside the query creator.)

**Rate limit.** Catalog/source queries are capped by a token bucket (`app.crawl.db.qps`, default 20).
One token is taken per connection borrow, so the steady-state query rate never exceeds the cap. Set
it lower for a fragile or shared database; `0` disables the limiter.

**Statement timeout.** Every guarded statement carries a JDBC `queryTimeout`
(`app.crawl.db.timeout-seconds`, default 30). A slow query is abandoned at the driver, never left to
hang a worker.

**Circuit breaker.** After `app.crawl.db.circuit-breaker-threshold` (default 5) consecutive query
failures the crawl **fails fast** with `"The database is not responding — aborting to protect it"`.
It does not retry for minutes; the customer DB is left alone.

**Batching.** Catalog lookups are batched (ONE ALL_OBJECTS query per BFS level; batched trigger-source
pull; batched reverse-dependency lookup). There are no per-name N+1 probes.

**Idempotency.** A re-crawl of an unchanged scope consults the per-analysis idempotency registry
(`<analysis parent>/.analysis-registry.json`, per-user) and skips objects whose LAST_DDL is unchanged
and whose previous analysis was complete — no fetch, no re-parse. `--force-refresh` (CLI) / the
`forceRefresh` analyze-endpoint field overrides it.

Operators tuning a fragile database: lower `app.crawl.db.qps` and `app.crawl.db.timeout-seconds`,
and lower `app.crawl.db.circuit-breaker-threshold`.
