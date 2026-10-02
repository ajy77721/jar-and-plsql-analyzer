# Storage Modes & Authentication — the complete guide

Everything this application persists goes through one `ArtifactStore` interface. A single property,
**`app.storage.mode`**, selects which backend backs it — and every other layer (the per-user SSO
routing, the write-behind uploader, the disk/heap caches, the entity-ingest pipeline) coheres around
that one choice. You do **not** have to set per-layer properties to make a mode work; the optional
overrides below are refinements, never requirements.

> Code anchors: mode selection is `analyzer-storage/.../StorageConfig.java` and
> `analyzer-storage/.../StorageFactory.java`; the per-user S3 routing is
> `analyzer-storage/.../sso/SsoStoreSwapper.java` + `SessionRoutingArtifactStore`; the auth gate is
> `unified-web/.../auth/ApiKeyAuthInterceptor.java`; the startup requirements block is
> `unified-web/.../config/StorageModeBootChecklist.java`.

---

## 1. The mode matrix

| | `local` | `s3` | `mongodb` | `hybrid` |
|---|---|---|---|---|
| **Where the bytes go** | files under `{app.data-dir.base}` | `s3://{bucket}/{prefix}/…` | Mongo manifest doc (small) + GridFS (large) | small → Mongo, large → S3 |
| **Required properties** | *(none)* | `app.storage.s3.bucket` | `app.storage.mongodb.uri`, `.database` | `app.storage.s3.bucket`, `app.storage.mongodb.uri`, `.database` |
| **Recommended** | — | `.region`, `app.storage.sso.region`, `app.storage.sso.start-url` | — | same as `s3` |
| **Per-user / shared** | one shared store | **per-user** (each SSO session's own bucket) | one shared store | Mongo shared, **S3 half per-user** |
| **Auth (`app.auth.required` auto)** | `false` | `true` | `false` | `true` |
| **Write-behind** | n/a | optional (`app.storage.s3.write-behind=true`) | n/a | ignored (small writes already local to Mongo) |
| **GridFS split** | n/a | n/a | `app.storage.mongodb.inline-threshold` (default 1 MiB, max 8 MiB) | same, on the Mongo half |
| **S3-vs-Mongo split** | n/a | n/a | n/a | `app.storage.hybrid.big-threshold` (default 1 MiB) |
| **`am_*` entity ingest** | works (needs a Mongo connection) | works (needs a Mongo connection) | works (reuses the Mongo connection) | works (reuses the Mongo connection) |
| **`repository=mongo`** | independent toggle | independent toggle | independent toggle | independent toggle |
| **Disk + heap cache** | on | on | on | on |

The same layout, key names and artifact contents are used in every mode:
`data/jar/<id>/analysis.json` is `s3://<bucket>/data/jar/<id>/analysis.json` in `s3`, and the document
`_id: "data/jar/<id>/analysis.json"` in Mongo. `ArtifactKey.s3Key(prefix)` produces that one string
for all of them, so flipping modes needs **no key rewriting** — rollback is a property change, not a
restore (`StorageMode` Javadoc, PLAN-09 §5).

### Per-feature overrides (optional)

`app.storage.parser.mode` and `app.storage.jar.mode` each default to `app.storage.mode`. They let
one feature (the PL/SQL parser, or the JAR/WAR analyzer) use a different backend without touching the
other — e.g. `app.storage.mode=s3` with `app.storage.parser.mode=mongodb`. When they are unset, one
property drives both features, which is the intended default.

The layer stack built for each feature is identical; only the mode differs.

---

## 2. What happens in each mode

### `local` (default)

- `LocalArtifactStore` rooted at `{app.data-dir.base}` (default `data`).
- **No** `SessionRoutingArtifactStore` is created, **no** store builder is registered with
  `SsoStoreSwapper` — there is no bucket and no per-user partition. This is the "nothing user-scoped
  to protect" case that turns authentication off.
- `config/`, `logs/`, `prompts/` always live on local disk in **every** mode; only the analysis
  artifacts follow the mode.

Setup:
```bash
java -jar unified-web/target/unified-web-1.0.0-SNAPSHOT.jar \
  --server.port=8083 --app.data-dir=data
# app.auth.required auto-derives to false — no login, no cookie, no AWS.
```

### `s3`

- `S3ArtifactStore` at `s3://{bucket}/{prefix}` (prefix default `data`; blank = bucket root).
- Credentials are **never** in properties — the AWS default provider chain, and at request time the
  calling SSO session's role via `SsoCredentialsResolver`.
- Wrapped in `SessionRoutingArtifactStore`: once a session connects a bucket through the AWS SSO
  device flow, that session's reads/writes go to *its* bucket signed with *its* credentials; a
  request with no session-bound state uses the startup store (default bucket + default chain).
- `app.storage.s3.write-behind=true` buffers small writes on the heap and uploads them on a bounded
  8-thread pool (`WriteBehindStore`), so a few-hundred-object analysis stops paying one serialized
  remote round trip per artifact. S3 only.
- `app.storage.s3.endpoint` (blank = real S3) points at MinIO/LocalStack with path-style addressing.
- **Auth required** (auto). The page-lock applies; `/api/**` returns 401 without a session.

Setup:
```bash
java -jar unified-web/target/unified-web-1.0.0-SNAPSHOT.jar \
  --app.storage.mode=s3 \
  --app.storage.s3.bucket=my-bucket \
  --app.storage.s3.region=ap-south-1 \
  --app.storage.s3.prefix=data \
  --app.storage.s3.write-behind=true \
  --app.storage.sso.region=us-east-1 \
  --app.storage.sso.start-url='https://d-xxxxxxxx.awsapps.com/start/#'
```

### `mongodb`

- `MongoArtifactStore`: artifacts at/below `app.storage.mongodb.inline-threshold` live inline in the
  manifest collection; larger ones go to **GridFS**. Default 1 MiB; hard ceiling 8 MiB (below the
  12 MB entity-document soft limit, so the two guards cannot disagree). `0` means "use the default".
- One shared `MongoConnection` pool, also reused by `repository=mongo` and (when enabled) by the
  entity-ingest pipeline — a driver client is a pool of sockets, so it is never opened twice.
- **No** routing store, **no** SSO requirement — a shared store with no per-user partition.
- An unreachable server fails the context at boot (`MongoConnection.open` pings first), naming the
  property to fix.

Setup:
```bash
java -jar unified-web/target/unified-web-1.0.0-SNAPSHOT.jar \
  --app.storage.mode=mongodb \
  --app.storage.mongodb.uri='mongodb://localhost:27017' \
  --app.storage.mongodb.database=analyzer \
  --app.storage.mongodb.inline-threshold=0
# app.auth.required auto-derives to false.
```

### `hybrid`

- `HybridArtifactStore(mongoStore, s3Store, bigThreshold)`: an artifact at/above
  `app.storage.hybrid.big-threshold` (default 1 MiB) goes straight to S3; smaller ones go to the
  Mongo half, which may still promote an individual artifact to GridFS internally per
  `inline-threshold` — the two thresholds are independent. Reads try Mongo first and fall back to S3.
- `app.storage.hybrid.s3-always-paths=sources` (comma-separated path segments) forces keys carrying
  those segments to S3 regardless of size — e.g. raw SQL sources belong in S3 while the small detail
  artifacts stay in Mongo.
- The S3 half is per-user via `SessionRoutingArtifactStore`; the Mongo half is shared.
- `app.storage.s3.write-behind` is **ignored** in hybrid (its small writes already go to Mongo).
- **Auth required** (auto). Both a bucket and a Mongo connection are required.

Setup:
```bash
java -jar unified-web/target/unified-web-1.0.0-SNAPSHOT.jar \
  --app.storage.mode=hybrid \
  --app.storage.s3.bucket=my-bucket --app.storage.s3.region=ap-south-1 \
  --app.storage.mongodb.uri='mongodb://localhost:27017' --app.storage.mongodb.database=analyzer \
  --app.storage.hybrid.big-threshold=1048576 \
  --app.storage.hybrid.s3-always-paths=sources
```

### The wrapper stack (outer → inner)

```
CachingArtifactStore            (disk read-through + heap micro-cache; every mode)
  └─ SessionRoutingArtifactStore (s3 / hybrid only — per-user bucket routing)
       └─ S3AlwaysPathsStore     (hybrid only, when s3-always-paths is set)
            └─ WriteBehindStore  (s3 only, when write-behind=true)
                 └─ CredentialGuardStore   (every mode — refuses to write secrets)
                      └─ LocalArtifactStore | S3ArtifactStore | MongoArtifactStore | HybridArtifactStore
```

### Caches (every mode)

- `app.cache.disk.enabled=true`, `app.cache.disk.max-mb=2048`: read-through cache under
  `{app.data-dir}/cache/store/`, LRU-by-last-access, 24 h revalidation. Writes/deletes go to the
  store first, then invalidate the cache.
- `app.cache.heap.max-mb=64`: a small in-heap LRU above the disk layer, only for read artifacts
  ≤ 256 KB each. `0` disables it. It never holds a large artifact.

---

## 3. Authentication per mode

Authentication exists only to protect **user-scoped** data. The decision is resolved once at startup
by `ApiKeyAuthInterceptor.resolveAuthRequired`:

1. **`app.auth.required=true|false`** — when explicitly set, it wins, whatever the mode.
2. **`app.auth.sso.required=true|false`** (legacy escape hatch) — honoured when
   `app.auth.required` is unset, so an existing local-dev deployment that turned the old SSO gate
   off is not silently re-locked.
3. **Auto-derived from the effective storage modes** — required iff the JAR analyzer **or** the
   PL/SQL parser resolves to `s3`/`hybrid` (honouring the per-feature overrides). `local`/`mongodb`
   for both features → not required.

| Effective modes (jar / parser) | `app.auth.required` (auto) | Behaviour |
|---|---|---|
| local / local | `false` | Zero verification: no SSO, no S3, no profile. Every `/api/**` GET/POST is 200 with no cookie. |
| mongodb / mongodb | `false` | Same — one shared store, nothing per-user. |
| s3 / s3 | `true` | SSO page-lock: `/api/**` 401 without a session, pages redirect to `/?login=1`. |
| hybrid / hybrid | `true` | Same. |
| s3 / local (or any mix with one S3/hybrid feature) | `true` | The S3-backed feature has per-user data, so a session is required for the app. |
| any / any, explicit `app.auth.required` | as set | The explicit value always wins (e.g. force auth off in s3, or on in local). |

**When auth is not required**, the interceptor is a passthrough — no 401 page-lock, no redirect. The
AWS SSO endpoints (`/api/auth/aws/**`) remain reachable, so an **optional** login is still possible.
But in a pure `local`/`mongodb` deployment there is no per-user store for a login to bind to: a
connected session's stores are never built (no builder is registered), so nothing changes. If you
later switch the same data directory to `s3`/`hybrid`, users must **re-connect** their bucket — a
mode switch is a re-connection, not a migration.

**Fail-open by design:** if the `SsoSessionRegistry` bean is absent (SSO support not compiled in),
the gate allows the request rather than locking every deployment out.

---

## 4. Entity ingest (`am_*`) and `repository=mongo`

These are **two parallel, independent** Mongo systems. Do not conflate them.

### `app.storage.entity-ingest.*` — the `am_*` entity schema

An orthogonal toggle (`app.storage.entity-ingest.enabled=false` by default), **not** a fifth storage
mode. When on, each completed analysis is also ingested in-process into the normalised `am_*`
collections (`analysis-mongo-ingest/.../schema/Collections.java`, installed by
`Catalog.all()` — 42 collections: `am_run`, `am_proc_node`, `am_java_class`, `am_call_edge`,
`am_db_object`, `am_facet_count`, …).

- It reads the just-written artifacts through the **feature's own** store — `parserArtifactStore`
  after a PL/SQL analyze, `jarArtifactStore` after a JAR analyze — so it works with **any** base
  mode. (Using the parser store for the JAR ingest was a bug when the two features were pointed at
  different backends; fixed.)
- It needs a **Mongo connection** for the `am_*` collections, resolved in this order:
  1. `app.storage.entity-ingest.mongo.uri` + `.database`, if set;
  2. the **shared** `MongoConnection` bean, when one is configured (parser/jar mode `mongodb`/`hybrid`,
     or `app.storage.repository=mongo`) — reusing that pool rather than opening a second one;
  3. the parser feature's own `app.storage.mongodb.*` settings, when `app.storage.parser.mode` is
     `mongodb`/`hybrid`.
- Enabling it with no resolvable connection **fails at boot**, naming the property to fix.
- Best-effort after that: a failed ingest is logged loudly but never fails the analysis job.
- `app.storage.entity-read.enabled=true` is the **read** side: detail pages are served from the
  `am_*`/entity layer instead of by scanning artifacts. Off by default; works in every base mode
  once the entity collections are populated.

### `app.storage.repository` — the query repository

`artifact-scan` (default) answers list/summary queries by scanning the JSON artifacts through the
store — works in every mode. `mongo` answers from Mongo indexes and uses its own collections:
`analysis_run`, `analysis_facet`, `analysis_row` (`MongoAnalysisRepository`) and `entity_run`,
`flow_node`, `flow_edge`, `table_usage` (`MongoPlsqlEntityIndex`). These are a different shape and
lifecycle from `am_*`, which is exactly why the `am_` prefix exists — two writers with incompatible
schemas on one collection is a corruption path, not a naming preference.

---

## 5. The startup requirements block

At boot the app prints one block (from `StorageModeBootChecklist`) listing each requirement for the
active mode with `✓` or `MISSING`:

```
[storage] mode check — active mode(s): [S3]; auth required=true (auto from s3/hybrid storage mode)
[storage]   requires: app.storage.s3.bucket=my-bucket ✓
[storage]   requires: app.storage.s3.region=ap-south-1 ✓
[storage]   recommends: app.storage.sso.start-url=(unset) MISSING — set app.storage.sso.start-url …
[storage]   optional: app.storage.s3.write-behind=true ✓
[storage]   optional: app.cache.disk.enabled=true, app.cache.disk.max-mb=2048, app.cache.heap.max-mb=64 ✓
[storage] OK, but a recommended property is missing (see WARN above)
```

- A missing **REQUIRED** property is a boot failure (the store beans also fail first, with the same
  property named).
- A missing **RECOMMENDED** property (region, SSO start-url/region) is a loud WARN — the app starts,
  but the affected feature degrades (e.g. no prefilled login portal URL).
- Everything else is printed for confirmation.

---

## 6. Decision table — "I want X → set Y"

| I want… | Set |
|---|---|
| Run entirely locally, no AWS, no login | `app.storage.mode=local` (auth auto-off) |
| Store artifacts in Mongo, no S3 | `app.storage.mode=mongodb` + `app.storage.mongodb.uri`, `.database` |
| Store artifacts in S3, per-user buckets | `app.storage.mode=s3` + `app.storage.s3.bucket`, `.region`; SSO auto-required |
| Small artifacts in Mongo, large in S3 | `app.storage.mode=hybrid` + bucket + Mongo uri/database |
| Keep raw SQL sources in S3 in hybrid | `app.storage.hybrid.s3-always-paths=sources` |
| Raise the GridFS inline limit | `app.storage.mongodb.inline-threshold=<bytes ≤ 8388608>` |
| Change the Mongo-vs-S3 size split | `app.storage.hybrid.big-threshold=<bytes>` |
| Speed up many small S3 writes | `app.storage.s3.write-behind=true` (s3 only) |
| Point at MinIO/LocalStack | `app.storage.s3.endpoint=http://host:9000` |
| Make the parser use a different backend than the JAR analyzer | `app.storage.parser.mode=…` / `app.storage.jar.mode=…` |
| Answer list queries from Mongo indexes | `app.storage.repository=mongo` + `app.storage.mongodb.uri`, `.database` |
| Populate the `am_*` entity collections | `app.storage.entity-ingest.enabled=true` (+ a Mongo connection) |
| Disable auth even in s3 (not for production) | `app.auth.required=false` |
| Require auth even in local | `app.auth.required=true` |
| Cap the disk / heap cache | `app.cache.disk.max-mb=…`, `app.cache.heap.max-mb=…` |
| Relocate all mutable state | `app.data-dir=/srv/analyzer/data` |

---

## 7. CLI / non-Spring entry points

Entry points outside Spring (the ingest CLIs) resolve the mode through
`StorageFactory.settingsFor`, which accepts the same values as system properties or environment
variables:

| Property | Environment variable |
|---|---|
| `app.storage.mode` | `APP_STORAGE_MODE` |
| `app.storage.s3.bucket` | `APP_STORAGE_S3_BUCKET` |
| `app.storage.s3.prefix` | `APP_STORAGE_S3_PREFIX` |
| `app.storage.s3.region` | `APP_STORAGE_S3_REGION` |
| `app.storage.s3.endpoint` | `APP_STORAGE_S3_ENDPOINT` |
| `app.storage.mongodb.uri` | `APP_STORAGE_MONGODB_URI` |
| `app.storage.mongodb.database` | `APP_STORAGE_MONGODB_DATABASE` |
| `app.storage.mongodb.prefix` | `APP_STORAGE_MONGODB_PREFIX` |
| `app.storage.mongodb.inline-threshold` | `APP_STORAGE_MONGODB_INLINE_THRESHOLD` |
| `app.storage.hybrid.big-threshold` | `APP_STORAGE_HYBRID_BIG_THRESHOLD` |

A system property wins over the environment variable, mirroring Spring's own precedence.

---

## 8. Migrating between modes

Because every backend reuses the same key string and the same prefix default (`data`), switching
`app.storage.mode` needs no key rewriting. Copy the artifacts to the new backend (or leave the old
one in place) and restart. If you move from a pure `local`/`mongodb` deployment to `s3`/`hybrid`,
authentication turns on automatically — users then sign in and **connect** a bucket; a mode switch is
a re-connection, not a migration.
