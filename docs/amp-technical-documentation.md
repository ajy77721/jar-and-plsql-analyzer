# AMP Analyzer — Technical Documentation

**Version:** 1.1 · **Date:** 2026-09-30 · **Repo:** `github.com/Modernization-Factory/amp-analyzer`
**Staging:** `https://amp-analyzer.amp-delivery.staging.corp.mongodb.com` · **Prod:** `https://amp-analyzer.amp-delivery.prod.corp.mongodb.com`

---

# Page 1 — Briefing, Project Structure & Terminology

## 1.1 What the platform does

AMP Analyzer statically analyzes legacy codebases and serves the complete analysis as searchable, precomputed views:

- **PL/SQL side:** connects to a live database (or takes source files) and analyzes every stored procedure/package — call chains, table operations, joins (with exact conditions), cursors, sequences, dynamic SQL, triggers, views, complexity — across 5 dialects (Oracle, SQL Server, MySQL, Db2, PostgreSQL).
- **JAR side:** accepts compiled Java JAR/WAR binaries (up to 200+ MB) and produces class inventories, endpoint call trees, decompiled source, and per-endpoint breakdowns.

Everything an analyst used to build by hand — the mental map of "what touches what" — is captured once at analysis time and then served instantly through 400+ REST APIs.

## 1.2 Project structure (Maven multi-module, Java 17 / Spring Boot 3.2)

```
amp-analyzer/
├── analyzer-storage/        Storage layer: artifact stores, routing, caching, security
├── plsql-parser/            PL/SQL parsers (5 dialects), crawler, chunked writer
├── plsql-config/            DB connection configs, TLS/wallet handling
├── plsql-analyzer-core/     PL/SQL analysis services (full + fast modes)
├── dialect-common/          Shared dialect abstractions
├── mysql-parser/  sqlserver-parser/  db2-parser/  postgres-parser/   Per-dialect parsers
├── jar-analyzer-core/       Java bytecode analysis, decompiler, endpoint writer
├── analysis-mongo-ingest/   Optional MongoDB entity ingest
├── unified-web/             The web application: REST APIs, auth, queue, admin portal
├── flow-poc/                Proof-of-concept module
├── scripts/                 Ops + verification tooling (endpoint sweep, log ops)
├── docs/                    Runbooks, plans, this document
├── Dockerfile               Multi-stage image (Maven build → JRE 17 runtime)
└── .drone.yml               CI/CD: build → GHCR → Kanopy deploy
```

## 1.3 Working terminology

| Term | Meaning |
|---|---|
| **Artifact** | Any persisted piece of analysis data (a JSON file, a stored jar copy, a sidecar), addressed by an `ArtifactKey` (a path-like key under the data root). |
| **ArtifactStore** | The storage abstraction: `LocalArtifactStore` (filesystem), `S3ArtifactStore`, `MongoArtifactStore`, `HybridArtifactStore` (small→Mongo, large→S3). Selected by `app.storage.mode`. |
| **Analysis identity name** | The unique name of one analysis. PL/SQL: `<DBTYPE>~<PROJECT>~<ENV>~<OBJECT>` (e.g. `ORACLE~ORACLEDEMO~LOCAL~ERP_USP_RUN_CYCLE`). JAR: the normalized jar name. |
| **Node** | One analyzed procedure (PL/SQL) — stored as one file `api/nodes/<nodeId>.json`. |
| **Sidecar / view** | A precomputed, ready-to-serve response fragment stored under `views/` (e.g. `views/sequences.json`). Trusted only while its mtime ≥ the analysis index's mtime; deleted wholesale by correction paths (`invalidate`). |
| **Write-behind** | Small writes buffer in memory and flush to S3 asynchronously (with the writer's credentials carried per entry); reads force a barrier so no reader sees stale data. |
| **Disk cache** | A 2 GB LRU mirror of store reads on the pod disk (24h revalidation) — warm reads never touch S3. |
| **Session routing** | The store layer resolves each request (and each background task) to the calling user's own store + AWS credentials. |
| **Byte-offset index** | A tiny companion file (`*.offsets.json`) recording each row's byte position inside a large document, so a page is served by seek, not by parse. |
| **Layout manifest** | `manifest.json` — one small file per analysis listing every artifact with sizes/counts; the routing record for all APIs. |
| **Queue job** | Long work (uploads, analyses, conversions) runs on a single ordered worker; the submitter's context is captured at submit and installed on the worker. |

## 1.4 Input / Output at a glance

| | |
|---|---|
| **Inputs** | Live DB connection (JDBC, 5 dialects, TLS/wallet supported) → full PL/SQL estate; JAR/WAR binary upload; analysis ZIP re-import |
| **Outputs** | 400+ REST endpoints serving precomputed views (sequences, cursors, dynamic SQL, joins, call trees, complexity, dependency maps, per-endpoint trees, decompiled source); S3-stored artifacts; admin audit |
| **Auth** | AWS SSO device flow (per-user, per-account), page-locked UI, per-user data isolation |
| **Performance** | p50 = 5 ms across 769 tested API combinations; static analysis of 2 lakh lines ≈ 10-15 min; batch validation 3-5 days → 2-3 hours |

## 1.5 Deployment & operations

- **CI/CD:** push to `main` → Docker build → GHCR → Helm deploy to Kanopy **staging**; version tags → **prod**.
- **Environments:** `environments/staging.yml` / `prod.yml` (mesh-enabled namespace `amp-delivery`, single pod, 10-12 Gi memory).
- **Ops:** `scripts/amp-ops.sh` (logs / follow / health / restart / clear-cache / clean-logs, `--prod` flag). Logs self-clean (10 MB roll, keep 1).
- **Quality gates:** 3,452-test reactor suite; golden byte-equivalence tests for every sidecar; adversarial data-integrity verification; 769-combination endpoint sweep (zero 5xx).

---

# Page 2 — PL/SQL Parse Pipeline (detail)

## 2.1 Input

| Source | Mechanism |
|---|---|
| Live database | User saves a connection (per-user, AES-encrypted, on S3) with TLS/wallet/cert support; a probe verifies read/upload/write before use |
| Files | PL/SQL source files / analysis ZIPs (auto-detected) |

The user submits an analysis for a project/environment/object scope (e.g. `ORACLE / ORACLEDEMO / LOCAL / ERP_USP_RUN_CYCLE`).

## 2.2 Processing pipeline (the queue worker executes the job)

1. **Crawl** (`DependencyCrawler`, plsql-parser): pooled JDBC reads every procedure/package/view/trigger source in scope. A bounded prefetch pool warms sources ahead of the parser. Typical: **minutes** for a full estate.
2. **Parse** (`FastAnalysisService`/`AnalysisService`, plsql-analyzer-core): per-dialect parsers turn source into node documents — one **node per procedure** (`<nodeId>`), each recording: statements, cursors (with per-cursor operations), sequences with access details (operation + line number), dynamic SQL (type, line, bound variables), joins (tables + exact ON conditions), table references, trigger bindings, complexity inputs (LOC, statement counts, nesting).
3. **Call graph** (`ChunkedFlowWriter` + call-graph services): builds the edge list across the whole estate — who calls whom, with line numbers and call counts.
4. **Chunked write** (`ChunkedFlowWriter`): writes the layout atomically (see 2.3). Every view a request will ever need is computed **in the same pass** — nothing is computed per request later.
5. **Sidecars** (`AnalysisViewWriter`): the 10 precomputed views (header, sequences, cursors, dynamic-sql, joins-summary, table-joins, table-columns, complexity grid, summary-stats, per-entry-point call trees + callers) + per-table split files + search facets.
6. **Manifest** (`LayoutManifest`): `api/manifest.json` — artifact inventory + counts, written **last** (after `api/index.json`, which is the staleness anchor).
7. **Warm-up** (`SidecarWarmUpService`): boot/periodic/login-triggered sweeps backfill any sidecar an older analysis lacks — old data upgrades itself lazily on first access.

## 2.3 Data layout (S3/`data` root)

```
plsql-parse/<DBTYPE>/<PROJECT>/<ENV>/<OBJECT>/
├── api/
│   ├── index.json                  Analysis summary + nodes[] (staleness anchor, written last before manifest)
│   ├── manifest.json               Artifact inventory + counts (routing record)
│   ├── nodes/<nodeId>.json         One file per procedure (~<370 KB each)
│   ├── call_graph.json             Deduplicated edges
│   ├── node_id_map.json            shortId → fullId
│   ├── tables/index.json           All tables + usedBy + operations + column detail
│   ├── statements/index.json       One row per statement
│   │   └── index.offsets.json      Byte-offset per row → page-by-seek
│   └── resolver/*.json             Schema-resolution outputs
├── tables.json                     Raw crawler table map
├── sources/<file>.sql              One artifact per source file (always on S3)
├── chunks/, edges/                 Crawler chunk outputs
└── views/                          THE FAST PATH — precomputed, byte-identical to live computation:
    ├── header.json, sequences.json, cursors.json, dynamic-sql.json, joins-summary.json,
    ├── table-joins.json + table-joins/<TABLE>.json, table-columns.json + table-columns/<TABLE>.json,
    ├── complexity.json (+ grid variants + hashed custom variants, LRU 8),
    ├── summary-stats.json, search.json, callers.json,
    └── call-tree/<nodeId>.json     Precomputed call tree per entry point (cap 40)
```

**Serving rule:** every read endpoint checks its sidecar first (fresh = mtime ≥ index mtime) → serve bytes; stale/missing → compute the legacy way once, write the sidecar (lazy backfill) → every later request is a hit. This is why old analyses get faster automatically on first touch.

## 2.4 API surface (family → cost)

| Family (examples) | Path | Serving |
|---|---|---|
| Facets | `/analyses/{n}/sequences|cursors|dynamic-sql|joins` | sidecar → **2 ms** |
| Call trees | `/analyses/{n}/call-tree/{id}`, `.../callers` | sidecar → **2-5 ms** |
| Search | `/analyses/{n}/global-search?q=` | search sidecar + in-memory filter → **7-19 ms** |
| Complexity | `/analyses/{n}/complexity/scores` (default/grid/custom) | sidecar / hashed variant cache |
| Tables | `/analyses/{n}/table-joins?table=`, `/table-columns?table=` | per-table split file → **<5 ms** |
| Stats | `/analyses/{n}/summary/stats` | sidecar |
| Listings | `/api/parser/analyses` | shallow listing + header sidecars |
| Paging | `/statements` pages | offset-index seek (ranged read) |

## 2.5 Measured results (live, real bucket data)

- Facet views: 71.5 s → **2 ms** · search: 35-41 s → **7-19 ms** · callers: full graph parse → **4.6 ms**
- Static analysis of 2+ lakh lines: **10-15 minutes**; batch validation: **3-5 days → 2-3 hours**
- Verification: golden byte-equivalence per sidecar + full-data adversarial check (0 mismatches across 5 analyses, ~2,000+ rows traced)

---

# Page 3 — JAR/WAR Analysis Pipeline (detail)

## 3.1 Input

A JAR/WAR binary uploaded via `POST /api/jar/jars` (or analyze-local path for files already on the host). Accepted up to 200+ MB; Spring Boot fat JARs (BOOT-INF/lib) handled. Optional per-jar config + base-package filter.

## 3.2 Processing pipeline (queue job, 5 steps)

1. **[1/5] Parse bytecode** (`JarParserService`): extracts every class's structure (methods, fields, references, annotations) into a streaming JSONL temp file — **one record per class** (a 105 MB / 11,264-class jar yields a ~330 MB class index; 200 MB jars process within the pod's limits).
2. **[2/5] Config extraction** — bundled resources, config files, dependency maps (bundled-jars, jar-dep-map, external links).
3. **[3/5] Call-graph index** — method-level invocation graph; **373 endpoints** discovered on the reference jar.
4. **[4/5] Enriched call trees** — per-endpoint flattened DAG trees (bounded expansion; overflow served as expandable fragments).
5. **[5/5] Persist + output** (`PersistenceService`, `EndpointOutputWriter`, `SourceEnrichmentService`):
   - `stored.jar` → S3 multipart (the durable copy)
   - `analysis.json` — the class + endpoint monolith, streamed from the JSONL in one pass (atomic move; no second pass over the document)
   - **per-endpoint folders** (see 3.3)
   - **source enrichment** — CFR decompiles each referenced class ONCE per analysis (local pinned file; decompile cache 48h, keyed by jar identity)
   - jar-side view sidecars: `views/summary-header.json`, `views/collections-count.json`, `_summary.json(+.gz)`, `_class-tree.json`, `_header.json`

## 3.3 Data layout

```
jar/<normalized-jar-name>/
├── stored.jar                      Uploaded binary (durable copy)
├── analysis.json                   Classes[] + endpoints[] with inline call trees (up to ~912 MB)
├── _header.json                    Listing sidecar (dashboard)
├── _summary.json (+ .gz)           Capped summary aggregates (beans 100, ext-calls 100, flows 200…)
├── _class-tree.json                Projected class-tree cache (paged views)
├── views/summary-header.json       /summary/header sidecar · views/collections-count.json
├── endpoints/<NNN_Controller.method>/
│   ├── endpoint.json               Endpoint metadata + aggregates (served verbatim)
│   ├── call-tree.json              Flat DAG call tree (compact form)
│   ├── nodes_*.json                Flattened node files
│   └── fragments/<id>.json         "Expand N more" overflow fragments
├── decompiled/<className>.java     CFR output cache (TTL 48h)
└── connections.json, domain-config.json, corrections/, chat/, resources/, improvement/
```

## 3.4 API surface (family → serving)

| Family | Path | Serving |
|---|---|---|
| Listing | `GET /api/jar/jars` | shallow walk + `_header.json` per jar |
| Header | `/summary/header` | sidecar + ETag/304 → bytes |
| Summary | `/summary`, `/summary/endpoints?page=` | `_summary.json` cache; pages served from the capped cache |
| Classes | `/classes/tree?page=`, `/classes/by-index/{i}` | class-tree cache / streaming pass (offset-index adoption planned — Phase 2) |
| Endpoints | `/endpoints/.../call-tree`, `/nodes` | per-endpoint sidecar files (100 KB-class) |
| Search | `/global-search?q=` | class-tree rows + endpoint scan |
| Source | `/decompiled/{class}` | CFR cache (48h TTL) |

## 3.5 Reliability specifics (JAR side)

- **Upload durability:** the stored jar persists via multipart with the uploader's credentials carried to the flush threads (verified live: 105 MB → 7 parts, 0 failed uploads).
- **Decompile correctness:** enrichment reads the jar by its stored identity (one local read per analysis — not per class); the containment guard only accepts app-owned temp files.
- **Large-artifact behavior:** monolith reads are streamed (never heap-loaded); eviction of a huge document cannot lose data — only re-fetch cost, which the sidecar layer avoids for all hot paths.

## 3.6 Phase 2 (planned, approved plan in `docs/performance-indexing-plan.md`)

Byte-offset indexes for `analysis.json`'s `classes[]`/`endpoints[]` built inside the streaming write (one class = one ~100 KB seek instead of a full-document parse), save-time summary/class-tree generation, a per-jar manifest, and class-tree chunking — removing the remaining first-hit full-document passes.

---

*Maintained by the AMP Analyzer team · Quality gates: 3,452-test reactor + 769-combination sweep + adversarial data-integrity verification · Last updated 2026-09-30.*
