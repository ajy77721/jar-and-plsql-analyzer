# AMP Analyzer — MASTER TASK LIST (all audits consolidated)

**Updated: 2026-10-02 · Rule: nothing publishes to main/staging/prod without local testing + user approval.**

---

## ✅ COMPLETED (all audits' fixed items)

| # | Task | Source audit | Commit |
|---|---|---|---|
| 1 | Thread-context fixes (13+ sites, per-user, multi-user safe) | context audit | 0179a0c…64b34f9, 407a169 |
| 2 | Security: redact AWS credentials from API logs | context verifier | 38334b8 |
| 3 | Fail-loud guards: empty crawl (PL/SQL) + 0-class JAR — proven live | crawl incident | 3bfec96 |
| 4 | Auto-reconnect (redeploy-proof sessions) + per-user isolation | SSO work | 93aebf2 |
| 5 | Boot-wiring fix (@Autowired) | local testing | d386f4a |
| 6 | Storage routing: no silent default-store/credential fallbacks (F1-F5) | storage audit | 7da7442 |
| 7 | Web: connect-guard, per-user cache keys, job ownership, SSE fix | web audit | 6bf85a9 |
| 8 | Modules: prefetch context, catch-close writes, prompt-static removal, source-cache race | module audit | 378f78c, 64b34f9, 167097f |
| 9 | Performance Phase 1: PL/SQL facet sidecars (71.5s→2ms, search→7-19ms) | perf plan | fe97830, 365944e |
| 10 | Performance Phase 2: JAR save-time precompute + offsets + chunks + manifest | perf plan | e841c23 |
| 11 | Heap micro-cache + gz sidecar twins + manifest listings | speed ask | 7c91b54 |
| 12 | Crawl parallelism (~3-5×) + persistent source cache | crawl ask | 070f459 |
| 13 | Estate facets: views/MVs + chains, triggers + chains, globals — grammar-first | user ask | e115098 |
| 14 | Estate detail enrichment (cross-links, willFire inverse map, per-proc footprints) | user ask | 030e77c, eccf18f |
| 15 | JAR→DB call detection (callable/stmt-function/spring-jdbc/jpa/mybatis) | user ask | b724962 |
| 15b | W1: overload SIGNATURE keys (Oracle type-signature nodes, FUNCTION/PROCEDURE split, Postgres oid-fetch, Db2 SPECIFICNAME, counted resolution fallbacks) | overload audit | 7c6add8 |
| 16 | UI screens: Estate tab (Views/Triggers/Globals + chains) + JAR Database Calls tab | user ask | 8836bbd |
| 17 | Regex→grammar Batch 1: dynamic-SQL recursive parse ×4 dialects + trigger bodies | regex audit | 2ab385f |
| 18 | False-pick fixes: globals noise (100%→0), SQLServer view base tables, DB2 phantom view + STATEMENT level, pseudo-table filter, trigger bodyCalls | false-pick audit | 78eca6e, 9f6fa1b |
| 19 | Log archiving disabled; 87 .bak files removed; docs (writeup + 3-page tech doc + DOCX) | user decisions | 5955d17, 3ed2d7b, 8242fa6, d27fbae |
| 20 | Clean slate: backup verified (3,438 files/445MB) → wipe → fresh analyses ×7 — 0 errors | user ask | live runs |

---

## 🔄 IN FLIGHT (agents working — land first)

| # | Task | Source audit |
|---|---|---|
| 21 | Regex Batch 2+3 finish: SQL Server visitor → grammar contexts | regex audit | **AC:** all 7 semantic audit-tests pass unmodified; every migrated pattern = grammar-primary with a counted fallback; full reactor green; byte-identical outputs on existing fixtures |
| 23 | W2+W3: per-analysis idempotency registry (never global) + DB-impact safety + 5 crawl-redundancy fixes | crawl audit | **AC (MET, 8f39ad5/7c6add8):** 2nd crawl skips unchanged objects; LAST_DDL change re-analyzes; partial runs re-analyze; user-B never skips on user-A's registry; QPS cap + timeouts + read-only guard + breaker tested; plsql-parser 1056 green |

---

## ⏳ PENDING — awaiting execution (in priority order)

| # | Task | Source audit | Priority |
|---|---|---|---|
| 24 | **Scheduled/infra entry points** (DBMS_SCHEDULER / SQL Agent / pg_cron / EVENTS): fetch strategies + a JOB unit type + SCHEDULES edges; stop suppressing as builtins | construct matrix | **AC:** a scheduled job appears as a node with a SCHEDULES edge to its target proc in EVERY dialect fixture; the suppression no longer hides it; facet/API visible; suites green |
| 25 | **Anonymous/DO blocks end-to-end** (Postgres plumbing + the Oracle splitter) | construct matrix | **AC:** a DO $$ block becomes an analyzable unit (Postgres); a DECLARE…BEGIN script section becomes a unit (Oracle); their table ops/calls captured; suites green |
| 26 | **Result-set-returning procs marker** | construct matrix | **AC:** a returnsResultSet/resultSets field on the routine model per dialect (SQLServer implicit SELECTs count, Db2 WITH RETURN, Oracle REF CURSOR OUT, Postgres refcursor/SETOF); visible in the cursors facet + UI; golden tests |
| 27 | **TVF/SETOF return shapes persisted** | construct matrix | **AC:** returnType/returnsColumns on SqlServer/Postgres routine models + through the multidb adapters into ParsedObject + visible in the facet |
| 28 | **Db2 modules** (ROUTINEMODULENAME → MODULE.SUB keying + module vars in globals) | construct matrix | **AC:** a module member keys as MODULE.SUB (like Oracle packages); module variables appear in the globals facet; fixtures prove it |
| 29 | **Nested-subprogram marking** | construct matrix | **AC:** nested=true/scopeDepth on SubprogramInfo; the procedures facet shows the nesting; existing ids unchanged for non-nested |
| 30 | **Regex deferred items** (MERGE/WITH promotion, qualified-call rule, CASE contexts, standalone synonym, JPQL grammar) | regex audit | **AC:** each migrated item: grammar-primary + counted fallback + the audit-test suites green; JPQL: either a small grammar or a documented last-resort decision |
| 31 | **Verify P4** (credentials for ALL queue-job types) | storage audit | **AC:** a test/inspection proving every QueueJob.Type's captured context carries credentials (or the job needs none — documented per type) |
| 32 | **ChatController persistence** | web audit | **AC:** chat writes route through the user's context (no default-store degradation) + a test |
| 33 | **Case-sensitivity normalization** (identity lookup) | false-pick session | **AC:** OracleDemo and ORACLEDEMO resolve to the SAME analysis on every endpoint (stats + facets + details) + tests |
| 34 | **Bucket-garbage check/cleanup** (~152 MB doubled-prefix) | housekeeping | **AC:** the prefix verified (count+bytes), backed up, deleted; the bucket listing clean |
| 35 | **Re-verification round** (fresh analyses all 6 + the false-pick re-audit) | false-pick audit | **AC:** every facet re-audited on the fresh data: 0 false picks, the fixed items verified fixed (globals real-only, sqlserver view edges, db2 level, no pseudo-tables, bodyCalls present) |
| 36 | **A JAR with REAL DB calls** (live db-calls proof) | false-pick audit | **AC:** a fixture jar (or real app jar) with each call shape → the API returns them with correct kind/class/method; verified in the UI |
| 37 | **A multi-level view-chain fixture** (view→view→view) | false-pick audit | **AC:** a 3-level chain rendered fully in the UI + every hop verified real |
| 38 | **Overload follow-ups** (named/default-arg resolution, per-package overload counts) | overload audit | **AC:** a named-argument call resolves to the right overload (or is counted-fallback); the index shows per-package overload counts |
| 39 | **Token UX** (iam-import expiry warning) | test session | **AC:** the iam-connect response carries an expires-in note + the docs mention the 1h wall |
| 40 | **Staging final deploy → USER REVIEW → prod v1.1.0** | — | **AC:** staging on the final build (green Drone); the user's checklist passes; the tag pushed; prod Ready; the health/log check clean |
| 43 | **MODES: single-assignment mode switch** — `app.storage.mode=X` alone must internally maintain ALL layers (cache, write-behind, routing, ingest wiring) coherently — no per-layer property hunting; per-feature overrides (`app.storage.parser.mode`/jar) remain OPTIONAL refinements. On startup the app VALIDATES the mode and PRINTS the checklist of what it needs to run: "**mode=s3 requires: bucket, region, SSO start-url — missing: …**" (a startup requirements report). **Acceptance criteria:** (a) one property switch = a fully coherent mode; (b) missing requirements are listed at boot in one clear line each; (c) docs/modes.md + README: per mode — required details, optional properties, what happens to the data, layer behavior, an example setup; (d) the am*/amp_* ingest collections verified/covered | user ask | 🔄 inventory agent in flight |
| 44 | **MODES: the AUTH FLAG for every mode** — `app.auth.required` (enable/disable SSO authentication) works for ALL modes, not just s3: s3 defaults true ("s3 it's fine"); local/mongo default false; an explicit value ALWAYS wins. **Acceptance criteria:** (a) local + auth off → zero verification (no SSO/S3/profile) — all APIs open; (b) local + auth ON → SSO still works (optional per-user); (c) s3 + auth off → allowed (shared/anon access — documented warning at boot); (d) s3 default → SSO page-lock; (e) tests for all four combos | user ask | 🔄 in flight |
| 45 | **MODES: the INGEST mode documented + supported everywhere** — the analysis-mongo-ingest pipeline (entities → Mongo collections — the existing am*/amp_* collections): its trigger, collection naming, properties (enable/uri/db/batch), and which storage modes it works with; fix any mode combination that should work but doesn't (e.g. local analysis + mongo ingest); its requirements appear in the startup checklist too. **Acceptance criteria:** (a) the collections listed by name in the doc; (b) the ingest runs (or cleanly reports why not) in EVERY storage mode; (c) its needs appear in the mode checklist at boot | user ask | 🔄 inventory agent |
| 46 | **MODES: verification** — a live test per mode (local: no-auth works; s3: per-user; mongo: GridFS; hybrid: the split; ingest: collections populated) — each mode's behavior matches its documentation + the startup checklist prints correctly. **Acceptance criteria:** each of the 4+ modes passes its documented behavior test + the README examples copy-paste work | user ask | after 43-45 |
| 41 | ✅ VERIFIED: JAR endpoint-level overloads — two controller methods with the same URL but different params = 2 distinct endpoints (ordinal-prefixed folders), call trees resolve the EXACT overload by bytecode descriptor, overrides fan out with dispatch markers (DIRECT/DYNAMIC_DISPATCH/AMBIGUOUS_IMPL/LIST_INJECT/DEFAULT_METHOD) | user question | DONE |
| 42 | Classifier-outage handling: when the safety classifier is down — no immediate retries; use Read/non-blocked tools; wait ≥5 min between attempts; hand the user the command when urgent; batch work for one recovery | user mandate | STANDING RULE |
| 47 | **STORAGE hardening** — write-ahead spill closes the S3 write-behind crash window; a millisecond write-stamp makes the sidecar staleness check robust to second-granular S3 mtimes; per-JAR `crossref/`+`flowanalysis/` and parser timestamped archives are now removed on analysis delete; the dual Mongo entity systems (`analysis_run` read index vs `am_*` ingest) are documented on `AnalysisRepository` | storage audit | DONE — see `WriteBehindStore`, `S3ArtifactStore`, `AnalysisService#deleteAnalysis`, `AnalyzerController#deleteAnalysis`. Known remaining unbounded: parser `.source-cache` (shared per scope — needs an eviction policy, not a per-analysis delete) |

---

## Execution order
**21-23 land → 24-29 + 31-33 → 30 → 34 → 35-38 → 39 → 40.** ALSO: 43-46 (the modes) run parallel with 24+ — the inventory agent → the doc + the auth fix → the per-mode verification (46) before 40.**
