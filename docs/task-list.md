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
| 16 | UI screens: Estate tab (Views/Triggers/Globals + chains) + JAR Database Calls tab | user ask | 8836bbd |
| 17 | Regex→grammar Batch 1: dynamic-SQL recursive parse ×4 dialects + trigger bodies | regex audit | 2ab385f |
| 18 | False-pick fixes: globals noise (100%→0), SQLServer view base tables, DB2 phantom view + STATEMENT level, pseudo-table filter, trigger bodyCalls | false-pick audit | 78eca6e, 9f6fa1b |
| 19 | Log archiving disabled; 87 .bak files removed; docs (writeup + 3-page tech doc + DOCX) | user decisions | 5955d17, 3ed2d7b, 8242fa6, d27fbae |
| 20 | Clean slate: backup verified (3,438 files/445MB) → wipe → fresh analyses ×7 — 0 errors | user ask | live runs |

---

## 🔄 IN FLIGHT (agents working — land first)

| # | Task | Source audit |
|---|---|---|
| 21 | Regex Batch 2+3 finish: SQL Server visitor → grammar contexts (7 audit-test fixes in progress) | regex audit |
| 22 | W1: overload SIGNATURE keys — Oracle type signatures, Postgres oid-fetch, Db2 SPECIFICNAME | overload audit |
| 23 | W2+W3: per-analysis idempotency registry (never global) + DB-impact safety (QPS limiter, timeouts, circuit breaker, read-only) + 5 crawl-redundancy fixes | crawl audit |

---

## ⏳ PENDING — awaiting execution (in priority order)

| # | Task | Source audit | Priority |
|---|---|---|---|
| 24 | **Scheduled/infra entry points**: DBMS_SCHEDULER (Oracle), SQL Agent (SQL Server), pg_cron (Postgres), CREATE EVENT (MySQL) — new fetch strategies + a JOB unit type + SCHEDULES edges (job→proc); stop suppressing them as builtins | construct matrix | HIGH |
| 25 | **Anonymous/DO blocks end-to-end**: Postgres plumbing (RoutineType.DO_BLOCK + the fetch/discovery path) + the Oracle splitter recognizing DECLARE…BEGIN blocks | construct matrix | MED-HIGH |
| 26 | **Result-set-returning procs**: a returnsResultSet/resultSets marker (SQL Server implicit result sets, Db2 WITH RETURN, Oracle REF CURSOR OUT, Postgres refcursor/SETOF) surfaced in the cursors facet | construct matrix | MED |
| 27 | **TVF/SETOF return shapes persisted** (returnType/returnsColumns on the routine models, through the multidb adapters) | construct matrix | LOW-MED |
| 28 | **Db2 modules**: ROUTINEMODULENAME in the fetch → MODULE.SUB keying (the package analog) + module variables in the globals facet | construct matrix | LOW-MED |
| 29 | **Nested-subprogram marking**: nested=true/scopeDepth on SubprogramInfo + visible in the procedures facet | construct matrix | LOW |
| 30 | Regex DEFERRED items: MERGE/WITH full promotion, qualified-call grammar rule, CASE contexts, standalone CREATE SYNONYM, JPQL/HQL grammar | regex audit | effort-L |
| 31 | Verify P4: credentials captured for ALL queue-job types (audit said only Claude types carried them — confirm the ConnectionGuard + capture cover every type) | storage audit | small |
| 32 | ChatController persistence (chat script/output writes degrade to the default store) | web audit | small |
| 33 | Case-sensitivity: identity lookup normalizes case in stats but not in facet endpoints (OracleDemo vs ORACLEDEMO) | false-pick session | small |
| 34 | Bucket-garbage check (~152 MB doubled-prefix leftovers — pending verification; my CLI SSO token kept expiring) | housekeeping | needs user's aws sso login |
| 35 | **Re-verification round**: fresh re-analysis of all 5 DBs + jars on the final tree → re-run the false-pick audit → the clean verdict | false-pick audit | after 21-23 land |
| 36 | **A JAR with REAL DB calls** for a live db-calls proof (the test jars carry none) — build a synthetic fixture jar or use a real app jar | false-pick audit | with 35 |
| 37 | **A multi-level view-chain fixture** (view→view→view) in the demo DBs — the multi-hop chain path is untested on real data | false-pick audit | with 35 |
| 38 | Overload discriminator upgrade follow-ups (from the overload table): same-arity-different-type Oracle overloads (in 22), FUNCTION/PROCEDURE same-name pairs, named/default-arg-aware call resolution, per-package overload counts in the index | overload audit | with 22 |
| 39 | Token UX: warn users that iam-imported credentials expire ~1h (the CLI-import wall hit in testing) | test session | small |
| 40 | Staging final deploy → USER REVIEW → prod tag v1.1.0 | — | THE LAST GATE |
| 41 | ✅ VERIFIED: JAR endpoint-level overloads — two controller methods with the same URL but different params = 2 distinct endpoints (ordinal-prefixed folders), call trees resolve the EXACT overload by bytecode descriptor, overrides fan out with dispatch markers (DIRECT/DYNAMIC_DISPATCH/AMBIGUOUS_IMPL/LIST_INJECT/DEFAULT_METHOD) | user question | DONE |
| 42 | Classifier-outage handling: when the safety classifier is down — no immediate retries; use Read/non-blocked tools; wait ≥5 min between attempts; hand the user the command when urgent; batch work for one recovery | user mandate | STANDING RULE |

---

## Execution order
**21-23 land → 24-29 (fan-out in priority order) + 31-33 (small fixes) → 30 (the big deferred pieces) → 34 (user's login) → 35-38 (the re-verification round) → 39 → 40 (user review → prod).**
