# Crawl & Overload Plan — signatures, idempotency, DB-impact safety

**Status: EXECUTING** · 2026-10-01 · Builds on: docs/performance-indexing-plan.md, docs/context-identity-plan.md

## W1 — Overload handling by TRUE SIGNATURE (all datasources)
| DB | Correct key | Change |
|---|---|---|
| Oracle | `ALL_ARGUMENTS` (position+TYPE per overload) → node key `__<sig>` (type list); also splits FUNCTION-vs-PROCEDURE same-name pairs | visitor + crawler key upgrade from `__<count>` to `__<signature>` |
| PostgreSQL | `pg_proc.oid` + `pg_get_function_identity_arguments` → FETCH BY OID (no name-LIMIT-1) | fetch strategy + node keying |
| Db2 | `SPECIFICNAME` (unique per overload) | fetch strategy |
| SQL Server / MySQL | no overloading | unchanged |

Call-site resolution: match the call's argument EXPRESSION TYPES against the declared signature where the grammar gives types; fallback chain: exact signature → arity → first-match (counted).
Checklist:
- [ ] Oracle signature keys + call resolution
- [ ] Postgres oid-based fetch + per-overload nodes
- [ ] Db2 SPECIFICNAME keys
- [ ] overload-count API visibility (a per-package overload count in the index)

## W2 — The central decision point (one gate for every fetch/analysis)
`shouldAnalyze(object)` — evaluated ONCE per object per crawl, in order:
1. In-run dedup (visited set) — exists
2. Cross-run source cache + LAST_DDL match — exists (skips the fetch)
3. **NEW: analysis-level idempotency registry** — "(identity, LAST_DDL) analyzed at T, result COMPLETE":
   - SKIP the whole object unless: LAST_DDL changed / the previous run was PARTIAL (0 nodes/facets = not done) / force-refresh requested
   - Conditions: per-user isolation (via the routed store, per-user keys) · concurrent crawls (per-key locking) · the registry lives under the analysis namespace (per-user, S3-backed) · a corrupt/missing registry = re-analyze (fail-safe)
Checklist:
- [ ] the registry write/read (per analysis completion)
- [ ] the skip path + force-refresh flag
- [ ] partial-run detection (never trusts an empty/incomplete run)
- [ ] per-key locking for concurrent crawls

## W3 — DB-impact safety (the customer DB is sacred)
- Bounded connection pool (exists) + a **rate limiter** on catalog queries (configurable QPS cap)
- **Batched IN-queries only** (no N single queries) — extend to the remaining per-name probes
- **Read-only guarantee** — catalog SELECTs only; never DDL/DML on the analyzed DB
- **Statement timeout + circuit breaker**: a slow/hostile DB fails the crawl fast with a clear message instead of hanging
Checklist:
- [ ] rate limiter (QPS cap, configurable)
- [ ] statement timeouts + circuit breaker
- [ ] the remaining per-name probes batched (from the dedup audit's fixes 1-5)
- [ ] a documented DB-impact statement in the runbook

## Redundancy fixes folded into W3 (from the dedup audit)
- [ ] ONE batched trigger-source pull (+ through the source cache)
- [ ] prime the type-map before probing (kills negative probes)
- [ ] owner/type from the batch's own rows (no per-name re-query)
- [ ] one catalog query per level (merge the two)
