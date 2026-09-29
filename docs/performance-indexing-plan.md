# Performance: Indexing & Fast-Read Plan (the "read in place" architecture)

**Status: PROPOSED — awaiting user approval before any code.**
**Goal:** every API under 200ms, for every combination, for analyses of ANY size (up to 912 MB),
without preloading everything (crash risk) and without duplicating data (no MongoDB-style amplification).
**Strategy: save-time precompute + byte-offset indexes + per-entity splits + a layout manifest —
never a per-request scan of a large file.** PL/SQL first (Phase 1), then JAR.

---

## Why things are slow today (measured from the code, anchored in live numbers)

| API family | Today | Cost |
|---|---|---|
| /sequences /cursors /dynamic-sql /joins (PL/SQL) | ✅ already fixed: views/ sidecar → 2ms | — |
| /call-tree (PL/SQL) | ✅ sidecar per entry point | — |
| /call-tree/{id}/callers | ❌ re-synthesises from call_graph.json EVERY request | MBs parsed per call |
| /global-search (PL/SQL) | ❌ scans tables index + ALL ~116 node files per request | 35-41s cold |
| /complexity custom weights, /summary custom scope | ❌ full node scans per request | same |
| /tables/{t}/joins lazy backfill | ⚠️ rewrites the WHOLE table map to add one table | O(all tables) |
| /analyses listing (PL/SQL) | ⚠️ statPrefix + N index reads | 4.7s → 2s |
| /classes/by-index/{i} (JAR) | ❌ FULL streaming pass of analysis.json (100 MB-912 MB) to reach ONE class | worst cost/benefit |
| /summary/endpoints page N (JAR) | ❌ full document parse PER PAGE | 100s of MB per page |
| /summary first hit (JAR) | ❌ lazy: first visitor pays a full-document pass | 429 KB response, 100s of MB read |
| /classes/tree paging (JAR) | ❌ re-parses the whole multi-MB class-tree cache per page | O(cache) per page |
| Big-artifact eviction | ⚠️ 912 MB docs vs 2 GB cache → LRU thrash → re-download 100s of MB to serve one page | structural |

**The pattern of every remaining slow path: a request parses a LARGE document to return a SMALL answer.**

## The fix pattern (4 techniques, applied per artifact)

1. **Sidecars** (proven: the parser's views/ — 71.5s → 2ms): precompute the exact response at save time.
2. **Byte-offset index**: JSONL/arrays get a tiny offsets file (recordIdx → offset,length) → serve one record by seek. Built **during the existing write pass** (near-zero cost — the writer already knows the offsets).
3. **Per-entity split**: one small file per entity (tables/<name>.json like nodes/<id>.json) → a request reads one small file; a lazy backfill writes one small file (not the whole map).
4. **Layout manifest** (the "indexing mechanism" for all APIs): ONE small JSON per analysis written last at save — {artifact list, counts, per-artifact sizes/mtimes, offsets refs} — so the server routes ANY API with ONE small read instead of listing fans + probing.

---

## PHASE 1 — PL/SQL (your focus first) — kills every remaining node/graph scan

| # | Item | Technique | Expected |
|---|---|---|---|
| 1a | callers sidecar: views/callers.json (per entry point) | sidecar, built in the SAME save-time node scan the writer already does | /call-tree/{id}/callers: MBs-parse → 2ms |
| 1b | search facets: precompute search rows (tables + procedures + statements) into views/search.json (+ per-facet files if large) | sidecar at save | /global-search: 35-41s cold scan → small read; typing stays instant |
| 1c | complexity variants: precompute a small weight grid at save (default + common), custom = compute-on-demand+cache | sidecar | custom weights: full scan → hit or one-time |
| 1d | per-table split: tables/<name>.json from the ONE tables scan; views/table-joins/columns become per-table files | per-entity | lazy backfill O(one table); /tables/{t}/*: one small file |
| 1e | statements/index.json → JSONL + offsets (statement rows are ordinal-addressed by paging already) | offset index | paging stops re-reading a large doc |
| 1f | layout manifest: manifest.json written LAST at save (parser already writes index.json last) — artifact list + counts + facet presence + mtimes | manifest | /analyses listing + every router decision: one small read; listing 2s → ~200ms |

Write cost: **near-zero** — 1a-1f are all built in the SAME scans the save phase already performs (the writer does 4 node scans + 2 table scans today; group them into one pass and emit the extra outputs). Old analyses: unchanged (readOrCompute lazy-backfills on first access, then it's fast forever).

## PHASE 2 — JAR big artifacts (the 912 MB problem)

| # | Item | Technique | Expected |
|---|---|---|---|
| 2a | offsets for classes[] + endpoints[] built INSIDE writeAnalysisStreamingTo (the generator knows the byte offsets as it writes) | offset index | /classes/by-index: O(document) → O(record, ~100 KB seek); eviction re-reads eliminated |
| 2b | _summary.json + _class-tree.json built at SAVE time (the JAR equivalent of AnalysisViewWriter), not on first visitor's request | sidecar | first hit of /summary: 100s-of-MB parse → small read |
| 2c | manifest.json per jar (joins _header.json): counts + endpoints folders + offsets refs | manifest | routing + paged endpoints stop probing |
| 2d | _class-tree.json → chunked per-package/per-500-classes files | per-entity | /classes/tree page N: O(page) |

## PHASE 3 — cross-cutting
- 3a: cache-awareness: never let a >100 MB artifact be the only path to a small answer (design rule — enforced by the arch test)
- 3b: keep the sidecar staleness contract (sidecar mtime >= index mtime; invalidate on correction paths)
- 3c: warm-up sweep covers new sidecars (lazy-backfill first release; promoted to save-time after a staging soak)

---

## Checklist (execution order — PL/SQL first per user)
- [x] 1a callers sidecar — fe97830 (golden byte-equivalence ✓)
- [x] 1b search facets precompute — fe97830 (search input cached; filter path unchanged)
- [x] 1c complexity variants (linear default + balanced + logarithmic grid; custom = hashed LRU 8) — fe97830
- [x] 1d per-table split (views/table-joins|columns/<TABLE>.json; backfill writes ONE file) — fe97830
- [x] 1e statements offset index — 365944e (offsets built in the write pass; StatementsOffsetIndexReader tested; ADOPTION NOTE: no endpoint pages statements by ordinal yet — search streams by design and is fast via 1b; wire readPage when a paged statements endpoint lands)
- [x] 1f layout manifest (parser) — 365944e (api/manifest.json, built during the write, KBs, written last)
- [ ] Staging soak: all parser APIs ≤200ms warm; cold first-access acceptable
- [ ] 2a jar offsets (in the streaming write)
- [ ] 2b jar save-time summary + class-tree
- [ ] 2c jar manifest
- [ ] 2d class-tree chunking
- [ ] Full reactor suite green + golden byte-equivalence tests for every sidecar
- [ ] Staging live: 200MB-jar class paging ≤200ms/page; PL/SQL all-APIs sweep ≤200ms warm

**Rules honored:** no preload-everything (sidecars are per-entity small files), no MongoDB, no data
duplication (sidecars are DERIVED, deletable, rebuilt by warm-up), revert-friendly small commits.
