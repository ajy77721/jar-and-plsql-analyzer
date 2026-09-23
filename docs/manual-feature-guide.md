# Feature Guide

This guide is an exhaustive, screen-by-screen and tab-by-tab inventory of the AMP Code Analyzer web app, captured against the live application at `http://localhost:8083` (MongoDB storage mode). Every feature described below is grounded in the actual HTML/JS source (`unified-web/src/main/resources/static/**`) and confirmed by clicking through the real UI with Playwright — nothing here is invented.

Screenshots referenced below live in `docs/screenshots/`.

---

## Home Dashboard

![Home Dashboard](/api/docs/screenshots/home-dashboard.png)


**Callout legend (12 numbered markers in the screenshot above):**

1. Switch between Classic and New chat (element role: button)
2. View application logs (element role: button)
3. 📖 User Manual & Documentation (element role: a)
4. Java · Spring MVC · Jakarta EE · MongoDB · Oracle (element role: span)
5. Java · Spring Boot · MongoDB · Oracle · JPA · JDBC (element role: span)
6. Oracle · Flow Analysis · 482 Grammar Tests (element role: span)
7. 🤖 Claude API Statistics No activity yet ▼ (element role: div)
8. ☰ Analysis Queue idle (element role: div)
9. Refresh (element role: button)
10. Ask AI (element role: button)
11. Help & Documentation (element role: button)
12. Dark theme (element role: button)

Source: `unified-web/src/main/resources/static/index.html`.

- Header banner "AMP Code Analyzer" with tagline "Static analysis, call graph visualization, and AI-powered verification for Java and Oracle PL/SQL codebases".
- "User Manual & Documentation" link (`onclick="UserManual.open(); return false;"`) — opens the in-app manual viewer.
- **JAR / Java Analyzer** card, links to `/jar/` — badge text "Java · Spring Boot · MongoDB · Oracle · JPA · JDBC"; shows live stats in `#jarStats` (loaded via `GET /api/jar/jars`).
- **WAR / Java Analyzer** card, links to `/war/` — badge "Java · Spring MVC · Jakarta EE · MongoDB · Oracle"; stats in `#warStats` (via `GET /api/war/wars`).
- **PL/SQL Parse Analyzer** card, links to `/parser/` — badge "Oracle · Flow Analysis · 482 Grammar Tests"; stats in `#parserStats` (via `GET /api/parser/analyses`).
- A fourth card, **PL/SQL → Java Conversion**, exists in the HTML but is explicitly commented out/hidden (marked `{WIP}`) — not reachable from the live UI.
- Each card shows a live "Loading..." stat block (class/endpoint/procedure counts) populated asynchronously per analyzer.
- Card icons color-coded per analyzer (jar = accent green, war = blue, plsql = orange) via `.card-icon.jar/.war/.plsql` classes.
- Hover interaction: cards lift (`translateY(-4px)`) and show an accent border/arrow highlight (`.card:hover`).
- Light/dark theme toggle is available globally via `shared/theme-toggle.js` (loaded on every page, including Home).
- Quick-stat badges use color classes `.badge.java` / `.badge.oracle` to visually distinguish the Java-based analyzers from the Oracle/PL/SQL one.

---

## JAR Analyzer

Source: `unified-web/src/main/resources/static/jar/index.html` + `jar/js/*.js`.

### Pick-a-JAR screen (sidebar + dashboard)

![JAR pick-analysis list](/api/docs/screenshots/jar-pick-analysis-list.png)

- **Sidebar JAR list** (`jar/js/sidebar.js`, `JA.sidebar.render`): groups JARs that share a base name (strips a `_<13-digit-timestamp>` suffix), showing the newest/CORRECTED version as primary and collapsing older ones behind a "+N older version(s)" toggle.
- Each sidebar item shows: name, size, class/endpoint counts, relative "analyzed" date, an `AI` badge (RUNNING/COMPLETE/FAILED Claude status), a `CORRECTED`/`C(n)` mode badge, an optional `pkg` badge (base package), and a delete (`×`) button (`JA.app.deleteJar`).
- **Search JARs** box (`#jarListSearch`) — client-side filter over the in-memory list (`JA.sidebar.filter`).
- **Upload JAR** button (`id="upload-label"`, `onclick="JA.app.showUploadModal()"`) — opens an upload modal accepting `.jar` files; posts via `JA.api.uploadJar(...)` → `POST /api/jar/jars?mode=...` (multipart).
- **Targeted (dev)** button exists in markup (`id="targeted-label"`, `JA.targeted.showModal()`, "Analyze a local JAR from specific entry points") but is hidden (`display:none`) by design.
- **Sessions** top-bar button (`id="sessions-btn"`, `JA.sessions.show()`) — opens the Claude Sessions overlay (list of running/completed AI verification sessions, `Refresh`/`Kill Session` buttons, backed by `GET /api/jar/jars/sessions`).
- **Logs** button (`JA.logviewer.toggle()`) — application log viewer overlay.
- **Dashboard/welcome view** (`jar/js/dashboard.js`, shown when no JAR is selected): aggregate stat cards for total JARs, classes, endpoints, total size, Claude-running count, Claude-done count; a `Claude Sessions` button and a `Performance Summary` toggle (aggregate averages, analysis time); a card grid per JAR with `Open` / `Claude` (triggers `claudeFullScan`) / `Delete` actions and live Claude progress bars.
- Both list and dashboard are populated from a single **`GET /api/jar/jars`** call — no server-side filtering; all filtering/grouping happens client-side.
- Global chat toggle switch (top-right, "Chat: New") for the AI assistant chatbox (`chatbox.js` / `chat-toggle.js`).

**Callout legend (18 numbered markers in the screenshot above):**

1. (`jar/js/sidebar.js`, `JA.sidebar.render`): groups JARs that share a base name (strips a `_<13-digit-timestamp>` suffix), showing the newest/CORRECTED version as primary and collapsing older ones behind a "+N older version(s)" toggle.
2. Each sidebar item shows: name, size, class/endpoint counts, relative "analyzed" date, an `AI` badge (RUNNING/COMPLETE/FAILED Claude status), a `CORRECTED`/`C(n)` mode badge, an optional `pkg` badge (base package), and a delete (`×`) button (`JA.app.deleteJar`).
3. box (`#jarListSearch`) — client-side filter over the in-memory list (`JA.sidebar.filter`).
4. button (`id="upload-label"`, `onclick="JA.app.showUploadModal()"`) — opens an upload modal accepting `.jar` files; posts via `JA.api.uploadJar(...)` → `POST /api/jar/jars?mode=...` (multipart).
5. button exists in markup (`id="targeted-label"`, `JA.targeted.showModal()`, "Analyze a local JAR from specific entry points") but is hidden (`display:none`) by design.
6. top-bar button (`id="sessions-btn"`, `JA.sessions.show()`) — opens the Claude Sessions overlay (list of running/completed AI verification sessions, `Refresh`/`Kill Session` buttons, backed by `GET /api/jar/jars/sessions`).
7. button (`JA.logviewer.toggle()`) — application log viewer overlay.
8. (`jar/js/dashboard.js`, shown when no JAR is selected): aggregate stat cards for total JARs, classes, endpoints, total size, Claude-running count, Claude-done count; a `Claude Sessions` button and a `Performance Summary` toggle (aggregate averages, analysis time); a card grid per JAR with `Open` / `Claude` (triggers `claudeFullScan`) / `Delete` actions and live Claude progress bars.
9. Both list and dashboard are populated from a single **`GET /api/jar/jars`** call — no server-side filtering; all filtering/grouping happens client-side.
10. Global chat toggle switch (top-right, "Chat: New") for the AI assistant chatbox (`chatbox.js` / `chat-toggle.js`).
11. Open (element role: button)
12. Claude (element role: button)
13. Delete (element role: button)
14. Open (element role: button)
15. Claude (element role: button)
16. Delete (element role: button)
17. Open (element role: button)
18. Claude (element role: button)


### Code Structure

![JAR Code Structure](/api/docs/screenshots/jar-code-structure.png)

- Top-level tab `data-tab="structure"`, label **Code Structure** (`onclick="switchTab('structure')"`).
- View-mode toggle: `JA.codeTree.setViewMode('package'|'project'|'visual')` — switches between Package, Project, and Visual tree layouts.
- Search box `#search-classes` (`oninput="filterTree()"`) — live filters classes/methods/annotations in the tree.
- `Expand All` / `Collapse All` buttons.
- Navigation history bar: `JA.codeTree.goBack()`, `goForward()`, and a "Jump History" toggle panel (`toggleHistory()`).
- Jar-source filter pills (`codeTreeViews.js`): filter the tree by which bundled JAR a class came from (`toggleJarSource`), with an "All" reset (`resetJarSourceFilter`).
- Per-class "Code" viewer button opens decompiled source in a side panel with a **Copy** button (`JA.codeTreePanel._copyCode()`).
- Bundled-JARs browser: lists nested JAR dependencies (`GET /api/jar/jars/{id}/bundled-jars`), content viewer (`/bundled-jars/content`), and a dependency map (`/jar-dep-map`).
- Visual view stereotype filter toggles (Controller/Service/Repository/etc., `JA.codeTreeViews.toggleStereo`) and clickable class cards (`jumpToClass`).
- Interface/abstract-class "implementation picker" popup (`nav.js`) when a call target has multiple concrete implementations.
- Backing data: `GET /api/jar/jars/{id}/classes/tree` and `/classes/by-index/{idx}`.

**Callout legend (18 numbered markers in the screenshot above):**

1. Top-level tab `data-tab="structure"`, label **Code Structure** (`onclick="switchTab('structure')"`).
2. View-mode toggle: `JA.codeTree.setViewMode('package'|'project'|'visual')` — switches between Package, Project, and Visual tree layouts.
3. Search box `#search-classes` (`oninput="filterTree()"`) — live filters classes/methods/annotations in the tree.
4. `Expand All` / `Collapse All` buttons.
5. Navigation history bar: `JA.codeTree.goBack()`, `goForward()`, and a "Jump History" toggle panel (`toggleHistory()`).
6. Jar-source filter pills (`codeTreeViews.js`): filter the tree by which bundled JAR a class came from (`toggleJarSource`), with an "All" reset (`resetJarSourceFilter`).
7. Per-class "Code" viewer button opens decompiled source in a side panel with a **Copy** button (`JA.codeTreePanel._copyCode()`).
8. Bundled-JARs browser: lists nested JAR dependencies (`GET /api/jar/jars/{id}/bundled-jars`), content viewer (`/bundled-jars/content`), and a dependency map (`/jar-dep-map`).
9. Visual view stereotype filter toggles (Controller/Service/Repository/etc., `JA.codeTreeViews.toggleStereo`) and clickable class cards (`jumpToClass`).
10. Interface/abstract-class "implementation picker" popup (`nav.js`) when a call target has multiple concrete implementations.
11. Backing data: `GET /api/jar/jars/{id}/classes/tree` and `/classes/by-index/{idx}`.
12. Summary (element role: button, data-tab=summary)
13. Export analysis data (element role: button)
14. REST API endpoints found in controller classes (element role: span)
15. Unique MongoDB collections accessed across all endpoints (element role: span)
16. MongoDB view collections (read-only aggregation pipelines) (element role: span)
17. Separate JAR modules/libraries detected in the call chain (element role: span)
18. Business domains identified from collection name prefixes (element role: span)


### Endpoint Flows

![JAR Endpoint Flows](/api/docs/screenshots/jar-endpoint-flows.png)

- Top-level tab `data-tab="endpoints"`, label **Endpoint Flows**.
- Endpoint search/filter box (`JA.endpointList.onSearch`), server-paged (50/page) via the shared `PA.tf` table-pager framework.
- Clickable endpoint cards (`JA.endpointList.select`) load a per-endpoint call tree: `GET /api/jar/jars/{id}/endpoints/by-index/{idx}/call-tree`.
- Interactive call-graph rendering (`JA.callGraph.render`) with breadcrumb navigation: `goBack()`, `goToRoot()`.
- Ambiguous-call drill-in (`JA.callGraph.drillToImpl`) for interface/abstract dispatch resolution.
- Node "open method" action jumps straight to the source/class panel.

**Callout legend (18 numbered markers in the screenshot above):**

1. Top-level tab `data-tab="endpoints"`, label **Endpoint Flows**.
2. Endpoint search/filter box (`JA.endpointList.onSearch`), server-paged (50/page) via the shared `PA.tf` table-pager framework.
3. Clickable endpoint cards (`JA.endpointList.select`) load a per-endpoint call tree: `GET /api/jar/jars/{id}/endpoints/by-index/{idx}/call-tree`.
4. Interactive call-graph rendering (`JA.callGraph.render`) with breadcrumb navigation: `goBack()`, `goToRoot()`.
5. Ambiguous-call drill-in (`JA.callGraph.drillToImpl`) for interface/abstract dispatch resolution.
6. Node "open method" action jumps straight to the source/class panel.
7. Search this JAR (press /) (element role: button)
8. View application logs (element role: button)
9. Search JARs... (element role: input)
10. Code Structure (element role: button, data-tab=structure)
11. Endpoint Flows (element role: button, data-tab=endpoints)
12. Summary (element role: button, data-tab=summary)
13. Filter endpoints... (element role: input)
14. 10/page20/page50/page100/page (element role: select)
15. First page (element role: button)
16. Previous page (element role: button)
17. 1 (element role: button)
18. 2 (element role: button)


### Summary

![JAR Summary — Endpoint Report](/api/docs/screenshots/jar-summary-endpoint-report.png)

- Top-level tab `data-tab="summary"`.
- Stats bar of 10 clickable chips (Endpoints, Collections, Views, Modules, Domains, Cross-Module, LOC, Txn Req, Batch, Scheduled) — each opens a detail popup (`JA.summary.showStatPopup(key)`).
- Global **Export** button (`.global-export-fab`, `JA.summary.showExportModal()`) opens a modal with:
  - Report-section checkboxes (Endpoint Report, Collection Analysis, Distributed Transactions, Batch Jobs, Views Analysis, External Dependencies).
  - Format buttons: **JSON**, **CSV**, **Excel**, **ZIP** (`_setExportFmt`).
  - For Excel: source toggle **Backend** (server-side Apache POI, `POST /api/jar/jars/export-excel`) vs **Frontend** (in-browser `xlsx-js-style` generation, no server round-trip).
  - ZIP-only options to bundle cached or freshly-decompiled source (`export/bundle`, `export/bundle/full-source-job` + progress via `/api/queue/{jobId}`).
  - Editable output filename; final "Export Selected Reports" action.
- 11 real sub-tabs (`tabDefs` in `jar/js/summary.js`), each a sortable/column-filterable table:

**Callout legend (18 numbered markers in the screenshot above):**

1. Top-level tab `data-tab="summary"`.
2. Stats bar of 10 clickable chips (Endpoints, Collections, Views, Modules, Domains, Cross-Module, LOC, Txn Req, Batch, Scheduled) — each opens a detail popup (`JA.summary.showStatPopup(key)`).
3. Global **Export** button (`.global-export-fab`, `JA.summary.showExportModal()`) opens a modal with:
4. 11 real sub-tabs (`tabDefs` in `jar/js/summary.js`), each a sortable/column-filterable table:
5. Enable Claude (element role: button)
6. Global domain config (no per-JAR snapshot) — click to view (element role: span)
7. Search this JAR (press /) (element role: button)
8. View application logs (element role: button)
9. Search JARs... (element role: input)
10. Code Structure (element role: button, data-tab=structure)
11. Endpoint Flows (element role: button, data-tab=endpoints)
12. Summary (element role: button, data-tab=summary)
13. Export analysis data (element role: button)
14. REST API endpoints found in controller classes (element role: span)
15. Unique MongoDB collections accessed across all endpoints (element role: span)
16. MongoDB view collections (read-only aggregation pipelines) (element role: span)
17. Separate JAR modules/libraries detected in the call chain (element role: span)
18. Business domains identified from collection name prefixes (element role: span)


#### Endpoint Report
![Endpoint Report](/api/docs/screenshots/jar-summary-endpoint-report.png)
- Full call-tree analysis per REST endpoint: collections touched, owning domain, LOC, DB operations, cross-module calls.
- Sortable columns (e.g. by Collections or LOC) to surface migration hotspots; domain filter to scope to one vertical.
- Row-level export to per-endpoint JSON (`exportEndpointDetail`).

**Callout legend (18 numbered markers in the screenshot above):**

1. Full call-tree analysis per REST endpoint: collections touched, owning domain, LOC, DB operations, cross-module calls.
2. Sortable columns (e.g. by Collections or LOC) to surface migration hotspots; domain filter to scope to one vertical.
3. Row-level export to per-endpoint JSON (`exportEndpointDetail`).
4. Upload JAR (element role: button)
5. Enable Claude (element role: button)
6. Global domain config (no per-JAR snapshot) — click to view (element role: span)
7. Search this JAR (press /) (element role: button)
8. View application logs (element role: button)
9. Search JARs... (element role: input)
10. Code Structure (element role: button, data-tab=structure)
11. Endpoint Flows (element role: button, data-tab=endpoints)
12. Summary (element role: button, data-tab=summary)
13. Export analysis data (element role: button)
14. REST API endpoints found in controller classes (element role: span)
15. Unique MongoDB collections accessed across all endpoints (element role: span)
16. MongoDB view collections (read-only aggregation pipelines) (element role: span)
17. Separate JAR modules/libraries detected in the call chain (element role: span)
18. Business domains identified from collection name prefixes (element role: span)


#### Collection Analysis
![Collection Analysis](/api/docs/screenshots/jar-summary-collection-analysis.png)
- Maps every MongoDB collection to its owning domain, with read/write/aggregate operation breakdown and verification status.
- Column filters to isolate a single domain and see cross-domain accessors; highlights shared collections and `NOT_IN_DB` (possible dead code) entries.

**Callout legend (18 numbered markers in the screenshot above):**

1. Maps every MongoDB collection to its owning domain, with read/write/aggregate operation breakdown and verification status.
2. Column filters to isolate a single domain and see cross-domain accessors; highlights shared collections and `NOT_IN_DB` (possible dead code) entries.
3. Go to home (element role: div)
4. Upload JAR (element role: button)
5. Enable Claude (element role: button)
6. Global domain config (no per-JAR snapshot) — click to view (element role: span)
7. Search this JAR (press /) (element role: button)
8. View application logs (element role: button)
9. Search JARs... (element role: input)
10. Code Structure (element role: button, data-tab=structure)
11. Endpoint Flows (element role: button, data-tab=endpoints)
12. Summary (element role: button, data-tab=summary)
13. Export analysis data (element role: button)
14. REST API endpoints found in controller classes (element role: span)
15. Unique MongoDB collections accessed across all endpoints (element role: span)
16. MongoDB view collections (read-only aggregation pipelines) (element role: span)
17. Separate JAR modules/libraries detected in the call chain (element role: span)
18. Business domains identified from collection name prefixes (element role: span)


#### External Dependencies
![External Dependencies](/api/docs/screenshots/jar-summary-external-dependencies.png)
- Cross-module JAR dependency list — which external libraries/modules each endpoint calls and call counts, to flag tight coupling and circular dependencies.

**Callout legend (18 numbered markers in the screenshot above):**

1. Cross-module JAR dependency list — which external libraries/modules each endpoint calls and call counts, to flag tight coupling and circular dependencies.
2. Switch between Classic and New chat (element role: button)
3. Go to home (element role: div)
4. Upload JAR (element role: button)
5. Enable Claude (element role: button)
6. Global domain config (no per-JAR snapshot) — click to view (element role: span)
7. Search this JAR (press /) (element role: button)
8. View application logs (element role: button)
9. Search JARs... (element role: input)
10. Code Structure (element role: button, data-tab=structure)
11. Endpoint Flows (element role: button, data-tab=endpoints)
12. Summary (element role: button, data-tab=summary)
13. Export analysis data (element role: button)
14. REST API endpoints found in controller classes (element role: span)
15. Unique MongoDB collections accessed across all endpoints (element role: span)
16. MongoDB view collections (read-only aggregation pipelines) (element role: span)
17. Separate JAR modules/libraries detected in the call chain (element role: span)
18. Business domains identified from collection name prefixes (element role: span)


#### Distributed Transactions
![Distributed Transactions](/api/docs/screenshots/jar-summary-distributed-transactions.png)
- Endpoints that write to multiple collections/domains — flagged "REQUIRED — cross-domain writes" as future consistency risks needing saga patterns.

**Callout legend (18 numbered markers in the screenshot above):**

1. Endpoints that write to multiple collections/domains — flagged "REQUIRED — cross-domain writes" as future consistency risks needing saga patterns.
2. Switch between Classic and New chat (element role: button)
3. Go to home (element role: div)
4. Upload JAR (element role: button)
5. Enable Claude (element role: button)
6. Global domain config (no per-JAR snapshot) — click to view (element role: span)
7. Search this JAR (press /) (element role: button)
8. View application logs (element role: button)
9. Search JARs... (element role: input)
10. Code Structure (element role: button, data-tab=structure)
11. Endpoint Flows (element role: button, data-tab=endpoints)
12. Summary (element role: button, data-tab=summary)
13. Export analysis data (element role: button)
14. REST API endpoints found in controller classes (element role: span)
15. Unique MongoDB collections accessed across all endpoints (element role: span)
16. MongoDB view collections (read-only aggregation pipelines) (element role: span)
17. Separate JAR modules/libraries detected in the call chain (element role: span)
18. Business domains identified from collection name prefixes (element role: span)


#### Batch Jobs
![Batch Jobs](/api/docs/screenshots/jar-summary-batch-jobs.png)
- Endpoints classified as batch/scheduler jobs by naming pattern/annotation; cross-referenced against Scheduled Jobs to see cron-triggered vs REST-triggered.

**Callout legend (18 numbered markers in the screenshot above):**

1. Endpoints classified as batch/scheduler jobs by naming pattern/annotation; cross-referenced against Scheduled Jobs to see cron-triggered vs REST-triggered.
2. Switch between Classic and New chat (element role: button)
3. Go to home (element role: div)
4. Upload JAR (element role: button)
5. Enable Claude (element role: button)
6. Global domain config (no per-JAR snapshot) — click to view (element role: span)
7. Search this JAR (press /) (element role: button)
8. View application logs (element role: button)
9. Search JARs... (element role: input)
10. Code Structure (element role: button, data-tab=structure)
11. Endpoint Flows (element role: button, data-tab=endpoints)
12. Summary (element role: button, data-tab=summary)
13. Export analysis data (element role: button)
14. REST API endpoints found in controller classes (element role: span)
15. Unique MongoDB collections accessed across all endpoints (element role: span)
16. MongoDB view collections (read-only aggregation pipelines) (element role: span)
17. Separate JAR modules/libraries detected in the call chain (element role: span)
18. Business domains identified from collection name prefixes (element role: span)


#### Scheduled Jobs
![Scheduled Jobs](/api/docs/screenshots/jar-summary-scheduled-jobs.png)
- `@Scheduled` methods — cron expressions, fixed-rate/fixed-delay tasks — with cross-domain collection-access flags.

**Callout legend (18 numbered markers in the screenshot above):**

1. `@Scheduled` methods — cron expressions, fixed-rate/fixed-delay tasks — with cross-domain collection-access flags.
2. Switch between Classic and New chat (element role: button)
3. Go to home (element role: div)
4. Upload JAR (element role: button)
5. Enable Claude (element role: button)
6. Global domain config (no per-JAR snapshot) — click to view (element role: span)
7. Search this JAR (press /) (element role: button)
8. View application logs (element role: button)
9. Search JARs... (element role: input)
10. Code Structure (element role: button, data-tab=structure)
11. Endpoint Flows (element role: button, data-tab=endpoints)
12. Summary (element role: button, data-tab=summary)
13. Export analysis data (element role: button)
14. REST API endpoints found in controller classes (element role: span)
15. Unique MongoDB collections accessed across all endpoints (element role: span)
16. MongoDB view collections (read-only aggregation pipelines) (element role: span)
17. Separate JAR modules/libraries detected in the call chain (element role: span)
18. Business domains identified from collection name prefixes (element role: span)


#### Aggregation Flows
![Aggregation Flows](/api/docs/screenshots/jar-summary-aggregation-flows.png)
- Detected MongoDB aggregation pipelines: `$lookup` joins, pipeline stages (`$match`/`$group`/`$project`/`$sort`), collection references, DYNAMIC (runtime-built) markers.
- Column filters on Collections/Pipeline Stages; click a row for full pipeline detail and controller→repository call path.

**Callout legend (18 numbered markers in the screenshot above):**

1. Detected MongoDB aggregation pipelines: `$lookup` joins, pipeline stages (`$match`/`$group`/`$project`/`$sort`), collection references, DYNAMIC (runtime-built) markers.
2. Column filters on Collections/Pipeline Stages; click a row for full pipeline detail and controller→repository call path.
3. Go to home (element role: div)
4. Upload JAR (element role: button)
5. Enable Claude (element role: button)
6. Global domain config (no per-JAR snapshot) — click to view (element role: span)
7. Search this JAR (press /) (element role: button)
8. View application logs (element role: button)
9. Search JARs... (element role: input)
10. Code Structure (element role: button, data-tab=structure)
11. Endpoint Flows (element role: button, data-tab=endpoints)
12. Summary (element role: button, data-tab=summary)
13. Export analysis data (element role: button)
14. REST API endpoints found in controller classes (element role: span)
15. Unique MongoDB collections accessed across all endpoints (element role: span)
16. MongoDB view collections (read-only aggregation pipelines) (element role: span)
17. Separate JAR modules/libraries detected in the call chain (element role: span)
18. Business domains identified from collection name prefixes (element role: span)


#### Dynamic Flows
![Dynamic Flows](/api/docs/screenshots/jar-summary-dynamic-flows.png)
- Non-direct dispatch patterns: INTERFACE_DISPATCH, Spring bean injection, REFLECTION, DYNAMIC_QUERY, strategy patterns — flagged as static-analysis blind spots.
- Row click shows resolved-from class and call path.

**Callout legend (18 numbered markers in the screenshot above):**

1. Non-direct dispatch patterns: INTERFACE_DISPATCH, Spring bean injection, REFLECTION, DYNAMIC_QUERY, strategy patterns — flagged as static-analysis blind spots.
2. Row click shows resolved-from class and call path.
3. Go to home (element role: div)
4. Upload JAR (element role: button)
5. Enable Claude (element role: button)
6. Global domain config (no per-JAR snapshot) — click to view (element role: span)
7. Search this JAR (press /) (element role: button)
8. View application logs (element role: button)
9. Search JARs... (element role: input)
10. Code Structure (element role: button, data-tab=structure)
11. Endpoint Flows (element role: button, data-tab=endpoints)
12. Summary (element role: button, data-tab=summary)
13. Export analysis data (element role: button)
14. REST API endpoints found in controller classes (element role: span)
15. Unique MongoDB collections accessed across all endpoints (element role: span)
16. MongoDB view collections (read-only aggregation pipelines) (element role: span)
17. Separate JAR modules/libraries detected in the call chain (element role: span)
18. Business domains identified from collection name prefixes (element role: span)


#### Verticalisation
![Verticalisation](/api/docs/screenshots/jar-summary-verticalisation.png)
- Domain-boundary violation report: cross-module direct method calls and out-of-domain collection accesses.
- Filter by source domain to see all outgoing dependencies (future API contracts) of one vertical.

**Callout legend (18 numbered markers in the screenshot above):**

1. Domain-boundary violation report: cross-module direct method calls and out-of-domain collection accesses.
2. Filter by source domain to see all outgoing dependencies (future API contracts) of one vertical.
3. Go to home (element role: div)
4. Upload JAR (element role: button)
5. Enable Claude (element role: button)
6. Global domain config (no per-JAR snapshot) — click to view (element role: span)
7. Search this JAR (press /) (element role: button)
8. View application logs (element role: button)
9. Search JARs... (element role: input)
10. Code Structure (element role: button, data-tab=structure)
11. Endpoint Flows (element role: button, data-tab=endpoints)
12. Summary (element role: button, data-tab=summary)
13. Export analysis data (element role: button)
14. REST API endpoints found in controller classes (element role: span)
15. Unique MongoDB collections accessed across all endpoints (element role: span)
16. MongoDB view collections (read-only aggregation pipelines) (element role: span)
17. Separate JAR modules/libraries detected in the call chain (element role: span)
18. Business domains identified from collection name prefixes (element role: span)


#### Claude Insights
![Claude Insights](/api/docs/screenshots/jar-summary-claude-insights.png)
- AI-powered per-endpoint analysis: business process names, risk flags, migration-complexity recommendations.
- "Start Full Scan" button triggers Claude enrichment (`claudeFullScan`) when no data exists yet (as captured — this JAR had none).

**Callout legend (18 numbered markers in the screenshot above):**

1. AI-powered per-endpoint analysis: business process names, risk flags, migration-complexity recommendations.
2. "Start Full Scan" button triggers Claude enrichment (`claudeFullScan`) when no data exists yet (as captured — this JAR had none).
3. Go to home (element role: div)
4. Upload JAR (element role: button)
5. Enable Claude (element role: button)
6. Global domain config (no per-JAR snapshot) — click to view (element role: span)
7. Search this JAR (press /) (element role: button)
8. View application logs (element role: button)
9. Search JARs... (element role: input)
10. Code Structure (element role: button, data-tab=structure)
11. Endpoint Flows (element role: button, data-tab=endpoints)
12. Summary (element role: button, data-tab=summary)
13. Export analysis data (element role: button)
14. REST API endpoints found in controller classes (element role: span)
15. Unique MongoDB collections accessed across all endpoints (element role: span)
16. MongoDB view collections (read-only aggregation pipelines) (element role: span)
17. Separate JAR modules/libraries detected in the call chain (element role: span)
18. Business domains identified from collection name prefixes (element role: span)


#### Claude Corrections
![Claude Corrections](/api/docs/screenshots/jar-summary-claude-corrections.png)
- Side-by-side static-vs-Claude-corrected collection comparison per endpoint: ADDED (missed by static analysis), REMOVED (false positives), verification rate per domain.
- Driven by `claudeCorrect` / `claudeCorrectSingle` / `revertClaude`, with correction-log browsing.

**Callout legend (18 numbered markers in the screenshot above):**

1. Side-by-side static-vs-Claude-corrected collection comparison per endpoint: ADDED (missed by static analysis), REMOVED (false positives), verification rate per domain.
2. Driven by `claudeCorrect` / `claudeCorrectSingle` / `revertClaude`, with correction-log browsing.
3. Go to home (element role: div)
4. Upload JAR (element role: button)
5. Enable Claude (element role: button)
6. Global domain config (no per-JAR snapshot) — click to view (element role: span)
7. Search this JAR (press /) (element role: button)
8. View application logs (element role: button)
9. Search JARs... (element role: input)
10. Code Structure (element role: button, data-tab=structure)
11. Endpoint Flows (element role: button, data-tab=endpoints)
12. Summary (element role: button, data-tab=summary)
13. Export analysis data (element role: button)
14. REST API endpoints found in controller classes (element role: span)
15. Unique MongoDB collections accessed across all endpoints (element role: span)
16. MongoDB view collections (read-only aggregation pipelines) (element role: span)
17. Separate JAR modules/libraries detected in the call chain (element role: span)
18. Business domains identified from collection name prefixes (element role: span)


- A 12th sub-tab, **Flow Analysis**, is defined in code (`tabDefs`) but commented out/hidden (marked WIP as of 2026-09-04) — not screenshotted since it is not reachable in the live UI.

---

## WAR Analyzer

WAR reuses the exact same `Code Structure` / `Endpoint Flows` / `Summary` tab structure and the identical `jar/js/summary.js` sub-tab set (Endpoint Report → Claude Corrections) as the JAR Analyzer — confirmed by `war/index.html` loading the same `jar/js/*` scripts, with `war/js/api.js` and `war/js/app-overrides.js` re-pointing every API call from `/api/jar/jars/...` to `/api/war/wars/...` (and redirecting the JAR-hardcoded `/api/jar/progress` SSE stream to `/api/war/progress`).

### Pick-a-WAR screen

![WAR pick-analysis list](/api/docs/screenshots/war-pick-analysis-list.png)

- Same sidebar/dashboard pattern as JAR, populated from **`GET /api/war/wars`**.
- Upload accepts `.war` files (`data-file-accept=".war"`) via `POST /api/war/wars?mode=...`.
- `getClaudeStatus()` is stubbed to always return "not configured" for WAR (per `app-overrides.js`), and `anySessionRunning()` deliberately checks the shared `JAR` session scope (WAR and JAR share one `ClaudeSessionManager`).
- Three list/tree endpoints have no dedicated WAR REST route and are computed client-side over already-fetched flat data instead: `endpointListPages`, `classTreePages`, `getClassPackages`.

**Callout legend (18 numbered markers in the screenshot above):**

1. Same sidebar/dashboard pattern as JAR, populated from **`GET /api/war/wars`**.
2. Upload accepts `.war` files (`data-file-accept=".war"`) via `POST /api/war/wars?mode=...`.
3. `getClaudeStatus()` is stubbed to always return "not configured" for WAR (per `app-overrides.js`), and `anySessionRunning()` deliberately checks the shared `JAR` session scope (WAR and JAR share one `ClaudeSessionManager`).
4. Three list/tree endpoints have no dedicated WAR REST route and are computed client-side over already-fetched flat data instead: `endpointListPages`, `classTreePages`, `getClassPackages`.
5. Upload WAR (element role: button)
6. View application logs (element role: button)
7. Claude Sessions (element role: button)
8. Performance Summary (element role: button)
9. Spring_MVC_t4_1789301289.war6 classes11 endpoints7.8 MB13 Se (element role: div)
10. Spring_MVC.war6 classes11 endpoints7.8 MB13 Sept 2026 17:35O (element role: div)
11. Open (element role: button)
12. Claude (element role: button)
13. Delete (element role: button)
14. Open (element role: button)
15. Claude (element role: button)
16. Delete (element role: button)
17. Ask AI (element role: button)
18. Help & Documentation (element role: button)


### Code Structure

![WAR Code Structure](/api/docs/screenshots/war-code-structure.png)

- Identical feature set to JAR's Code Structure tab (Package/Project/Visual view modes, search, expand/collapse, jar-source filter pills, decompiled-source viewer with Copy).
- Data via `GET /api/war/wars/{id}/classes/tree` and `/classes/by-index/{idx}`.

**Callout legend (18 numbered markers in the screenshot above):**

1. Identical feature set to JAR's Code Structure tab (Package/Project/Visual view modes, search, expand/collapse, jar-source filter pills, decompiled-source viewer with Copy).
2. Data via `GET /api/war/wars/{id}/classes/tree` and `/classes/by-index/{idx}`.
3. Switch between Classic and New chat (element role: button)
4. Go to home (element role: div)
5. Enable Claude (element role: button)
6. Global domain config (no per-JAR snapshot) — click to view (element role: span)
7. Upload WAR (element role: button)
8. Search this WAR (press /) (element role: button)
9. View application logs (element role: button)
10. Code Structure (element role: button, data-tab=structure)
11. Endpoint Flows (element role: button, data-tab=endpoints)
12. Summary (element role: button, data-tab=summary)
13. Export analysis data (element role: button)
14. REST API endpoints found in controller classes (element role: span)
15. Unique MongoDB collections accessed across all endpoints (element role: span)
16. MongoDB view collections (read-only aggregation pipelines) (element role: span)
17. Separate JAR modules/libraries detected in the call chain (element role: span)
18. Business domains identified from collection name prefixes (element role: span)


### Endpoint Flows

![WAR Endpoint Flows](/api/docs/screenshots/war-endpoint-flows.png)

- Identical to JAR's Endpoint Flows: search/filter, endpoint cards, call-tree graph with breadcrumb nav.
- Data via `GET /api/war/wars/{id}/endpoints/by-index/{idx}/call-tree`; endpoint list itself is built client-side from `GET /api/war/wars/{id}/summary`.

**Callout legend (18 numbered markers in the screenshot above):**

1. Identical to JAR's Endpoint Flows: search/filter, endpoint cards, call-tree graph with breadcrumb nav.
2. Data via `GET /api/war/wars/{id}/endpoints/by-index/{idx}/call-tree`; endpoint list itself is built client-side from `GET /api/war/wars/{id}/summary`.
3. Switch between Classic and New chat (element role: button)
4. Go to home (element role: div)
5. Enable Claude (element role: button)
6. Global domain config (no per-JAR snapshot) — click to view (element role: span)
7. Upload WAR (element role: button)
8. Search this WAR (press /) (element role: button)
9. View application logs (element role: button)
10. Code Structure (element role: button, data-tab=structure)
11. Endpoint Flows (element role: button, data-tab=endpoints)
12. Summary (element role: button, data-tab=summary)
13. Filter endpoints... (element role: input)
14. 10/page20/page50/page100/page (element role: select)
15. First page (element role: button)
16. Previous page (element role: button)
17. 1 (element role: button)
18. Next page (element role: button)


### Summary

Same Export modal (JSON/CSV/Excel/ZIP, backend-vs-frontend Excel toggle) and identical 11 sub-tabs as JAR, all backed by `/api/war/wars/{id}/summary/...` routes:

#### Endpoint Report
![WAR Endpoint Report](/api/docs/screenshots/war-summary-endpoint-report.png)
- Same full call-tree table as JAR's Endpoint Report, scoped to `Spring_MVC.war` (6 classes / 11 endpoints in this analysis).

**Callout legend (18 numbered markers in the screenshot above):**

1. Same full call-tree table as JAR's Endpoint Report, scoped to `Spring_MVC.war` (6 classes / 11 endpoints in this analysis).
2. View application logs (element role: button)
3. Switch between Classic and New chat (element role: button)
4. Go to home (element role: div)
5. Enable Claude (element role: button)
6. Global domain config (no per-JAR snapshot) — click to view (element role: span)
7. Upload WAR (element role: button)
8. Search this WAR (press /) (element role: button)
9. View application logs (element role: button)
10. Code Structure (element role: button, data-tab=structure)
11. Endpoint Flows (element role: button, data-tab=endpoints)
12. Summary (element role: button, data-tab=summary)
13. Export analysis data (element role: button)
14. REST API endpoints found in controller classes (element role: span)
15. Unique MongoDB collections accessed across all endpoints (element role: span)
16. MongoDB view collections (read-only aggregation pipelines) (element role: span)
17. Separate JAR modules/libraries detected in the call chain (element role: span)
18. Business domains identified from collection name prefixes (element role: span)


#### Collection Analysis
![WAR Collection Analysis](/api/docs/screenshots/war-summary-collection-analysis.png)
- Collection-to-domain ownership map, same as JAR.

**Callout legend (18 numbered markers in the screenshot above):**

1. Collection-to-domain ownership map, same as JAR.
2. View application logs (element role: button)
3. Switch between Classic and New chat (element role: button)
4. Go to home (element role: div)
5. Enable Claude (element role: button)
6. Global domain config (no per-JAR snapshot) — click to view (element role: span)
7. Upload WAR (element role: button)
8. Search this WAR (press /) (element role: button)
9. View application logs (element role: button)
10. Code Structure (element role: button, data-tab=structure)
11. Endpoint Flows (element role: button, data-tab=endpoints)
12. Summary (element role: button, data-tab=summary)
13. Export analysis data (element role: button)
14. REST API endpoints found in controller classes (element role: span)
15. Unique MongoDB collections accessed across all endpoints (element role: span)
16. MongoDB view collections (read-only aggregation pipelines) (element role: span)
17. Separate JAR modules/libraries detected in the call chain (element role: span)
18. Business domains identified from collection name prefixes (element role: span)


#### External Dependencies
![WAR External Dependencies](/api/docs/screenshots/war-summary-external-dependencies.png)
- Cross-module dependency list, same as JAR.

**Callout legend (18 numbered markers in the screenshot above):**

1. Cross-module dependency list, same as JAR.
2. View application logs (element role: button)
3. Switch between Classic and New chat (element role: button)
4. Go to home (element role: div)
5. Enable Claude (element role: button)
6. Global domain config (no per-JAR snapshot) — click to view (element role: span)
7. Upload WAR (element role: button)
8. Search this WAR (press /) (element role: button)
9. View application logs (element role: button)
10. Code Structure (element role: button, data-tab=structure)
11. Endpoint Flows (element role: button, data-tab=endpoints)
12. Summary (element role: button, data-tab=summary)
13. Export analysis data (element role: button)
14. REST API endpoints found in controller classes (element role: span)
15. Unique MongoDB collections accessed across all endpoints (element role: span)
16. MongoDB view collections (read-only aggregation pipelines) (element role: span)
17. Separate JAR modules/libraries detected in the call chain (element role: span)
18. Business domains identified from collection name prefixes (element role: span)


#### Distributed Transactions
![WAR Distributed Transactions](/api/docs/screenshots/war-summary-distributed-transactions.png)
- Cross-domain write detection, same as JAR.

**Callout legend (18 numbered markers in the screenshot above):**

1. Cross-domain write detection, same as JAR.
2. View application logs (element role: button)
3. Switch between Classic and New chat (element role: button)
4. Go to home (element role: div)
5. Enable Claude (element role: button)
6. Global domain config (no per-JAR snapshot) — click to view (element role: span)
7. Upload WAR (element role: button)
8. Search this WAR (press /) (element role: button)
9. View application logs (element role: button)
10. Code Structure (element role: button, data-tab=structure)
11. Endpoint Flows (element role: button, data-tab=endpoints)
12. Summary (element role: button, data-tab=summary)
13. Export analysis data (element role: button)
14. REST API endpoints found in controller classes (element role: span)
15. Unique MongoDB collections accessed across all endpoints (element role: span)
16. MongoDB view collections (read-only aggregation pipelines) (element role: span)
17. Separate JAR modules/libraries detected in the call chain (element role: span)
18. Business domains identified from collection name prefixes (element role: span)


#### Batch Jobs
![WAR Batch Jobs](/api/docs/screenshots/war-summary-batch-jobs.png)
- Batch/scheduler-classified endpoints, same as JAR.

**Callout legend (18 numbered markers in the screenshot above):**

1. Batch/scheduler-classified endpoints, same as JAR.
2. View application logs (element role: button)
3. Switch between Classic and New chat (element role: button)
4. Go to home (element role: div)
5. Enable Claude (element role: button)
6. Global domain config (no per-JAR snapshot) — click to view (element role: span)
7. Upload WAR (element role: button)
8. Search this WAR (press /) (element role: button)
9. View application logs (element role: button)
10. Code Structure (element role: button, data-tab=structure)
11. Endpoint Flows (element role: button, data-tab=endpoints)
12. Summary (element role: button, data-tab=summary)
13. Export analysis data (element role: button)
14. REST API endpoints found in controller classes (element role: span)
15. Unique MongoDB collections accessed across all endpoints (element role: span)
16. MongoDB view collections (read-only aggregation pipelines) (element role: span)
17. Separate JAR modules/libraries detected in the call chain (element role: span)
18. Business domains identified from collection name prefixes (element role: span)


#### Scheduled Jobs
![WAR Scheduled Jobs](/api/docs/screenshots/war-summary-scheduled-jobs.png)
- `@Scheduled` cron/fixed-rate/fixed-delay task report, same as JAR.

**Callout legend (18 numbered markers in the screenshot above):**

1. `@Scheduled` cron/fixed-rate/fixed-delay task report, same as JAR.
2. View application logs (element role: button)
3. Switch between Classic and New chat (element role: button)
4. Go to home (element role: div)
5. Enable Claude (element role: button)
6. Global domain config (no per-JAR snapshot) — click to view (element role: span)
7. Upload WAR (element role: button)
8. Search this WAR (press /) (element role: button)
9. View application logs (element role: button)
10. Code Structure (element role: button, data-tab=structure)
11. Endpoint Flows (element role: button, data-tab=endpoints)
12. Summary (element role: button, data-tab=summary)
13. Export analysis data (element role: button)
14. REST API endpoints found in controller classes (element role: span)
15. Unique MongoDB collections accessed across all endpoints (element role: span)
16. MongoDB view collections (read-only aggregation pipelines) (element role: span)
17. Separate JAR modules/libraries detected in the call chain (element role: span)
18. Business domains identified from collection name prefixes (element role: span)


#### Aggregation Flows
![WAR Aggregation Flows](/api/docs/screenshots/war-summary-aggregation-flows.png)
- MongoDB aggregation pipeline detection, same as JAR (via `GET /api/war/wars/{id}/summary/aggregation-flows`).

**Callout legend (18 numbered markers in the screenshot above):**

1. MongoDB aggregation pipeline detection, same as JAR (via `GET /api/war/wars/{id}/summary/aggregation-flows`).
2. View application logs (element role: button)
3. Switch between Classic and New chat (element role: button)
4. Go to home (element role: div)
5. Enable Claude (element role: button)
6. Global domain config (no per-JAR snapshot) — click to view (element role: span)
7. Upload WAR (element role: button)
8. Search this WAR (press /) (element role: button)
9. View application logs (element role: button)
10. Code Structure (element role: button, data-tab=structure)
11. Endpoint Flows (element role: button, data-tab=endpoints)
12. Summary (element role: button, data-tab=summary)
13. Export analysis data (element role: button)
14. REST API endpoints found in controller classes (element role: span)
15. Unique MongoDB collections accessed across all endpoints (element role: span)
16. MongoDB view collections (read-only aggregation pipelines) (element role: span)
17. Separate JAR modules/libraries detected in the call chain (element role: span)
18. Business domains identified from collection name prefixes (element role: span)


#### Dynamic Flows
![WAR Dynamic Flows](/api/docs/screenshots/war-summary-dynamic-flows.png)
- Non-direct dispatch pattern report, same as JAR (via `/summary/dynamic-flows`).

**Callout legend (18 numbered markers in the screenshot above):**

1. Non-direct dispatch pattern report, same as JAR (via `/summary/dynamic-flows`).
2. View application logs (element role: button)
3. Switch between Classic and New chat (element role: button)
4. Go to home (element role: div)
5. Enable Claude (element role: button)
6. Global domain config (no per-JAR snapshot) — click to view (element role: span)
7. Upload WAR (element role: button)
8. Search this WAR (press /) (element role: button)
9. View application logs (element role: button)
10. Code Structure (element role: button, data-tab=structure)
11. Endpoint Flows (element role: button, data-tab=endpoints)
12. Summary (element role: button, data-tab=summary)
13. Export analysis data (element role: button)
14. REST API endpoints found in controller classes (element role: span)
15. Unique MongoDB collections accessed across all endpoints (element role: span)
16. MongoDB view collections (read-only aggregation pipelines) (element role: span)
17. Separate JAR modules/libraries detected in the call chain (element role: span)
18. Business domains identified from collection name prefixes (element role: span)


#### Verticalisation
![WAR Verticalisation](/api/docs/screenshots/war-summary-verticalisation.png)
- Domain-boundary violation report, same as JAR.

**Callout legend (18 numbered markers in the screenshot above):**

1. Domain-boundary violation report, same as JAR.
2. View application logs (element role: button)
3. Switch between Classic and New chat (element role: button)
4. Go to home (element role: div)
5. Enable Claude (element role: button)
6. Global domain config (no per-JAR snapshot) — click to view (element role: span)
7. Upload WAR (element role: button)
8. Search this WAR (press /) (element role: button)
9. View application logs (element role: button)
10. Code Structure (element role: button, data-tab=structure)
11. Endpoint Flows (element role: button, data-tab=endpoints)
12. Summary (element role: button, data-tab=summary)
13. Export analysis data (element role: button)
14. REST API endpoints found in controller classes (element role: span)
15. Unique MongoDB collections accessed across all endpoints (element role: span)
16. MongoDB view collections (read-only aggregation pipelines) (element role: span)
17. Separate JAR modules/libraries detected in the call chain (element role: span)
18. Business domains identified from collection name prefixes (element role: span)


#### Claude Insights
![WAR Claude Insights](/api/docs/screenshots/war-summary-claude-insights.png)
- AI-powered per-endpoint analysis, same UI as JAR ("Start Full Scan" shown since no Claude data exists for this WAR yet).

**Callout legend (18 numbered markers in the screenshot above):**

1. AI-powered per-endpoint analysis, same UI as JAR ("Start Full Scan" shown since no Claude data exists for this WAR yet).
2. View application logs (element role: button)
3. Switch between Classic and New chat (element role: button)
4. Go to home (element role: div)
5. Enable Claude (element role: button)
6. Global domain config (no per-JAR snapshot) — click to view (element role: span)
7. Upload WAR (element role: button)
8. Search this WAR (press /) (element role: button)
9. View application logs (element role: button)
10. Code Structure (element role: button, data-tab=structure)
11. Endpoint Flows (element role: button, data-tab=endpoints)
12. Summary (element role: button, data-tab=summary)
13. Export analysis data (element role: button)
14. REST API endpoints found in controller classes (element role: span)
15. Unique MongoDB collections accessed across all endpoints (element role: span)
16. MongoDB view collections (read-only aggregation pipelines) (element role: span)
17. Separate JAR modules/libraries detected in the call chain (element role: span)
18. Business domains identified from collection name prefixes (element role: span)


#### Claude Corrections
![WAR Claude Corrections](/api/docs/screenshots/war-summary-claude-corrections.png)
- Static-vs-Claude collection comparison, same as JAR.

**Callout legend (18 numbered markers in the screenshot above):**

1. Static-vs-Claude collection comparison, same as JAR.
2. View application logs (element role: button)
3. Switch between Classic and New chat (element role: button)
4. Go to home (element role: div)
5. Enable Claude (element role: button)
6. Global domain config (no per-JAR snapshot) — click to view (element role: span)
7. Upload WAR (element role: button)
8. Search this WAR (press /) (element role: button)
9. View application logs (element role: button)
10. Code Structure (element role: button, data-tab=structure)
11. Endpoint Flows (element role: button, data-tab=endpoints)
12. Summary (element role: button, data-tab=summary)
13. Export analysis data (element role: button)
14. REST API endpoints found in controller classes (element role: span)
15. Unique MongoDB collections accessed across all endpoints (element role: span)
16. MongoDB view collections (read-only aggregation pipelines) (element role: span)
17. Separate JAR modules/libraries detected in the call chain (element role: span)
18. Business domains identified from collection name prefixes (element role: span)


---

## PL/SQL Parser

Two distinct front-ends exist for PL/SQL analysis: **`parser/`** (the current, actively developed UI) and **`plsql/`** (an older, standalone UI). They are **not the same app under a different route** — verified from code:

- Different script bundles: `parser/index.html` loads `router.js`, `history.js`, `callGraphViz.js`, `complexity.js`, `perfSummary.js`, `db-selector.js`, `conn-form*.js`, etc.; `plsql/index.html` loads a disjoint set (`predicates.js`, `triggerModal.js`, `summary-analyzer.js`, `summary-progress.js`, etc.) that does not exist under `parser/js/`.
- Different top-level tab layouts (Parser nests Sequences/Join Summary/Dynamic SQL/Transactions under one "Details" tab; `plsql` exposes Joins, Cursors, Predicates as separate flat top-level tabs, and has a standalone "Claude AI" tab that Parser lacks).
- Different, though converging, API surfaces: `plsql/js/api.js` calls `/api/plsql/...` natively but, when a parser-style analysis is loaded, transparently delegates most reads to `/api/parser/analyses/{name}/...` instead — i.e. `plsql/` is being kept alive as a legacy shell riding on the newer `parser/` backend where possible, while still owning its own routes for `/api/plsql/db/*` (ad-hoc DB browsing/querying), `/api/plsql/claude/*`, and `/api/plsql/config/projects/*` (connection/project CRUD).

Both UIs share the same **Analysis History** data pool but display *different* history entries in this environment — Parser's history shows the rich, fully-crawled analyses (e.g. `USP_RUNMRPCYCLE`, 55 procs / 337 edges), while the `plsql/` legacy history shows separate 0-procedure stub entries from ad-hoc single-object runs.

### Parser — Home screen

![Parser Home — Analysis History](/api/docs/screenshots/parser-home-history.png)

- Three home tabs (`data-htab`): **Analysis History** (`history`), **Performance Summary** (`perfSummary`), **Queue Analysis** (`queue`).
- New-analysis form: Connection dropdown + environment, Owner/schema selector, Target input (procedure/package name) + object-type dropdown, a "Fast" checkbox, and an **Analyze** button (`PA.startAnalysis()` → `POST /api/parser/analyze`).
- **Quick Submit** panel: multi-line textarea to batch-submit multiple `SCHEMA.PKG.PROC` entries at once ("Submit All" → `PA.quickSubmit.submitAll()`), which queues one analyze job per line and switches to the Queue tab.
- Analysis History cards: each shows entry point, schema, DB dialect badge, project/environment/connection badges, timestamp, and `{procs} | {tables} | {loc} LOC | {duration} | {errors}` — with **Load** (`PA.loadAnalysis`) and **Delete** (`DELETE /api/parser/analyses/{name}`) buttons.
- History search box and datasource/project filter controls, all client-side over `GET /api/parser/analyses` (paged) / `GET /api/parser/analyses/facets`.
- **Manage Connections** button (`PA.config.showModal()`) — connection CRUD modal supporting 5 database types: **Oracle, MySQL, SQL Server, IBM Db2, PostgreSQL** (`db-selector.js` `DB_DEFS`), each with its own field-set module (`conn-form-fields-{oracle,mysql,sqlserver,db2,postgres}.js`), common Username/Password/Role fields, and a connection-test action.
- Global top-bar buttons: **Logs**, **Clear Cache** (per connection/project/env), **Prompt Manager** (Claude prompt template editor), **Help**.

**Callout legend (18 numbered markers in the screenshot above):**

1. Three home tabs (`data-htab`): **Analysis History** (`history`), **Performance Summary** (`perfSummary`), **Queue Analysis** (`queue`).
2. New-analysis form: Connection dropdown + environment, Owner/schema selector, Target input (procedure/package name) + object-type dropdown, a "Fast" checkbox, and an **Analyze** button (`PA.startAnalysis()` → `POST /api/parser/analyze`).
3. panel: multi-line textarea to batch-submit multiple `SCHEMA.PKG.PROC` entries at once ("Submit All" → `PA.quickSubmit.submitAll()`), which queues one analyze job per line and switches to the Queue tab.
4. Analysis History cards: each shows entry point, schema, DB dialect badge, project/environment/connection badges, timestamp, and `{procs} | {tables} | {loc} LOC | {duration} | {errors}` — with **Load** (`PA.loadAnalysis`) and **Delete** (`DELETE /api/parser/analyses/{name}`) buttons.
5. History search box and datasource/project filter controls, all client-side over `GET /api/parser/analyses` (paged) / `GET /api/parser/analyses/facets`.
6. button (`PA.config.showModal()`) — connection CRUD modal supporting 5 database types: **Oracle, MySQL, SQL Server, IBM Db2, PostgreSQL** (`db-selector.js` `DB_DEFS`), each with its own field-set module (`conn-form-fields-{oracle,mysql,sqlserver,db2,postgres}.js`), common Username/Password/Role fields, and a connection-test action.
7. Global top-bar buttons: **Logs**, **Clear Cache** (per connection/project/env), **Prompt Manager** (Claude prompt template editor), **Help**.
8. Select environment (element role: select)
9. Auto-detect (all schemas) (element role: select)
10. Select a database first… (element role: input)
11. PACKAGE PROCEDURE FUNCTION (element role: select)
12. Analyze (element role: button)
13. What to select & type for the chosen database (element role: button)
14. Analysis History (element role: button, data-tab=history)
15. Performance Summary (element role: button, data-tab=perfSummary)
16. Queue Analysis (element role: button, data-tab=queue)
17. Search history... (element role: input)
18. All datasourcesOracleSQL ServerMySQLDb2PostgreSQL (element role: select)


#### Performance Summary
![Parser Performance Summary](/api/docs/screenshots/parser-home-performance-summary.png)
- Sortable table of past analysis runs by name/entry point/datasource/timestamp/duration, with pagination.

**Callout legend (18 numbered markers in the screenshot above):**

1. Sortable table of past analysis runs by name/entry point/datasource/timestamp/duration, with pagination.
2. View application logs (element role: button)
3. Manage & clear cache per connection (datasource / project /  (element role: button)
4. Manage Claude prompt templates & global extras (element role: button)
5. What to enter for each database before clicking Analyze (element role: button)
6. Select database Oracle MySQL SQL Server IBM Db2 PostgreSQL (element role: select)
7. Select projectOracleDemoMysqlDemoDb2DemoSqlServerDemoPostgre (element role: select)
8. Select environment (element role: select)
9. Auto-detect (all schemas) (element role: select)
10. Select a database first… (element role: input)
11. PACKAGE PROCEDURE FUNCTION (element role: select)
12. Analyze (element role: button)
13. What to select & type for the chosen database (element role: button)
14. Analysis History (element role: button, data-tab=history)
15. Performance Summary (element role: button, data-tab=perfSummary)
16. Queue Analysis (element role: button, data-tab=queue)
17. All datasourcesOracleSQL ServerMySQLDb2PostgreSQL (element role: select)
18. All projectsDb2DemoMysqlDemoOracleDemoPostgresDemoSqlServerD (element role: select)


#### Queue Analysis
![Parser Queue Analysis](/api/docs/screenshots/parser-home-queue-analysis.png)
- Live view of `GET /api/queue` jobs (the real progress-tracking endpoint), with per-job status and a Cancel action (`POST /api/queue/{jobId}/cancel`).

**Callout legend (18 numbered markers in the screenshot above):**

1. Live view of `GET /api/queue` jobs (the real progress-tracking endpoint), with per-job status and a Cancel action (`POST /api/queue/{jobId}/cancel`).
2. View application logs (element role: button)
3. Manage & clear cache per connection (datasource / project /  (element role: button)
4. Manage Claude prompt templates & global extras (element role: button)
5. What to enter for each database before clicking Analyze (element role: button)
6. Select database Oracle MySQL SQL Server IBM Db2 PostgreSQL (element role: select)
7. Select projectOracleDemoMysqlDemoDb2DemoSqlServerDemoPostgre (element role: select)
8. Select environment (element role: select)
9. Auto-detect (all schemas) (element role: select)
10. Select a database first… (element role: input)
11. PACKAGE PROCEDURE FUNCTION (element role: select)
12. Analyze (element role: button)
13. What to select & type for the chosen database (element role: button)
14. Analysis History (element role: button, data-tab=history)
15. Performance Summary (element role: button, data-tab=perfSummary)
16. Queue Analysis (element role: button, data-tab=queue)
17. Help & Documentation (element role: button)
18. Dark theme (element role: button)


### Parser — Analysis workspace

Once an analysis is loaded (`USP_RUNMRPCYCLE`, 55 procs / 19,213 LOC / depth 24 in this capture):

![Parser analysis opened](/api/docs/screenshots/parser-analysis-opened.png)

- Header stats: procs, LOC, call edges, internal/external calls, tables touched, depth, statement-type counts (IF/CASE/WHILE LOOP/SUBQUERY/CTE/MERGE/TRY-CATCH).
- Top action bar: **Static/Claude** view toggle, **Enable Claude** (verification trigger), **Chunks** (verification chunk viewer), **Sessions** (Claude session list), connection/environment badges, a live "✓ Connection" indicator.
- **Pull** (pull latest server data), **Refresh** (refresh current tab), and a global **Export** button (`PA.export.show()` → format buttons JSON/CSV/Excel(XLSX)/ZIP, `PA.export.download()`).
- Left panel: **Call Flow** tab (procedure/function tree, filterable via a text box) and **Tables** tab (all DB tables touched).

**Callout legend (18 numbered markers in the screenshot above):**

1. Header stats: procs, LOC, call edges, internal/external calls, tables touched, depth, statement-type counts (IF/CASE/WHILE LOOP/SUBQUERY/CTE/MERGE/TRY-CATCH).
2. Top action bar: **Static/Claude** view toggle, **Enable Claude** (verification trigger), **Chunks** (verification chunk viewer), **Sessions** (Claude session list), connection/environment badges, a live "✓ Connection" indicator.
3. (pull latest server data), **Refresh** (refresh current tab), and a global **Export** button (`PA.export.show()` → format buttons JSON/CSV/Excel(XLSX)/ZIP, `PA.export.download()`).
4. Left panel: **Call Flow** tab (procedure/function tree, filterable via a text box) and **Tables** tab (all DB tables touched).
5. Run Claude AI enrichment on this analysis (element role: button)
6. View verification chunks (element role: button)
7. View Claude sessions (element role: button)
8. Connected in 379ms (element role: span)
9. Pull latest data from server (element role: button)
10. Refresh current tab (element role: button)
11. Export analysis data (element role: button)
12. View application logs (element role: button)
13. Switch between Classic and New chat (element role: button)
14. Procedures and functions in the call tree, sorted by depth (element role: button, data-tab=procedures)
15. Database tables accessed across the analysis (element role: button, data-tab=tables)
16. ERP.USP_RUNMRPCYCLE.sql (element role: span)
17. Filter... (element role: input)
18. Direct: only THIS procedure/node — nothing it calls. (element role: button)


![Parser left panel — Tables](/api/docs/screenshots/parser-left-tables.png)

- Per-procedure detail header shows CREATE/DROP/INSERT/MERGE/SELECT/TRUNCATE/UPDATE operation badges and a scope selector (Direct/Subtree/SubFlow/Full).

**Callout legend (18 numbered markers in the screenshot above):**

1. Per-procedure detail header shows CREATE/DROP/INSERT/MERGE/SELECT/TRUNCATE/UPDATE operation badges and a scope selector (Direct/Subtree/SubFlow/Full).
2. Search this analysis (press /) (element role: button)
3. Static (element role: button)
4. Claude (element role: button)
5. Run Claude AI enrichment on this analysis (element role: button)
6. View verification chunks (element role: button)
7. View Claude sessions (element role: button)
8. Connected in 379ms (element role: span)
9. Pull latest data from server (element role: button)
10. Refresh current tab (element role: button)
11. Export analysis data (element role: button)
12. View application logs (element role: button)
13. Switch between Classic and New chat (element role: button)
14. Procedures and functions in the call tree, sorted by depth (element role: button, data-tab=procedures)
15. Database tables accessed across the analysis (element role: button, data-tab=tables)
16. ERP.USP_RUNMRPCYCLE.sql (element role: span)
17. Filter... (element role: input)
18. Direct: only THIS procedure/node — nothing it calls. (element role: button)


Seven right-panel top tabs (`data-view`):

#### Explore
![Explore — Hierarchy](/api/docs/screenshots/parser-right-explore.png)

**Callout legend (18 numbered markers in the screenshot above):**

1. Back to home (element role: button)
2. Search this analysis (press /) (element role: button)
3. Static (element role: button)
4. Claude (element role: button)
5. Run Claude AI enrichment on this analysis (element role: button)
6. View verification chunks (element role: button)
7. View Claude sessions (element role: button)
8. Connected in 379ms (element role: span)
9. Pull latest data from server (element role: button)
10. Refresh current tab (element role: button)
11. Export analysis data (element role: button)
12. View application logs (element role: button)
13. Switch between Classic and New chat (element role: button)
14. Procedures and functions in the call tree, sorted by depth (element role: button, data-tab=procedures)
15. Database tables accessed across the analysis (element role: button, data-tab=tables)
16. ERP.USP_RUNMRPCYCLE.sql (element role: span)
17. Filter... (element role: input)
18. Direct: only THIS procedure/node — nothing it calls. (element role: button)

Four sub-tabs (`data-exview`):
- **Hierarchy** — interactive call tree with "Make Root" per node, breadcrumb, Expand All / Collapse All, search box.
  ![Hierarchy](/api/docs/screenshots/parser-explore-hierarchy.png)
  *Callout legend (18 numbered markers): 1=Back to home; 2=Search this analysis (press /); 3=Static; 4=Claude; 5=Run Claude AI enrichment on th; 6=View verification chunks; 7=View Claude sessions; 8=Connected in 379ms; 9=Pull latest data from server; 10=Refresh current tab; 11=Export analysis data; 12=View application logs; 13=Switch between Classic and New; 14=Procedures and functions in th; 15=Database tables accessed acros; 16=ERP.USP_RUNMRPCYCLE.sql; 17=Filter...; 18=Direct: only THIS procedure/no*
- **Trace** — flat sequential call trace in execution order with depth bars; type filter (INTERNAL/EXTERNAL/TRIGGER/DYNAMIC/CIRCULAR); Unique toggle (collapses repeated calls with a count); Expand/Collapse All.
  ![Trace](/api/docs/screenshots/parser-explore-trace.png)
  *Callout legend (18 numbered markers): 1=Back to home; 2=Search this analysis (press /); 3=Static; 4=Claude; 5=Run Claude AI enrichment on th; 6=View verification chunks; 7=View Claude sessions; 8=Connected in 379ms; 9=Pull latest data from server; 10=Refresh current tab; 11=Export analysis data; 12=View application logs; 13=Switch between Classic and New; 14=Procedures and functions in th; 15=Database tables accessed acros; 16=ERP.USP_RUNMRPCYCLE.sql; 17=Filter...; 18=Direct: only THIS procedure/no*
- **References** — reverse-dependency lookup ("Calls" sub-view active; "Called By"/"Tables" variants exist in markup but are commented out/disabled); search box and scope-filter pills; Clear filters.
  ![References](/api/docs/screenshots/parser-explore-references.png)
  *Callout legend (18 numbered markers): 1=Back to home; 2=Search this analysis (press /); 3=Static; 4=Claude; 5=Run Claude AI enrichment on th; 6=View verification chunks; 7=View Claude sessions; 8=Connected in 379ms; 9=Pull latest data from server; 10=Refresh current tab; 11=Export analysis data; 12=View application logs; 13=Switch between Classic and New; 14=Procedures and functions in th; 15=Database tables accessed acros; 16=ERP.USP_RUNMRPCYCLE.sql; 17=Filter...; 18=Direct: only THIS procedure/no*
- **Transactions** — transaction-boundary trace with table-level pagination; row click jumps to source at the exact line (`PA.sourceView.openNodeAtLine`).
  ![Transactions](/api/docs/screenshots/parser-explore-transactions.png)

#### Table Ops
![Table Ops](/api/docs/screenshots/parser-right-table-ops.png)
- All table accesses across the analysis with operation-type filter pills, search box, live total/filtered counts, and pagination.

**Callout legend (18 numbered markers in the screenshot above):**

1. All table accesses across the analysis with operation-type filter pills, search box, live total/filtered counts, and pagination.
2. Search this analysis (press /) (element role: button)
3. Static (element role: button)
4. Claude (element role: button)
5. Run Claude AI enrichment on this analysis (element role: button)
6. View verification chunks (element role: button)
7. View Claude sessions (element role: button)
8. Connected in 379ms (element role: span)
9. Pull latest data from server (element role: button)
10. Refresh current tab (element role: button)
11. Export analysis data (element role: button)
12. View application logs (element role: button)
13. Switch between Classic and New chat (element role: button)
14. Procedures and functions in the call tree, sorted by depth (element role: button, data-tab=procedures)
15. Database tables accessed across the analysis (element role: button, data-tab=tables)
16. ERP.USP_RUNMRPCYCLE.sql (element role: span)
17. Filter... (element role: input)
18. Direct: only THIS procedure/node — nothing it calls. (element role: button)


#### Details
![Details](/api/docs/screenshots/parser-right-details.png)

**Callout legend (18 numbered markers in the screenshot above):**

1. Back to home (element role: button)
2. Search this analysis (press /) (element role: button)
3. Static (element role: button)
4. Claude (element role: button)
5. Run Claude AI enrichment on this analysis (element role: button)
6. View verification chunks (element role: button)
7. View Claude sessions (element role: button)
8. Connected in 379ms (element role: span)
9. Pull latest data from server (element role: button)
10. Refresh current tab (element role: button)
11. Export analysis data (element role: button)
12. View application logs (element role: button)
13. Switch between Classic and New chat (element role: button)
14. Procedures and functions in the call tree, sorted by depth (element role: button, data-tab=procedures)
15. Database tables accessed across the analysis (element role: button, data-tab=tables)
16. ERP.USP_RUNMRPCYCLE.sql (element role: span)
17. Filter... (element role: input)
18. Direct: only THIS procedure/node — nothing it calls. (element role: button)

Four sub-tabs (`data-dtview`):
- **Sequences** — sequence usage report; operation filter pills; scope selector; sortable columns (Sequence, Schema, Usages, Procedures).
  ![Sequences](/api/docs/screenshots/parser-details-sequences.png)
  *Callout legend (18 numbered markers): 1=Back to home; 2=Search this analysis (press /); 3=Static; 4=Claude; 5=Run Claude AI enrichment on th; 6=View verification chunks; 7=View Claude sessions; 8=Connected in 379ms; 9=Pull latest data from server; 10=Refresh current tab; 11=Export analysis data; 12=View application logs; 13=Switch between Classic and New; 14=Procedures and functions in th; 15=Database tables accessed acros; 16=ERP.USP_RUNMRPCYCLE.sql; 17=Filter...; 18=Direct: only THIS procedure/no*
- **Join Summary** — table-join relationships per query: Left Table, Right Table, Join Type, Predicates, Procedure (`GET /api/parser/analyses/{name}/joins/summary/queries`).
  ![Join Summary](/api/docs/screenshots/parser-details-join-summary.png)
  *Callout legend (18 numbered markers): 1=Back to home; 2=Search this analysis (press /); 3=Static; 4=Claude; 5=Run Claude AI enrichment on th; 6=View verification chunks; 7=View Claude sessions; 8=Connected in 379ms; 9=Pull latest data from server; 10=Refresh current tab; 11=Export analysis data; 12=View application logs; 13=Switch between Classic and New; 14=Procedures and functions in th; 15=Database tables accessed acros; 16=ERP.USP_RUNMRPCYCLE.sql; 17=Filter...; 18=Direct: only THIS procedure/no*
- **Dynamic SQL** — detected dynamically-built SQL expressions with a Copy-to-clipboard action and links to source/procedure.
  ![Dynamic SQL](/api/docs/screenshots/parser-details-dynamic-sql.png)
- **Transactions** — commit/rollback/savepoint trace, row-click jumps to source.
  ![Transactions](/api/docs/screenshots/parser-details-transactions.png)

#### Summary
![Summary — Dashboard](/api/docs/screenshots/parser-right-summary.png)

**Callout legend (18 numbered markers in the screenshot above):**

1. Back to home (element role: button)
2. Search this analysis (press /) (element role: button)
3. Static (element role: button)
4. Claude (element role: button)
5. Run Claude AI enrichment on this analysis (element role: button)
6. View verification chunks (element role: button)
7. View Claude sessions (element role: button)
8. Connected in 379ms (element role: span)
9. Pull latest data from server (element role: button)
10. Refresh current tab (element role: button)
11. Export analysis data (element role: button)
12. View application logs (element role: button)
13. Switch between Classic and New chat (element role: button)
14. Procedures and functions in the call tree, sorted by depth (element role: button, data-tab=procedures)
15. Database tables accessed across the analysis (element role: button, data-tab=tables)
16. ERP.USP_RUNMRPCYCLE.sql (element role: span)
17. Filter... (element role: input)
18. Direct: only THIS procedure/node — nothing it calls. (element role: button)

Three sub-tabs (`data-sumview`):
- **Dashboard** — aggregated stats: scope indicator, Object Types breakdown, Schemas breakdown, Highlights (Most LOC / Deepest / Most Tables / Most Calls), and Transaction Scope counts (Commits/Rollbacks/Savepoints/Autonomous).
- **Claude Insights** (`data-sumview="claudeInsights"`) — AI verification insight report for this analysis.
  ![Claude Insights](/api/docs/screenshots/parser-summary-claude-insights.png)
  *Callout legend (18 numbered markers): 1=Back to home; 2=Search this analysis (press /); 3=Static; 4=Claude; 5=Run Claude AI enrichment on th; 6=View verification chunks; 7=View Claude sessions; 8=Connected in 379ms; 9=Pull latest data from server; 10=Refresh current tab; 11=Export analysis data; 12=View application logs; 13=Switch between Classic and New; 14=Procedures and functions in th; 15=Database tables accessed acros; 16=ERP.USP_RUNMRPCYCLE.sql; 17=Filter...; 18=Direct: only THIS procedure/no*
- **Claude Corrections** (`data-sumview="claudeCorrections"`) — accept/reject-per-finding correction workflow: Toggle All, Refresh, Accept All, Reject All, Apply Accepted buttons.
  ![Claude Corrections](/api/docs/screenshots/parser-summary-claude-corrections.png)

#### Complexity
![Complexity](/api/docs/screenshots/parser-right-complexity.png)
- Per-procedure complexity scoring (LOC, table count, call count) with risk-level filter pills/cards (e.g. High/Medium/Low) and sortable columns.

**Callout legend (18 numbered markers in the screenshot above):**

1. Per-procedure complexity scoring (LOC, table count, call count) with risk-level filter pills/cards (e.g. High/Medium/Low) and sortable columns.
2. Search this analysis (press /) (element role: button)
3. Static (element role: button)
4. Claude (element role: button)
5. Run Claude AI enrichment on this analysis (element role: button)
6. View verification chunks (element role: button)
7. View Claude sessions (element role: button)
8. Connected in 379ms (element role: span)
9. Pull latest data from server (element role: button)
10. Refresh current tab (element role: button)
11. Export analysis data (element role: button)
12. View application logs (element role: button)
13. Switch between Classic and New chat (element role: button)
14. Procedures and functions in the call tree, sorted by depth (element role: button, data-tab=procedures)
15. Database tables accessed across the analysis (element role: button, data-tab=tables)
16. ERP.USP_RUNMRPCYCLE.sql (element role: span)
17. Filter... (element role: input)
18. Direct: only THIS procedure/node — nothing it calls. (element role: button)


#### Graph
![Graph](/api/docs/screenshots/parser-right-graph.png)
- Visual call-graph diagram; toolbar: Back, Forward, Zoom In, Zoom Out, Fit All, Reset (layout), Show All.

**Callout legend (18 numbered markers in the screenshot above):**

1. Visual call-graph diagram; toolbar: Back, Forward, Zoom In, Zoom Out, Fit All, Reset (layout), Show All.
2. Search this analysis (press /) (element role: button)
3. Static (element role: button)
4. Claude (element role: button)
5. Run Claude AI enrichment on this analysis (element role: button)
6. View verification chunks (element role: button)
7. View Claude sessions (element role: button)
8. Connected in 379ms (element role: span)
9. Pull latest data from server (element role: button)
10. Refresh current tab (element role: button)
11. Export analysis data (element role: button)
12. View application logs (element role: button)
13. Switch between Classic and New chat (element role: button)
14. Procedures and functions in the call tree, sorted by depth (element role: button, data-tab=procedures)
15. Database tables accessed across the analysis (element role: button, data-tab=tables)
16. ERP.USP_RUNMRPCYCLE.sql (element role: span)
17. Filter... (element role: input)
18. Direct: only THIS procedure/node — nothing it calls. (element role: button)


#### Source
![Source](/api/docs/screenshots/parser-right-source.png)
- Syntax-highlighted PL/SQL source viewer, opened via `PA.sourceView.openAtLine(file, line)` from any row elsewhere in the app.

**Callout legend (18 numbered markers in the screenshot above):**

1. Syntax-highlighted PL/SQL source viewer, opened via `PA.sourceView.openAtLine(file, line)` from any row elsewhere in the app.
2. Search this analysis (press /) (element role: button)
3. Static (element role: button)
4. Claude (element role: button)
5. Run Claude AI enrichment on this analysis (element role: button)
6. View verification chunks (element role: button)
7. View Claude sessions (element role: button)
8. Connected in 379ms (element role: span)
9. Pull latest data from server (element role: button)
10. Refresh current tab (element role: button)
11. Export analysis data (element role: button)
12. View application logs (element role: button)
13. Switch between Classic and New chat (element role: button)
14. Procedures and functions in the call tree, sorted by depth (element role: button, data-tab=procedures)
15. Database tables accessed across the analysis (element role: button, data-tab=tables)
16. ERP.USP_RUNMRPCYCLE.sql (element role: span)
17. Filter... (element role: input)
18. Direct: only THIS procedure/node — nothing it calls. (element role: button)


### Export modal
![Export modal](/api/docs/screenshots/parser-export-modal.png)
- Format buttons: **JSON**, **CSV**, **Excel (XLSX)**, **ZIP** (`PA.export.setFormat`), then **Download** (`PA.export.download()`).

**Callout legend (18 numbered markers in the screenshot above):**

1. Format buttons: **JSON**, **CSV**, **Excel (XLSX)**, **ZIP** (`PA.export.setFormat`), then **Download** (`PA.export.download()`).
2. Back to home (element role: button)
3. Search this analysis (press /) (element role: button)
4. Static (element role: button)
5. Claude (element role: button)
6. Run Claude AI enrichment on this analysis (element role: button)
7. View verification chunks (element role: button)
8. View Claude sessions (element role: button)
9. Export Analysis × USP_RUNMRPCYCLE&var(--badge-teal); 55 proc (element role: div)
10. Connected in 379ms (element role: span)
11. Pull latest data from server (element role: button)
12. Refresh current tab (element role: button)
13. Export analysis data (element role: button)
14. View application logs (element role: button)
15. Switch between Classic and New chat (element role: button)
16. × (element role: button)
17. Procedures and functions in the call tree, sorted by depth (element role: button, data-tab=procedures)
18. Database tables accessed across the analysis (element role: button, data-tab=tables)


---

### `plsql/` — legacy PL/SQL analyzer

#### Home / Analysis History
![PLSQL Home](/api/docs/screenshots/plsql-home-history.png)
- Same 3-step new-analysis form pattern (Connection/Owner/Target) plus **Jobs**, **Claude Sessions**, and a 3-way **History / Performance / Queue** tab set — but this is `plsql/`'s own independent rendering (separate JS module from Parser's `history.js`), and in this environment shows different (stub, 0-procedure) history entries than Parser's history.

**Callout legend (18 numbered markers in the screenshot above):**

1. Same 3-step new-analysis form pattern (Connection/Owner/Target) plus **Jobs**, **Claude Sessions**, and a 3-way **History / Performance / Queue** tab set — but this is `plsql/`'s own independent rendering (separate JS module from Parser's `history.js`), and in this environment shows different (stub, 0-procedure) history entries than Parser's history.
2. ⚙ Manage Connections (element role: button)
3. Legacy (global)OracleDemoMysqlDemoDb2DemoSqlServerDemoPostgr (element role: select)
4. Select env (element role: select)
5. Auto-detect (all schemas)analyzer (element role: select)
6. e.g. PKG_CUSTOMER or PKG.PROC_NAME (element role: input)
7. PACKAGE PROCEDURE FUNCTION TRIGGER (element role: select)
8. on (element role: input)
9. Analyze (element role: button)
10. 0 (element role: span)
11. History 15 (element role: button)
12. 15 (element role: span)
13. Performance (element role: button)
14. Queue (element role: button)
15. Search jobs... (element role: input)
16. Search history... (element role: input)
17. ERP.No AI 9/13/2026, 1:34:23 PM 0 procs | 0 tables | 0 seqs  (element role: div)
18. No Claude analysis yet (element role: span)


#### Analysis workspace
![PLSQL analysis opened](/api/docs/screenshots/plsql-analysis-opened.png)
- Left panel: **Call Flow** / **Tables** tabs, same pattern as Parser.
- Ten top-level right tabs (`data-view`), flatter than Parser's nested structure:

**Callout legend (18 numbered markers in the screenshot above):**

1. Left panel: **Call Flow** / **Tables** tabs, same pattern as Parser.
2. Ten top-level right tabs (`data-view`), flatter than Parser's nested structure:
3. View application logs (element role: button)
4. View Claude AI sessions (element role: button)
5. Run Claude AI verification on this analysis (element role: button)
6. Search this analysis (press /) (element role: button)
7. Switch between Classic and New chat (element role: button)
8. Procedures and functions in the call flow — click to view de (element role: button, data-tab=procedures)
9. Database tables accessed in this analysis — click to view ta (element role: button, data-tab=tables)
10. Interactive call tree — click row to see details, double-cli (element role: button, data-tab=callTrace)
11. Flat sequential trace — all calls in execution order with de (element role: button, data-tab=trace)
12. Who calls this procedure? Reverse dependency lookup (element role: button, data-tab=refs)
13. All database tables accessed in this flow with operation typ (element role: button, data-tab=tableOps)
14. JOIN relationships between tables — join types, ON predicate (element role: button, data-tab=joins)
15. Cursor declarations and usage — explicit, REF, FOR LOOP curs (element role: button, data-tab=cursors)
16. WHERE clause predicates — filter conditions per table with c (element role: button, data-tab=predicates)
17. PL/SQL source code viewer — shows focused proc view with pac (element role: button, data-tab=source)
18. Claude AI verification — validates static analysis with AI r (element role: button, data-tab=claude)


| Tab | Screenshot | What it shows |
|---|---|---|
| Explore (`callTrace`) | ![Explore](/api/docs/screenshots/plsql-explore.png) | Interactive call tree, click row for details, double-click drills down, click name opens source |
| Trace | ![Trace](/api/docs/screenshots/plsql-trace.png) | Flat sequential trace in execution order with depth bars |
| References | ![References](/api/docs/screenshots/plsql-references.png) | Reverse dependency lookup — who calls this procedure |
| Table Operations | ![Table Ops](/api/docs/screenshots/plsql-table-operations.png) | All tables accessed with operation types and triggering procs |
| Joins | ![Joins](/api/docs/screenshots/plsql-joins.png) | JOIN relationships — join types, ON predicates, procedures; join-type filter pills |
| Cursors | ![Cursors](/api/docs/screenshots/plsql-cursors.png) | Cursor declarations/usage — explicit, REF, FOR LOOP cursors; type filter pills |
| Predicates | ![Predicates](/api/docs/screenshots/plsql-predicates.png) | WHERE-clause predicates per table with column detail — a tab that exists **only** in `plsql/`, not in `parser/` |
| Source | ![Source](/api/docs/screenshots/plsql-source.png) | PL/SQL source viewer with focused-procedure view and package variables |
| Claude AI | ![Claude AI](/api/docs/screenshots/plsql-claude-ai.png) | Standalone AI-verification tab (Start Verification button) — a top-level tab here, whereas Parser folds the equivalent into Summary sub-tabs |
| Summary | ![Summary dashboard](/api/docs/screenshots/plsql-summary-dashboard.png) | 3-way inner view selector: **Procedures**, **Static Analysis**, **Claude AI** |


**Callout legends for the table above (numbers correspond to the numbered markers drawn on each screenshot):**

- `plsql-explore.png`: 1=Back to home; 2=Export; 3=View application logs; 4=View Claude AI sessions; 5=Run Claude AI verification on ; 6=Search this analysis (press /); 7=Switch between Classic and New; 8=Procedures and functions in th; 9=Database tables accessed in th; 10=Interactive call tree — click ; 11=Flat sequential trace — all ca; 12=Who calls this procedure? Reve; 13=All database tables accessed i; 14=JOIN relationships between tab; 15=Cursor declarations and usage ; 16=WHERE clause predicates — filt; 17=PL/SQL source code viewer — sh; 18=Claude AI verification — valid
- `plsql-trace.png`: 1=Back to home; 2=Export; 3=View application logs; 4=View Claude AI sessions; 5=Run Claude AI verification on ; 6=Search this analysis (press /); 7=Switch between Classic and New; 8=Procedures and functions in th; 9=Database tables accessed in th; 10=Interactive call tree — click ; 11=Flat sequential trace — all ca; 12=Who calls this procedure? Reve; 13=All database tables accessed i; 14=JOIN relationships between tab; 15=Cursor declarations and usage ; 16=WHERE clause predicates — filt; 17=PL/SQL source code viewer — sh; 18=Claude AI verification — valid
- `plsql-references.png`: 1=Back to home; 2=Export; 3=View application logs; 4=View Claude AI sessions; 5=Run Claude AI verification on ; 6=Search this analysis (press /); 7=Switch between Classic and New; 8=Procedures and functions in th; 9=Database tables accessed in th; 10=Interactive call tree — click ; 11=Flat sequential trace — all ca; 12=Who calls this procedure? Reve; 13=All database tables accessed i; 14=JOIN relationships between tab; 15=Cursor declarations and usage ; 16=WHERE clause predicates — filt; 17=PL/SQL source code viewer — sh; 18=Claude AI verification — valid
- `plsql-table-operations.png`: 1=Back to home; 2=Export; 3=View application logs; 4=View Claude AI sessions; 5=Run Claude AI verification on ; 6=Search this analysis (press /); 7=Switch between Classic and New; 8=Procedures and functions in th; 9=Database tables accessed in th; 10=Interactive call tree — click ; 11=Flat sequential trace — all ca; 12=Who calls this procedure? Reve; 13=All database tables accessed i; 14=JOIN relationships between tab; 15=Cursor declarations and usage ; 16=WHERE clause predicates — filt; 17=PL/SQL source code viewer — sh; 18=Claude AI verification — valid
- `plsql-joins.png`: 1=Back to home; 2=Export; 3=View application logs; 4=View Claude AI sessions; 5=Run Claude AI verification on ; 6=Search this analysis (press /); 7=Switch between Classic and New; 8=Procedures and functions in th; 9=Database tables accessed in th; 10=Interactive call tree — click ; 11=Flat sequential trace — all ca; 12=Who calls this procedure? Reve; 13=All database tables accessed i; 14=JOIN relationships between tab; 15=Cursor declarations and usage ; 16=WHERE clause predicates — filt; 17=PL/SQL source code viewer — sh; 18=Claude AI verification — valid
- `plsql-cursors.png`: 1=Back to home; 2=Export; 3=View application logs; 4=View Claude AI sessions; 5=Run Claude AI verification on ; 6=Search this analysis (press /); 7=Switch between Classic and New; 8=Procedures and functions in th; 9=Database tables accessed in th; 10=Interactive call tree — click ; 11=Flat sequential trace — all ca; 12=Who calls this procedure? Reve; 13=All database tables accessed i; 14=JOIN relationships between tab; 15=Cursor declarations and usage ; 16=WHERE clause predicates — filt; 17=PL/SQL source code viewer — sh; 18=Claude AI verification — valid
- `plsql-predicates.png`: 1=Back to home; 2=Export; 3=View application logs; 4=View Claude AI sessions; 5=Run Claude AI verification on ; 6=Search this analysis (press /); 7=Switch between Classic and New; 8=Procedures and functions in th; 9=Database tables accessed in th; 10=Interactive call tree — click ; 11=Flat sequential trace — all ca; 12=Who calls this procedure? Reve; 13=All database tables accessed i; 14=JOIN relationships between tab; 15=Cursor declarations and usage ; 16=WHERE clause predicates — filt; 17=PL/SQL source code viewer — sh; 18=Claude AI verification — valid
- `plsql-source.png`: 1=Back to home; 2=Export; 3=View application logs; 4=View Claude AI sessions; 5=Run Claude AI verification on ; 6=Search this analysis (press /); 7=Switch between Classic and New; 8=Procedures and functions in th; 9=Database tables accessed in th; 10=Interactive call tree — click ; 11=Flat sequential trace — all ca; 12=Who calls this procedure? Reve; 13=All database tables accessed i; 14=JOIN relationships between tab; 15=Cursor declarations and usage ; 16=WHERE clause predicates — filt; 17=PL/SQL source code viewer — sh; 18=Claude AI verification — valid
- `plsql-claude-ai.png`: 1=Back to home; 2=Export; 3=View application logs; 4=View Claude AI sessions; 5=Run Claude AI verification on ; 6=Search this analysis (press /); 7=Switch between Classic and New; 8=Procedures and functions in th; 9=Database tables accessed in th; 10=Interactive call tree — click ; 11=Flat sequential trace — all ca; 12=Who calls this procedure? Reve; 13=All database tables accessed i; 14=JOIN relationships between tab; 15=Cursor declarations and usage ; 16=WHERE clause predicates — filt; 17=PL/SQL source code viewer — sh; 18=Claude AI verification — valid
- `plsql-summary-dashboard.png`: 1=Back to home; 2=Export; 3=View application logs; 4=View Claude AI sessions; 5=Run Claude AI verification on ; 6=Search this analysis (press /); 7=Switch between Classic and New; 8=Procedures and functions in th; 9=Database tables accessed in th; 10=Interactive call tree — click ; 11=Flat sequential trace — all ca; 12=Who calls this procedure? Reve; 13=All database tables accessed i; 14=JOIN relationships between tab; 15=Cursor declarations and usage ; 16=WHERE clause predicates — filt; 17=PL/SQL source code viewer — sh; 18=Claude AI verification — valid

- **Summary → Procedures** view: sortable/paged procedure report table (Procedure, Schema, Package, Type, Tables, LOC, Complexity, Cross-Schema, Operations, and per-row **Details/Trace/Explore** action buttons); free-text filter box + schema dropdown + Clear.
- **Summary → Static Analysis** view exposes 5 further sub-tabs (`data-stab`), each a real feature distinct from Parser's set:
  - **Table Analysis** — consolidated view of all tables. ![Table Analysis](/api/docs/screenshots/plsql-summary-table-analysis.png)
  *Callout legend (18 numbered markers): 1=Back to home; 2=Export; 3=View application logs; 4=View Claude AI sessions; 5=Run Claude AI verification on ; 6=Search this analysis (press /); 7=Switch between Classic and New; 8=Procedures and functions in th; 9=Database tables accessed in th; 10=Interactive call tree — click ; 11=Flat sequential trace — all ca; 12=Who calls this procedure? Reve; 13=All database tables accessed i; 14=JOIN relationships between tab; 15=Cursor declarations and usage ; 16=WHERE clause predicates — filt; 17=PL/SQL source code viewer — sh; 18=Claude AI verification — valid*
  - **Transactions** — transaction-management analysis. ![Transactions](/api/docs/screenshots/plsql-summary-transactions.png)
  *Callout legend (18 numbered markers): 1=Back to home; 2=Export; 3=View application logs; 4=View Claude AI sessions; 5=Run Claude AI verification on ; 6=Search this analysis (press /); 7=Switch between Classic and New; 8=Procedures and functions in th; 9=Database tables accessed in th; 10=Interactive call tree — click ; 11=Flat sequential trace — all ca; 12=Who calls this procedure? Reve; 13=All database tables accessed i; 14=JOIN relationships between tab; 15=Cursor declarations and usage ; 16=WHERE clause predicates — filt; 17=PL/SQL source code viewer — sh; 18=Claude AI verification — valid*
  - **Schema Slice** — schema-ownership mapping. ![Schema Slice](/api/docs/screenshots/plsql-summary-schema-slice.png)
  *Callout legend (18 numbered markers): 1=Back to home; 2=Export; 3=View application logs; 4=View Claude AI sessions; 5=Run Claude AI verification on ; 6=Search this analysis (press /); 7=Switch between Classic and New; 8=Procedures and functions in th; 9=Database tables accessed in th; 10=Interactive call tree — click ; 11=Flat sequential trace — all ca; 12=Who calls this procedure? Reve; 13=All database tables accessed i; 14=JOIN relationships between tab; 15=Cursor declarations and usage ; 16=WHERE clause predicates — filt; 17=PL/SQL source code viewer — sh; 18=Claude AI verification — valid*
  - **External Deps** — cross-schema dependency report. ![External Deps](/api/docs/screenshots/plsql-summary-external-deps.png)
  *Callout legend (18 numbered markers): 1=Back to home; 2=Export; 3=View application logs; 4=View Claude AI sessions; 5=Run Claude AI verification on ; 6=Search this analysis (press /); 7=Switch between Classic and New; 8=Procedures and functions in th; 9=Database tables accessed in th; 10=Interactive call tree — click ; 11=Flat sequential trace — all ca; 12=Who calls this procedure? Reve; 13=All database tables accessed i; 14=JOIN relationships between tab; 15=Cursor declarations and usage ; 16=WHERE clause predicates — filt; 17=PL/SQL source code viewer — sh; 18=Claude AI verification — valid*
  - **Batch Jobs** — `DBMS_SCHEDULER`/`DBMS_JOB` pattern detection. ![Batch Jobs](/api/docs/screenshots/plsql-summary-batch-jobs.png)
  *Callout legend (18 numbered markers): 1=Back to home; 2=Export; 3=View application logs; 4=View Claude AI sessions; 5=Run Claude AI verification on ; 6=Search this analysis (press /); 7=Switch between Classic and New; 8=Procedures and functions in th; 9=Database tables accessed in th; 10=Interactive call tree — click ; 11=Flat sequential trace — all ca; 12=Who calls this procedure? Reve; 13=All database tables accessed i; 14=JOIN relationships between tab; 15=Cursor declarations and usage ; 16=WHERE clause predicates — filt; 17=PL/SQL source code viewer — sh; 18=Claude AI verification — valid*
- **Summary → Claude AI** view: AI verification summary/progress panel. ![Claude AI view](/api/docs/screenshots/plsql-summary-claude-ai-view.png)
  *Callout legend (18 numbered markers): 1=Back to home; 2=Export; 3=View application logs; 4=View Claude AI sessions; 5=Run Claude AI verification on ; 6=Search this analysis (press /); 7=Switch between Classic and New; 8=Procedures and functions in th; 9=Database tables accessed in th; 10=Interactive call tree — click ; 11=Flat sequential trace — all ca; 12=Who calls this procedure? Reve; 13=All database tables accessed i; 14=JOIN relationships between tab; 15=Cursor declarations and usage ; 16=WHERE clause predicates — filt; 17=PL/SQL source code viewer — sh; 18=Claude AI verification — valid*
- Top action bar: **Claude Sessions**, **Enable Claude**, **Search**, **Export**, and a Chat toggle — mirroring Parser's equivalents but as `plsql/`'s own independent implementation.

**Conclusion on `plsql/` vs `parser/`:** they are genuinely distinct front-ends with different tab taxonomies (`plsql/` has a standalone Predicates tab and a standalone Claude AI tab that Parser lacks; Parser has Complexity and a full-screen Graph visualizer that `plsql/` lacks), but they increasingly share the same analysis data — `plsql/js/api.js` transparently reads from `/api/parser/analyses/{name}/...` when a parser-style analysis name is available, falling back to its own native `/api/plsql/analysis/...` routes only for legacy/ad-hoc runs.
