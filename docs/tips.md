# Tips & Quick Reference

Practical shortcuts and best practices for daily use of both analyzers.

---

## JAR / WAR Analyzer

### JAR vs WAR

- WAR files have their **own page at `/war/`** (own tool card on the Home Dashboard) — don't
  look for a WAR upload option on the `/jar/` page, they're separate now
- The WAR page reuses almost all of the JAR page's rendering — if something looks/works
  identically to JAR, that's expected; it's the same underlying JS
- Global Search (see below) is **not** available on the `/war/` page today, only on `/jar/`

### Global Search (JAR / PL/SQL / Parser)

- Press **`/`** or click **Search** in the top bar to open Global Search — same 4-segment
  layout (search bar, pagination bar, results table) on all three pages that have it
- Use the **Aa / ab / .\*** toggles for case-sensitive, whole-word, or regex matching
- In the PL/SQL / Parser search results, a table row's **kind badge** (Local Temp / Global
  Temp / Table Var) tells you at a glance whether it's a real table or a SQL Server
  temp table / table variable before you click into it

### Navigation in the Decompile Popup

| Situation | What to do |
|-----------|-----------|
| Method call is a concrete `@Service` field call | Click the method name — it links directly to that class |
| Method call shows a colored underline | Hover to see resolution type (QUALIFIED / HEURISTIC / AMBIGUOUS) |
| AMBIGUOUS link (orange) | Click to open the implementation picker and choose which impl to open |
| Multiple calls on one line | Each method name is independently clickable |
| Call to an interface with one impl | Click — redirects automatically to the single implementation |
| Call to an interface with multiple impls | Click — opens an impl picker listing all options |
| Want to go back after navigating | Use the **Back** button in the decompile modal header |
| Want to see the breadcrumb path | It updates live in the modal header as you drill in |

### Endpoint Flows — Fastest Ways to Explore

- Click an endpoint in the left pane to see its full call chain on the right
- Use **Flat Trace** for a Dynatrace-style depth-indented tree of all method calls
- Use **Node Navigator** to step through the tree node-by-node (good for deep chains)
- Use **Interactive Explorer** for a drill-down breadcrumb approach — click any child to descend
- Filter the endpoint list by HTTP method, path substring, or controller name using the search box

### Summary Tab — Getting the Most Out of It

- The Summary tab renders on first click — give it a moment for large JARs
- Click any **stat chip** in the top bar (Endpoints, Collections, Domains…) to see a detail popup
- Use **domain filter pills** in the Endpoint Report to scope down to one team's endpoints
- Use the **advanced filters panel** for range sliders on DB ops, LOC, collection count etc.
- The **Verticalisation tab** is the fastest way to spot domain boundary violations before splitting into microservices
- The **Aggregation Flows tab** highlights all `$lookup` stages in red when they cross domain boundaries

### Export

- Opening the Export modal always builds all report data first — even if you skipped those tabs
- Use **per-endpoint export** (export icon on an endpoint card) to generate a focused report for one feature
- Use **per-collection export** (export icon in Collection Analysis) for a domain-level view
- **Excel (Backend)** produces the best formatting — use it for stakeholder reports
- **JSON** is useful for feeding results into other tools or diffing analysis runs

### Claude Enrichment

- Run Claude after static analysis is complete — not during
- The **CORRECTED** badge means Claude corrections are merged; use **View Static** to compare with raw bytecode results
- After corrections: **Re-scan Claude** runs a fresh pass — useful when code changed since last scan
- The **Correction Log Browser** sub-tab shows every raw Claude response for debugging unexpected changes
- Progress bar in the header shows % of endpoints enriched; large JARs may take 10–30 minutes

### Code Structure

- **Package View** is best for navigating by domain package
- **Project View** mirrors what an IDE would show — good for finding files quickly
- **Visual Overview** groups by Spring stereotype — useful for a quick headcount of controllers, services, repositories
- The **JAR Dependencies** section (bottom of left panel) shows only libraries actually called by app code — not every bundled JAR
- Click any class name in the **JAR Dependencies** section to jump straight to that class in the tree

---

## PL/SQL Analyzer

### Running an Analysis

| Goal | How |
|------|-----|
| Analyze entire package | Enter `PKG_NAME`, type = PACKAGE |
| Analyze one procedure in a package | Enter `PKG_NAME.PROC_NAME` |
| Analyze standalone procedure | Enter `PROC_NAME`, type = PROCEDURE |
| Analyze a trigger | Enter `TRIGGER_NAME`, type = TRIGGER |
| Unsure which schema | Select "Auto-detect (all schemas)" in the Owner dropdown |
| Refresh after code changed | Use **Pull** button in the top bar (clears cache, re-fetches from DB) |

### Exploring Results

- **Scope Controls** (Direct / Subtree / SubFlow / Full) are the most important UX concept — use them to focus Table Ops, Complexity, and all other tabs on just the part you care about
- **Call Flow tab** (left panel) lists all procedures by depth — click any to view its source
- **Tables tab** (left panel) lists all referenced tables — click any to open the Table Detail Modal
- In the **Hierarchy sub-tab**, click any line-number badge to jump directly to that source line in the Source Modal
- In the **Source tab**, procedure names on call lines are clickable — they open the called procedure in the Source Modal
- Use **Ctrl+F** in both the Source Tab and Source Modal to search within code
- **Back/Forward** arrows in the Source Modal let you retrace your navigation path

### Table Detail Modal

- Opens from anywhere you see a table name — Table Ops tab, left-panel Tables tab, Complexity tab
- The **Joins tab** shows both incoming and outgoing joins for the table, with clickable procedure links and line numbers
- The **Triggers tab** shows triggers with timing (BEFORE/AFTER), event (INSERT/UPDATE/DELETE), and source (DB metadata or parsed source)
- The **Columns tab** loads column metadata live from Oracle
- The **Query button** in the modal header opens the Query Runner pre-filled with `SELECT * FROM <table>`

### Reading the Call Tree

| Indicator | Meaning |
|-----------|---------|
| Depth 0 | Your entry point |
| `INTERNAL` badge | Call within the same package |
| `EXTERNAL` badge | Call to a different package or schema |
| `DYNAMIC` badge | Call via EXECUTE IMMEDIATE or dynamic SQL — target may not be resolved |
| Lock icon | Source is wrapped/encrypted — cannot be parsed |
| Cycle badge | This node is part of a circular call dependency |

### Complexity Tab Tips

- Sort by **Score** descending to find the most complex procedures first
- Expand any row to see the full score breakdown (LOC, tables, calls, cursors, dynamic SQL, depth — each with its weight and contribution)
- **HIGH** risk procedures are the ones most likely to cause issues during migration or refactoring
- The risk filter pills at the top let you focus on just HIGH or MEDIUM items

### Claude Enrichment (PL/SQL)

- Click **Verify** in the top bar after analysis completes
- Use **Chunks** button to see how the analysis was split for Claude — each chunk is one API call
- **Claude Corrections** sub-tab shows a side-by-side diff of what Claude added, removed, or confirmed
- The **Claude** tab in the Table Detail Modal shows per-access verification status

### Common Issues

| Symptom | Cause | Fix |
|---------|-------|-----|
| Procedure missing from results | Not found in any configured schema | Check object name spelling; select correct owner |
| Source shows "Truncated" warning | ALL_SOURCE view incomplete | Ensure DB user has `EXECUTE` on `DBMS_METADATA` |
| Lock icon on procedure | Source is Oracle-wrapped (encrypted) | Ask DBA for unwrapped source |
| Analysis takes very long | Very deep or wide call tree | Use `calltree.max-depth` property to cap depth |
| ALL_SOURCE returns wrong version | Multiple schemas have same object name | Select the specific owner in Step 2 instead of Auto-detect |

---

## Home Dashboard

- The **Quick Submit panel** lets you queue PL/SQL analysis jobs without navigating to the parser — paste one procedure name per line
- The **Queue modal** (click the queue bar) lets you reorder pending jobs and cancel any job
- The **Progress Report panels** show a live count of AI-enriched vs. not-enriched items across both tools
- The **Claude Sessions table** shows all active and completed AI sessions across both analyzers

---

## Global Shortcuts

| Shortcut | Action |
|----------|--------|
| `Escape` | Close any open modal |
| `Ctrl+F` | Open in-code search (inside source/decompile views) |
| `Ctrl+Enter` | Run query (inside Query Runner) |
| Click outside modal | Close the modal |
