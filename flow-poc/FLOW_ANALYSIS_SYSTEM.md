# Flow Analysis System — Developer Reference

> Branch: `poc/dynamic-flow-executor`  
> Last updated: 2026-05-05

---

## What this system does

Given a Spring Boot JAR and a MongoDB database, the system:

1. **Statically** walks every API endpoint's call tree (Controller → Service → Repository)
2. **Dynamically** (optionally) executes the actual code, intercepts all DB calls without touching the real DB
3. **Classifies** each query as Static (cacheable) or Transactional (on-demand)
4. **Detects** optimization opportunities: missing indexes, N+1 patterns, aggregation rewrites, bulk op candidates
5. **Asks Claude AI** to explain the impact of existing findings and suggest fix priority
6. **Generates** a JSON report (for tooling) and a Markdown report (for developers)
7. **Exposes** a REST API + browser UI so any team member can trigger analysis with one click

---

## Architecture

```
YOUR JAR  ──►  JAR Analyzer (Layer 1)  ──►  analysis.json
                                                │
analysis.json  ──►  Flow Engine  ──►  FlowResult (one per endpoint)
                         │
                 AnalyzerPipeline (Chain of Responsibility)
                         │
                 ┌───────┴──────────────────────────────────┐
                 │  QueryClassificationAnalyzer (STATIC/TXN) │
                 │  HierarchyFlowAnalyzer (depth violations) │
                 │  MongoMissingIndexAnalyzer                 │
                 │  NplusOneAnalyzer                          │
                 │  BulkOperationAnalyzer                     │
                 │  AggregationRewriteAnalyzer                │
                 │  ClaudeFlowAnalyzer  ← AI enrichment last  │
                 └───────────────────────────────────────────┘
                         │
                 MarkdownReporter ──► flow-analysis-report.md
                 JsonReporter     ──► stdout / REST response
```

---

## How to run

### Option A — Java API (programmatic)

```java
PocConfig config = PocConfig.builder()
    .analysisJson("/data/analysis.json")     // from JAR Analyzer
    .mongo("mongodb://localhost:27017", "myapp")
    .layer2("/apps/myapp.jar")               // optional: enables dynamic execution
    .claude("sk-ant-...")                    // optional: enables Claude AI suggestions
    .reportDir("/reports")                   // optional: writes flow-analysis-report.md here
    .sampleSize(10)
    .maxEndpoints(0)                         // 0 = all endpoints
    .build();

new PocRunner(config).run(System.out);       // JSON to stdout
```

### Option B — Command line

```bash
java -jar flow-poc.jar \
  /data/analysis.json \   # required: analysis.json path
  myapp \                 # required: MongoDB database name
  mongodb://localhost:27017 \
  10                      # optional: max endpoints (0=all)
```

### Option C — Browser UI (no code needed)

If the `flow-poc` module is deployed in a Spring Boot app:

1. Open `http://your-app/api/analysis/ui`
2. Fill in the form fields
3. Click **▶ Run Analysis**
4. View findings inline, download `.md` report

---

## REST API

| Method | URL | Description |
|--------|-----|-------------|
| `GET`  | `/api/analysis/ui` | Browser UI page with form + trigger button |
| `POST` | `/api/analysis/run` | Start analysis, returns `{ "id": "...", "status": "running" }` |
| `GET`  | `/api/analysis/result/{id}` | Poll for results (status: running/done/error) |
| `GET`  | `/api/analysis/report/{id}` | Download completed `.md` report |
| `GET`  | `/api/analysis/summary` | Counts of all runs |

### POST /api/analysis/run — request body

```json
{
  "analysisJson": "/data/analysis.json",
  "mongoUri":     "mongodb://localhost:27017",
  "mongoDb":      "myapp",
  "jarPath":      "/apps/myapp.jar",
  "claudeApiKey": "sk-ant-...",
  "maxEndpoints": 10
}
```

### Integrating the button in your existing UI

The `/api/analysis/ui` endpoint is a standalone HTML page. To embed a trigger button in your **existing** Spring Boot UI:

```html
<!-- Add this button anywhere in your existing UI -->
<button onclick="window.open('/api/analysis/ui', '_blank')">
  🔍 Run Flow Analysis
</button>
```

Or trigger it programmatically from your existing frontend:

```javascript
// Call from your existing React/Angular/Vue frontend
async function runFlowAnalysis(config) {
  const { id } = await fetch('/api/analysis/run', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(config)
  }).then(r => r.json());

  // Poll until done
  while (true) {
    const result = await fetch('/api/analysis/result/' + id).then(r => r.json());
    if (result.status === 'done')   return result.results;
    if (result.status === 'error')  throw new Error(result.error);
    await new Promise(r => setTimeout(r, 2000));
  }
}
```

---

## Static vs Transactional classification

Every `SELECT` query is tagged as one of:

| Class | Meaning | Example | Recommendation |
|-------|---------|---------|----------------|
| `STATIC` | Same result every time; no bind params; collection looks like reference data | `findAll()` on `countries`, `findByCode("USD")` on `currencies` | `@Cacheable` or preload at `ApplicationReadyEvent` |
| `TRANSACTIONAL` | Result depends on request context (userId, session, write in same flow) | `findByUserId(?0)` on `orders` | Must execute on every request |
| `UNKNOWN` | Could not determine | Aggregation with dynamic stages | Review manually |

**Detection rules:**
- No bind params (`?0`, `:name`, `${...}`) + collection name contains `config/type/code/lookup/setting/status/enum/role/permission` → `STATIC`
- Filter on `userId/sessionId/tenantId/customerId` → `TRANSACTIONAL`  
- Any write to same collection in same flow → `TRANSACTIONAL`
- Bind params present → `TRANSACTIONAL`

---

## Optimization categories

| Category | Severity | What it means | Suggested fix |
|----------|----------|---------------|---------------|
| `MISSING_INDEX` | MEDIUM | Filter field has no apparent index | `db.collection.createIndex({ field: 1 })` |
| `N_PLUS_ONE` | HIGH | Same collection queried N times at different depths | Replace with `$lookup` aggregation |
| `PREFETCH_CANDIDATE` | MEDIUM | FIND A then FIND B by FK | Single `$lookup` pipeline |
| `BULK_WRITE` | MEDIUM | ≥2 INSERTs/UPDATEs on same collection | `insertAll()` or `bulkOps().execute()` |
| `BULK_READ` | MEDIUM | ≥2 FINDs on same collection at same depth | Single `$in` query |
| `FULL_TABLE_SCAN` | HIGH | FIND with no filter | Add predicate or `limit()` |
| `STATIC_CACHEABLE` | MEDIUM | Reference data re-fetched on every request | `@Cacheable` or startup preload |
| `AGGREGATION_REWRITE` | HIGH/MEDIUM | Two queries can become one `$lookup` or `$in` | See suggestedCode in finding |
| `PIPELINE_MISSING_PROJECT` | LOW | Aggregation returns all fields | Add `{ $project: { field1:1 } }` |
| `PIPELINE_UNBOUNDED_SORT` | HIGH | `$sort` without `$limit` | Add `{ $limit: N }` after sort |
| `CONTROLLER_DB_ACCESS` | HIGH | Controller directly queries DB | Extract to `@Service` method |
| `CROSS_LAYER_QUERY` | MEDIUM | Same collection at multiple call-tree depths | Consolidate in repository layer |
| `UNBOUNDED_DEEP_READ` | HIGH | No filter at repository depth ≥2 | Add filter or load at startup |
| `OTHER` (Claude AI) | HIGH/MEDIUM/LOW | Claude enrichment of above findings | See Claude's impact explanation |

---

## Shadow engine (how writes stay safe)

When Layer 2 dynamic execution is enabled, the engine:

1. **Blocks all writes** — `INSERT/UPDATE/DELETE` never reach real DB
2. **Pre-seeds shadow** — before blocking a write, fetches matching real docs with the same filter (up to 200)
3. **Applies write in memory** — the shadow store has the post-write state
4. **Read-after-write works** — next `FIND` in the same flow reads from shadow (sees the write result)
5. **Reports impact count** — `impactCount` on `CapturedCall` = how many real docs would have been modified

```
Flow: updateMany({status:"PENDING"}) → find({status:"PROCESSING"})

Step 1: updateMany intercepted
  → preSeedIfNeeded: fetch real docs with {status:"PENDING"} → 47 docs seeded
  → applyWriteWithImpact: set status=PROCESSING on 47 shadow docs
  → CapturedCall: [WRITE-BLOCKED:UPDATE_MANY] orders IMPACT=47 docs

Step 2: find({status:"PROCESSING"})
  → shadowFirst: shadow has 47 matching docs → returns them
  → real DB not called
  → CapturedCall: [FIND] orders INPUT {status:PROCESSING} resultCount=47
```

---

## Report format

### JSON report fields

```json
{
  "endpoint": "GET /api/orders/{id}",
  "optimizations": [
    {
      "category":      "N_PLUS_ONE",
      "severity":      "HIGH",
      "table":         "orders",
      "column":        null,
      "description":   "Collection 'orders' queried 3 times at different depths",
      "location":      "OrderService.findAll",
      "evidence":      "Repeated FIND on orders",
      "suggestedCode": "db.orders.aggregate([{ $lookup: ... }])"
    }
  ],
  "testDataSets": [...],
  "flowSteps":    { "class": "...", "method": "...", "children": [...] }
}
```

### Markdown report sections

1. **Executive Summary** — finding counts by severity, quick-win callouts
2. **Static vs Transactional Summary** — table per endpoint showing query counts
3. **Per-endpoint detail**:
   - ASCII call hierarchy tree with inline query annotations
   - Findings table sorted HIGH → MEDIUM → LOW
   - Code suggestion blocks
   - Claude AI recommendations block
4. **Index appendix** — ready-to-run `createIndex` commands for all flagged fields

---

## Claude AI integration

**Design principle:** Claude does NOT detect new issues. It reads findings already computed by the static analyzers and:
- Explains the business/performance impact in plain English
- Suggests fix priority order
- Rates overall endpoint health (GOOD / NEEDS_WORK / CRITICAL)

**How to enable:**
```java
// In PocConfig
.claude("sk-ant-api03-...")

// Or set environment variable
export ANTHROPIC_API_KEY=sk-ant-api03-...
```

**Model used:** `claude-haiku-4-5-20251001` (fast, low cost for bulk analysis)

**What Claude receives:** existing findings from all 6 static analyzers + query classification summary + call hierarchy

**What Claude returns:** SUGGESTION lines that become `OptimizationFinding` records with `category=OTHER` and `location="Claude AI"` — they appear in the report's **Claude AI Recommendations** section.

---

## Adding a new analyzer

```java
// 1. Implement OptimizationAnalyzer
public class MyNewAnalyzer implements OptimizationAnalyzer {
    @Override
    public List<OptimizationFinding> analyze(FlowResult flowResult) {
        List<OptimizationFinding> findings = new ArrayList<>();
        for (FlowStep step : flowResult.allSteps()) {
            // ... your detection logic ...
            findings.add(new OptimizationFinding(
                Category.OTHER, Severity.MEDIUM,
                "collectionName", "fieldName",
                "description", "ClassName.method", "evidence", "suggested code"));
        }
        return findings;
    }
}

// 2. Register in AnalyzerPipeline
AnalyzerPipeline.builder()
    .withMongoDefaults()
    .add(new MyNewAnalyzer())  // ← add here
    .build();
```

---

## Key files

| File | Purpose |
|------|---------|
| `PocRunner.java` | Main entry point — wires everything together |
| `PocConfig.java` | Immutable config (Builder pattern) |
| `FlowWalker.java` | Reads analysis.json, builds FlowResult per endpoint |
| `AnalyzerPipeline.java` | Chain of Responsibility — runs all analyzers |
| `QueryClassificationAnalyzer.java` | Static vs Transactional tagging |
| `HierarchyFlowAnalyzer.java` | Depth-by-depth violation detection |
| `AggregationRewriteAnalyzer.java` | $lookup / $in rewrite suggestions |
| `ClaudeFlowAnalyzer.java` | AI enrichment via Anthropic API |
| `DynamicFlowExecutor.java` | Layer 2 — reflective execution with shadow store |
| `UniversalMongoInterceptor.java` | ByteBuddy proxy — intercepts ALL MongoTemplate calls |
| `ShadowMongoStore.java` | In-memory MongoDB — filter/update/aggregation |
| `ShadowFilterMatcher.java` | Evaluates $gt/$in/$and/$or/etc. in memory |
| `ShadowUpdateApplier.java` | Applies $set/$inc/$push/etc. in memory |
| `ShadowAggregationExecutor.java` | Runs $match/$group/$sort/$lookup in memory |
| `MarkdownReporter.java` | Generates developer-readable .md report |
| `AnalysisController.java` | Spring REST API + browser UI trigger page |
| `FLOW_ANALYSIS_SYSTEM.md` | This document |

---

## Quickstart for a new developer

```bash
# 1. Build
cd jar-plsql-analyzer
mvn clean package -DskipTests

# 2. Run all 65 tests
mvn -pl flow-poc test -am

# 3. Run analysis against your app
java -jar flow-poc/target/flow-poc-*.jar \
  path/to/analysis.json \
  your-mongo-db \
  mongodb://localhost:27017

# 4. With Claude AI + Markdown report
ANTHROPIC_API_KEY=sk-ant-... java \
  -Dpoc.claude.enabled=true \
  -jar flow-poc/target/flow-poc-*.jar \
  path/to/analysis.json your-mongo-db

# 5. Via browser UI (if deployed in Spring Boot app)
open http://localhost:8080/api/analysis/ui
```
