# AMP Analyzer — Impact Writeup

## What it is, and why it matters

AMP Analyzer is a static analyzer for legacy ERP modernization whose **primary target is Oracle stored
procedures** — the large ERP packages an Oracle ERP conversion has to take apart, e.g. `USP_RUN_CYCLE` and
`ERP$USP_PROCESSSALESORDER`. For these packages it surfaces the evidence analysts need — sequences,
cursors, dynamic SQL, joins, table dependencies, complexity scoring, and the call chains inside Oracle
packages — as the evidence base for the migration. It is deployed internally at MongoDB on **Kanopy**
(staging + prod), built and shipped by **Drone CI** to **GHCR**, with analysis artifacts stored in
**AWS S3** and **per-user AWS SSO identity** carried through every request and background job. The "why"
behind this work: results were already correct, but the read paths were too slow to use interactively and,
for the largest inputs, silently incomplete.

## Before → After: the analyst's workflow

### BEFORE — analyzing one stored procedure by hand

- Open the procedure source, read it start to end, and manually track every operation: which tables it
  reads, inserts, updates, and deletes; which other procedures it calls — and what **those** call, level
  after level (proc → proc → proc).
- Long call chains live across many files and schemas. Tracking them by hand means jumping between files,
  grepping, keeping mental notes — and still missing branches when the chain is deep.
- Finding "where is this table touched?" across hundreds of procedures means searching the code by hand,
  over and over for every question.
- Triggers, views, and related objects that silently affect the procedure are easy to miss entirely.
- Debugging, conversion planning, and validation all depend on this hand-built map — so they need deep
  prior knowledge of the schema and take days.

### AFTER — with AMP Analyzer

- Every operation on any procedure is **captured at analysis time, start to end**: the full call chain,
  every table operation, every join (with exact join views), related tables/views, and triggers — all
  precomputed and searchable in milliseconds.
- "Where is this table touched?" is a search box, not a day of grepping.
- **Conversion planning:** the summary views (complexity, dependencies, call trees) show what to convert
  first and what it will touch — with minimal prior knowledge of the schema, because the tool surfaces it.
- **Debugging:** the call tree / callers views pinpoint where a procedure is used from and what it calls —
  in clicks.
- **Validation:** piece-to-piece checking of every converted piece against its original call chain and
  data operations.

## Metrics

| Capability | Before | After | Work saved (assumptions inline) |
|---|---|---|---|
| Oracle analysis facets (`/sequences`, `/cursors`, `/dynamic-sql`, `/joins`) | 71.5s per request | Precomputed sidecars: **0.6s** first access, **2ms** warm; p50 across all **769** endpoint combinations = **5ms** (405 successful, zero 5xx) | At **50 facet lookups/day**: 50 × 71.5s ≈ **1 hour/day** of analyst wait eliminated (after: 50 × 2ms = 0.1s/day) |
| Global search (Oracle package estate) | **35–41s** per query, scanning ~**116** files | **7–19ms** | At **20 searches/day** and 38s midpoint: ≈12.7 min/day → ~0.2s/day, i.e. **~12–13 min/day saved** |
| Call-tree callers (Oracle package call chains) | Full call-graph re-parse **every request** | **4.6ms** | Removes a re-parse from every callers lookup; effectively instant at interactive click rates |
| Large-artifact handling | Per-class decompile of the **105MB / 11,264-class** artifact failed entirely (source views silently missing) | Decompiles with **one** local read per analysis; **200MB** uploads persist to S3 (**7-part multipart**) and stay visible | Turns an unusable analysis into source-accurate results (no before/after time ratio — it previously did not complete) |
| Data durability | Critical bug silently lost **every** background S3 upload (artifacts gone on pod restart) | Fixed with per-entry credential carry; **0 failed uploads** verified | Prevents total loss of completed analyses |
| Reliability | — | **3,452**-test suite green; **40+** new contract tests; a **769**-combination automated sweep with zero 5xx | Steady-state latency regression caught before users see it |
| Multi-user | Identity and connection state could be dropped across threads/redeploys | Per-user AWS SSO identity travels with every background task; concurrent users verified isolated (dedicated tests); page-lock for unauthenticated users | Safe concurrent use across accounts |
| Deployment & login UX | Manual steps, state lost on redeploy | Fully automated CI/CD (push → build → GHCR → Kanopy staging; version tags → prod), self-cleaning logs, ops tooling (`scripts/amp-ops.sh`), admin portal for user/connection/analysis visibility; login auto-reconnects from saved per-user config | Redeploy-proof connection — no manual reconnect after a deploy |

*Assumption note:* the two "work saved" figures are simple multiplications of the measured per-request
times by a stated daily click/query count (50 facet views/day, 20 searches/day); they are not telemetry
from real sessions. Numbers elsewhere are exactly as measured on the project.

## Batch validation (in preparation)

The same outputs make **batch-optimize** and **batch-validation** possible: instead of validating one
procedure at a time, whole batches run at once — because every procedure's full map is precomputed and
served in milliseconds. Removing **~1 hour/day** of analyst waiting and returning search in milliseconds is
precisely what makes those batch workflows practical.

## Reliability and platform

This work converted a single-user-oriented prototype into an operationally trustworthy service. A latent
bug that dropped every background S3 upload was fixed by carrying credentials per buffer entry, verified at
zero failed uploads. Identity is explicit end to end: a user's AWS SSO identity is captured on the request
thread, carried with each task, and installed on the worker, with no silent fallback to default
credentials; concurrent users are proven isolated and unauthenticated access is page-locked. Confidence
comes from a 3,452-test suite (including 40+ new contract tests) and a 769-combination endpoint sweep with
zero 5xx. Delivery is fully automated — every push to `main` builds and deploys to Kanopy staging via
GHCR, version tags promote to prod — with self-cleaning logs, an ops script (`scripts/amp-ops.sh`), and an
admin portal giving visibility into users, connections, and analyses. Connections survive redeploys by
auto-reconnecting from each user's saved config.

## An employee-led innovation pattern (India-based reference)

AMP fits a well-known Indian pattern of employee-led innovation. Infosys' **"Zero Distance"** (2013,
championed by Narayana Murthy) asked every project owner to innovate inside their own project — to close
the "distance" between the work and the person doing it — rather than wait for a central team. HCL
Technologies' **"Employees First, Customers Second"** (Vineet Nayar) made the same bet from the other
direction: give the people closest to the work the authority and tools to fix it, and the value reaches
customers on its own. The pattern in both is qualitative and repeatable — **an engineer hits a daily
bottleneck, solves it inside their own work, and the fix compounds into an organization-level
capability.** No company-specific numbers are claimed here; the measurable results belong to AMP itself.

AMP is exactly this pattern. The analyzer was built by the engineer who hit the bottleneck every day —
waiting 71.5s for a single facet view, 35–41s per search, watching a 105MB / 11,264-class artifact fail to
decompile. Solving that personal pain, piece by piece, produced precomputed sidecars that serve facets in
2ms and search in 7–19ms, made 105MB / 11,264-class artifacts analyze end-to-end, and — after a
769-combination sweep with zero 5xx — turned a local workaround into a deployed internal platform. The
same dynamic shows up in the numbers: **~1 hour/day of analyst waiting eliminated** per analyst, from one
engineer's daily annoyance to a shared capability.

## Scope — the same engine also covers more

Oracle stored procedures are the primary target. The same engine also reads Microsoft SQL Server, MySQL,
IBM Db2, and PostgreSQL procedures **and** the Java side — for JAR/WAR archives it maps every endpoint,
call tree, and class relationship (11,264+ classes on a single 105 MB jar), so Java modernization gets the
same evidence-first treatment.
