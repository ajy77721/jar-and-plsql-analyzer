# AMP Analyzer — Impact Writeup

## What it is, and why it matters

AMP Analyzer is a static analyzer for legacy modernization: it parses PL/SQL across **five dialects**
(Oracle, Microsoft SQL Server, MySQL, IBM Db2, PostgreSQL stored procedures) and Java JAR/WAR archives,
then surfaces the evidence analysts need to convert legacy systems — sequences, cursors, dynamic SQL,
joins, table dependencies, complexity scoring, call trees, and endpoint inventories. It is deployed
internally at MongoDB on **Kanopy** (staging + prod), built and shipped by **Drone CI** to **GHCR**,
with analysis artifacts stored in **AWS S3** and **per-user AWS SSO identity** carried through every
request and background job. The "why" behind this work: results were already correct, but the read paths
were too slow to use interactively and, for the largest inputs, silently incomplete — an analyst doing a
modernization assessment waited seconds to minutes per click and could not trust large-jar source views.
The hardening described below turned the analyzer into a responsive, durable, multi-user platform.

## Before → After

| Capability | Before | After | Work saved (assumptions inline) |
|---|---|---|---|
| Analysis facets (`/sequences`, `/cursors`, `/dynamic-sql`, `/joins`) | 71.5s per request | Precomputed sidecars: **0.6s** first access, **2ms** warm; p50 across all **769** endpoint combinations = **5ms** (405 successful, zero 5xx) | At **50 facet lookups/day**: 50 × 71.5s ≈ **1 hour/day** of analyst wait eliminated (after: 50 × 2ms = 0.1s/day) |
| Global search | **35–41s** per query, scanning ~**116** files | **7–19ms** | At **20 searches/day** and 38s midpoint: ≈12.7 min/day → ~0.2s/day, i.e. **~12–13 min/day saved** |
| Call-tree callers | Full call-graph re-parse **every request** | **4.6ms** | Removes a re-parse from every callers lookup; effectively instant at interactive click rates |
| Large-jar handling | Per-class decompile of the **105MB / 11,264-class** jar failed entirely (source views silently missing) | Decompiles with **one** local read per analysis; **200MB** uploads persist to S3 (**7-part multipart**) and stay visible | Turns an unusable analysis into source-accurate results (no before/after time ratio — it previously did not complete) |
| Data durability | Critical bug silently lost **every** background S3 upload (artifacts gone on pod restart) | Fixed with per-entry credential carry; **0 failed uploads** verified | Prevents total loss of completed analyses |
| Reliability | — | **3,452**-test suite green; **40+** new contract tests; a **769**-combination automated sweep with zero 5xx | Steady-state latency regression caught before users see it |
| Multi-user | Identity and connection state could be dropped across threads/redeploys | Per-user AWS SSO identity travels with every background task; concurrent users verified isolated (dedicated tests); page-lock for unauthenticated users | Safe concurrent use across accounts |
| Deployment & login UX | Manual steps, state lost on redeploy | Fully automated CI/CD (push → build → GHCR → Kanopy staging; version tags → prod), self-cleaning logs, ops tooling (`scripts/amp-ops.sh`), admin portal for user/connection/analysis visibility; login auto-reconnects from saved per-user config | Redeploy-proof connection — no manual reconnect after a deploy |

*Assumption note:* the two "work saved" figures are simple multiplications of the measured per-request
times by a stated daily click/query count (50 facet views/day, 20 searches/day); they are not telemetry
from real sessions. Numbers elsewhere are exactly as measured on the project.

## Customer experience

For an analyst running a modernization assessment, the difference is the shift from "wait and wonder" to
"instant and complete." Facet views that used to stall for more than a minute now render from precomputed
sidecars in milliseconds, so exploring sequences, cursors, dynamic SQL, and joins across a stored-procedure
estate feels like flipping through a document rather than queuing jobs. Global search across an analysis
returns in milliseconds instead of half a minute, which makes the analyzer viable as a primary evidence
tool during a conversion workshop. Call-tree "who calls this?" questions answer in ~4.6ms. And the largest
inputs — 105MB jars with 11,264 classes, 200MB uploads — are no longer a silent dead end: source views
actually appear, and uploads survive. The net effect is that assessments that once required defensive
batching of queries can proceed at conversational speed.

## Reliability and platform

This work converted a single-user-oriented prototype into an operationally trustworthy service. A latent
bug that dropped every background S3 upload — losing analyses whenever a pod restarted — was fixed by
carrying credentials per buffer entry, verified at zero failed uploads. Identity is now explicit end to
end: a user's AWS SSO identity is captured on the request thread, carried with each task, and installed on
the worker, with no silent fallback to default credentials; concurrent users are proven isolated and
unauthenticated access is page-locked. Confidence comes from a 3,452-test suite (including 40+ new
contract tests) and a 769-combination endpoint sweep with zero 5xx. Delivery is fully automated — every
push to `main` builds and deploys to Kanopy staging via GHCR, version tags promote to prod — with
self-cleaning logs, an ops script (`scripts/amp-ops.sh`), and an admin portal that gives visibility into
users, connections, and analyses. Connections now survive redeploys by auto-reconnecting from each user's
saved config, so a deploy no longer costs the team a manual reconnect.

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
waiting 71.5s for a single facet view, 35–41s per search, watching a 105MB / 11,264-class jar fail to
decompile. Solving that personal pain, piece by piece, produced precomputed sidecars that serve facets in
2ms and search in 7–19ms, made 105MB / 11,264-class jars analyze end-to-end, and — after a
769-combination sweep with zero 5xx — turned a local workaround into a deployed internal platform. The
same dynamic shows up in the numbers: **~1 hour/day of analyst waiting eliminated** per analyst, from one
engineer's daily annoyance to a shared capability.

## How the team uses it directly — piece-to-piece validation

AMP is not a demo handed over to someone else; our own team uses it every day, and we are building
**batch-optimize** and **batch-validation** workflows on top of it.

- **Piece-to-piece (proc-to-proc) validation.** When modernizing a large stored procedure, we validate the
  conversion **piece by piece**. The tool's call-tree, callers, and cross-reference output let an analyst
  confirm that each converted piece behaves like its original — same call chain, same data touched —
  instead of testing randomly and hoping.
- **Why confidence scales with size.** The bigger the procedure and the longer the call chain, the more
  that human testing alone misses. AMP makes every branch of the chain visible and checkable, so confidence
  comes from evidence, not hope.
- **Batch-optimize / batch-validation (in preparation).** The same analysis outputs let us run these
  validations across whole batches of procedures rather than one at a time. The sidecar / per-endpoint
  architecture — every endpoint's tree and fragment precomputed and served in milliseconds — is what makes
  batch-scale validation feasible at all.
- **The metrics are the enabler.** Removing **~1 hour/day** of waiting per analyst and returning search in
  milliseconds is precisely what makes those batch workflows practical.
