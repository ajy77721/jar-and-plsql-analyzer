# AMP Analyzer — Impact Writeup

> **Headline outcome — batch validation of 200,000+ lines of legacy code: 3-5 days of manual work → 2-3 hours**
> with the tool's batch-validation skill. Initial static analysis of that codebase takes 10-15 minutes; after
> that, everything — search, call chains, validation, planning — is instant.

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

| Task | Before — by hand (human estimate*) | Before — old tool | After — AMP now |
|---|---|---|---|
| **Batch validation (200K+ lines)** | **3-5 days** | — | **2-3 hours** |
| "Where is this table touched?" across ~500 procedures | **hours to a full day** of grep/read/verify | 35-41s per query | **7-19 ms** |
| Full call-chain map of ONE big procedure (every branch) | **hours per procedure**, branches still missed | 71.5s per view | **2 ms** |
| Debugging: finding what calls a procedure / what it affects | **hours** (mental tracing) | — | **4.6 ms** |
| Conversion planning for a package (dependencies, complexity, order) | **days** | — | **minutes** (summary views) |
| Piece-to-piece validation of a conversion | **days per package, still incomplete** | — | **minutes per piece, complete** |

*"Human estimates: vary with procedure size and the analyst's familiarity with the schema; the point is
the ORDER of magnitude — hand-tracing a deep call chain is hours-to-days of focused work and still
error-prone, where the tool returns the complete answer in milliseconds. The 71.5s/35-41s figures are the
tool's own measured response times before the indexing work — the human manual times are the real 'before'
that motivated building it."

## Batch validation — in production use

The team runs **batch-optimize** and **batch-validation** skills on top of the platform today, at batch
scale rather than one procedure at a time. Validating **200,000+ lines** of legacy code used to be **3-5
days** of manual work; with the tool it is **2-3 hours**. The initial static analysis of that codebase takes
**10-15 minutes**, and after that everything — search, call chains, validation, planning — is instant,
because every procedure's full map is precomputed and served in milliseconds. The piece-to-piece validation
described above, run across a whole batch, is what turned a 3-5 day validation cycle into 2-3 hours.
Removing **~1 hour/day** of analyst waiting per analyst remains a secondary benefit that makes the batch
workflows practical.

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

## Employee-led innovation

This is employee-led innovation in its simplest form: an engineer in India hit the same bottleneck every
day, built the solution themselves, and it is now a deployed platform the whole team uses. No approvals,
no vendor, no cost — just one developer's daily frustration turned into hours saved for everyone, every
week.

## Scope — the same engine also covers more

Oracle stored procedures are the primary target. The same engine also reads Microsoft SQL Server, MySQL,
IBM Db2, and PostgreSQL procedures **and** the Java side — for JAR/WAR archives it maps every endpoint,
call tree, and class relationship (11,264+ classes on a single 105 MB jar), so Java modernization gets the
same evidence-first treatment.
