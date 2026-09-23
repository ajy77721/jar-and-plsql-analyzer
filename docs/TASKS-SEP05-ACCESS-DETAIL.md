# Sep05 — Access detail: what a table access actually did

Requested: on the table screen, expanding a row should say *how* a table was used — read or written,
plain select or a join, the predicate, the assigned value, the `INTO` target — and the table popup
should show the same with a source snippet, so nobody has to open the code. Today most of those
cells are empty.

**Audit first, then fix.** This document is the audit. Findings are numbered `Sep05-NN`.

Legend: ☐ not started · ◐ in progress · ☑ done (with evidence) · ✗ rejected (with reason)

---

## 0. The reasoning, before any change

The instinct is "the parser does not extract predicates". That is **wrong**, and checking it first
changed the whole shape of the work.

`SqlAnalysisResult` — what the SQL analyser produces per statement — already carries:

| field | meaning |
|---|---|
| `whereFilters` | the predicate: column, operator, value |
| `joinInfos` | join type, left/right table, ON condition |
| `sequenceReferences` | `seq.NEXTVAL` provenance |
| `columnUsages` | per column: `usage`, `qualifier`, `expression`, `alias`, `intoVariable`, `valueSource`, `lineNumber` |

So the analysis computes nearly everything asked for. **It is then discarded on the way to the
screen** — in three places, independently.

### Layer 1 — the enricher drops two of the four collections

`TableOperationEnricher.enrich` transfers exactly two things onto the operation:

```java
op.getColumnUsages().addAll(mine);
if (!r.getDdl().isEmpty()) op.getDdl().addAll(r.getDdl());
```

`whereFilters` and `joinInfos` are computed and never transferred. Worse, `TableOperationInfo` — the
type that reaches the UI — **has no `whereFilters` field at all**, so there is nowhere to put them.

This also explains a defect found earlier from the other end: the old Access Details renderer had a
`d.whereFilters` branch that rendered for nobody, and was removed as dead code. It was not dead by
design — it was waiting for data that stopped one layer short.

### Layer 2 — the statements index emits 4 of 7 column attributes

`ChunkedFlowWriter.writeStatementIndex` writes per column:

```java
c.put("name", …);  c.put("usage", …);  c.put("valueSource", …);  c.put("expression", …);
```

Dropped: `intoVariable` (the `SELECT … INTO v_x` target the user explicitly asked for), `qualifier`
(the table alias that disambiguates `a.id` from `b.id`), and the per-usage `lineNumber`.

### Layer 3 — the UI renders only column names

`shared/access-details.js` renders `cols` — an array of **names**. `usage`, `valueSource` and
`expression` are present in the served payload and shown nowhere. So even the attributes that
survive layers 1 and 2 are invisible.

### Why this stayed hidden

Each layer looks locally reasonable. The enricher's job is arguably "columns"; the producer emits
"the useful ones"; the panel shows "the columns". Nothing errors, and every screen renders. The loss
is only visible end-to-end, which is why it took a user asking "why is this empty" to find it.

---

## Findings

### Sep05-01 ☑ Predicates are computed and thrown away — HIGH, and it is the main cause

**Measured, and it reframes everything below.** The analyser does not record a predicate column as a
`ColumnUsage` at all — it records it in `whereFilters`:

```
SELECT qty_on_hand FROM erp_stock_level WHERE product_id = :p AND warehouse_id = 7
   filters=2                      <- product_id, warehouse_id live here
   col=qty_on_hand usage=SELECT   <- the only ColumnUsage produced

DELETE FROM erp_gtt_stage WHERE run_id = :r
   filters=1, zero columnUsages
```

So for a table used mainly as a join/filter target, *every* column fact about it is in the dropped
collection. On the user's screen `ERP_PRODUCT` has 475 accesses: rows showing `PRODUCT_ID` are the
ones where it happens to sit in the select list, and the `—` rows are where it is filtered or joined
on — the most useful thing to know about it.

This also explains the per-operation coverage measured earlier: `DELETE` and `MERGE` at **0%** were
never a JSqlParser weakness. A DELETE has no select list and no SET, so 100% of its column
information is predicate.

`ownedBy` compounds it: for a SELECT op it keeps only `usage == "SELECT"`, so a predicate column
would be filtered out even if it were promoted to a `ColumnUsage`.

**Correction to this document's own earlier claim.** Section 0 first reported Oracle's 1,728 blank
cells as "truly none — no column of that table is referenced". That was wrong. They are predicate
columns the pipeline discards. `columnsParsed=true` with an empty list is accurate about
`ColumnUsage` and misleading about the statement, so the labelling is part of this fix.

`whereFilters` exists on `SqlAnalysisResult`, is populated, and has no destination on
`TableOperationInfo`. Needs a field, transfer in the enricher, emission in the producer, and a
renderer. Affects all five dialects, since the enricher is shared.

### Sep05-02 ☑ Join detail is not attributed to the access — HIGH

`joinInfos` likewise. `TableOperationInfo.joins` exists but is filled only by the per-dialect
visitors, not by the shared enricher — so join detail is inconsistent between dialects rather than
absent. Needs measuring per dialect before deciding whether to transfer or reconcile.

### Sep05-03 ☑ "What kind of use" is not distinguishable — HIGH

The user asks whether an access is a plain select, a select feeding a join, a subquery, and so on.
`ColumnUsage.usage` carries SELECT/SET/INSERT, and `TableOperationInfo.subqueryContext` exists, but
neither reaches the screen. A read inside a join currently looks identical to a standalone read.

### Sep05-04 ☑ `INTO` targets dropped by the producer — MEDIUM

`ColumnUsage.intoVariable` is captured and not emitted. Explicitly requested.

### Sep05-05 ☑ `qualifier` dropped, so ambiguous columns cannot be read — MEDIUM

Without the alias, `id` on a three-table join is unattributable on screen.

### Sep05-06 ☑ The panel shows names only — HIGH

Rendering must show usage, value source and expression per column, not just the name. This is where
the user's "select value / assignment" lands, and the data is already served.

### Sep05-07 ☑ The table popup needs a source snippet — MEDIUM

`sqlText` is already stored per statement (capped at 2,000 chars) and already served by the search
API. The popup can show it without a new endpoint. Requested so the user need not open the source.

### Sep05-08 ☑ Coverage must be stated per dialect, not assumed — HIGH

Measured column coverage differs sharply: MySQL 69%, DB2 75%, SQL Server 43%, Oracle 27%,
PostgreSQL 17%. Any new cell must distinguish *absent* from *not captured*, as the column cell now
does — otherwise this fix reintroduces the confusion it is meant to remove.

---

## Ordering

`Sep05-01` … `Sep05-05` are one vertical slice each: model → enricher → producer → served payload.
Doing them one at a time, end to end, is what keeps a field from stopping a layer short again — the
failure this whole audit is about. `Sep05-06`/`07` are the render, and are worth nothing until the
data arrives. `Sep05-08` is a condition on all of them.

---

## Progress — measured, 2026-09-05

| dialect | statements | columns | **predicates** | **ddl** | any detail | before |
|---|---|---|---|---|---|---|
| DB2 | 4,063 | 3,054 | 1,022 | 0 | **89%** | 75% |
| MYSQL | 5,015 | 3,450 | 1,778 | 0 | **85%** | 69% |
| ORACLE | 2,436 | 662 | 1,460 | 0 | **67%** | 27% |
| SQLSERVER | 1,742 | 753 | 499 | 492 | **72%** | 43% |
| POSTGRES | 2,662 | 442 | 684 | 0 | **42%** | 17% |
| **total** | 15,918 | | | | **75%** | |

Oracle is the clearest evidence for the diagnosis: **1,460 statements carry a predicate against 662
with columns** — more than twice as much information was in the discarded collection as in the kept
one.

### Sep05-09 ☑ DDL column definitions were never emitted

Found while investigating why SQL Server barely moved (43% → 44%). Of its 675 statements that had
SQL and produced nothing, most were temp-table `CREATE`/`DROP`/`TRUNCATE` — no predicate and no
read/write usage, but `CREATE TABLE #stage6(ProductId INT, Qty DECIMAL(18,4))` *does* name its
columns, and `DdlInfo.columns` had been captured all along and never written to the statements
index. Emitting it took SQL Server to **72%**.

### Sep05-10 ☑ Aggregate-only reads carry an INTO target and no column

PostgreSQL's remaining gap is not a plumbing failure: **1,303 of its 1,553 empty rows are
aggregate-only reads** — `SELECT COUNT(*) INTO v_n FROM erp.erp_product`. There is no plain column
and no predicate, so an empty column cell is truthful. But there *is* something worth showing: the
`INTO` target and the aggregate expression. Surfacing those needs a change in `SqlAnalyzer` to
record an aggregate select-item as a usage with `expression` and `intoVariable` set — deeper than
the plumbing in Sep05-01, and worth doing separately rather than bundling.

### Also fixed on the way

- captured statement text is cut at the first unquoted semicolon (quote-aware). A grammar context
  spanning two statements produced `SELECT … ; CALL …`, which parses as neither. Semicolons in
  PostgreSQL `sqlText` fell from most rows to 270, all in a different visitor path.
- the capture prefers the `SELECT` context over the control-flow statement wrapping it, so
  `FOR rec IN SELECT … LOOP` no longer yields text ending in a bare `LOOP`. PostgreSQL parse
  failures fell 979 → 330.

## Sep05-02 / Sep05-03 — measured, 2026-09-05

| dialect | statements | **joins** | any detail | was | top access kinds |
|---|---|---|---|---|---|
| DB2 | 4,063 | 2,070 | **93%** | 89% | join_read 2,008 · write 1,331 · read 712 |
| MYSQL | 5,015 | 2,314 | **93%** | 85% | join_read 2,181 · write 1,914 · read 788 |
| ORACLE | 2,436 | 923 | **85%** | 67% | read 1,027 · join_read 861 · write 230 |
| SQLSERVER | 1,742 | 445 | **76%** | 72% | write 790 · ddl 492 · read 375 |
| POSTGRES | 2,662 | 1,063 | **72%** | 42% | read 1,249 · join_read 1,062 · write 270 |
| **total** | 15,918 | | **86%** | 75% | |

Joins went from **0 rows across all five dialects** to 6,815. The producer had never emitted
`op.joins` at all, even though the per-dialect visitors populate it — so join detail existed in the
model and reached nothing.

Two `JoinInfo` classes exist and are not interchangeable: the analyser's carries
`leftTable`/`rightTable`/`onPredicate`, the model's (which the UI sees) carries
`joinedTable`/`condition` plus table aliases and a `queryId`. The enricher converts rather than
substitutes, and contributes only when the visitor found nothing — so a dialect that resolves
aliases properly keeps its better answer.

`accessKind` is derived from evidence already on the operation and is **absent when there is none**,
rather than defaulting to something plausible. `AGGREGATE_READ` names the shape that dominates the
PostgreSQL gap (`SELECT COUNT(*) INTO v_n FROM t`), which is why Postgres moved 42% → 72% without
any new column extraction: the row now says what it is instead of showing a blank.

## Sep05-06 / Sep05-07 — done, 2026-09-05

Verified against the live API on `ERP_PRODUCT` (500 accesses, Oracle): 50 rows rendered carrying
**63 predicate fragments** and **45 access-kind badges**, with per-column usage markers present.

**Sep05-06.** Each column chip now carries its `usage`, and a written column (`SET`/`INSERT`) is
visually distinct from a read one — the difference between "this table is used here" and "this table
is *changed* here". `qualifier`, `valueSource`, `expression` and `into` are on the chip's tooltip.
All of it was already in the served payload and rendered nowhere.

**Sep05-07.** The popup gained a **Detail & SQL** column: access kind, first join with its ON
condition, predicates, assignment/INTO, and the statement text itself (truncated in the row, full on
hover). It reuses `AccessDetails.fetchDetail` / `applyDetail` / `detailKey` rather than a second copy
of the join — two copies of a join key would drift and the two screens would then disagree about the
same statement, which is worse than one being sparse. Enrichment is asynchronous so the table paints
first, and re-renders only if that tab is still open.

Also: a local `escAttr` fallback was added to the shared module, matching the existing `esc`/`escJs`
pattern, rather than assuming `PA.escAttr` on every page that might later load it.

## Sep05-10 / Sep05-08 — done, 2026-09-05. Series complete.

All five analyses were re-run against the live databases after the change (schema version bumped
`v10` → `v11-aggregate-select-items`, which invalidates the parse cache — without that bump the fix
would have been invisible, as happened earlier in this series).

### Sep05-10 — the aggregate select item

`SqlAnalyzer` was **already** recording it: `SELECT COUNT(*) INTO v_n FROM t` yields one
`ColumnUsage` with `columnName == null`, `expression == "COUNT(*)"`, `intoVariable == "v_n"`. The
loss was a single line in `ChunkedFlowWriter`:

```java
if (u.getColumnName() == null || u.getColumnName().isBlank()) continue;
```

So this was the same shape as every other finding in this series — computed, then discarded one
layer short — and not the deeper analyser change the audit predicted. Recording that correction
matters more than the fix: the audit's estimate of *where* the defect lived was wrong, and only
reading the code disproved it.

**Kept as detail, deliberately not as a column.** `COUNT(*)` is not a column of the table, so it
goes to `columnDetail` and never to `columns`. Adding it to `columns` would have made the blank cells
fill faster and made a column filter match a non-column — the same class of error as the original
loss, in the opposite direction. `AggregateSelectItemEmittedTest` pins both halves, and was
calibrated by restoring the old `continue` (it fails: *"the aggregate select item was dropped"*).

Measured per dialect, rows carrying a nameless expression select item:

| dialect | statements | expression-only rows |
|---|---|---|
| POSTGRES | 2,662 | **1,888** |
| ORACLE | 2,436 | 1,027 |
| MYSQL | 5,015 | 1,099 |
| DB2 | 4,063 | 936 |
| SQLSERVER | 1,742 | 176 |

Live, through the real renderer against the real API — `erp_product` on PostgreSQL, 500 accesses:
**428 rows now carry an aggregate expression and 355 an INTO target**, where all 428 previously
rendered an empty Detail cell.

### Sep05-08 — the coverage line

Each panel now states, in its header, how many of its accesses carry detail, splitting the remainder
into **"not parsed"** (our gap — the SQL did not analyse) and **"nothing to show"** (a truthful
answer: it parsed and references no column, predicate or join). It is computed from the rows in hand,
so it cannot disagree with what the reader is looking at.

**A mistake worth recording.** The first version counted `accessKind` as detail. Every row has one,
because it is *derived* from the operation rather than extracted — so the line read
`detail on 500 of 500 (100%)` for a table whose predicates were captured on 72 rows of 500. A gauge
pinned at 100% is not reassurance, it is a broken instrument, and it would have concealed exactly the
gap this task exists to expose. It now counts only what the two cells can actually display:

```
POSTGRES erp_product      detail on 428 of 500 (86%) · 72 not parsed
POSTGRES erp_sales_order   detail on 499 of 500 (100%) · 1 not parsed
ORACLE   ERP_PRODUCT       detail on 489 of 500 (98%) · 11 nothing to show
```

### What the Detail cell now renders, measured over all 15,918 statements

| dialect | statements | Detail cell renders | rows with both cells empty | did not parse |
|---|---|---|---|---|
| DB2 | 4,063 | **98%** | 70 | 73 |
| ORACLE | 2,436 | **95%** | 96 | 0 |
| POSTGRES | 2,662 | **91%** | 248 | 330 |
| MYSQL | 5,015 | **88%** | 267 | 0 |
| SQLSERVER | 1,742 | **85%** | 260 | 12 |

Against the 75% "any detail" measured when this audit opened — and that earlier figure was the
generous one, since it counted a derived access kind as detail.

### Found while verifying, not fixed here

`LATERAL` is being recorded as a **table name** in the PostgreSQL visitor
(`SELECT ... FROM ... LEFT JOIN LATERAL (...)`). It is a phantom table, the same defect class as the
MySQL/MERGE visitor inventing a table called `SET`, which was fixed earlier in this series. It
inflates the table list and gives that fake table its own access rows. Out of scope for Sep05-10 and
worth its own item rather than a quiet edit inside an unrelated fix.

The 330 PostgreSQL statements that do not parse are the largest remaining honest gap. They are now
*labelled* as our gap rather than rendered as an empty cell, which was the Sep05-08 requirement, but
they are not yet analysed.

---

## Sep05-11 ☑ Phantom tables — a keyword read as a table name

Two dialects invented tables that cannot exist. A phantom is worse than a gap: it appears in the
table list indistinguishable from a real object, so nothing prompts anyone to check it, and the
statements attributed to it are attributed to nothing.

| dialect | phantom | rows | cause |
|---|---|---|---|
| POSTGRES | `LATERAL` | 81 | no `LATERAL` token, so `tableRef: id` matched it |
| SQLSERVER | `SET` | 13 | `MERGE … THEN UPDATE SET` read as `UPDATE <table>` |

**PostgreSQL.** `LATERAL` was not a lexer token at all, so `CROSS JOIN LATERAL (SELECT …)` parsed
as a join against a table named "LATERAL". It is now a reserved token and deliberately **not** added
to the `id` rule — no unquoted object can be called LATERAL, and a quoted `"lateral"` still reaches
`id` via `DQ_ID`, which is pinned by a test.

*Correction to my own first claim.* I said the parser-rule change (`LATERAL?` in `tableReference`
and `joinClause`) was the fix. Measured, it changes nothing: tables, joins and error count are
identical with and without it — **the lexer token alone removes the phantom.** The rules are kept
for a different and smaller reason, now stated in the grammar: without them LATERAL would be a token
appearing in no rule, so every LATERAL query would drop out of the structured SELECT path into the
permissive catch-all, parsing by accident rather than by rule.

**SQL Server.** MERGE reaches the generic `otherStatement` rule, so its tables can only be recovered
by scanning text — and the scan reads `UPDATE` followed by a word. The guard went into `addTableOp`,
the single funnel all three recovery patterns pass through, rather than into the regex that exposed
it. That placement is the point: this was the **second** dialect to show this exact `SET` phantom
(the MySQL MERGE path had it earlier), and fixing one regex leaves the rest to be rediscovered one
keyword at a time.

## Sep05-12 ☑ Captured SQL that runs past the end of its statement

The 330 PostgreSQL parse failures were not a JSqlParser weakness. They were **our capture** handing
it text that is not a statement, and the cost is total rather than partial: an unparseable statement
loses every column, predicate and join it had.

| class | rows | what the captured text looked like |
|---|---|---|
| ran into the loop body | 249 | `SELECT … WHERE … LOOP v_cnt := v_cnt + 1` |
| closed a paren it never opened | 81 | `SELECT … GROUP BY f.product_id) s ON (…) WHEN MATCHED THEN` |

`originalText` extends from the context's stop to the next `;` — necessary, because a DML rule ends
before its `SET`/`WHERE`. But for `FOR rec IN SELECT … LOOP <body>;` the next semicolon is *inside
the loop body*, and for a MERGE's `USING (SELECT …)` the scan runs out through the closing paren.

Both cuts are structural rather than another keyword list:

- **block keyword** — stop at `LOOP`/`BEGIN` (plus `DO`, DB2's loop opener). `THEN` and `END` are
  deliberately excluded: both occur inside `CASE … END` in a select list or WHERE clause, and
  cutting there would truncate valid SQL — trading 249 known failures for an unknown number of new
  ones. A test pins a `CASE` expression surviving.
- **parenthesis depth** — a `)` that closes a paren the capture never opened means the scan has left
  the construct it started in, whatever that construct is.

**DB2 had both defects too**, in a forked copy of the same method (54 running into a `FOR … DO`
body, 19 unbalanced), measured on the current analysis rather than assumed from PostgreSQL. Fixed in
both. *The fork itself remains* — two copies of this method now carry the same two fixes, and the
next one will have to be applied twice again.

**SQL Server's last 12** were a different class: `FROM erp.vw_ProductSales WITH (NOEXPAND)`.
JSqlParser rejects a table hint outright, so a planner instruction cost the statement all of its
detail. Hints are now stripped for SQLSERVER through the existing `Dialect.candidates` mechanism,
before the other rewrites, with the untouched original still tried last. The risk in that pattern is
not the hint but the **CTE** — `WITH x AS (…)` starts with the same keyword — so the match requires
the parenthesis to follow `WITH` immediately, and a test pins a CTE passing through untouched.

### Result — measured after re-running all five analyses

| dialect | statements | Detail cell renders | did not parse | phantoms |
|---|---|---|---|---|
| DB2 | 4,063 | 98% → **100%** | 73 → **0** | 0 |
| POSTGRES | 2,581 | 91% → **100%** | 330 → **0** | 81 → **0** |
| SQLSERVER | 1,729 | 85% → **86%** | 12 → **0** | 13 → **0** |
| ORACLE | 2,436 | 95% | 0 | 0 |
| MYSQL | 5,015 | 88% | 0 | 0 |
| **total** | **15,824** | **94%** | **0** | **0** |

**Zero unparseable statements and zero phantom tables across all five dialects.** The statement
totals fall by exactly 81 and 13 — the phantom rows — which is the corroboration that those rows
were the phantoms and not real work being lost.

MySQL's 88% and Oracle's 95% are not gaps: those rows carry columns and no predicate, join or
aggregate (`INSERT INTO t VALUES (…)`, plain column reads). They render in the **Columns** cell, and
the Sep05-08 coverage line counts them, which is why it reads 100% for tables where the Detail cell
does not.

### A measurement mistake, recorded

An intermediate report of mine put MySQL at 88% → 95% and Oracle at 95% → 96% after Sep05-10. That
was **my metric changing, not the code improving**: the later script counted plain `columns` as
"Detail cell", which that cell does not render. Re-measured like-for-like against the archived
artifacts, both were unchanged by these fixes, as they should be — the fixes are dialect-specific and
touch neither. The table above is the like-for-like comparison.

---

## Revalidation, 2026-09-05 — and one defect it caught

Re-checked from the UI side rather than the data side, because the earlier checks proved fields
were *attached to objects* and not that anything reached the screen. Two harnesses now drive the
**real** modules against the **live** API and assert on the HTML they emit:

- the inline Access Details panel (`shared/access-details.js`), capturing the row renderer the
  module hands to `PA.tf.init`;
- the table popup (`parser/js/tableDetail.js`), driving `_renderAccesses()` and capturing its
  inline renderer.

10 runs — 2 surfaces × 5 dialects — all pass: every row renders, the Detail cell is present on
every row, kinds/predicates/columns/INTO/SQL snippets appear in the markup, no phantom name
appears, and no `undefined`/`NaN` leaks.

### The defect this caught: "nothing to show" was an over-claim

`columnsParsed` has **three** states — true, false, and absent — and the coverage line derived its
quiet count by subtraction, folding *absent* in with *true*. So a statement we never analysed was
reported as one that "genuinely references no column, predicate or join".

| dialect | detail | parsed=false | parsed=true, nothing | **parsed ABSENT** |
|---|---|---|---|---|
| SQLSERVER | 1,494 | 0 | 0 | **235** |
| ORACLE | 2,340 | 0 | 63 | **33** |
| MYSQL | 4,748 | 0 | 267 | 0 |
| DB2 / POSTGRES | all | 0 | 0 | 0 |

On SQL Server that was its **entire** remaining gap, stated to the reader as a fact about their
code. Telling someone a table is unused where we simply did not look is the worst answer this panel
can give — worse than a blank cell, because it sounds like a finding. The three states are now
counted separately and the line reads `98% · 11 not analysed`, with only "nothing to show"
presented as a finding.

### A harness bug worth recording, since it looked exactly like a product bug

The first run reported `undefined` throughout the rendered HTML. `PA.tf` passes its own escaper as
the row renderer's **third argument** (`tableFramework.js:406`) and the harness stubbed it with a
no-op. Real escaping, real markup, harness at fault — checked before touching any product code.

### Still open

The 235 SQL Server and 33 Oracle **not analysed** rows are a real gap, now labelled honestly rather
than hidden. They are statements that reach the table list with no `sqlText`, so the enricher never
sees them — most are MERGE operations recovered by text scanning, which records the table but not
the statement. Surfacing their SQL is the next piece of work, and is not done.

---

## Sep05-15 ☑ De-forking statement-text capture

`originalText` existed as byte-identical copies in the PostgreSQL and DB2 visitors. Both carried the
same two defects, and each was diagnosed and fixed **twice**, on separate days, from separate
measurements — the DB2 copy was only found because the numbers were re-measured per dialect. Nothing
in either file pointed at the other.

The logic now lives in a new `dialect-common` module as `StatementTextCapture`. It is deliberately
tiny and depends on nothing but the ANTLR runtime: the dialect modules are independent by design,
and this exists only for logic that was *already* duplicated and had already cost two fixes.

The dialect difference is expressed as data rather than by re-forking the method:

| set | keywords | why |
|---|---|---|
| `DEFAULT_BLOCK_KEYWORDS` | `LOOP`, `BEGIN` | PostgreSQL and the general case |
| `DB2_BLOCK_KEYWORDS` | + `DO` | DB2 spells its loop opener `FOR … DO` |

`THEN` and `END` are absent from both, and a test asserts it: they occur inside `CASE … END` in a
select list, so cutting there would truncate SQL that parses correctly today.

Two things guard it. `StatementTextCaptureTest` covers the rules at the single source, and
`NoForkedStatementCaptureGateTest` fails the build if a dialect visitor re-declares `parenDepth` or
`isBlockKeywordEndingCapture` — copying is easier than depending, so the copy has to break the build
rather than rely on review.

**Verified behaviour-preserving**, which is the only claim that matters for a refactor: all five
analyses were re-run and every figure matches the pre-refactor run — detail 100/95/90/86/100%,
**0 mis-attributed statements and 0 phantom tables** across all dialects. 1,789 tests, 0 failures.

## Sep05-14 ☑ Statement text attributed to the wrong operation

Reported from the screen: an `ERP_PRODUCT` row displayed `UPDATE erp_mrp_run SET status = 'DONE'`.

`currentDmlCtx` is set when a DML statement is entered and never cleared, so it acted as "the last
DML seen for the rest of the file"; any later operation without text of its own inherited it.

Restoring the field in each of the five visit methods was tried first and abandoned: those methods
have up to fourteen early returns each, so the restore would have to be a try/finally around every
one, and the next `return` added past it would silently reintroduce the leak. The check instead
encodes the requirement — **text may attach to an operation only if that statement spans the
operation's line** — in the one place operations are added.

**A regression this caused, and its fix.** Removing the wrong text dropped Oracle's detail from 98%
to **38%**: the carry-over had been concealing how many operations never had a statement of their
own. Four call sites already held the owning context (`ctx.select_statement()`) and discarded it;
passing it through took Oracle to **90%** with rows carrying SQL going 1,147 → 2,250. 90% correct is
worth more than 96% partly invented.

**Honest limit.** `DmlTextNotCarriedOverTest` does *not* reproduce the original defect — reverting
the fix leaves it passing, because in a small procedure every operation has its own statement. It is
an invariant guard; the verification was the full-analysis sweep showing 0 mis-attributed rows.

## Sep05-16 ☑ A closed panel kept a query it was no longer applying

Closing a modal dropped the table's filter state but left the DOM untouched, so reopening showed an
unfiltered list with the previous query still in the box — the box and the rows disagreeing, with
nothing on screen saying which to believe.

`SearchQuery.resetWithin(panel)` clears search-role inputs on close, wired into **13 sites**. It is
scoped, not a blanket clear: the chat panels hold a half-written **message** in a plain text input,
and wiping that would be a worse bug than the one being fixed. Seven cases are tested, three of
which must survive.

**My first gate for this was bad and was replaced.** It asked only "does this file hide anything,
and does it mention a search box anywhere", and flagged seven files that hide a count badge, a
toolbar or collapsed tree children — every one a false positive. A gate with a 100% false-positive
rate teaches people to delete gates. Requiring the hidden element to be modal-shaped then surfaced
**3 real sites** that had been missed (`browseModal`, `homeMenuPanel`, `nav-history-panel`).
