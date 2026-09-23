# PLAN-11 — Field-level mapping: JSON path → column or child table

**This document exists because PLAN-04/05 did not cover it.** They model 65 entities and **68**
tables with per-entity source citations, but they never answer, systematically:

- for **every** field in **every** JSON, does it become a column or a child table?
- which field paths appear in **more than one** artifact, and do they mean the same thing?
- which fields are **sometimes a scalar and sometimes an array**?

That third question is the dangerous one. Jackson deserialises both shapes without complaint, so a
schema that assumes one is wrong only for *some* inputs — and the failure is a missing row, not an
exception. It cannot be answered by reading code; only the data knows.

## How this was produced

`.claude/skills/db-mode/scripts/field-scan.py` walks every JSON under `data/plsql-parse`,
`data/jar` and `data/plsql`, recording each field path and the **set of types actually observed**
for it, per artifact. Re-runnable; re-run it whenever new analyses land.

```
scanned 4,308 JSON files
distinct field paths: 7,715
```

## 1. Shape census — what each path becomes

| observed shape | paths | maps to |
|---|---|---|
| scalar (`string`/`number`/`bool`) | **4,936** | a **column** on the owning entity |
| array | **2,090** | a **child table** with an FK to the owner |
| object | **689** | embedded columns, or an FK if it has identity |
| appearing in **more than one** artifact | **606** | see §3 — reconcile before mapping |

The 2,090 array paths are the reason the schema has 68 tables and 106 foreign keys: an array in JSON
is a child table, and nesting depth becomes a chain of them. That part PLAN-05 got right; what
follows is what it missed.

## 2. THE HAZARD — fields that are sometimes scalar, sometimes array

Six field paths change shape **in real data**. Each is a defect waiting in any mapper that assumes
one shape.

### 2.1 `annotations[].attributes.value` — `string` OR `array<string>`

Appears as `annotationDetails[].attributes.value` and `nodes[].annotationDetails[].attributes.value`
too — four paths, one cause.

```
analysis.json      -> ['array<string>', 'string']   ← both shapes in ONE file
_class-tree.json   -> ['array<string>', 'string']
call-tree.json     -> ['array<string>']
node_0.json        -> ['array<string>']
node_2.json        -> ['string']                    ← same artifact type, different file
```

**Cause:** it mirrors Java annotation syntax. `@RequestMapping(value = "/orders")` yields a string;
`@RequestMapping(value = {"/orders", "/order"})` yields an array. Both are ordinary.

**Mapping:** child table `annotation_attribute_value(annotation_attribute_id, ordinal, value)`.
**Always** a child table, even for the single-string case — that is one row with `ordinal = 0`.
Storing a scalar in a column and an array in a side table would make "which endpoints map to
`/orders`" return different answers depending on how the annotation happened to be written.

> The mapper must normalise on read: `value` absent → no rows; scalar → one row; array → n rows.
> A test must feed it all three, from the real files named above.

### 2.2 `endpoints` — `number` OR `array<object>`

```
analysis.json         -> ['array<object>', 'array<empty>']
_summary_static.json  -> ['array<object>', 'array<empty>']
_summary_stats.json   -> ['number']                        ← a COUNT, not the collection
```

**This is not a polymorphic field. It is two different fields that share a name.** In
`_summary_stats.json` it is the headline count; everywhere else it is the endpoint collection.

**Mapping:** `analysis_run.endpoint_count` (integer) and the `endpoint` table — **never unified**.
A field-mapping table keyed on name alone would merge them, and the schema would then have an
`endpoints` column holding either a count or a foreign key depending on provenance.

### 2.3 `modules` — `number` OR `array<string>`

```
_summary_stats.json -> ['number']         ← count of modules
endpoint.json       -> ['array<string>']  ← the module names for one endpoint
```

Same trap as §2.2. **Mapping:** `analysis_run.module_count` and
`endpoint_module(endpoint_id, ordinal, module_name)`.

### 2.4 The rule this yields

> **Never map by field name across artifacts. Map by (artifact, path).**
>
> Two of the six hazards are name collisions where the same identifier means a count in one document
> and a collection in another. Name-keyed reconciliation — the obvious way to build a cross-JSON
> mapping table — produces a wrong schema in exactly those cases, and nothing detects it later.

## 3. Cross-artifact field paths — 606 of them

606 paths occur in more than one artifact. Most are benign repetition of the same logical field, and
that is precisely what normalisation removes. The highest-fanout ones:

| path | artifacts | maps to |
|---|---|---|
| `nodeId` | 827 | `flow_node.node_id` — the join key of the whole PL/SQL side |
| `depth` | 827 | `flow_node.depth` (run-scoped; **not** a key — the crawl is non-deterministic) |
| `schema`, `objectType`, `lineStart`, `lineEnd`, `linesOfCode` | 739 | `flow_node` columns |
| `statementSummary` | 694 | child table `(node_id, statement_type, count)` — **never an enum**, the key set is 11–28 dialect-specific tokens |
| `parameters[].{name,dataType,direction,line}` | 585–586 | child table `flow_node_parameter` |
| `calls[].type` | 582 | `flow_edge.call_type` |

**Each of these 827 files becoming 827 duplicated copies of the same six columns is the duplication
PLAN-05 measured** (~8.7 MB of every 14.3 MB PL/SQL analysis). One `flow_node` row per node replaces
all of them.

Before mapping any shared path, confirm it means the same thing in every artifact. §2.2 and §2.3 are
the two known cases where it does not; the scan output in `/tmp/fieldmap.json` lists per-artifact
types for all 606 so the check is mechanical.

## 4. What the mapper must do

Derived from the above, not from theory:

1. **Read by `(artifact, path)`**, never by bare field name.
2. **Normalise scalar-or-array to a child table with an `ordinal`** — always, for the four
   `attributes.value` paths.
3. **Keep the two count/collection collisions separate**, with different target columns.
4. **Emit nothing for `null`** — absent and null must not become a zero row, or counts drift.
5. **Never key on `depth`, node ordinals, or array position** — re-running renumbers.
6. **Fail loudly on an unobserved shape.** If a field arrives as an object where the scan only ever
   saw strings, that is new information about the data and must surface, not be coerced.

> **Corrected:** this document twice said **71** tables. PLAN-05 §0.1 is authoritative at **68**,
> and its breakdown sums correctly (26 JAR-side + 27 PL/SQL-side + 15 shared/LLM/job = 68). The
> foreign-key count of 106 is unaffected. Recorded rather than silently edited, because the
> discrepancy was itself listed as an open item and had been mis-stated in the other direction
> ("PLAN-05 says 68 but contains 71") — PLAN-05 was right all along.

## 5. What is still unknown

- **The scan covers only the analyses on this machine** — 4,308 files. A dialect or code pattern not
  represented here can introduce a seventh polymorphic field. Rule 6 is what turns that into a
  visible failure rather than a silent one.
- **`crossref/`, `flowanalysis/`, `conversion/`, `claude-chatbot/` and `external-links*` were not
  scanned** — those trees do not exist in any snapshot (PLAN-04 §8, PLAN-09 §2.2). Their field
  shapes are code-derived and **unverified**; re-run the scan once real data exists.
- **Depth is capped at 6** in the scan. Deeper nesting exists in call trees; those paths are
  truncated and their leaf shapes unrecorded.
