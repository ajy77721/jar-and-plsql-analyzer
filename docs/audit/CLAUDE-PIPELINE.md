# Claude Enrichment Pipeline — Failure Handling & Checkpointing Audit

Scope: `jar-analyzer-core/src/main/java/com/jaranalyzer/service/Claude*.java`,
`unified-web/src/main/java/com/analyzer/queue/ClaudeJobExecutor.java`, and the JAR summary UI.
`_migration-backup-*` excluded. Audit only — no code changed.

Verdict on the starting fact: **CONFIRMED, all three links, verbatim.** It is worse than stated in
two ways: (a) the *blank-output* case skips the merge entirely and still checkpoints DONE, and (b)
the per-call audit log (`ClaudeCallLogger`) also records `success=true` for the same failed run, so
there is no surviving record anywhere that the run failed.

---

## Failure sequence (the exact path)

1. `ClaudeAnalysisService.enrichFromAnalysis()` picks endpoint `ep`, calls
   `tracker.markProcessing(jarName, epKey)` (`ClaudeAnalysisService.java:453`).
2. `processRunner.runClaudeProcess(...)` (`:486`) starts `claude -p`. Claude hits a rate limit /
   auth error / OOM and exits nonzero, printing a prose message (e.g. `Claude AI usage limit
   reached|1748...`) on stdout or stderr.
3. `ClaudeProcessRunner.java:223-230`: exit code is inspected, logged at WARN, and **thrown away**.
   Method returns stdout as if the run succeeded. No exception, no status object.
4. Back in the caller (`:487-491`), the *success* branch of the try runs, so
   `callLogger.logCall(..., true, null)` records `success=true, error=null` in
   `claude-calls.jsonl`.
5. `:500` `if (result != null && !result.isBlank())` — two sub-cases:
   - **Non-blank garbage** (the rate-limit prose): written to disk as
     `{traceId}_output.json`, then `resultMerger.mergeClaudeResult(ep, result)` at `:504`.
     `extractJson` finds no `{`, returns `null`, merger logs `Incomplete merge` and returns
     `false`. **The boolean is discarded.**
   - **Blank stdout** (process died before writing anything): the `if` is false, so the merge is
     never even attempted. Silent.
6. `:652` `endpointOutputWriter.writeEndpointOutput(...)` writes the *unenriched, static-only*
   endpoint to the per-endpoint output as though it were Claude-enriched.
7. `:653` `tracker.markEndpointComplete(jarName, epKey)` → `EpStatus.DONE`, `completedEndpoints++`,
   persisted to `_tracker.json`.
8. `:654,657` `completedKeys.add(epKey); writeCheckpoint(...)` → `epKey` is durably written into
   `_checkpoint.json`.
9. `ClaudeJobExecutor.executeRescan():211-213` sets `analysis.setAnalysisMode("CORRECTED")`, saves,
   and calls `claudeTracker.markComplete(jarName)` — job status `COMPLETE`.
10. UI (`summary-claude.js:286,307`) renders the job badge as `CORRECTED … complete` and the
    endpoint badge as a green `✓ DONE`. Clicking the badge (`:340`) jumps to the endpoint report,
    which shows static-only data presented as Claude-verified.
11. User clicks **Resume Scan** (default `resume=true`, `AnalyzerController.java:1439`).
    `ClaudeAnalysisService.java:444-449` sees `epKey` in `completedKeys` and *skips it*, calling
    `tracker.markEndpointComplete` again. The endpoint is now unreachable by resume, permanently.

---

## Findings, ranked by severity

### 1. CRITICAL — A failed Claude run is durably checkpointed as complete and never re-run

`ClaudeAnalysisService.java:653-657`

```java
endpointOutputWriter.writeEndpointOutput(jarName, ep, done);
tracker.markEndpointComplete(jarName, epKey);
completedKeys.add(epKey);

// Checkpoint: save completed keys so enrichment can resume after interruption
writeCheckpoint(checkpointFile, completedKeys);
```

These four lines run unconditionally at the end of the `try`, with no reference to whether any
Claude output was produced or merged. Same shape without the checkpoint at
`ClaudeAnalysisService.java:354-355` (the `enrichEndpoints` source-mode path).

Consequence: an endpoint whose Claude run was rate-limited is written into `_checkpoint.json`.
Resume defaults to `true` (`AnalyzerController.java:1439`, `:1118`, `:1452` all
`defaultValue = "true"`), and `:444` skips any key in that file. The endpoint can *never* be
re-enriched by a resume — only by a Fresh Scan, which discards and re-runs **all** endpoints at
full API cost. There is no per-endpoint retry.

Severity: **Critical** — silent, durable data loss presented as success.

Minimal fix: capture the merge outcome and branch.
```java
// ClaudeAnalysisService.java:500-505
boolean merged = false;
if (result != null && !result.isBlank()) { ...; merged = resultMerger.mergeClaudeResult(ep, result); }
// :652
if (!merged) { tracker.markEndpointError(jarName, epKey, "Claude produced no usable JSON"); }
else { endpointOutputWriter.writeEndpointOutput(...); tracker.markEndpointComplete(...);
       completedKeys.add(epKey); writeCheckpoint(...); }
```
Change at `ClaudeAnalysisService.java:500`, `:504`, `:636-638`, `:652-657` (and `:235-238`,
`:348-350`, `:354-355` for the source-mode twin).

---

### 2. CRITICAL — `ClaudeProcessRunner` discards the process exit code

`ClaudeProcessRunner.java:218-230`

```java
int exitCode = process.exitValue();
...
if (exitCode != 0) {
    String stderr = stderrBuf.toString().trim();
    log.warn("Claude CLI exited with code {} | stderr: {}", exitCode,
            stderr.isEmpty() ? "(empty)" : stderr.substring(0, Math.min(500, stderr.length())));
}

String result = stdoutBuf.toString();
return result;
```

The exit code is read, logged, and never returned to any caller. `String` is the only return type,
so callers structurally *cannot* distinguish success from failure. Every caller in
`ClaudeAnalysisService` and `ClaudeCorrectionService` then treats the return as a success.

Note the two failure modes that *are* handled correctly and prove the pattern is intentional
elsewhere: `TimeoutException` at `:215` and interruption at `:204` both throw, which reaches the
`catch` at `:663` and correctly calls `tracker.markEndpointError` **without** writing a checkpoint.
Nonzero exit is the one hole.

Severity: **Critical** — the root enabler; every downstream guard is blind because of it.

Minimal fix: at `ClaudeProcessRunner.java:223`, throw instead of warn when the output is unusable:
```java
if (exitCode != 0) {
    log.warn(...);
    throw new IOException("Claude CLI exited " + exitCode + ": " + stderr);
}
```
This alone routes every nonzero-exit run into the existing, already-correct `catch` →
`markEndpointError` → no checkpoint path. Callers need no change. (If a partial-output-on-nonzero
case must be preserved, return a record `ClaudeRun(int exitCode, String stdout, String stderr)`
instead — but throwing is the cheaper correct fix and matches the timeout precedent 8 lines above.)

---

### 3. HIGH — `mergeClaudeResult`'s boolean return is discarded at all four call sites

`ClaudeResultMerger.java:35` — `boolean mergeClaudeResult(EndpointInfo ep, String rawOutput)`,
documented `@return true if the merge was complete (all key fields present), false otherwise`.

Discarded at, verbatim:
- `ClaudeAnalysisService.java:237` — `resultMerger.mergeClaudeResult(ep, result);`
- `ClaudeAnalysisService.java:349` — `resultMerger.mergeClaudeResult(ep, output);`
- `ClaudeAnalysisService.java:504` — `resultMerger.mergeClaudeResult(ep, result);`
- `ClaudeAnalysisService.java:637` — `resultMerger.mergeClaudeResult(ep, output);`

Four call sites, zero assignments. The method is package-private with no other callers, so the
return value is dead code across the entire codebase.

**What actually gets merged on malformed input** (traced through `ClaudeResultMerger`):

| Input | Behaviour |
|---|---|
| `null` / blank | Never reaches the merger — guarded by `if (result != null && !result.isBlank())`. Silent no-op, still checkpointed. |
| Rate-limit prose, no `{` | `extractJson` (`:92-118`) returns `null` → `:39-42` logs `Incomplete merge … no JSON found`, returns `false`. **Clean no-op — no corruption.** |
| Truncated JSON (stream cut mid-object) | `extractJson`'s brace-depth scan at `:106-116` never reaches depth 0, so it returns `null` → same clean no-op. If the text *starts* with `{`, the `:95` fast path returns the whole truncated string, `objectMapper.readTree` throws, caught at `:86-89`, returns `false`. **Clean no-op.** |
| Valid JSON, missing `procName`/`collections`/`riskFlags` | `:57-59` logs `Partial merge … missing fields`, then **continues and merges anyway** (`:61-81`), returns `false`. |
| Hallucinated collection names in valid JSON | Merged. `enrichCallTreeFromClaude:126` and `addMissingCollections:207` only ever **add** to `collectionsAccessed` — never remove. |

So the good news: malformed input does **not** overwrite good data — the merger is defensively
written and fails clean. The damage is entirely (a) the *absence* of enrichment being reported as
presence, and (b) the partial-merge case at `:57`, where an incomplete Claude answer is merged and
the `false` return that would have flagged it is thrown away.

`propagateOperationTypes` (`:143-171`) and `ep.computeAggregates()` (`:80`) do run on the partial
path, so a partial merge *can* mutate derived fields (`operationType` set to `null` at `:163`) off
an incomplete answer.

Severity: **High.**

Minimal fix: assign the boolean at all four sites and gate the checkpoint on it (see finding 1).

---

### 4. HIGH — The call log records `success=true` for a nonzero-exit run

`ClaudeAnalysisService.java:487-491` (and the identical shape at `:225-227`, `:259-263`,
`:309-312`, `:531-535`, `:587-591`):

```java
result = processRunner.runClaudeProcess(command, prompt, null, progressService);
long _callMs = System.currentTimeMillis() - _callStart;
try { callLogger.logCall(jarName, sessionId, "ENRICH", epKey, 0,
        prompt != null ? prompt.length() / 1024 : 0,
        result != null ? result.length() / 1024 : 0,
        _callMs, true, null); } catch (Exception _ignored) {}
```

`success` is the literal `true` and `error` the literal `null`, because the only signal the code
has is "did `runClaudeProcess` throw". `ClaudeCallLogger.logCall:58` takes a real
`boolean success, String error` — the contract exists and is correct; it is fed a constant.

Consequence: the Claude call log table in the UI (`summary-claude.js:783`,
`c.error ? 'ERROR' : 'OK'`) shows **OK** for the failed call. This is the last independent place a
user could have noticed, and it agrees with the lie. Post-hoc forensics are impossible: neither
`_tracker.json`, `_checkpoint.json`, nor `claude-calls.jsonl` records the failure. Only the
server-side SLF4J WARN at `ClaudeProcessRunner.java:225` — not surfaced in any UI.

Severity: **High** — destroys the audit trail; also means "is my data real?" cannot be answered
retroactively even after the bug is fixed.

Minimal fix: falls out of finding 2 for free — if `runClaudeProcess` throws on nonzero exit, the
existing `catch (Exception _ex)` at `:492-497` fires and logs `success=false` with the message.

---

### 5. HIGH — No way to detect or re-run endpoints that "completed" with no result

There is no per-endpoint retry and no integrity check. Enumerated recovery options:

- `_checkpoint.json` (`ClaudeAnalysisService.java:397`, written at `:681-687`) is a **flat list of
  endpoint key strings** — no timestamp, no output size, no merge status. Structurally incapable of
  distinguishing "enriched" from "checkpointed after a failure".
- `_tracker.json` (`ClaudeEnrichmentTracker.java:23,160-170`) has an `EpStatus.ERROR` value that
  would express this — but on the failure path `markEndpointComplete` is what runs.
- The `{traceId}_output.json` fragment does exist on disk containing the rate-limit prose, but
  nothing reads it back to validate; it is only browsable via `listFragments`.
- `resume=false` (Fresh Scan) is the **only** recovery, and it re-runs every endpoint. The UI
  offers it (`summary-claude.js:497-521`) but has no information to tell the user it is needed.

**Checkpoint files on disk right now: zero.** `find data -name "_checkpoint.json" -o -name
"_tracker.json"` returns nothing; `data/jar/` contains no `claude/` subdirectory. No enrichment run
has been checkpointed in this working copy, so there is no corrupted state to clean up locally —
but nothing prevents it in a deployed instance.

Severity: **High.**

Minimal fix: make the checkpoint self-describing — write
`{"endpoint": key, "mergedAt": ts, "complete": bool}` objects instead of bare strings at
`ClaudeAnalysisService.java:683`, and at `:444` skip only entries with `complete == true`. Keep the
bare-string read path for backward compatibility.

---

### 6. HIGH — The analysis is relabelled `CORRECTED` even when every endpoint failed

`ClaudeJobExecutor.java:211-213` (also `:162-166`, `:188-190`):

```java
claudeAnalysisService.enrichFromAnalysis(endpoints, jarName, jarProgressService, resume);
analysis.setAnalysisMode("CORRECTED");
jarPersistence.saveCorrected(analysis);
claudeTracker.markComplete(jarName);
```

`enrichFromAnalysis` swallows every per-endpoint failure internally (`catch` at `:663`) and returns
normally even if 100% of endpoints errored. So `markFailed` at `:170` fires only on an
infrastructure-level throw (JAR not found, Claude CLI missing). A whole-run rate-limit is reported
to the user as `CORRECTED` / job `COMPLETE`.

Consequence: `summary-claude.js:286` renders the green `CORRECTED — Full scan complete — N
endpoints corrected and applied` banner over an entirely unenriched analysis.

Severity: **High.**

Minimal fix: have `enrichFromAnalysis` return a count of successfully merged endpoints, and at
`ClaudeJobExecutor.java:212` only set `"CORRECTED"` when that count is > 0; call `markFailed` when
it is 0. Change at `ClaudeAnalysisService.java:384` (signature) and `ClaudeJobExecutor.java:211`.

---

### 7. MEDIUM — Chunked path: partial branch failures are invisible and still checkpoint DONE

`ClaudeAnalysisService.java:614-617` (and the twin at `:328-331`):

```java
} catch (Exception e) {
    log.warn("    Chunk {} [{}] failed: {}", chunkLabel, cTraceId, e.getMessage());
    fragmentStore.writeFragmentSafe(workDir, cTraceId + "_error.txt", e.getMessage());
}
```

A failed chunk is logged to a sidecar and swallowed. Execution reaches `:636-638`, merging whatever
branches did succeed, then falls through to the unconditional `markEndpointComplete` at `:653`.

The information needed to detect this is *written and then not used*: `:640-647` writes
`{traceId}_assembled.json` containing both `"totalChunks"` and `"processedChunks"`. Nothing ever
compares them.

Consequence: a 12-chunk endpoint where 11 branches were rate-limited is checkpointed DONE with 1/12
of its call tree enriched, indistinguishable from a full success.

Severity: **Medium** (partial rather than total loss, but harder to spot).

Minimal fix: at `ClaudeAnalysisService.java:646`, compare `branchOutputs.size()` to
`chunks.size()`; if short, `markEndpointError` and skip the checkpoint. The values are already in
scope.

---

### 8. MEDIUM — `ClaudeCorrectionService` has the identical shape (`[EMPTY]` → DONE)

`ClaudeCorrectionService.java:136-140`:

```java
} else {
    empty.incrementAndGet();
    runLog(logPath, "[EMPTY] " + epKey + " — no response from Claude (" + epMs + "ms)");
}
tracker.markEndpointComplete(jarName, epKey);
```

The code *knows* the result was empty — it increments a dedicated `empty` counter and writes a
distinct `[EMPTY]` log line — and then calls `markEndpointComplete` anyway, three lines below the
`markEndpointError` it should have called (`:146`).

Mitigating factor: correction resume keys off `persistence.loadCorrection(jarName, epKey) != null`
(`:100`), not off a checkpoint file. An empty result saves nothing, so a resume **does** re-run the
endpoint. This path is recoverable; the enrichment path is not. It is a useful proof that
persistence-derived resume is the more robust design.

Severity: **Medium** — wrong UI badge (green ✓) but no permanent skip.

Minimal fix: `ClaudeCorrectionService.java:140` → move inside the `if (result != null)` branch and
add `tracker.markEndpointError(jarName, epKey, "empty response")` to the `else`.

---

### 9. MEDIUM — Stdin truncation is silently absorbed into a "successful" run

`ClaudeProcessRunner.java:129-134`:

```java
if (stdinContent.length() > TreeChunker.MAX_PROMPT_CHARS) {
    log.warn("Stdin content exceeds {}KB limit ({} chars) — truncating", ...);
    stdinContent = stdinContent.substring(0, TreeChunker.MAX_PROMPT_CHARS - 200)
            + "\n\n[TRUNCATED — input exceeded " + TreeChunker.MAX_PROMPT_CHARS + " char limit]";
}
```

Claude is asked to analyse a truncated call tree, answers plausibly about the fragment it saw, and
the answer merges cleanly and checkpoints DONE. Nothing records that the *input* was incomplete —
the `_input.json` fragment written at `:475` is the **untruncated** original, so even manual
inspection of the fragments will not reveal it.

Severity: **Medium** — plausible-looking wrong answers, no trace.

Minimal fix: return the truncation flag (or record it via `progressService`/`callLogger`) and write
the truncated text as the `_input.json` fragment so input and output correspond.

---

### 10. LOW — Resume marks skipped endpoints DONE without verifying the prior result exists

`ClaudeAnalysisService.java:444-449`:

```java
if (completedKeys.contains(epKey)) {
    tracker.markEndpointComplete(jarName, epKey);
    log.info("  Skipping {} (already done) [{}/{}]", epKey, done, total);
```

The checkpoint is trusted absolutely. If the fragment/output files were deleted, or the checkpoint
is stale relative to a re-parsed JAR whose endpoint set changed, the endpoint is still reported
DONE with no enrichment at all. Also note the in-memory `EndpointInfo` for a skipped endpoint is
never enriched in this process, yet `ClaudeJobExecutor.java:212` saves it as `"CORRECTED"` —
so a resume run actively **overwrites** the persisted enriched analysis with the static one for
every skipped endpoint. Worth a separate look; it is the only *destructive* item in this audit.

Severity: **Low-Medium** (depends on whether `saveCorrected` merges or replaces — not fully traced).

Minimal fix: at `:444`, verify `store.exists(workDir.resolve(traceId + "_output.json"))` before
honouring the skip.

---

## Tests

**There is no test anywhere covering a failed Claude run — and no test covering a successful one
either.**

Searched all of `*/src/test` for `mergeClaudeResult`, `ClaudeProcessRunner`, `ClaudeResultMerger`,
`ClaudeEnrichmentTracker`, `markEndpointError`. The only hit in the whole repo is
`unified-web/src/test/java/com/analyzer/arch/DirectFileIoGateTest.java`, an architecture test that
greps for direct file I/O — it does not exercise any Claude behaviour.

The Claude-named tests that do exist test adjacent plumbing only:
- `jar-analyzer-core/.../ClaudeSessionAnyRunningTest.java` — session bookkeeping
- `unified-web/.../ClaudeResultLoaderProgressTest.java` — progress counting from fragment filenames
- `unified-web/.../ClaudeCallLogDeltaTest.java` — log cursor paging

`ClaudeProcessRunner.runStdinPipeTest()` (`:237-339`) is not a JUnit test — it is a manual
diagnostic endpoint requiring a live Claude CLI, and it is the **only** code in the repo that
checks a Claude response for correctness. Notably it does so by inspecting the *string content*
(`response.trim().contains("PONG")`, `:255`) rather than an exit code, which is itself evidence
that the exit code was never considered a signal.

So: it is not that the failure fixture is missing while a success fixture exists. `ClaudeResultMerger`
— a 219-line class with a documented boolean contract and five distinct malformed-input branches —
has zero unit tests. Nothing would have caught this. That is why it shipped.

---

## Root cause chain

```
ClaudeProcessRunner.java:223   nonzero exit → log.warn, return stdout        (signal destroyed)
        ↓  no exception, so the success branch runs
ClaudeAnalysisService.java:491 callLogger.logCall(..., true, null)           (audit trail falsified)
        ↓
ClaudeAnalysisService.java:504 resultMerger.mergeClaudeResult(ep, result);   (boolean discarded ×4)
        ↓  merge cleanly no-ops; ep is unchanged, static-only
ClaudeAnalysisService.java:653 tracker.markEndpointComplete(...)             (green ✓ in UI)
ClaudeAnalysisService.java:657 writeCheckpoint(...)                          (durably unreachable)
        ↓
ClaudeJobExecutor.java:212     analysis.setAnalysisMode("CORRECTED")         (labelled verified)
        ↓
AnalyzerController.java:1439   resume defaults to true → :444 skips forever
```

The correct mechanisms all exist and are simply not reached:
`ClaudeEnrichmentTracker.markEndpointError` (`:71`), `EpStatus.ERROR` (`:26`), the merger's boolean
(`ClaudeResultMerger.java:35`), `ClaudeCallLogger`'s `boolean success, String error` (`:58`), the
`processedChunks`/`totalChunks` pair (`ClaudeAnalysisService.java:645-646`), and the `catch` at
`:663` that already does the right thing for timeouts. This is a wiring defect, not a design gap.

---

## The single highest-value fix

**`ClaudeProcessRunner.java:223` — throw instead of warn on nonzero exit.**

```java
if (exitCode != 0) {
    String stderr = stderrBuf.toString().trim();
    log.warn("Claude CLI exited with code {} | stderr: {}", exitCode, ...);
    throw new IOException("Claude CLI exited " + exitCode + ": "
            + (stderr.isEmpty() ? "(no stderr)" : stderr));
}
```

Roughly five lines, one file, no caller changes. It routes every nonzero-exit run into the
`catch (Exception e)` blocks that **already exist and are already correct** at
`ClaudeAnalysisService.java:663` and `:357`, which call `markEndpointError` and — critically — never
reach `writeCheckpoint`. It simultaneously repairs findings 1, 2, 4, 5, 6 and 8 for the rate-limit
and process-error cases, because those all hang off the same swallowed signal.

It does **not** cover the case where Claude exits 0 but returns unusable output (a refusal, an empty
answer, a hallucinated non-JSON reply). For that, follow with the finding-3 fix: assign the
`mergeClaudeResult` boolean at `:237, :349, :504, :637` and gate `markEndpointComplete` +
`writeCheckpoint` on it. Those two changes together close the whole chain.

Suggested test to add alongside: `ClaudeResultMergerTest` asserting `mergeClaudeResult` returns
`false` and leaves `ep` untouched for (a) rate-limit prose, (b) truncated JSON, (c) valid JSON
missing `procName`.
