# UI Polling Audit

Audit of every mechanism in `unified-web/src/main/resources/static/` that causes the browser to
issue the same HTTP request more than once.

**Date:** 2026-09-04
**Method:** static resolution of every timer/SSE site, plus a live measurement (see below).
**Scope:** `index.html` (home), `jar/`, `parser/`, `plsql/`, `war/`, `conversion/`, `shared/`.
**No application code was changed by this audit.**

---

## 0. How the numbers were obtained

The app was built and run locally with Tomcat access logging enabled:

```
mvn -q -pl unified-web -DskipTests install
java -jar unified-web/target/unified-web-1.0.0-SNAPSHOT.jar --server.port=18097 \
  --server.tomcat.basedir=/tmp/pollaudit-tomcat \
  --server.tomcat.accesslog.enabled=true --server.tomcat.accesslog.buffered=false
```

Each page was then loaded in headless Chrome (timer throttling disabled) and left **idle for 70
seconds**, and the access log was diffed per page. Measured `/api` requests per page:

| Page | `/api` requests observed in 70 s idle |
|---|---|
| `/` (home) | **21 × `/api/queue`**, 3 × `/api/sessions`, 2 × `/api/parser/analyses`, 2 × `/api/jar/jars`, 1 each of `/api/war/wars`, `/api/sessions/claude-stats`, `/api/parser/claude/sessions`, `/api/config/polling`, 2 chat |
| `/jar/` | 3 × `/api/sessions/running?analyzer=JAR`, 1 each `/api/jar/jars`, `/api/config/polling`, 2 chat |
| `/war/` | 3 × `/api/sessions/running?analyzer=JAR`, 1 each `/api/war/wars`, `/api/config/polling`, 2 chat |
| `/plsql/` | all singletons (1 × `/api/queue`, 1 × claude status, etc.) |
| `/parser/` | all singletons |
| `/conversion/` | all singletons |

**Caveat, stated explicitly:** `data/` was wiped before this audit, so there are **no analyses and
no jobs**. Every page therefore had nothing to watch. The measurement above is a *floor*, not a
ceiling — it proves what fires with nothing running. Job-scoped pollers (Claude progress, job
status, conversion status, flow status) could not be exercised and were resolved **statically**;
those rows are marked "static only" in the tables below.

The `/api/queue` result is the important one: **18–21 requests/minute from an idle home page with
an empty queue.** That is not a job-scoped poller misbehaving; it is unconditional.

### Configured intervals (live, from `GET /api/config/polling`)

```json
{"sessionPollMs":30000,"claudeProgressMs":30000,"jobStatusMs":10000,
 "logRefreshMs":10000,"correctionMs":30000}
```

These match the defaults hard-coded in `shared/poll-config.js`, so a call site that captures the
defaults early instead of awaiting the fetch is currently **invisible** — it produces the right
number today and the wrong number the moment the server config diverges.

---

## 1. Summary table — every polling site

`PollConfig?` = does the site get its interval through `shared/poll-config.js`.
"Still polls when finished?" is the highest-value column.

### 1a. Home page — `static/index.html`

| file:line | endpoint(s) | interval | what stops it | PollConfig? | polls after finish? |
|---|---|---|---|---|---|
| `index.html:998` | `/api/queue` | **3000 ms** (hardcoded) | **nothing — runs for the life of the tab** | ❌ literal | **YES — polls with an empty queue** |
| `index.html:652` | `/api/sessions` | **30000 ms** (hardcoded) | nothing — runs forever | ❌ literal | YES — polls with zero sessions |
| `index.html:991-992` (inside `pollQueue`) | `/api/jar/jars`, `/api/war/wars`, `/api/parser/analyses`, `/api/parser/claude/sessions`, `/api/sessions/claude-stats` | on queue-drain edge | edge-triggered | n/a | no (edge only) |

### 1b. `jar/`

| file:line | endpoint(s) | interval | what stops it | PollConfig? | polls after finish? |
|---|---|---|---|---|---|
| `jar/js/app.js:98` | `/api/sessions/running?analyzer=JAR` | 30000 | nothing (page lifetime) | ✅ `every('sessionPollMs')` | YES (by design — it is a liveness dot) |
| `jar/js/app.js:94` | same | 60000 (= `sessionPollMs*2`) | replaces the above after 3 consecutive failures | ✅ derived | backoff path, intentional |
| `jar/js/app.js:418` | `/api/queue/{jobId}` | 10000 | job COMPLETE/FAILED/CANCELLED, or 3 consecutive "not found" | ✅ `every('jobStatusMs')` | no |
| `jar/js/app.js:536` | Claude progress | 30000 | `PollConfig.clear` at `:580` on completion | ✅ | no |
| `jar/js/sessions.js:145` | sessions list | 30000 | overlay closed, or explicit stop | ✅ | stops when the overlay is gone |
| `jar/js/targeted.js:224` | `/api/queue/{jobId}` | 10000 | completion / failure / 3 misses | ✅ | no |
| `jar/js/summary-claude.js:536` | Claude progress | 30000 | cleared on 3 terminal branches | ✅ | no |
| `jar/js/summary-corrections.js:394` | corrections status | 30000 | cleared on terminal branches | ✅ | no |
| `jar/js/logviewer.js:105` | log tail | 10000 | `_stopPoll` (`:112`) when Auto is off | ✅ | n/a |
| `jar/js/dash-panel.js:34` | log/dash refresh | 10000 | `clear` at `:29` | ✅ | n/a |
| `jar/js/flow-analysis.js:646` | `/api/jar/jars/{id}/flow/status/{runId}` | **1500 ms** (hardcoded, recursive `setTimeout`) | reschedules only while `status === 'running'`; stops on any other status and on fetch error | ❌ literal | no |
| `jar/js/utils.js:297` | none | — | — | marked `poll-exempt` | ✅ correct |

### 1c. `parser/`

| file:line | endpoint(s) | interval | what stops it | PollConfig? | polls after finish? |
|---|---|---|---|---|---|
| `parser/js/perfSummary.js:471` | `/api/queue` (queue tab) | 10000 | `clear` at `:474` when no job is active | ✅ | no |
| `parser/js/history.js:428` / `:436` | `/api/queue/{jobId}` | 10000 | `done` flag on the next tick | ✅ | one extra tick after finish, then stops |
| `parser/js/claudeInsights.js:142` | Claude progress | 30000 | cleared on terminal status | ✅ | no |
| `parser/js/claudeSessions.js:34` | Claude sessions | 30000 | `clear` at `:39` | ✅ | overlay-scoped |
| `parser/js/logViewer.js:99` | log tail | 10000 | `clear` at `:106` | ✅ | n/a |
| `parser/js/claudeEnrichment.js:438` | none | — | — | marked `poll-exempt` | ✅ correct |

### 1d. `plsql/`

| file:line | endpoint(s) | interval | what stops it | PollConfig? | polls after finish? |
|---|---|---|---|---|---|
| `plsql/js/app.js:3734` | `/api/queue` | **3000 ms** (hardcoded, recursive `setTimeout`) | only that the Home-menu "queue" panel is still the open panel — **reschedules unconditionally even when the queue is empty** | ❌ literal | **YES** |
| `plsql/js/app.js:3953` | `/api/queue` | **3000 ms** (hardcoded, recursive `setTimeout`) | guarded: only reschedules `if (running \|\| queued.length > 0)` **and** the queue tab is active | ❌ literal | no |
| `plsql/js/app.js:1069` | jobs list | 10000 | terminal status, or a 30-minute hard cap | ⚠️ reads `PollConfig.jobStatusMs` directly at registration | no |
| `plsql/js/app.js:1305` | `/api/plsql/jobs/{id}` | 10000 | COMPLETE/FAILED/CANCELLED, 30-min cap, 20 consecutive errors | ⚠️ direct read | no |
| `plsql/js/app.js:783` | Claude progress | 30000 | `!progress.running` | ⚠️ direct read | no |
| `plsql/js/claude.js:339` | Claude progress | 30000 | `!progress.running` (3 branches), 2-hour cap, 10 errors | ⚠️ direct read | no |
| `plsql/js/summary-claude.js:330` | Claude progress | 30000 | `!progress.running` | ⚠️ direct read | no |
| `plsql/js/summary-progress.js:231` | Claude progress | 30000 | `!progress.running` → `_stopProgressPoll`; also skips the fetch when the tab is hidden | ⚠️ direct read | no |
| `plsql/js/logViewer.js:104` | log tail | 10000 | `_stopAuto` | ⚠️ direct read | n/a |

### 1e. `conversion/`, `war/`, `shared/`

| file:line | endpoint(s) | interval | what stops it | PollConfig? | polls after finish? |
|---|---|---|---|---|---|
| `conversion/index.html:449` | conversion job status | 10000 | 404 (`:471`) or terminal status (`:493`) | ✅ | no |
| `conversion/index.html:636` | none | — | — | marked `poll-exempt` | ✅ correct |
| `shared/log-fab.js:243` | log tail | 10000 | `clear` at `:250` when Auto is unchecked | ✅ | n/a |
| `war/js/app-overrides.js:6` | rewrites `/api/jar/progress` → `/api/war/progress` | n/a | n/a | n/a | not a poller |

### 1f. SSE / EventSource

| file:line | stream | closed by | reconnect risk |
|---|---|---|---|
| `plsql/js/claude.js:304` | `/api/queue/events` | **never** — `es.onerror = () => {}` at `:325` swallows the error without closing | **YES.** The browser auto-reconnects a failed `EventSource` roughly every 3 s, forever. Nothing else in this file closes `_queueSSE` except a subsequent `_connectQueueSSE()` call. |
| `plsql/js/api.js:381` | `/api/plsql/claude/progress/stream` | closes after 3 errors | bounded ✅ |
| `plsql/js/api.js:554` | `/api/plsql/progress` | closes on `complete`, on a server error event, or after 5 errors | bounded ✅ |
| `plsql/js/app.js:1229` | `/api/queue/events` | closed on job completion | bounded ✅ |
| `jar/js/app.js:375` | `/api/jar/progress` | closed in the `finally` at `:522` | bounded ✅ |
| `jar/js/app.js:451` | `/api/queue/events` | `onerror` closes and falls back to polling (`:478`) | bounded ✅ |
| `parser/js/history.js:398` | `/api/queue/events` | `onerror` closes and falls back to polling (`:425`) | bounded ✅ |
| `parser/js/claudeEnrichment.js:180` | parser claude progress stream | `error`/`complete` both call `_stopSSE` | bounded ✅ |
| `jar/js/cross-ref.js:995` | claude run endpoint | opened then **immediately** closed on the next line | Not a loop, but it fires one **completely wasted GET** to a POST-only endpoint on every run. Dead code. |

### 1g. Duplicate fetch of the same endpoint on one page load

Measured on `/`: `/api/jar/jars` twice, `/api/parser/analyses` twice.

`index.html:595` runs `Promise.all([loadStats(), loadProgress(), loadSessions()])`. Both
`loadStats` and `loadProgress` call `fetchJars()` / `fetchParserAnalyses()`
(`index.html:428-448`), which memoise with `if (!_cachedJars) { ... await fetch ... }`. Because the
two callers start concurrently, **both see the cache still empty and both issue the request** — a
classic cache stampede. The memo caches the *value*, not the *in-flight promise*.

---

## 2. Ranked findings — worst first

Ranked by (requests/minute) × (does it ever stop?).

### #1 — Home page polls `/api/queue` every 3 s forever. MEASURED: 18–21 req/min on an idle tab.
`static/index.html:998` — `_queuePollId = setInterval(pollQueue, 3000);`

This is almost certainly the behaviour the user is reporting. It is unconditional: there is no
check for whether anything is running, no `visibilitychange` guard, and no `clearInterval` anywhere
in the file. Open the home page and walk away and it issues ~1,200 requests/hour against an empty
queue. Every tick also calls `renderQueue()`, rebuilding the queue DOM 20 times a minute.

It also **hardcodes `3000`** while `jobStatusMs` is configured to `10000` — it polls 3.3× faster
than the configured value. The comment above it ("Poll queue state instead of SSE — avoids holding
a persistent connection…") explains why SSE was dropped but not why the interval is 3 s.

Compounding: `static/index.html` loads `shared/poll-config.js` at **line 1002**, *after* the inline
script block that needs it, so `PollConfig` is not even defined when this line executes.

### #2 — `PollingConfigGateTest` does not cover the home page, which is where the worst offender lives.
`unified-web/src/test/java/com/analyzer/arch/PollingConfigGateTest.java:53`

`POLLING_DIRS = List.of("jar", "parser", "plsql", "conversion", "shared")`. Files directly under
`static/` — i.e. `static/index.html`, holding both worst-in-class literals (3000 and 30000) — are
never scanned. The guard is green while the two loudest pollers in the product sit outside it.

Second gap: `LITERAL_INTERVAL` only inspects `setInterval`. The doc comment justifies this
("a `setTimeout` is a one-shot"), but three real pollers in this codebase are recursive
`setTimeout` loops and are consequently invisible to the guard — `plsql/js/app.js:3734`,
`plsql/js/app.js:3953`, and `jar/js/flow-analysis.js:646`. Both of the 3-second `/api/queue`
pollers in `plsql/js/app.js` are of exactly this shape.

### #3 — PL/SQL Home-menu queue panel: `/api/queue` every 3 s, no empty-queue guard.
`plsql/js/app.js:3734`

```js
PA.homeMenu._queueTimer = setTimeout(() => {
    if (PA.homeMenu._current === 'queue') PA.homeMenu._renderQueue(el);
}, 3000);
```

`_renderQueue` (`plsql/js/app.js:3701`) fetches `/api/queue` and then unconditionally reschedules
itself. 20 req/min for as long as the panel is open, whether or not anything is queued. Its sibling
at `plsql/js/app.js:3953` does this correctly — it reschedules only `if (running || queued.length > 0)`
— so the fix already exists twelve lines away. This is also a *fetch inside a render path*: the
network call and the DOM build are the same function, so any re-render is also a request.

### #4 — `plsql/js/claude.js:304`: `/api/queue/events` SSE that can never be closed by an error.
`plsql/js/claude.js:325` — `es.onerror = () => { /* SSE errors are normal on reconnect */ };`

Every other `/api/queue/events` consumer in the codebase closes on error and either gives up or
falls back to polling. This one does not, so a dropped stream becomes a permanent browser
reconnect loop (~1 connection attempt every 3 s by the default EventSource retry) that persists
after the Claude job it was opened for has finished. Unlike the timer findings this one does not
appear as `/api/queue` in an access log — it appears as repeated `/api/queue/events` connections —
so it is easy to miss. It could not be exercised in this run (no jobs exist), so it is a static
finding.

### #5 — Home page polls `/api/sessions` every 30 s forever, hardcoded.
`static/index.html:652` — `setInterval(loadSessions, 30000);`

MEASURED: 3 calls in 70 s idle. Lower volume than #1 but the same defect class — never cleared,
never guarded, bypasses `PollConfig.sessionPollMs` (which happens to hold the same value today, so
the bypass is currently silent). Runs with "No Claude sessions yet" on screen.

### #6 — Duplicate `/api/jar/jars` and `/api/parser/analyses` on every home page load.
`static/index.html:428-448` + `:595`

MEASURED: each fired twice. Cache-stampede in the memo helpers (see §1g). Doubles the cost of the
heaviest two calls on the page's cold path.

### #7 — `jar/js/flow-analysis.js:646` polls flow status every 1500 ms, hardcoded.
Fastest interval in the codebase, 40 req/min while a flow run is active. It *does* stop correctly
(only reschedules while `status === 'running'`, and stops on error), which is why it ranks below the
unbounded ones — but 1.5 s is 6.7× faster than the configured `jobStatusMs` of 10 s, and it is
invisible to the gate test because it uses `setTimeout`. Static finding; no flow run was possible.

### #8 — Nine PL/SQL call sites read `PollConfig.<key>` directly instead of using `PollConfig.every()`.
`plsql/js/app.js:815, 1073, 1359`, `plsql/js/claude.js:389`, `plsql/js/summary-claude.js:353`,
`plsql/js/summary-progress.js:235`, `plsql/js/logViewer.js:104`

These pass the gate test (the value is not a literal) but reintroduce **defect 1** that
`poll-config.js` was written to fix: they read the property at `setInterval` registration time
rather than awaiting `PollConfig.ready()`. If the timer starts before `GET /api/config/polling`
resolves, it silently captures the hard-coded default and keeps it for the lifetime of the timer.
Today the defaults equal the served values, so this is latent, not active — it becomes a live bug
the first time anyone changes the server-side config. Most of these are user-triggered well after
page load, so the race rarely loses; `plsql/js/logViewer.js:104` and `plsql/js/app.js:815` are the
ones most likely to fire early.

### #9 — `jar/js/cross-ref.js:995`: a wasted GET on every Claude cross-ref run.
`new EventSource(url)` immediately followed by `evtSource.close()`. The constructor has already
dispatched a GET to a POST-only endpoint by the time `close()` runs. One request, not a loop, but
it is pure waste and the adjacent comment shows the author knew it was wrong.

---

## 3. Recommended change per finding

| # | Exact location to edit | Change |
|---|---|---|
| 1 | `static/index.html:998` | Replace `setInterval(pollQueue, 3000)` with `PollConfig.every('jobStatusMs', pollQueue)` and delete the explicit `pollQueue()` on `:997` (`every` fires immediately). Add an empty-queue guard: if `d.current` is null and `d.queued.length === 0`, stop the timer and restart it on the next user action or SSE nudge. At minimum add a `visibilitychange` listener that clears while `document.hidden`. **Also move `<script src="shared/poll-config.js">` from `:1002` to before the inline script block** — otherwise `PollConfig` is undefined at `:998`. |
| 2 | `PollingConfigGateTest.java:53` | Add the static root itself to the scan (or add `""`/a top-level-file pass) so `static/index.html` is covered. Separately, extend the `setInterval`-only scan to catch recursive `setTimeout` — e.g. flag a `setTimeout` with a literal ≥ 1000 ms whose callback re-invokes the enclosing function — or require an explicit `poll-exempt` on any `setTimeout` ≥ 1000 ms in these directories. |
| 3 | `plsql/js/app.js:3734` | Copy the guard from its sibling at `:3953`: only reschedule when `current || queued.length > 0`. Use `PollConfig.jobStatusMs`, not `3000`. Better still, split the fetch out of `_renderQueue` so a re-render does not imply a request. |
| 4 | `plsql/js/claude.js:325` | Give it the same bounded handler the rest of the codebase uses: count errors and `es.close()` after N (`plsql/js/api.js:385` is the pattern), and close the stream in `stopPoll()` at `:397` — that method already closes `sseConnection` but not `_queueSSE`. |
| 5 | `static/index.html:652` | `PollConfig.every('sessionPollMs', loadSessions)` (after the script-order fix from #1). Consider stopping when the last response had zero RUNNING sessions. |
| 6 | `static/index.html:429`, `:436`, `:443` | Memoise the **promise**, not the resolved value: `if (!_cachedJars) _cachedJars = fetch('/api/jar/jars').then(r => r.json()); return _cachedJars;` and make `invalidateCache()` (`:449`) null it as it already does. Concurrent callers then share one request. |
| 7 | `jar/js/flow-analysis.js:646` | Replace the literal `1500` with `PollConfig.jobStatusMs`, or add a `flowStatusMs` key to `shared/poll-config.js` and `PollingConfigController` if 1.5 s is genuinely required for UX. |
| 8 | `plsql/js/app.js:815, 1073, 1359`; `plsql/js/claude.js:389`; `plsql/js/summary-claude.js:353`; `plsql/js/summary-progress.js:235`; `plsql/js/logViewer.js:104` | Convert each `setInterval(fn, PollConfig.X)` to `PollConfig.every('X', fn)` and store the returned promise, cancelling via `PollConfig.clear()`. Several of these already have bespoke `clearInterval` teardown that would need to become `PollConfig.clear`. Low urgency, but it is the difference between the config mechanism working and looking like it works. |
| 9 | `jar/js/cross-ref.js:995-996` | Delete both lines. The `fetch`-based SSE reader below them is the real implementation. |

---

## 4. What already behaves well — do not "fix" these

- **`shared/poll-config.js` itself.** `every()`/`clear()` is the right shape: it awaits the config
  before scheduling, fires once immediately, and returns a cancellable handle that is safe to clear
  before the config lands. Twenty-plus call sites use it correctly.
- **All of `jar/`** except `flow-analysis.js`. Measured idle cost of `/jar/` over 70 s was three
  requests total, all of them the intentional 30 s session-liveness dot. Every job-scoped poller in
  `jar/` clears itself on COMPLETE/FAILED/CANCELLED, and `jar/js/app.js:418`, `jar/js/targeted.js:224`
  additionally bail out after 3 consecutive "job not found" responses (server-restart protection).
- **`jar/js/app.js:91-95`** — the ×2 backoff after three consecutive session-poll failures is a
  deliberate, correct degradation, not a hardcoded interval.
- **`conversion/index.html:449`** — textbook: `PollConfig.every('jobStatusMs', …)`, stops on 404 and
  on terminal status, and the comment at `:461` shows the double-tick issue was already thought
  through.
- **`plsql/js/app.js:3953`** (rightTabs queue) — correctly guarded on `running || queued.length > 0`
  *and* on the tab being active. This is the model for fixing `:3734`.
- **`plsql/js/app.js:1069` and `:1305`** — 30-minute hard cap plus a consecutive-error ceiling.
  `plsql/js/claude.js:339` has a 2-hour cap and a 10-error ceiling. Bounded runaway protection like
  this is rare and worth keeping.
- **`plsql/js/summary-progress.js:231`** — skips the fetch entirely when its tab is not visible. The
  only site in the codebase that does visibility-aware polling; the pattern should spread, not go.
- **`plsql/js/claude.js:308-311`** — the comment records that a previous bug turned every
  `job-progress` SSE event into a full `/api/queue` fetch, and the code now reads state off the
  event instead. That fix is correct and should not be undone.
- **The three `poll-exempt` markers** (`jar/js/utils.js:297`, `parser/js/claudeEnrichment.js:438`,
  `conversion/index.html:636`) are all genuine: elapsed-time counters and a DOM-ready wait, no
  network traffic.
- **SSE teardown in `jar/`, `parser/`, and `plsql/js/api.js`** is bounded everywhere except
  `plsql/js/claude.js:325`.

---

## 5. Caveats on this audit

- `data/` was empty. No job-scoped poller could be observed running; those rows are static analysis
  only and are labelled as such. In particular findings #4, #7 and most of #8 were **not** measured.
- Headless Chrome was run with background throttling disabled. A real backgrounded tab would see
  browsers throttle `setInterval` to ~1/minute, which softens but does not remove finding #1 — the
  home page is typically a foreground tab.
- The measurement window was 70 s per page. Pollers slower than that (none found) would be missed.
- Tomcat access logging does not record the duration of a long-lived SSE connection, so reconnect
  loops (finding #4) would need a longer run with a deliberately killed stream to observe directly.
