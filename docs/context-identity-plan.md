# Context & Identity Management — Master Plan (the system-wide contract)

**Status: EXECUTING** · Created 2026-09-29 · Branch main · This document is the single source of truth.
**The problem we are permanently fixing:** for 3 days, bugs kept appearing wherever work crossed a thread
or a redeploy wiped state. This plan defines the COMPLETE model so every case is handled by design, not
patch-by-patch.

---

## 1. The model (how identity works — 4 layers)

```
L1. IDENTITY SOURCE     AWS SSO device flow → STS GetCallerIdentity → user email/account/role
                        (browser cookie = HTTP session; SSO creds refreshable for 24h)
L2. CONNECTION BINDING  user → bucket/region (POST /api/auth/aws/connect)
                        PERSISTED per-user on S3 (UserConnectionStore) — survives redeploys
                        per-user STORES built with FIXED credentials (SsoStoreSwapper)
L3. EXECUTION CONTEXT   ThreadLocal snapshot: SsoJobContext (stores+credentials) +
                        CurrentHttpSession (session id / propagated scope)
L4. THE INVARIANT       when work moves to another thread, the snapshot moves WITH it:
                        CAPTURE on the submitting thread → CARRY with the task →
                        INSTALL (try-with-resources) on the worker → never read ThreadLocal
                        blindly on a worker; NEVER fall back silently to default credentials
```

## 2. The 5 invariants (checked class-by-class below)

- **I1** Every store-touching task on a non-request thread runs under an installed context.
- **I2** A logged-in user with NO connected bucket can never start work that needs a bucket — refused at the door with a clear message.
- **I3** No silent fallback to the default store / default credential chain — degradation is a LOUD error (a true no-identity legacy path logs a throttled WARN).
- **I4** No shared mutable per-user state (statics, loose-keyed caches) — two concurrent users can never affect each other's prompts, caches, jobs, or data.
- **I5** Connection binding survives redeploys: login auto-reconnects from the saved per-user config (probe-verified) — the user never re-connects manually after a deploy.

## 3. Flow-by-flow verification (every flow, end to end)

| # | Flow | Path | Invariants | Status |
|---|---|---|---|---|
| F1 | Login | browser → device flow → identity → **auto-reconnect (I5)** → warm-up | I5 | ✅ fix in flight (agent D) |
| F2 | Connect | request thread → swapper.connect → probe → per-user stores + persist config | I2, I5 | ✅ verified |
| F3 | Upload/submit | request → **connection guard (I2)** → captureCurrent → enqueue snapshot | I2 | ✅ fix in flight (agent B) |
| F4 | Queue worker | worker → SsoJobContext.install(snapshot) → executor | I1 | ✅ verified (+fix for null-snapshot trap) |
| F5 | Write-behind flush | buffer carries per-entry context → flush installs | I1 | ✅ landed (0179a0c) |
| F6 | Parallel listing (ShallowTree/prefetch) | scope captured → installed per task | I1 | ✅ landed |
| F7 | Source enrich + decompile | jar read ONCE (pinJar) → cache R/W under context | I1 | ✅ landed (407a169) |
| F8 | Prefetch (plsql-parser src-prefetch pool) | capture on crawl thread → install per task | I1 | ✅ fix in flight (agent C) |
| F9 | Error paths (catch blocks writing fragments/tracker) | re-install context in catch | I1 | ✅ fix in flight (agent C) |
| F10 | Claude prompts | per-job extras — NO static shared state | I4 | ✅ fix in flight (agent C) |
| F11 | Reader caches (ParserAnalysisReader/StaticAnalysisReader) | cache key includes user identity | I4 | ✅ fix in flight (agent B) |
| F12 | Cross-ref / conversion / flow / Claude-verify bare threads | capture+install+guard+ownership | I1, I2, I4 | ✅ fix in flight (agent B) |
| F13 | Log archive (@Scheduled) | pick a connected user → propagate → ship; skip when none | I1, I2 | ✅ landed (64b34f9) |
| F14 | Warm-up sweeps | login-triggered (context) + boot sweep (fails soft) | I1 | ✅ verified |
| F15 | Routing resolution | resolvable-but-not-connected → THROW; no-session → throttled WARN | I3 | ✅ fix in flight (agent A) |
| F16 | Credential resolution | no-context → try ambient → else THROW NotAuthenticatedException | I3 | ✅ fix in flight (agent A) |
| F17 | Store outliving logout | per-user provider THROWS instead of default-chain | I3 | ✅ fix in flight (agent A) |
| F18 | Job ownership | conversion/flow ids owned by creator; others get 404 | I4 | ✅ fix in flight (agent B) |
| F19 | flow-poc bare thread | POC-only; no SSO wiring; documented boundary | — | ✅ documented |
| F20 | Analysis repository / view sidecars | per-user routing above cache; cache keyed per-user | I4 | ✅ verified (storage audit) |

## 4. Fix checklist (the execution list)

- [x] Write-behind per-entry credential carry — commit 0179a0c
- [x] Decompile by key + guard fix — commit 5814770
- [x] Batch-1: 7 silent wrong-bucket async sites — commit 378f78c
- [x] Batch-2: 5 feature-broken background jobs — commit 64b34f9
- [x] Source-enrich decompile reads + jar-read-once — commit 407a169
- [x] A: routing never silently falls back (I3) — 7da7442 (16 new tests)
- [x] B: connect-guard on all work endpoints + per-user cache keys + ownership (I2, I4) — 6bf85a9 (ConnectionGuard, 7 tests)
- [x] C: prefetch context + catch-path writes + prompt static removal (I1, I4) — 167097f
- [x] D: auto-reconnect on login (I5) — 93aebf2 (9 new tests)
- [x] Exception advice surfaces the real reason (`reason` field) + IO-gate budget 12→10 — 12c5fb4
- [x] BOOT FIX: @Autowired on the resolver's primary constructor (two ctors broke Spring wiring — caught by LOCAL boot verification before it burned prod) — d386f4a
- [x] Log-archive connected-user gate matched the wrong layer (cache wraps router) — pass ran context-less every minute — 464aadd
- [x] LOCAL end-to-end verification (8085, S3 mode): connect → upload → COMPLETE (84 classes) → **0 context errors in the whole run**; log-archive skips cleanly pre-connect ✓
- [ ] Full reactor suite (3,452+ tests) green on the merged tree
- [ ] Staging deploy + live log verification (connect → upload → 5.6 fast → sources visible)
- [ ] Multi-user spot check: two sessions concurrent uploads (per-entry credential test exists; verify live)
- [ ] Redeploy survival check: deploy → login → upload works with NO manual connect (I5 live)
- [ ] Mark DONE

## 5. Test plan (the gate before "done")

1. **Unit/module**: every agent's module suite + the new contract tests (throw semantics, per-entry carry, auto-reconnect).
2. **Full reactor**: `mvn clean package` — 3,452+ tests, 0 failures.
3. **Live staging — single user**: login → upload jar → job completes with sources; 0 "Upload failed"/"Unable to load credentials" in the log.
4. **Live staging — redeploy survival**: deploy → login → upload works with NO manual connect.
5. **Live staging — two users (parallel)**: two browsers/sessions upload simultaneously; each job signs as its owner; admin portal shows both; no cross-user cache/prompt bleed.
6. **Negative**: upload WITHOUT connect → clear "Connect your AWS account first" message (no AWS stack traces).
