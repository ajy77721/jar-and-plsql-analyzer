# Frontend tests

`npm test` from this directory. **No `npm install` is required and there are no dependencies.**

## Why not Playwright

The plan for this harness (`docs/TASKS-FRONTEND.md`, FE-001) suggested Playwright. This does not use
it, and the reason is worth stating rather than leaving as a silent substitution.

Playwright needs an `npm install` and roughly 300 MB of browser binaries inside a repository that is
otherwise built entirely by Maven, and it needs them on every machine and CI agent that runs the
build. That is a large standing cost, and it buys real-browser fidelity that this codebase mostly
does not need: the frontend is plain script tags with no bundler, and its rendering functions are
ordinary functions that take data and return HTML strings.

So the tests here call those functions directly against a small DOM stub and assert on the markup
they produce. This is the same technique that found the defects fixed during the audit — an inline
panel rendering `undefined` for every escaped value, and a table popup that showed less than the
panel beside it — so it is not a theoretical substitute.

**What this cannot check**, stated plainly so nobody assumes otherwise: real layout, CSS, actual
event dispatch, and anything depending on browser quirks. A test that needs those needs a browser,
and the right response then is to add Playwright for that test rather than to pretend this harness
covers it.

## Layout

- `run.mjs` — the runner; discovers and executes `*.test.mjs`
- `lib/dom.mjs` — the DOM stub and the page-global fakes (`PA`, `JA`, `PA.tf`)
- `lib/assert.mjs` — assertions that report the failing value, not just a boolean
- `fixtures/` — API responses; `fixtures/generate.mjs` builds the large one
