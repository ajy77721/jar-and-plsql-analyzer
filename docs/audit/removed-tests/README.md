# Tests removed, and why

## AsyncBoundaryTest (379 lines, 18 tests) — removed 2026-09-04

Every one of its 18 tests asserted async-boundary detection: `CallNode.isAsyncBoundary()`,
`isAsyncChainOp()`, `setAsyncChainOp()`, and `ASYNC_CHAIN` call types.

**None of that exists.** Checked before removing:

- no `async` reference in `CallNode`, or in any module's `src/main`
- no `ASYNC_CHAIN` in any main source
- nothing in the UI (`unified-web/.../static/**/*.js`)
- nothing in `docs/`
- not in the pre-migration backup's main sources either (`_migration-backup-20260901-173235`)

So this is not a regression and not a lost feature — the test was written against something that
never shipped. It could never compile, which is why it sat in the `testExcludes` list.

It is kept here rather than deleted outright: it is a precise specification of the feature, so if
async-boundary detection is ever built, this is the behaviour someone intended. Restore it from here
and re-add it to the module's test sources.

The other 10 excluded classes were repaired in place and now run.
