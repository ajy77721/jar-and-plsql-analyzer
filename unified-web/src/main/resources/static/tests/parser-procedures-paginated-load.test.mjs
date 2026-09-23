/**
 * UI-024 (§113) — `PA.api.getProcedures()` used to be a single unparamterised `fetch`, which routes
 * to `FacetPageResponder`'s legacy bare-array path (no `limit`, no `Accept` header) — one unbounded
 * server-side read and response, on every analysis load (`app.js:30,1070`). Converted to loop the
 * same paged `/procedures` endpoint everyone else already uses, bounding each request, while keeping
 * the method's own external contract (returns the full array) identical — so neither caller needed
 * to change. Making the 39 downstream `PA.nodeIndex` reads (20 files) genuinely lazy is separate,
 * higher-risk work with no corpus here to validate a cache-miss redesign against — recorded in the
 * registry rather than attempted blind.
 */
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { ok, equal } from './lib/assert.mjs';

const STATIC = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (rel) => readFile(path.join(STATIC, rel), 'utf8');

function node(id) {
  return { nodeId: id, name: id, schema: 'ERP', linesOfCode: 10 };
}

async function loadApi(fetchImpl) {
  globalThis.window = globalThis;
  globalThis.document = { getElementById: () => null, body: { appendChild() {} }, createElement: () => ({ style: {} }) };
  globalThis.PA = globalThis.PA || {};
  globalThis.fetch = fetchImpl;
  new Function(await read('shared/page-envelope.js'))();
  new Function(await read('parser/js/api.js'))();
  return globalThis.PA.api;
}

export async function test_getProcedures_loops_pages_until_exhausted_and_concatenates() {
  const pages = [
    { items: [node('A'), node('B')], page: { nextCursor: 'c1', hasMore: true, total: 3, totalIsExact: true, limit: 2, sort: null } },
    { items: [node('C')], page: { nextCursor: null, hasMore: false, total: 3, totalIsExact: true, limit: 2, sort: null } }
  ];
  let calls = 0;
  const api = await loadApi(async (url) => {
    ok(url.includes('limit='), 'each request must be bounded by a limit parameter: ' + url);
    const body = pages[calls++];
    return { ok: true, status: 200, json: async () => body };
  });
  api._analysisName = 'A1';

  const nodes = await api.getProcedures();
  equal(calls, 2, 'must have made exactly 2 bounded requests, not one unbounded one');
  equal(nodes.length, 3);
  equal(nodes[0].nodeId, 'A');
  equal(nodes[2].nodeId, 'C');
}

export async function test_getProcedures_returns_the_empty_sentinel_on_failure_not_a_throw() {
  const api = await loadApi(async () => ({ ok: false, status: 500, url: '/x' }));
  api._analysisName = 'A1';

  const nodes = await api.getProcedures();
  ok(Array.isArray(nodes), 'a failed load must still be array-shaped so every render path keeps working');
  equal(nodes.length, 0);
}

/**
 * UI-024 (§165) — superseded, not just stale: `app.js` no longer calls `getProcedures()` at all,
 * on either of its own two call sites (`PA.loadAnalysis`/`PA.pullData`). That eager fetch was
 * *removed*, not merely proven unaffected by §113's own pagination fix — `PA.nodeIndex
 * .ensureLoaded()` is now the sole caller, triggered lazily by whichever consumer needs node data
 * first. §113's real claim — that `getProcedures()` itself kept its external contract (still
 * returns the full array) — remains true and is still exercised directly by
 * `test_getProcedures_loops_pages_until_exhausted_and_concatenates` above; this test's own narrower
 * claim (that `app.js` specifically would never need to change) did not survive the later fix.
 */
export async function test_app_js_no_longer_eagerly_calls_getProcedures_at_all() {
  const src = await read('parser/js/app.js');
  ok(!src.includes('result.nodes = await PA.api.getProcedures();'),
    'app.js must not eagerly fetch every node on load any more — PA.nodeIndex.ensureLoaded() owns that now');
}
