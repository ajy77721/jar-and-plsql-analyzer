/**
 * UI-026h §79 — `jar/js/summary-tables.js`'s `sum-ep` (the main Endpoint Report card grid), the
 * last table in this file and the tenth of twelve `_initPage` call sites converted overall.
 *
 * Two things distinguish it from every table converted so far:
 *  - `_epCardSort`, a plain dropdown that re-sorts the already-filtered array directly (not a
 *    column-header sort `PA.tf.sort` could drive) — kept as direct `PA.tf.state('sum-ep').filtered`
 *    manipulation, the same shape the old code already used.
 *  - No `renderDetail` at all: `_renderEpCard` builds its own eager detail div, toggled by the
 *    *other* lazy mechanism the shared `toggleDetail` already has (`dataset.lazyEp`, reading
 *    `this._epReports` directly) — independent of which pagination framework owns the table, so
 *    it needed no change, the same shape `sum-claude`'s conversion (§73) already established.
 *
 * Converting `sum-ep` also surfaced a real gap in `_clearFilterPA` itself (used by every table
 * converted so far that has a "Clear" button): it only ever reset text/domain, never the
 * per-table advanced-filter state (`_epFilterState`/`_vertFilterState`) or their DOM — meaning
 * `sum-vert`'s own Clear button has been silently incomplete since §78. Fixed retroactively.
 */
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { includes, excludes, ok } from './lib/assert.mjs';

const STATIC = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (rel) => readFile(path.join(STATIC, rel), 'utf8');

const code = (src) =>
  src.replace(/\/\*[\s\S]*?\*\//g, '').replace(/^\s*\/\/.*$/gm, '');

async function loadSummary() {
  globalThis.window = globalThis;
  globalThis.JA = globalThis.JA || {};
  new Function(await read('jar/js/summary-tables.js'))();
  new Function(await read('jar/js/summary-helpers.js'))();
  return globalThis.JA.summary;
}

function ep(overrides) {
  return Object.assign({
    totalCollections: 1, totalViews: 0, totalDbOperations: 1, inScopeCalls: 0,
    externalScopeCalls: 0, totalMethods: 1, totalLoc: 1,
    httpMethod: 'GET', sizeCategory: 'S', performanceImplication: 'Low Impact',
    operationTypes: ['READ']
  }, overrides);
}

export async function test_sum_ep_uses_pa_tf_not_the_local_framework() {
  const src = code(await read('jar/js/summary-tables.js'));
  includes(src, "PA.tf.init('sum-ep'", 'sum-ep must initialise through PA.tf');
  includes(src, "PA.tf.render('sum-ep')", 'sum-ep must render through PA.tf');
  excludes(src, "this._initPage('sum-ep'", 'the old local-framework call must be gone');
  excludes(src, "this._pageState['sum-ep']", 'sum-ep must not read the old framework\'s state anymore');
  excludes(src, "this._pageFilter('sum-ep')", 'sum-ep must not call the old framework\'s filter anymore');
}

export async function test_toggle_detail_for_ep_is_untouched() {
  // No renderDetail passed to PA.tf.init — the lazyEp mechanism doesn't care which framework owns
  // the table, so the shared toggleDetail('ep', idx) call must remain exactly as it was.
  const src = code(await read('jar/js/summary-tables.js'));
  includes(src, "JA.summary.toggleDetail('ep',", 'the eager-detail-div toggle must be untouched');
}

export async function test_range_filter_bounds() {
  const summary = await loadSummary();
  summary._epFilterState = { rangeFilters: { totalLoc: { min: 10, max: 100 } }, catFilters: {} };
  ok(!summary._epExtraFilter(ep({ totalLoc: 5 })), 'below min must be excluded');
  ok(summary._epExtraFilter(ep({ totalLoc: 50 })), 'inside the range must pass');
  ok(!summary._epExtraFilter(ep({ totalLoc: 200 })), 'above max must be excluded');
}

export async function test_generic_category_filter_on_httpMethod() {
  const summary = await loadSummary();
  summary._epFilterState = { rangeFilters: {}, catFilters: { httpMethod: new Set(['POST']) } };
  ok(!summary._epExtraFilter(ep({ httpMethod: 'GET' })), 'GET must be excluded when only POST is checked');
  ok(summary._epExtraFilter(ep({ httpMethod: 'POST' })), 'POST must pass');
}

export async function test_operationTypes_is_array_membership_not_equality() {
  // An endpoint can have more than one operation type — the filter must match if ANY of the
  // endpoint's own operation types is in the allowed set, not require the whole array to match.
  const summary = await loadSummary();
  summary._epFilterState = { rangeFilters: {}, catFilters: { operationTypes: new Set(['WRITE']) } };
  ok(!summary._epExtraFilter(ep({ operationTypes: ['READ'] })), 'no overlap must be excluded');
  ok(summary._epExtraFilter(ep({ operationTypes: ['READ', 'WRITE'] })),
    'partial overlap must pass — this is membership, not exact-set equality');
}

export async function test_range_and_category_filters_combine_with_and() {
  const summary = await loadSummary();
  summary._epFilterState = {
    rangeFilters: { totalCollections: { min: 1, max: 5 } },
    catFilters: { sizeCategory: new Set(['S']) }
  };
  ok(summary._epExtraFilter(ep({ totalCollections: 2, sizeCategory: 'S' })), 'matching both must pass');
  ok(!summary._epExtraFilter(ep({ totalCollections: 2, sizeCategory: 'L' })),
    'failing only the category filter must exclude the item (AND, not OR)');
}

export async function test_clear_filter_pa_resets_ep_advanced_state_for_real() {
  // The real gap this section found: _clearFilterPA only ever touched text/domain before this fix
  // — sum-vert's own Clear button has been silently incomplete since §78. Executed, not just
  // checked as source text, since "the reset call is present somewhere" doesn't prove it actually
  // clears the right object.
  globalThis.window = globalThis;
  globalThis.document = { getElementById: () => null, querySelectorAll: () => [] };
  globalThis.JA = globalThis.JA || {};
  globalThis.PA = { tf: { state: () => ({}), setSearch() {}, setDomainFilter() {}, filter() {} } };
  new Function(await read('jar/js/summary-helpers.js'))();
  const summary = globalThis.JA.summary;

  summary._epFilterState = { rangeFilters: { totalLoc: { min: 1, max: 2 } }, catFilters: { httpMethod: new Set(['GET']) } };
  summary._clearFilterPA('sum-ep');
  ok(Object.keys(summary._epFilterState.rangeFilters).length === 0, '_clearFilterPA must empty sum-ep\'s range filters');
  ok(Object.keys(summary._epFilterState.catFilters).length === 0, '_clearFilterPA must empty sum-ep\'s category filters');

  summary._vertFilterState = { typeFilter: 'VIEW', verifFilter: 'verified', rangeFilters: { x: {} }, catFilters: { y: new Set() } };
  summary._clearFilterPA('sum-vert');
  ok(summary._vertFilterState.typeFilter === '', '_clearFilterPA must reset sum-vert\'s type filter too');
  ok(summary._vertFilterState.verifFilter === '', '_clearFilterPA must reset sum-vert\'s verification filter too');
}
