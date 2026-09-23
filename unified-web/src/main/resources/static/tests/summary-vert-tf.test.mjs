/**
 * UI-026h §78 — `jar/js/summary-tables.js`'s `sum-vert` (Collection Analysis table), the hardest
 * of the twelve `_initPage` call sites: a type toggle, a verification toggle, and a full "Advanced
 * Filters" panel combining five numeric range filters and two checkbox category filters, all
 * previously implemented inside `_pageFilter`'s own per-table branches. Translated into one
 * `extraFilter` closure, `_vertExtraFilter`, reading `_vertFilterState` instead of the old
 * per-table `s._typeFilter`/`s._verifFilter`/`s._rangeFilters`/`s._catFilters` fields.
 *
 * `_vertExtraFilter` is executed for real against realistic fixtures, not just checked as source
 * text — a combined filter predicate like this is exactly where a transcription error (a wrong
 * `&&`/`||`, a swapped `min`/`max`, a missed category key) would otherwise ship silently.
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
  return globalThis.JA.summary;
}

function coll(overrides) {
  return Object.assign({
    name: 'ORDERS', type: 'COLLECTION', domain: 'Orders', usageCount: 1,
    endpoints: new Set(['A.x']), opCounts: {}, _complexity: 'Low', verification: null
  }, overrides);
}

export async function test_sum_vert_uses_pa_tf_not_the_local_framework() {
  const src = code(await read('jar/js/summary-tables.js'));
  includes(src, "PA.tf.init('sum-vert'", 'sum-vert must initialise through PA.tf');
  includes(src, "PA.tf.render('sum-vert')", 'sum-vert must render through PA.tf');
  excludes(src, "this._initPage('sum-vert'", 'the old local-framework call must be gone');
  excludes(src, "this._pageState['sum-vert']", 'sum-vert must not read the old framework\'s state anymore');
  excludes(src, "this._pageFilter('sum-vert')", 'sum-vert must not call the old framework\'s filter anymore');
  includes(src, "PA.tf.toggleDetail('sum-vert'", 'sum-vert\'s row click handler must toggle through PA.tf');
}

export async function test_type_filter_alone_excludes_the_other_type() {
  const summary = await loadSummary();
  summary._vertFilterState = { typeFilter: 'VIEW', verifFilter: '', rangeFilters: {}, catFilters: {} };
  ok(!summary._vertExtraFilter(coll({ type: 'COLLECTION' })), 'a COLLECTION must be excluded when filtering to VIEW');
  ok(summary._vertExtraFilter(coll({ type: 'VIEW' })), 'a VIEW must pass when filtering to VIEW');
}

export async function test_verification_filter_variants() {
  const summary = await loadSummary();
  const base = () => ({ typeFilter: '', rangeFilters: {}, catFilters: {} });

  summary._vertFilterState = { ...base(), verifFilter: 'verified' };
  ok(summary._vertExtraFilter(coll({ verification: 'VERIFIED' })), 'VERIFIED must pass the "verified" filter');
  ok(summary._vertExtraFilter(coll({ verification: 'CLAUDE_VERIFIED' })), 'CLAUDE_VERIFIED must also pass "verified"');
  ok(!summary._vertExtraFilter(coll({ verification: 'NOT_IN_DB' })), 'NOT_IN_DB must fail the "verified" filter');

  summary._vertFilterState = { ...base(), verifFilter: 'not_in_db' };
  ok(summary._vertExtraFilter(coll({ verification: 'NOT_IN_DB' })), 'NOT_IN_DB must pass its own filter');
  ok(!summary._vertExtraFilter(coll({ verification: 'VERIFIED' })), 'VERIFIED must fail the "not_in_db" filter');

  summary._vertFilterState = { ...base(), verifFilter: 'other' };
  ok(summary._vertExtraFilter(coll({ verification: null })), 'an unverified collection must pass the "other" filter');
  ok(!summary._vertExtraFilter(coll({ verification: 'VERIFIED' })), 'VERIFIED must fail the "other" filter');
}

export async function test_range_filter_min_and_max_bounds() {
  const summary = await loadSummary();
  summary._vertFilterState = {
    typeFilter: '', verifFilter: '',
    rangeFilters: { usageCount: { min: 2, max: 5, fn: c => c.usageCount } },
    catFilters: {}
  };
  ok(!summary._vertExtraFilter(coll({ usageCount: 1 })), 'below min must be excluded');
  ok(summary._vertExtraFilter(coll({ usageCount: 3 })), 'inside the range must pass');
  ok(!summary._vertExtraFilter(coll({ usageCount: 6 })), 'above max must be excluded');
}

export async function test_category_filters_for_complexity_and_type() {
  const summary = await loadSummary();
  summary._vertFilterState = {
    typeFilter: '', verifFilter: '', rangeFilters: {},
    catFilters: { vert_complexity: new Set(['High']), vert_type: new Set(['VIEW']) }
  };
  ok(!summary._vertExtraFilter(coll({ _complexity: 'Low', type: 'VIEW' })),
    'a Low-complexity item must be excluded when only High is checked');
  ok(!summary._vertExtraFilter(coll({ _complexity: 'High', type: 'COLLECTION' })),
    'a COLLECTION must be excluded when only VIEW is checked');
  ok(summary._vertExtraFilter(coll({ _complexity: 'High', type: 'VIEW' })),
    'an item matching both category filters must pass');
}

export async function test_all_four_filters_combine_with_and_not_or() {
  const summary = await loadSummary();
  summary._vertFilterState = {
    typeFilter: 'COLLECTION', verifFilter: 'verified',
    rangeFilters: { usageCount: { min: 1, max: 10, fn: c => c.usageCount } },
    catFilters: { vert_complexity: new Set(['Low']) }
  };
  const matchesAll = coll({ type: 'COLLECTION', verification: 'VERIFIED', usageCount: 5, _complexity: 'Low' });
  ok(summary._vertExtraFilter(matchesAll), 'an item matching every filter must pass');

  const failsOnlyType = { ...matchesAll, type: 'VIEW' };
  ok(!summary._vertExtraFilter(failsOnlyType), 'failing just the type filter must exclude the item (AND, not OR)');
}

export async function test_default_filter_state_excludes_nothing() {
  const summary = await loadSummary();
  summary._vertFilterState = { typeFilter: '', verifFilter: '', rangeFilters: {}, catFilters: {} };
  ok(summary._vertExtraFilter(coll({})), 'with no active filters, every item must pass');
}
