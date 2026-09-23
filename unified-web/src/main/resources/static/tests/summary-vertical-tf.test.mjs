/**
 * UI-026h §80 — `jar/js/summary-vertical.js`'s `sum-vertm`/`sum-vertc`, the eleventh and twelfth
 * (and last) of the twelve `_initPage` call sites converted. This closes UI-026h entirely.
 *
 * `_vertCrossFilter` is the trickiest piece in the whole bucket: it doesn't filter existing rows,
 * it replaces `sum-vertc`'s underlying array outright, and for the two "-ops" modes it flattens
 * one row per collection into one row per collection×operation pair — a shape change no
 * `extraFilter` predicate could express. `PA.tf.setData(id, data)` is the right primitive instead.
 * Executed for real against realistic fixtures, since a data-replacement/flattening path like this
 * is exactly where a source-text check would miss a wrong filter predicate or a dropped field.
 */
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { includes, excludes, ok, equal } from './lib/assert.mjs';

const STATIC = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (rel) => readFile(path.join(STATIC, rel), 'utf8');

const code = (src) =>
  src.replace(/\/\*[\s\S]*?\*\//g, '').replace(/^\s*\/\/.*$/gm, '');

export async function test_both_tables_use_pa_tf_not_the_local_framework() {
  const src = code(await read('jar/js/summary-vertical.js'));
  for (const id of ['sum-vertm', 'sum-vertc']) {
    includes(src, `PA.tf.init('${id}'`, `${id} must initialise through PA.tf`);
    excludes(src, `this._initPage('${id}'`, `${id}'s old local-framework call must be gone`);
    excludes(src, `this._pageState['${id}']`, `${id} must not read the old framework's state anymore`);
  }
}

export async function test_neither_detail_renderer_builds_a_tr_wrapper() {
  const src = code(await read('jar/js/summary-vertical.js'));
  excludes(src, 'id="sum-vertm-detail-', '_renderVertmDetail must not build its own wrapper <tr id=...>');
  excludes(src, 'id="sum-vertc-detail-', '_renderVertcDetail must not build its own wrapper <tr id=...>');
}

async function loadSummary() {
  globalThis.window = globalThis;
  globalThis.JA = globalThis.JA || {};
  const calls = { setData: [], filter: [] };
  globalThis.PA = {
    tf: {
      _states: {},
      state(id) { return this._states[id] || (this._states[id] = { colFilters: {}, sortCol: -1, sortDir: 'asc' }); },
      init(id) { this._states[id] = this._states[id] || { colFilters: {}, sortCol: -1, sortDir: 'asc' }; },
      setData(id, data) { calls.setData.push({ id, data }); this._states[id].data = data; },
      filter(id) { calls.filter.push(id); },
      sort() {}, render() {}, toggleDetail() {}, initColFilters() {}
    }
  };
  new Function(await read('jar/js/summary-vertical.js'))();
  const summary = globalThis.JA.summary;
  summary._calls = calls;
  return summary;
}

function coll(name, ownerDomain, accessedByDomains, ops) {
  return { name, type: 'COLLECTION', ownerDomain, accessedByDomains: new Set(accessedByDomains),
    accessedByEndpoints: new Set(['A.x']), operations: new Set(ops), _srcIdx: 0,
    accessType: 'DIRECT_COLLECTION_ACCESS', recommendation: 'REST_API' };
}

export async function test_all_mode_restores_the_full_unflattened_dataset() {
  const summary = await loadSummary();
  const all = [coll('A', 'Orders', ['Orders'], ['READ'])];
  summary._vertCrossAllData = all;
  summary._vertCrossPrimary = 'Orders';
  PA.tf.state('sum-vertc');
  summary._vertCrossFilter('all');
  const call = summary._calls.setData.at(-1);
  ok(call.data === all, 'the "all" mode must pass the original, unflattened array straight through');
}

export async function test_c2e_coll_mode_keeps_other_owned_collections_reachable_from_us() {
  const summary = await loadSummary();
  const all = [
    coll('OURS', 'Orders', ['Orders'], ['READ']),          // owned by us — must be excluded
    coll('THEIRS', 'Billing', ['Orders'], ['READ']),        // owned by Billing, we access it — included
    coll('UNRELATED', 'Billing', ['Shipping'], ['READ'])    // owned by Billing, but we never access it
  ];
  summary._vertCrossAllData = all;
  summary._vertCrossPrimary = 'Orders';
  summary._vertCrossFilter('c2e-coll');
  const data = summary._calls.setData.at(-1).data;
  equal(data.length, 1, 'only the collection owned elsewhere AND accessed by us must remain');
  equal(data[0].name, 'THEIRS');
}

export async function test_ops_mode_flattens_one_row_per_collection_operation_pair() {
  const summary = await loadSummary();
  const all = [coll('THEIRS', 'Billing', ['Orders'], ['READ', 'WRITE'])];
  summary._vertCrossAllData = all;
  summary._vertCrossPrimary = 'Orders';
  summary._vertCrossFilter('c2e-ops');
  const data = summary._calls.setData.at(-1).data;
  equal(data.length, 2, 'two operations on one collection must flatten into two rows');
  const ops = data.map(r => r._singleOp).sort();
  equal(JSON.stringify(ops), JSON.stringify(['READ', 'WRITE']));
  for (const row of data) {
    ok(row.operations instanceof Set && row.operations.size === 1,
      'each flattened row\'s own operations set must contain only its one operation');
  }
}

export async function test_e2c_mode_is_the_mirror_direction() {
  const summary = await loadSummary();
  const all = [
    coll('OURS_SHARED', 'Orders', ['Orders', 'Billing'], ['READ']), // ours, accessed by someone else too
    coll('OURS_PRIVATE', 'Orders', ['Orders'], ['READ']),           // ours, only we access it
    coll('THEIRS', 'Billing', ['Billing'], ['READ'])
  ];
  summary._vertCrossAllData = all;
  summary._vertCrossPrimary = 'Orders';
  summary._vertCrossFilter('e2c-coll');
  const data = summary._calls.setData.at(-1).data;
  equal(data.length, 1, 'only our own collection accessed by an outside domain must remain');
  equal(data[0].name, 'OURS_SHARED');
}
