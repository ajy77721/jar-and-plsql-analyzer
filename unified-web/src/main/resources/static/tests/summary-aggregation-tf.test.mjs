/**
 * UI-026h §74 — `jar/js/summary-aggregation.js`'s Aggregation Flows table, the second `_initPage`
 * call site converted to `PA.tf`. Unlike `sum-claude` (§73), this table has an operation-type pill
 * filter (`_aggFilterOp`) and column filters (`initColFilters`) — checked that both survive the
 * conversion, since `typeFilterFn` has no direct `PA.tf` counterpart and had to become an
 * `extraFilter` closure instead.
 */
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { includes, excludes } from './lib/assert.mjs';

const STATIC = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (rel) => readFile(path.join(STATIC, rel), 'utf8');

const code = (src) =>
  src.replace(/\/\*[\s\S]*?\*\//g, '').replace(/^\s*\/\/.*$/gm, '');

export async function test_aggregation_tab_uses_pa_tf_not_the_local_framework() {
  const src = code(await read('jar/js/summary-aggregation.js'));
  includes(src, 'PA.tf.init(\'sum-agg\'', 'must initialise through PA.tf');
  includes(src, 'PA.tf.render(\'sum-agg\')', 'must render through PA.tf');
  excludes(src, 'this._initPage(\'sum-agg\'', 'the old local-framework call must be gone');
  excludes(src, 'this._pageRender(\'sum-agg\')', 'the old local-framework render call must be gone');
  excludes(src, "this._pageSort(\\'sum-agg\\'", 'sort headers must call PA.tf.sort, not the old framework');
  includes(src, "PA.tf.sort(\\'sum-agg\\'", 'sort headers must call PA.tf.sort');
}

export async function test_the_operation_type_pill_filter_survives_as_an_extraFilter() {
  const src = code(await read('jar/js/summary-aggregation.js'));
  // typeFilterFn had no PA.tf counterpart — must have become an extraFilter closure that reads
  // the same _aggActiveOp state _aggFilterOp sets.
  excludes(src, 'typeFilterFn:', 'typeFilterFn is a local-framework-only option, must not remain');
  includes(src, 'extraFilter: item => !this._aggActiveOp || item.operationType === this._aggActiveOp',
    '_aggFilterOp\'s pill filter must survive as an extraFilter closure over the same state');
  includes(src, 'PA.tf.filter(\'sum-agg\')', '_aggFilterOp must re-run PA.tf\'s filter after changing state');
}

export async function test_column_filters_use_pa_tf_not_the_col_filter_module() {
  const src = code(await read('jar/js/summary-aggregation.js'));
  includes(src, 'PA.tf.initColFilters(\'sum-agg\'', 'must use PA.tf\'s own column filter popovers');
  excludes(src, 'this._initColFilters(\'sum-agg\'',
    'must not use summary-col-filter.js\'s _pageState-coupled implementation');
}

export async function test_the_detail_row_toggle_targets_pa_tf() {
  const src = code(await read('jar/js/summary-aggregation.js'));
  includes(src, "PA.tf.toggleDetail('sum-agg'",
    'the row click handler must toggle through PA.tf, which now owns this table\'s lazy detail state');
  excludes(src, "JA.summary.toggleDetail('agg'",
    'the old shared toggleDetail(prefix, idx) call must be gone for this table');
}
