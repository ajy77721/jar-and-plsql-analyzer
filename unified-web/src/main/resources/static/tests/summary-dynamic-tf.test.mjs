/**
 * UI-026h §75 — `jar/js/summary-dynamic.js`'s Dynamic Flows table, the third `_initPage` call site
 * converted to `PA.tf`. Same shape as `summary-aggregation.js` (§74): a type-pill filter with no
 * direct `PA.tf` counterpart, translated to `extraFilter`.
 */
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { includes, excludes } from './lib/assert.mjs';

const STATIC = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (rel) => readFile(path.join(STATIC, rel), 'utf8');

const code = (src) =>
  src.replace(/\/\*[\s\S]*?\*\//g, '').replace(/^\s*\/\/.*$/gm, '');

export async function test_dynamic_tab_uses_pa_tf_not_the_local_framework() {
  const src = code(await read('jar/js/summary-dynamic.js'));
  includes(src, 'PA.tf.init(\'sum-dyn\'', 'must initialise through PA.tf');
  includes(src, 'PA.tf.render(\'sum-dyn\')', 'must render through PA.tf');
  excludes(src, 'this._initPage(\'sum-dyn\'', 'the old local-framework call must be gone');
  excludes(src, 'this._pageRender(\'sum-dyn\')', 'the old local-framework render call must be gone');
}

export async function test_the_dispatch_type_pill_filter_survives_as_an_extraFilter() {
  const src = code(await read('jar/js/summary-dynamic.js'));
  excludes(src, 'typeFilterFn:', 'typeFilterFn is a local-framework-only option, must not remain');
  includes(src, 'extraFilter: item => !this._dynActiveType || item.dispatchType === this._dynActiveType',
    '_dynFilterType\'s pill filter must survive as an extraFilter closure over the same state');
}

export async function test_detail_renderer_returns_bare_content_not_a_tr_wrapper() {
  // The bug caught mid-conversion: PA.tf.toggleDetail wraps renderDetail's return value in its
  // own <td>, so a caller-supplied <tr>/<td> would nest invalidly. Both _renderAggDetail (§74)
  // and _renderDynDetail (this section) had this exact shape before being fixed.
  const dyn = code(await read('jar/js/summary-dynamic.js'));
  excludes(dyn, "id=\"sum-dyn-detail-", '_renderDynDetail must not build its own wrapper <tr id=...>');
  const agg = code(await read('jar/js/summary-aggregation.js'));
  excludes(agg, "id=\"sum-agg-detail-", '_renderAggDetail must not build its own wrapper <tr id=...> either');
}

export async function test_the_detail_row_toggle_targets_pa_tf() {
  const src = code(await read('jar/js/summary-dynamic.js'));
  includes(src, "PA.tf.toggleDetail('sum-dyn'", 'the row click handler must toggle through PA.tf');
  excludes(src, "JA.summary.toggleDetail('dyn'", 'the old shared toggleDetail(prefix, idx) call must be gone');
}
