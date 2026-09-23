/**
 * UI-026h §77/§78 — `jar/js/summary-tables.js`'s `sum-vs` and `sum-vert`, both sharing one detail
 * renderer, `_renderVertDetail`. §77 converted `sum-vs` first, while `sum-vert` (the hardest
 * remaining table — type/verification/range/category filters combined) still needed the old
 * framework's full `<tr>` wrapper shape, so `sum-vs` got a thin stripping shim, `_renderVertDetailPA`.
 * §78 converted `sum-vert` too, at which point both callers were on `PA.tf` and the shim's whole
 * reason to exist went away — `_renderVertDetail` now returns bare content directly, shared,
 * unmodified, by both.
 */
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { includes, excludes } from './lib/assert.mjs';

const STATIC = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (rel) => readFile(path.join(STATIC, rel), 'utf8');

const code = (src) =>
  src.replace(/\/\*[\s\S]*?\*\//g, '').replace(/^\s*\/\/.*$/gm, '');

export async function test_sum_vs_uses_pa_tf_not_the_local_framework() {
  const src = code(await read('jar/js/summary-tables.js'));
  includes(src, "PA.tf.init('sum-vs'", 'sum-vs must initialise through PA.tf');
  includes(src, "PA.tf.render('sum-vs')", 'sum-vs must render through PA.tf');
  excludes(src, "this._initPage('sum-vs'", 'the old local-framework call must be gone for sum-vs');
  excludes(src, "this._pageState['sum-vs']", 'sum-vs must not read the old framework\'s state anymore');
  includes(src, "PA.tf.toggleDetail('sum-vs'", 'sum-vs\'s row click handler must toggle through PA.tf');
}

export async function test_the_wrapper_stripping_shim_is_gone_now_both_share_pa_tf() {
  const src = code(await read('jar/js/summary-tables.js'));
  excludes(src, '_renderVertDetailPA', 'the shim §77 needed is obsolete once sum-vert also converted');
  // Both callers now pass _renderVertDetail directly.
  const detailCalls = [...src.matchAll(/renderDetail: \(c, i, esc\) => this\._renderVertDetail\(c, i, esc\)/g)];
  if (detailCalls.length < 2) {
    throw new Error(`expected both sum-vs and sum-vert to call _renderVertDetail directly, found ${detailCalls.length}`);
  }
}
