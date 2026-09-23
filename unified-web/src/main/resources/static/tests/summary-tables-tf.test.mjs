/**
 * UI-026h §76 — `jar/js/summary-tables.js`'s `sum-dist`/`sum-batch`/`sum-ext` tables, the fourth
 * through sixth `_initPage` call sites converted to `PA.tf`. None of the three has a type-pill
 * filter, so no `extraFilter` translation was needed — the conversion is mechanical.
 */
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { includes, excludes } from './lib/assert.mjs';

const STATIC = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (rel) => readFile(path.join(STATIC, rel), 'utf8');

const code = (src) =>
  src.replace(/\/\*[\s\S]*?\*\//g, '').replace(/^\s*\/\/.*$/gm, '');

const TABLES = ['sum-dist', 'sum-batch', 'sum-ext'];

export async function test_all_three_tables_use_pa_tf_not_the_local_framework() {
  const src = code(await read('jar/js/summary-tables.js'));
  for (const id of TABLES) {
    includes(src, `PA.tf.init('${id}'`, `${id} must initialise through PA.tf`);
    includes(src, `PA.tf.render('${id}')`, `${id} must render through PA.tf`);
    excludes(src, `this._initPage('${id}'`, `${id}'s old local-framework call must be gone`);
    excludes(src, `this._pageRender('${id}')`, `${id}'s old local-framework render call must be gone`);
    includes(src, `PA.tf.toggleDetail('${id}'`, `${id}'s row click handler must toggle through PA.tf`);
  }
}

export async function test_none_of_the_three_detail_renderers_build_a_tr_wrapper() {
  const src = code(await read('jar/js/summary-tables.js'));
  excludes(src, 'id="sum-dist-detail-', '_renderDistDetail must not build its own wrapper <tr id=...>');
  excludes(src, 'id="sum-batch-detail-', '_renderBatchDetail must not build its own wrapper <tr id=...>');
  excludes(src, 'id="sum-ext-detail-', '_renderExtDetail must not build its own wrapper <tr id=...>');
}

export async function test_column_filters_use_pa_tf() {
  const src = code(await read('jar/js/summary-tables.js'));
  for (const id of TABLES) {
    includes(src, `PA.tf.initColFilters('${id}'`, `${id} must use PA.tf's own column filter popovers`);
  }
}

export async function test_summary_tables_js_has_no_local_framework_call_sites_left() {
  // §79 (sum-ep) was the last one in this file — every table summary-tables.js owns is on PA.tf.
  const src = code(await read('jar/js/summary-tables.js'));
  excludes(src, 'this._initPage(', 'summary-tables.js must have no _initPage call sites left');
}
