/**
 * UI-026h §73 — `jar/js/summary-claude.js`'s Claude Insights card grid is the first of the twelve
 * `_initPage` call sites converted to the shared `PA.tf` framework. The other eleven are untouched
 * (still on `_initPage`/`_pageState`) — this is a single, isolated conversion, not a framework
 * rewrite, so this test only checks the one file that changed.
 */
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { includes, excludes } from './lib/assert.mjs';

const STATIC = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (rel) => readFile(path.join(STATIC, rel), 'utf8');

const code = (src) =>
  src.replace(/\/\*[\s\S]*?\*\//g, '').replace(/^\s*\/\/.*$/gm, '');

export async function test_claude_tab_uses_pa_tf_not_the_local_framework() {
  const src = code(await read('jar/js/summary-claude.js'));
  includes(src, 'PA.tf.init(\'sum-claude\'', 'the Claude tab must initialise through PA.tf');
  includes(src, 'PA.tf.render(\'sum-claude\')', 'the Claude tab must render through PA.tf');
  excludes(src, 'this._initPage(\'sum-claude\'',
    'the old local-framework call must be gone from this file');
  excludes(src, 'this._pageRender(\'sum-claude\')',
    'the old local-framework render call must be gone from this file');
}

export async function test_claude_tab_uses_the_pa_tf_wired_filter_bar() {
  const src = code(await read('jar/js/summary-claude.js'));
  includes(src, '_buildFilterBarPA(\'sum-claude\'',
    'must use the PA.tf-wired filter bar, not the local-framework one');
  excludes(src, 'this._buildFilterBar(\'sum-claude\'',
    'the old filter bar builder must no longer be used for this table');
}

export async function test_the_pa_tf_helpers_are_additive_not_a_replacement() {
  const helpers = code(await read('jar/js/summary-helpers.js'));
  // The original functions must still exist — the other eleven call sites depend on them.
  includes(helpers, '_buildFilterBar(id, data, domainFn)',
    'the original _buildFilterBar must be untouched for the other eleven tables');
  includes(helpers, '_applyFilter(id)', 'the original _applyFilter must still exist');
  includes(helpers, '_clearFilter(id)', 'the original _clearFilter must still exist');
  // The new PA.tf-wired counterparts must exist alongside them.
  includes(helpers, '_buildFilterBarPA(id, data, domainFn)',
    'a PA.tf-wired filter bar builder must exist for converted tables');
  includes(helpers, '_applyFilterPA(id)', 'a PA.tf-wired apply-filter must exist');
  includes(helpers, '_clearFilterPA(id)', 'a PA.tf-wired clear-filter must exist');
}

export async function test_no_initPage_call_sites_remain_anywhere() {
  // UI-026h is complete as of §80: every one of the original twelve `_initPage` call sites has
  // been converted to `PA.tf`. `_initPage`/`_pageState`/`_pageFilter`/`_pageRender`/`_pageSort`/
  // `_initColFilters` themselves are deliberately left in `summary-helpers.js`/
  // `summary-col-filter.js` rather than deleted — see UI-026h's own registry row for why (they
  // are the local framework's implementation, now simply unused; removing dead code is a
  // separate, lower-risk cleanup from the conversion itself).
  for (const f of ['jar/js/summary-tables.js', 'jar/js/summary-vertical.js',
      'jar/js/summary-aggregation.js', 'jar/js/summary-dynamic.js', 'jar/js/summary-claude.js']) {
    const src = code(await read(f));
    excludes(src, 'this._initPage(', `${f} must have no _initPage call sites left`);
  }
}
