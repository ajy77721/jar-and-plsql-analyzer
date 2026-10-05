/**
 * Task 52 — the Details sub-tab labels read their NAME only (the "Cursors (43)" count suffixes
 * are retired by user decision), and the Views & MVs / Triggers lists carry a per-row "def" link
 * that opens the definition SQL in one click (the Tables tab's own affordance), with the view's
 * column names clickable to find them in the definition.
 */
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { equal, ok, includes, excludes } from './lib/assert.mjs';
import { installDom, esc } from './lib/dom.mjs';

const STATIC = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (p) => readFile(path.join(STATIC, p), 'utf8');
const code = (src) =>
  src.replace(/\/\*[\s\S]*?\*\//g, '').replace(/^\s*\/\/.*$/gm, '');

/* ───────────────────────── 1. sub-tab labels: name only ───────────────────────── */

export async function test_details_subtab_labels_no_longer_carry_count_suffixes() {
  const src = code(await read('parser/js/app.js'));
  const at = src.indexOf('updateLabels: function');
  ok(at > 0, 'PA.detailsTab.updateLabels was not found');
  const body = src.slice(at, src.indexOf('};', at));
  excludes(body, 'dataset.baseLabel', 'the count-suffix derivation is still alive in updateLabels');
  excludes(body, "' (' + n + ')'", 'a count suffix is still being appended to a sub-tab label');
  excludes(body, '_setTabCount', 'the async triggers-fired count suffix path is still wired');
}

export async function test_updateLabels_stays_a_safe_noop_for_its_callers() {
  // switchRightTab / detailHeader call it guarded; the function must exist so those callers
  // never break even after the suffix logic was retired.
  const src = code(await read('parser/js/app.js'));
  includes(src, 'updateLabels: function(detail) { }',
    'updateLabels must remain callable (as a no-op) — detailHeader.js and switchRightTab call it');
}

/* ───────────────────────── 2. the def modal ───────────────────────── */

async function loadModal() {
  const dom = installDom();
  globalThis.PA = { esc, escJs: (s) => String(s == null ? '' : s).replace(/'/g, "\\'") };
  globalThis.window = globalThis;
  new Function(await read('parser/js/definitionModal.js'))();
  return dom;
}

export async function test_definition_modal_escapes_definition_sql() {
  await loadModal();
  const overlay = PA.definitionModal.open({
    title: 'View — EVIL',
    sections: [{ title: 'Definition', pre: '<script>alert(1)</script>', preId: 'dm-pre' }]
  });
  const html = overlay.innerHTML;
  excludes(html, '<script>', 'definition SQL reached the DOM as markup — DB-sourced data must be escaped');
  includes(html, '&lt;script&gt;', 'the escaped SQL text should be present');
  PA.definitionModal.close();
}

export async function test_definition_modal_highlight_marks_and_escapes() {
  // The DOM half of highlight is glue over this pure function (the stub cannot parse rendered
  // HTML back into elements, so the marking is asserted on the pure half).
  await loadModal();
  const html = PA.definitionModal.highlightText('SELECT <a> FROM t WHERE amt > x AND amt < 9', 'AMT');
  includes(html, '<mark>amt</mark>', 'the case-insensitive hit was not marked');
  excludes(html, '<a>', 'rebuilding the pre must not turn the original text into markup');
  ok(html.includes('&lt;a&gt;'), 'the non-matching fragments must stay escaped after the rebuild');
  equal(PA.definitionModal.highlightText('no such term here', 'zzz'), '',
    'a term that does not occur must produce no rebuild at all');
}

/* ───────────────────────── 3. the def links on the list rows ───────────────────────── */

export async function test_views_list_rows_carry_a_def_link() {
  const src = code(await read('parser/js/estateViews.js'));
  const at = src.indexOf('_renderRow: function(v, idx, esc)');
  const body = src.slice(at, src.indexOf('_renderDetail: function', at));
  includes(body, "PA.estateViews.openDef", 'the Views list row has no def link');
  includes(body, "event.stopPropagation()", 'the def link must not toggle the row expansion');
}

export async function test_triggers_list_rows_carry_a_def_link() {
  const src = code(await read('parser/js/estateTriggers.js'));
  const at = src.indexOf('_renderRow: function(t, idx, esc)');
  const body = src.slice(at, src.indexOf('_renderDetail: function', at));
  includes(body, "PA.estateTriggers.openDef", 'the Triggers list row has no def link');
  includes(body, "event.stopPropagation()", 'the def link must not toggle the row expansion');
}

export async function test_view_def_modal_reuses_the_expand_panel_cache_and_endpoint() {
  const src = code(await read('parser/js/estateViews.js'));
  includes(src, 'PA.estateViews._details[name]', 'openDef must share the expand panel\'s detail cache');
  includes(src, 'PA.api.getViewDetail(name)', 'openDef must use the same per-view endpoint, not a new one');
}

export async function test_trigger_def_modal_reuses_the_expand_panel_cache_and_endpoint() {
  const src = code(await read('parser/js/estateTriggers.js'));
  includes(src, 'PA.estateTriggers._details[name]', 'openDef must share the expand panel\'s detail cache');
  includes(src, 'PA.api.getTriggerDetail(name)', 'openDef must use the same per-trigger endpoint, not a new one');
}

/* ───────────────────────── 4. clickable columns in the view detail ───────────────────────── */

export async function test_view_detail_columns_are_clickable_into_the_definition() {
  const src = code(await read('parser/js/estateViews.js'));
  includes(src, '_highlightCol', 'the detail panel has no column→definition highlight hook');
  includes(src, 'vw-def-', 'the detail panel\'s definition <pre> carries no id to highlight against');
  includes(src, 'PA.definitionModal.highlight', 'the highlight must go through the shared modal\'s helper');
}

export async function test_definition_modal_is_registered_on_the_page() {
  const html = await read('parser/index.html');
  includes(html, 'js/definitionModal.js', 'the modal module is not loaded by the page');
}

/* ───────────────────────── 5. Line values are clickable ─────────────────────────
 * Task 52 follow-up: in the detail panels below a clicked row, the tables that carry a "Line"
 * value must render it clickable. What a line OPENS depends on what the line points INTO:
 * a join's line is in the view's OWN definition (the def modal, that line marked); a usage /
 * dynamic-SQL / fired-by line is in a PROCEDURE'S SOURCE (sourceView.openNodeAtLine); a body
 * call's line is in the trigger's own body (the def modal, that line marked). */

export async function test_view_join_lines_open_the_definition_at_that_line() {
  const src = code(await read('parser/js/estateViews.js'));
  const at = src.indexOf('_joinsHtml: function');
  const body = src.slice(at, src.indexOf('},', src.indexOf('badges: function', at)));
  includes(body, 'lineLink', 'the Joins table\'s Line values are not clickable');
  includes(body, "PA.estateViews.openDef", 'a join line must open the view\'s own definition');
  includes(body, 'viewName', 'the joins renderer must know the view name to open');
}

export async function test_view_usage_lines_open_the_procedure_source() {
  const src = code(await read('parser/js/estateViews.js'));
  const at = src.indexOf('_usedByHtml: function');
  const body = src.slice(at, src.indexOf('},', src.indexOf('source: PA.usageTable.nodeSource', at)));
  includes(body, 'lineLink', 'the Used-By table\'s Line values are not clickable');
  includes(body, 'PA.sourceView.openNodeAtLine', 'a usage line must open the procedure source');
}

export async function test_view_dynamic_sql_lines_open_the_procedure_source() {
  const src = code(await read('parser/js/estateViews.js'));
  const at = src.indexOf('_dynamicSqlHtml: function');
  const body = src.slice(at, src.indexOf('},', src.indexOf('source: PA.usageTable.nodeSource', at)));
  includes(body, 'lineLink', 'the Dynamic-SQL table\'s Line values are not clickable');
  includes(body, 'PA.sourceView.openNodeAtLine', 'a dynamic-SQL line must open the procedure source');
}

export async function test_trigger_body_call_lines_open_the_trigger_body() {
  const src = code(await read('parser/js/estateTriggers.js'));
  const at = src.indexOf('_callsHtml: function');
  const body = src.slice(at, src.indexOf('},', src.indexOf('badges: function', at)));
  includes(body, 'lineLink', 'the Body-calls table\'s Line values are not clickable');
  includes(body, "PA.estateTriggers.openDef", 'a body-call line must open the trigger\'s own body');
}

export async function test_trigger_fired_by_lines_open_the_procedure_source() {
  const src = code(await read('parser/js/estateTriggers.js'));
  const at = src.indexOf('_firedByHtml: function');
  const body = src.slice(at, src.indexOf('},', src.indexOf('source: PA.usageTable.nodeSource', at)));
  includes(body, 'lineLink', 'the Will-fire table\'s Line values are not clickable');
  includes(body, 'PA.sourceView.openNodeAtLine', 'a fired-by line must open the procedure source');
}

export async function test_line_links_only_render_when_something_can_open() {
  // usage-table renders the line as a plain L<n> when lineLink returns null — a row with no
  // node id (or no lines) must not render a dead link.
  await loadModal();
  const { render, paginate } = await loadUsageTable();
  const html = render('ut-test', [
    { nodeId: 'P1', operations: { SELECT: [7] } },
    { operations: { SELECT: [9] } }          // no nodeId — no source to open
  ], {
    name: function (r) { return r.nodeId || '-'; },
    line: function (r) { return PA.usageTable.firstLine(r); },
    lineLink: function (r) {
      var ln = PA.usageTable.firstLine(r);
      return (r.nodeId && ln != null)
        ? "PA.sourceView.openNodeAtLine('" + PA.escJs(r.nodeId) + "', " + ln + ")" : null;
    }
  });
  const linked = (html.match(/ut-linelink/g) || []).length;
  equal(linked, 1, 'exactly the row WITH a node id may render a clickable line');
  includes(html, 'ut-line-no', 'the row without an opener must still show its plain L value');
}

/** Loads shared/usage-table.js against the same stubbed PA (it self-installs its styles). */
async function loadUsageTable() {
  new Function(await read('shared/usage-table.js'))();
  return { render: PA.usageTable.render };
}
