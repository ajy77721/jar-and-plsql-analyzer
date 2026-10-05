/**
 * The Details tab's Statements drill-down is a real table, not a flat list.
 *
 * <h2>What was wrong</h2>
 *
 * Clicking a statement-type row rendered `s.procs` as an unbounded run of `<div class=
 * "to-detail-item">` lines — no header, no sort, no pagination and no filter. On live data that is
 * 87 lines for PostgreSQL's `IF` and 44 for DB2's `SUBQUERY`, so "which procedures use this" meant
 * scrolling a wall of undifferentiated text.
 *
 * <h2>Why this runs the code rather than reading it</h2>
 *
 * The claim being made is behavioural — a second page exists, and the filter boxes actually remove
 * rows. A source-level check for `PA.tf.init` would pass on a panel whose pager never rendered and
 * whose `extraFilter` never matched. So this loads the real `shared/tableFramework.js` and the real
 * `parser/js/statements.js` over the DOM stub and asserts on what lands in `-tbody` and `-pager`.
 */
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { installDom, esc } from './lib/dom.mjs';
import { ok, equal, includes, excludes } from './lib/assert.mjs';

const STATIC = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (rel) => readFile(path.join(STATIC, rel), 'utf8');

const ID = 'stmtd-0';

/** 87 procedures, the real PostgreSQL `IF` fan-out, so page 2 is not hypothetical. */
function fixture(n = 87) {
  const procs = [];
  for (let i = 0; i < n; i++) {
    procs.push({
      nodeId: 'ERP$P' + i,
      name: 'ERP_USP_' + (i % 3 === 0 ? 'RUN_' : 'PROC_') + i,
      schema: i % 2 === 0 ? 'ERP' : 'STAGING',
      sourceFile: 'ERP.P' + i + '.sql',
      count: i + 1,
    });
  }
  return { type: 'IF', totalCount: procs.reduce((a, p) => a + p.count, 0), procCount: n, procs };
}

/**
 * Runs `body` against a mounted panel, then puts the globals back.
 *
 * <p>Restoring matters, and not only for tidiness. Loading the real `tableFramework.js` defines
 * `PA.classifyTableKind` as a side effect; left on `globalThis`, that made a *different* test file
 * — one asserting that very symbol is missing from its own harness — start passing depending on
 * the order the suite happened to run in. A test that silently repairs another test's subject is
 * worse than no test, so this file owns its globals for exactly as long as it needs them.
 */
async function withPanel(stmt, body) {
  const prevPA = globalThis.PA;
  const prevDoc = globalThis.document;
  try {
    return await body(await boot(stmt));
  } finally {
    if (prevPA === undefined) delete globalThis.PA; else globalThis.PA = prevPA;
    if (prevDoc === undefined) delete globalThis.document; else globalThis.document = prevDoc;
  }
}

async function boot(stmt) {
  const dom = installDom();
  globalThis.PA = { esc, escAttr: esc, escJs: (s) => String(s == null ? '' : s).replace(/'/g, "\\'") };
  globalThis.window = globalThis;
  new Function(await read('shared/tableFramework.js'))();
  new Function(await read('parser/js/statements.js'))();
  globalThis.PA.getSchemaColor = () => ({ bg: '#eee', fg: '#333' });

  // _renderDetail defers its mount to a zero timeout (the panel is not in the document yet when
  // it returns). The mount is driven explicitly below instead, so the deferred one is swallowed —
  // left to fire, it would run after the next test has replaced the global PA.
  const realSetTimeout = globalThis.setTimeout;
  globalThis.setTimeout = () => 0;
  let detailHtml;
  try {
    detailHtml = PA.statements._renderDetail(stmt, 0, esc);
  } finally {
    globalThis.setTimeout = realSetTimeout;
  }
  // The panel markup would be assigned to the detail row's innerHTML by PA.tf.toggleDetail; the
  // stub does not parse HTML, so the three elements the framework looks up are registered here.
  dom.add(ID + '-tbody');
  dom.add(ID + '-pager');
  dom.add(ID + '-shown');
  PA.statements._mountDetail(ID);
  return { dom, detailHtml };
}

export async function test_statement_row_click_renders_a_real_table_not_a_div_list() {
  await withPanel(fixture(), async ({ detailHtml }) => {
    includes(detailHtml, '<table class="to-table ad-table" id="stmtd-0">',
      'the drill-down must be a real table reusing the shared .ad-panel table markup');
    includes(detailHtml, '<tbody id="stmtd-0-tbody">', 'PA.tf needs its {id}-tbody contract');
    includes(detailHtml, 'id="stmtd-0-pager" class="pagination-bar ad-pager"',
      'the pager must use the app-wide pagination-bar class, not one of its own');
    for (const h of ['Procedure', 'Schema', 'Count', 'Share', 'Source']) {
      includes(detailHtml, '>' + h + '</th>', 'missing column header: ' + h);
    }
    excludes(detailHtml, 'to-detail-item',
      'the old unbounded div list must be gone, not rendered alongside the table');
  });
}

export async function test_the_drill_down_paginates_and_page_two_holds_the_remainder() {
  await withPanel(fixture(87), async ({ dom }) => {
    const rows = (html) => (html.match(/<tr class="ad-row">/g) || []).length;

    equal(rows(dom.get(ID + '-tbody').innerHTML), 10,
      'page 1 must hold exactly one page (the 10-row default) of the 87 procedures');
    const pager = dom.get(ID + '-pager').innerHTML;
    includes(pager, '87 items', 'the pager must report the true row count');
    includes(pager, "PA.tf.goPage('stmtd-0',1)", 'a second page must be reachable');

    PA.tf.goPage(ID, 1);
    equal(rows(dom.get(ID + '-tbody').innerHTML), 10, 'page 2 must hold the next 10');
  });
}

export async function test_sorted_heaviest_first_and_every_occurrence_is_accounted_for() {
  const stmt = fixture(87);
  await withPanel(stmt, async ({ dom }) => {
    const body = dom.get(ID + '-tbody').innerHTML;
    // Default sort is count desc — the panel answers "who does the most of this".
    includes(body, '>87&times;</span>', 'the heaviest user must be on the first page');
    excludes(body, '>1&times;</span>', 'the lightest user belongs on page 2 under a desc sort');

    const s = PA.tf.state(ID);
    equal(s.filtered.length, stmt.procCount,
      'the table must hold every procedure the summary counted');
    equal(s.filtered.reduce((a, p) => a + p.count, 0), stmt.totalCount,
      'the per-procedure counts must add up to the type total shown on the parent row');
  });
}

export async function test_the_search_box_narrows_the_table() {
  await withPanel(fixture(87), async ({ dom }) => {
    PA.statements.detailSearch(ID, 'RUN_');
    const s = PA.tf.state(ID);
    equal(s.filtered.length, 29, 'only the 29 ERP_USP_RUN_* procedures should survive the search');
    ok(s.filtered.every((p) => p.name.includes('RUN_')), 'a non-matching procedure was kept');
    equal(dom.get(ID + '-shown').textContent, '29 of 87',
      'a narrowed list must say so rather than look like the whole truth');

    PA.statements.detailSearch(ID, '');
    equal(PA.tf.state(ID).filtered.length, 87, 'clearing the search must restore every row');
    equal(dom.get(ID + '-shown').textContent, '', 'an unfiltered list needs no "n of m" note');
  });
}

export async function test_the_column_filters_narrow_by_schema_and_by_minimum_count() {
  await withPanel(fixture(87), async (_ctx) => {

    PA.statements.detailColFilter(ID, 'schema', 'staging');
    const bySchema = PA.tf.state(ID).filtered;
    equal(bySchema.length, 43, 'the odd-indexed STAGING procedures should be the only ones left');
    ok(bySchema.every((p) => p.schema === 'STAGING'), 'a foreign schema survived the column filter');

    // A numeric box is a threshold, not a substring: "show me the heavy users" is the question.
    PA.statements.detailColFilter(ID, 'schema', '');
    PA.statements.detailColFilter(ID, 'count', '80');
    const heavy = PA.tf.state(ID).filtered;
    equal(heavy.length, 8, 'counts 80..87 are the eight at or above the threshold');
    ok(heavy.every((p) => p.count >= 80), 'a row below the threshold survived');

    PA.statements.detailColFilter(ID, 'proc', 'PROC_8');
    ok(PA.tf.state(ID).filtered.every((p) => p.count >= 80 && p.name.includes('PROC_8')),
      'the column filters must compose rather than replace one another');

    PA.statements.detailColFilter(ID, 'count', '');
    PA.statements.detailColFilter(ID, 'proc', '');
    equal(PA.tf.state(ID).filtered.length, 87, 'clearing every box must restore the full list');
  });
}

export async function test_an_empty_drill_down_still_says_so_rather_than_rendering_an_empty_table() {
  await withPanel({ type: 'IF', totalCount: 0, procCount: 0, procs: [] }, async ({ detailHtml }) => {
    includes(detailHtml, 'No procedure details', 'an empty drill-down must explain itself');
    excludes(detailHtml, 'ad-table', 'no table chrome for a panel with nothing in it');
  });
}
