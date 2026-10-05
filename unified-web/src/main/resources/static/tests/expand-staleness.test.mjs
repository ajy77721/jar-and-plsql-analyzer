/**
 * Task 51 — the expand-panel staleness bugs: "wrong data in a few cases, nothing at all in
 * others" on the Table Ops expand and the Details→Statements drill-down.
 *
 * Four mechanisms, one suite:
 *
 * 1. `PA.tf.toggleDetail` deleted the detail row's lazy flag even when the item could not be
 *    resolved (a refetch mid-click), leaving a permanently EMPTY detail row that no later click
 *    could ever fill.
 * 2. `AccessDetails.fetchDetail` joined the statement index on (nodeId, operation, line) while
 *    the server's `table:` term is a CONTAINS match — another table's rows (ORDERS_ARCHIVE for
 *    ORDERS; the other half of a same-line JOIN) entered the map last-write-wins and this
 *    table's panel showed someone else's columns and predicates.
 * 3. `PA.statements.applyScope` was an unserialized fetch→setData: two overlapping scope
 *    changes resolved out of order and the table kept the STALE scope's rows.
 * 4. A stale panel's late enrichment could repaint a DIFFERENT table's panel that had since
 *    reused its id.
 *
 * The first three are behavioural (the real module, loaded against the DOM stub); the wiring
 * assertions at the bottom pin the plsql-side fixes the same source-level way the other suites
 * here do.
 */
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { equal, ok, includes } from './lib/assert.mjs';
import { installDom, esc } from './lib/dom.mjs';

const STATIC = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');

const FRAMEWORK = path.join(STATIC, 'shared/tableFramework.js');
const ACCESS = path.join(STATIC, 'shared/access-details.js');
const STATEMENTS = path.join(STATIC, 'parser/js/statements.js');

const code = (src) =>
  src.replace(/\/\*[\s\S]*?\*\//g, '').replace(/^\s*\/\/.*$/gm, '');

/** Loads a real module source against the DOM stub, with the shared PA helpers installed. */
async function loadModule(filePath) {
  const dom = installDom();
  globalThis.PA = {
    esc,
    escAttr: esc,
    escJs: (s) => String(s == null ? '' : s).replace(/'/g, "\\'"),
  };
  globalThis.window = globalThis;
  const src = await readFile(filePath, 'utf8');
  new Function(src)();
  return dom;
}

const ROWS = [
  { name: 'ORDERS', op: 'SELECT', line: 10 },
  { name: 'ORDERS_ARCHIVE', op: 'INSERT', line: 20 },
];

/* ───────────────────────── 1. toggleDetail stays retryable ───────────────────────── */

async function loadFrameworkWithDetailRow(dom) {
  const src = await readFile(FRAMEWORK, 'utf8');
  new Function(src)();
  const tf = globalThis.PA.tf;
  tf.init('t', ROWS, 10, (d, i, e) => `<tr><td>${e(d.name)}</td></tr>`, {
    renderDetail: (item) => `<div class="detail-of">${item.name}</div>`,
  });
  tf.render('t');
  // PA.tf.render builds detail rows as innerHTML STRINGS, so the stub cannot see them; register
  // the one row the way the browser would have it after the first render: lazy, hidden.
  const detail = dom.add('t-detail-0');
  detail.dataset.lazy = '1';
  return tf;
}

export async function test_toggle_detail_leaves_the_row_lazy_when_the_item_is_gone() {
  const dom = await loadModule(FRAMEWORK);
  const tf = await loadFrameworkWithDetailRow(dom);

  // A refetch/debounced filter mid-click: the DOM still shows page rows but the state's rows are
  // already emptied by _restart.
  tf.state('t').filtered = [];
  tf.toggleDetail('t', 0);

  const detail = dom.get('t-detail-0');
  ok(detail.dataset.lazy === '1',
    'a row whose item cannot be resolved must KEEP its lazy flag so a later click can retry');
  ok(detail.innerHTML === '' || detail.innerHTML === undefined,
    'a row whose item cannot be resolved must not be filled with an empty detail');
}

export async function test_toggle_detail_still_renders_when_the_item_exists() {
  const dom = await loadModule(FRAMEWORK);
  const tf = await loadFrameworkWithDetailRow(dom);

  tf.state('t').filtered = ROWS;   // the normal, in-sync case
  tf.toggleDetail('t', 0);

  const detail = dom.get('t-detail-0');
  ok(!('lazy' in detail.dataset), 'a successfully rendered detail row must lose its lazy flag');
  includes(detail.innerHTML, 'ORDERS', 'the rendered detail is not the row item\'s own panel');
}

/* ───────────────────────── 2. the enrichment join is per-table ───────────────────────── */

export async function test_fetch_detail_ignores_rows_of_any_other_table() {
  const dom = await loadModule(ACCESS);
  const fetches = [];
  globalThis.fetch = async (url) => {
    fetches.push(String(url));
    return {
      ok: true,
      json: async () => ({
        rows: [
          { nodeId: 'P1', operation: 'SELECT', line: 10, tableName: 'ORDERS' },
          // Same (nodeId, operation, line) — a JOIN between ORDERS and ORDERS_ARCHIVE on one line.
          { nodeId: 'P1', operation: 'SELECT', line: 10, tableName: 'ORDERS_ARCHIVE' },
          // The contains-match pollution the server answers with.
          { nodeId: 'P2', operation: 'UPDATE', line: 5, tableName: 'CLIENT_ORDERS' },
          // Unattributable.
          { nodeId: 'P3', operation: 'DELETE', line: 7, tableName: '' },
        ],
      }),
    };
  };

  // The module exposes itself on `window`, not on PA.
  const byKey = await globalThis.AccessDetails.fetchDetail('ANALYSIS', 'ORDERS');

  const keys = Object.keys(byKey);
  equal(keys.length, 1, `only the requested table's own row may enter the join map, got: ${keys.join(', ')}`);
  equal(keys[0], 'P1|SELECT|10');
  equal(byKey['P1|SELECT|10'].tableName, 'ORDERS',
    'the surviving row for a colliding key must be this table\'s own evidence, not the other table\'s');
  includes(fetches[0], 'limit=1000', 'the enrichment read is no longer capped at 500 rows');
}

/* ───────────────────────── 3. the stale scope response is dropped ───────────────────────── */

export async function test_statements_apply_scope_drops_a_stale_response() {
  await loadModule(STATEMENTS);   // also resets PA for the statements module's own globals

  let resolveSlow;
  let callCount = 0;
  const setDataCalls = [];
  globalThis.PA.tf = {
    setData: (id, data) => setDataCalls.push(data),
    state: () => null,
    setLoadError: () => {},
  };
  globalThis.PA.failed = () => false;
  globalThis.PA.context = { procId: 'SCHEMA.PKG.PROC' };
  globalThis.PA._scope = 'direct';
  globalThis.PA._currentScopeId = () => 'SCHEMA.PKG.PROC';
  // Call 1 (proc A) gets a promise the test resolves later; call 2 (proc B) resolves at once.
  globalThis.PA.api = {
    getStatementsSummary: () => {
      callCount++;
      if (callCount === 1) return new Promise((r) => { resolveSlow = r; });
      return Promise.resolve([{ type: 'IF', fast: true }]);
    },
  };

  // Click proc A (slow fetch in flight), then proc B (resolves immediately).
  const stale = globalThis.PA.statements.applyScope();
  globalThis.PA._scope = 'subtree';
  const fresh = globalThis.PA.statements.applyScope();
  await fresh;
  equal(setDataCalls.length, 1, 'the newer scope change must set the table data exactly once');

  // proc A's response lands LAST — it must be discarded, not painted over B's data.
  resolveSlow([{ type: 'LOOP', slow: true }]);
  await stale;
  equal(setDataCalls.length, 1,
    'a scope response that resolves after a newer call started must not overwrite the table');
  ok(setDataCalls[0][0] && setDataCalls[0][0].fast,
    'the table keeps the newer scope\'s rows');
}

/* ───────────────────────── wiring pins (source-level) ───────────────────────── */

export async function test_show_triggers_rebases_the_row_index_on_a_server_paged_grid() {
  const src = code(await readFile(path.join(STATIC, 'parser/js/tableOps.js'), 'utf8'));
  const at = src.indexOf('showTriggers: function(idx)');
  ok(at > 0, 'PA.tableOps.showTriggers was not found');
  const body = src.slice(at, src.indexOf('\n    },', at));
  includes(body, 'idx - s.page * s.pageSize',
    'showTriggers reads the global row index un-rebased — every trigger click past page 1 silently does nothing');
}

export async function test_statements_apply_scope_is_token_guarded() {
  const src = code(await readFile(STATEMENTS, 'utf8'));
  includes(src, '_scopeSeq', 'the scope fetch lost its stale-response token');
}

export async function test_plsql_table_detail_dispatches_to_the_parser_backend() {
  const src = code(await readFile(path.join(STATIC, 'plsql/js/api.js'), 'utf8'));
  const at = src.indexOf('async getTableDetail(tableName)');
  ok(at > 0, 'plsql PA.api.getTableDetail was not found');
  const body = src.slice(at, src.indexOf('\n    },', at));
  includes(body, '_parserName()',
    'getTableDetail must dispatch by _parserName like every other read — a parser-backed analysis was served by the live-crawl endpoint');
  includes(body, '/api/parser/analyses/',
    'the parser branch must ask the parser backend for the table');
  includes(body, '_adaptParserTableRow(t)',
    'the parser row must go through the grid\'s own adapter so the panel shape is unchanged');
}

export async function test_plsql_expand_panel_ids_are_unique_per_expand() {
  const src = code(await readFile(path.join(STATIC, 'plsql/js/tableOps.js'), 'utf8'));
  const at = src.indexOf('_renderDetail(table, idx, esc)');
  ok(at > 0, 'plsql tableOps _renderDetail was not found');
  const body = src.slice(at, src.indexOf('\n    },', at));
  includes(body, '_detailSeq',
    'the expand panel id must carry a per-expand token — an index-only id let a slow getTableDetail response write one table\'s access details into another table\'s panel');
}
