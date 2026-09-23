/**
 * FE-012 — the left panel must not download the full table-ops document.
 *
 * <h2>The defect</h2>
 *
 * `_renderLeftTables` awaited `getTableOperations()`, which fetches `/tables` and flat-maps
 * `usedBy[].operations` into one object **per access line** — about 1M at the stated target. The
 * panel then renders a name, a row of operation badges and a count, and discards every object it
 * built. Measured on the ORACLE analysis in `data/`: 148.4 KB and 2,435 access-line objects for
 * four fields per table, which `/tables/list` now serves in 4.9 KB.
 *
 * <h2>What is asserted</h2>
 *
 * The request, not the rendering. A panel that renders correctly while fetching the whole document
 * is exactly the bug being fixed, so the assertion has to be about which URL was called — that is
 * the only thing that distinguishes the two.
 */
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { installDom } from './lib/dom.mjs';
import { ok, equal, includes, atLeast } from './lib/assert.mjs';

const STATIC = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');

/** Records every URL fetched and answers with a list-shaped document. */
function installFetchLog(tables) {
  const log = [];
  globalThis.fetch = async (url) => {
    log.push(String(url));
    return {
      ok: true,
      status: 200,
      json: async () => ({ tables }),
    };
  };
  return log;
}

async function loadPanel() {
  const dom = installDom();
  globalThis.PA = {
    esc: (s) => String(s ?? ''),
    escAttr: (s) => String(s ?? ''),
    escJs: (s) => String(s ?? ''),
    apiFail: () => null,
    analysisData: { entryId: 'E', entrySchema: 'APP' },
    getSchemaColor: () => ({ bg: '#000', fg: '#fff' }),
    _lp: { table: { all: [], shown: [], rendered: 0 }, proc: { all: [], shown: [], rendered: 0 } },
    _lpRender: () => {},
  };
  for (const f of ['parser/js/api.js', 'parser/js/leftPanel.js']) {
    new Function(await readFile(path.join(STATIC, f), 'utf8'))();
  }
  globalThis.PA.api._analysisName = 'ORACLE~P~DEV~DEMO';
  return dom;
}

const SAMPLE = [
  { name: 'ORDERS', schema: 'APP', objectType: 'TABLE', operations: ['SELECT', 'INSERT'], accessCount: 6 },
  { name: 'CUSTOMER', schema: 'APP', objectType: 'TABLE', operations: ['SELECT'], accessCount: 2 },
];

export async function test_the_left_panel_never_fetches_the_full_tables_document() {
  const dom = await loadPanel();
  dom.add('lpTables');
  const log = installFetchLog(SAMPLE);

  await globalThis.PA._renderLeftTables();

  atLeast(log.length, 1, 'the panel issued no request at all');
  const full = log.filter((u) => /\/tables(\?|$)/.test(u));
  equal(full.length, 0,
    `the panel fetched the full tables document: ${full.join(', ')}`);
  ok(log.some((u) => u.includes('/tables/list')),
    `expected a /tables/list request, saw: ${log.join(', ')}`);
}

export async function test_the_list_shape_still_renders_the_row() {
  // The saving is worthless if the panel now renders blanks. These are the four fields the row
  // reader touches, and each is checked in the markup rather than in the intermediate object.
  const dom = await loadPanel();
  dom.add('lpTables');
  installFetchLog(SAMPLE);

  await globalThis.PA._renderLeftTables();

  const rows = globalThis.PA._lp.table.all;
  equal(rows.length, 2, 'the panel did not keep the rows it fetched');
  const html = globalThis.PA._renderLeftTableItem(rows.find((r) => r.tableName === 'ORDERS'));
  includes(html, 'ORDERS', 'the table name is missing from the row');
  includes(html, '>6<', 'the access count is missing from the row');
  includes(html, 'op-badge SELECT', 'the operation badges are missing from the row');
}

export async function test_the_list_call_does_not_build_access_line_objects() {
  // `accessDetails` must be absent rather than empty. An empty array would let a caller read
  // "this table has no accesses" out of a document that simply does not carry them — a wrong
  // answer that looks like data, which is worse than a missing field that throws.
  const dom = await loadPanel();
  dom.add('lpTables');
  installFetchLog(SAMPLE);

  const rows = await globalThis.PA.api.getTableList();
  equal(rows.length, 2);
  for (const r of rows) {
    ok(!('accessDetails' in r),
      'getTableList fabricated an accessDetails array; callers cannot tell it apart from "no accesses"');
  }
  equal(rows[0].accessCount, 6, 'the count did not survive the mapping');
}

export async function test_the_full_path_still_exists_for_callers_that_need_it() {
  // Three call sites genuinely need the access lines: the Table Ops grid, the detail modal and
  // export. This task removes one caller, not the capability.
  const src = await readFile(path.join(STATIC, 'parser/js/api.js'), 'utf8');
  includes(src, 'async getTableOperations()',
    'the full table-ops path was removed; the grid, detail modal and export need it');
  includes(src, 'accessDetails',
    'getTableOperations no longer builds the access lines its callers read');
}
