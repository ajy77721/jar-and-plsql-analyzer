/**
 * UI-002 (dedicated project, §133+) — `plsql/js/tableOps.js`'s grid row no longer carries
 * `accessDetails`/`_fullAccessDetails` (see `AnalyzerController.getTableOperationsPage`'s own doc
 * comment: those were the actual unbounded per-access-line payload this row's DoD names). The expand
 * panel now fetches the one table's own detail on demand via the already-existing, already-bounded
 * `/tables/{tableName}` endpoint, and reproduces scope filtering client-side via
 * `_scopedAccessDetails` — the exact algorithm `PlsqlScopeFilter.trim` already applies server-side,
 * so a scoped row's reported `accessCount` and its expand panel's own filtered array never disagree.
 */
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { ok, equal, excludes, includes } from './lib/assert.mjs';

const STATIC = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (rel) => readFile(path.join(STATIC, rel), 'utf8');

const code = (src) =>
  src.replace(/\/\*[\s\S]*?\*\//g, '').replace(/^\s*\/\/.*$/gm, '');

export async function test_render_detail_no_longer_reads_accessDetails_off_the_row() {
  const src = code(await read('plsql/js/tableOps.js'));
  const start = src.indexOf('_renderDetail(table, idx, esc) {');
  const end = src.indexOf('if (triggers.length > 0)', start);
  const body = src.slice(start, end);
  excludes(body, 'table.accessDetails', '_renderDetail must not read table.accessDetails directly anymore');
  includes(body, 'PA.api.getTableDetail(table.tableName)', '_renderDetail must fetch the one table on demand');
}

function stubDom() {
  const elements = {};
  return {
    elements,
    getElementById: (id) => elements[id] || null,
    createElement: () => ({ style: {}, classList: { add() {}, remove() {} } })
  };
}

async function loadTableOps() {
  globalThis.window = globalThis;
  globalThis.document = stubDom();
  globalThis.PA = globalThis.PA || {};
  globalThis.PA.escJs = (v) => String(v);
  globalThis.PA.escAttr = (v) => String(v);
  globalThis.PA.esc = (v) => String(v);
  globalThis.AccessDetails = {
    render: (id, details) => `<div id="${id}-tbody" data-count="${details.length}"></div>`,
    mount: () => {}
  };
  new Function(await read('plsql/js/tableOps.js'))();
  return globalThis.PA.tableOps;
}

export async function test_scopedAccessDetails_direct_mode_matches_package_qualified_and_bare_ids() {
  const tableOps = await loadTableOps();
  globalThis.PA.context = { procId: 'ERP.PKG.PROC_A' };
  tableOps.scopeMode = 'scoped';
  const details = [
    { procedureId: 'ERP.PKG.PROC_A' },   // exact match
    { procedureId: 'X.PKG.PROC_A' },     // package+name match, different schema
    { procedureId: 'ERP.PKG.PROC_B' }    // no match
  ];
  const out = tableOps._scopedAccessDetails(details);
  equal(out.length, 2, 'exact match and package-qualified match must both survive');
}

export async function test_scopedAccessDetails_full_flow_mode_uses_callTreeNodeIds_with_fuzzy_match() {
  const tableOps = await loadTableOps();
  globalThis.PA.context = { procId: 'ERP$P1', callTreeNodeIds: new Set(['ERP$P2']) };
  tableOps.scopeMode = 'all';
  const details = [
    { procedureId: 'ERP$P2' },       // exact
    { procedureId: 'SUB.ERP$P2' },   // fuzzy contains
    { procedureId: 'ERP$P3' }        // no match
  ];
  const out = tableOps._scopedAccessDetails(details);
  equal(out.length, 2);
}

export async function test_scopedAccessDetails_returns_everything_when_no_procId() {
  const tableOps = await loadTableOps();
  globalThis.PA.context = null;
  const details = [{ procedureId: 'A' }, { procedureId: 'B' }];
  const out = tableOps._scopedAccessDetails(details);
  equal(out.length, 2);
}

export async function test_renderDetail_fetches_and_mounts_the_full_table_detail() {
  const tableOps = await loadTableOps();
  globalThis.PA.context = null;
  tableOps.scopeMode = 'all';
  let fetchedName = null;
  globalThis.PA.api = {
    _analysisName: 'A1',
    getTableDetail: async (name) => {
      fetchedName = name;
      return { accessDetails: [{ procedureId: 'P1' }, { procedureId: 'P2' }] };
    }
  };

  const html = tableOps._renderDetail({ tableName: 'ERP_ORDER', triggers: [] }, 0, (v) => v);
  // The wrap id carries a per-expand sequence token now (task 51 — an index-only id let a slow
  // getTableDetail response write one table's details into a later panel that reused the id), so
  // the FIRST expand on a fresh module is seq 1: ad-s-1-wrap.
  ok(html.includes('ad-s-1-wrap'), 'must render a placeholder wrap div synchronously');

  // Simulates PA.tf.toggleDetail actually inserting the returned HTML into a live DOM element —
  // this test exercises _renderDetail in isolation, so the insertion is stubbed rather than real.
  globalThis.document.elements['ad-s-1-wrap'] = { innerHTML: '' };

  // Let the deferred async work (setTimeout 0 + the fetch) run.
  await new Promise((resolve) => setTimeout(resolve, 10));
  await new Promise((resolve) => setTimeout(resolve, 10));

  equal(fetchedName, 'ERP_ORDER', 'must fetch the exact table whose panel opened');
  const wrap = globalThis.document.elements['ad-s-1-wrap'];
  ok(wrap.innerHTML.includes('data-count="2"'), 'must mount all 2 fetched details when unscoped: ' + wrap.innerHTML);
}
