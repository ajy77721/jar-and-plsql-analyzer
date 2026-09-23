/**
 * UI-002 (dedicated project, §133+) — the parser app's own `parser/js/tableOps.js`/`api.js` used to
 * flatten every grid row's `usedBy[]` into a full `accessDetails[]` array on every page turn (all 50
 * rows), even though only the one row whose panel is expanded ever needs it. `usedBy` is now kept on
 * the row (already the compact wire shape); `_renderRow`/`_renderUsageSummary` read it directly
 * (each entry already is one distinct accessing procedure); `_renderDetail` builds `accessDetails`
 * from it only when a row's own panel opens — synchronously, no network round trip, unlike the
 * live-crawl sibling which needs one (see `plsql-tableops-accessdetails-ondemand.test.mjs`).
 */
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { ok, equal, excludes, includes } from './lib/assert.mjs';

const STATIC = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (rel) => readFile(path.join(STATIC, rel), 'utf8');

const code = (src) =>
  src.replace(/\/\*[\s\S]*?\*\//g, '').replace(/^\s*\/\/.*$/gm, '');

export async function test_tablesPageSource_no_longer_flattens_every_row() {
  const src = code(await read('parser/js/api.js'));
  const start = src.indexOf('tablesPageSource(params) {');
  const end = src.indexOf('\n    },', start);
  const body = src.slice(start, end);
  excludes(body, 'details.push', 'tablesPageSource must not build an accessDetails array anymore');
  includes(body, 'usedBy: usedBy', 'the row must keep usedBy for lazy use at expand time');
}

function usedByRow(nodeId, nodeName, ops) {
  return { nodeId: nodeId, nodeName: nodeName, operations: ops };
}

async function loadTableOpsApiPair() {
  globalThis.window = globalThis;
  globalThis.document = { getElementById: () => null };
  globalThis.PA = globalThis.PA || {};
  globalThis.PA.esc = (v) => String(v);
  globalThis.PA.escJs = (v) => String(v);
  // Stub the shared page-envelope factory, not PA.api itself — the file being loaded assigns its
  // own PA.api object literal, which would clobber any stub placed on PA.api beforehand.
  let capturedMapRow = null;
  globalThis.PA.pageSource = (url, params, mapRow) => { capturedMapRow = mapRow; return {}; };
  new Function(await read('parser/js/api.js'))();
  globalThis.PA.api._analysisName = 'A1';
  globalThis.PA.api.tablesPageSource({});
  return capturedMapRow;
}

export async function test_mapRow_keeps_usedBy_and_sums_accessCount_without_flattening() {
  const mapRow = await loadTableOpsApiPair();
  const raw = {
    name: 'ERP_ORDER', schema: 'ERP', objectType: 'TABLE', allOperations: ['SELECT'],
    usedBy: [
      usedByRow('ERP$P1', 'P1', { SELECT: [10, 20] }),
      usedByRow('ERP$P2', 'P2', { SELECT: [30], UPDATE: [40, 50] })
    ]
  };
  const row = mapRow(raw);
  equal(row.accessCount, 5, 'sum of all line arrays across all usedBy entries');
  equal(row.usedBy.length, 2, 'usedBy must be kept on the row, not discarded');
  ok(!('accessDetails' in row), 'accessDetails must not be pre-built on the row');
}

async function loadTableOps() {
  globalThis.window = globalThis;
  globalThis.document = { getElementById: () => null };
  globalThis.PA = globalThis.PA || {};
  globalThis.PA.esc = (v) => String(v);
  globalThis.PA.escJs = (v) => String(v);
  globalThis.AccessDetails = {
    render: (id, details) => `<div id="${id}-tbody" data-count="${details.length}"></div>`,
    mount: () => {}
  };
  new Function(await read('parser/js/tableOps.js'))();
  return globalThis.PA.tableOps;
}

export async function test_renderRow_procCount_from_usedBy_length() {
  const tableOps = await loadTableOps();
  const row = { tableName: 'T', usedBy: [usedByRow('P1', 'P1', {}), usedByRow('P2', 'P2', {})] };
  const html = tableOps._renderRow(row, 0, (v) => v);
  includes(html, '2 procs', '2 usedBy entries must render as 2 procs');
}

export async function test_renderDetail_flattens_synchronously_from_usedBy_no_fetch_needed() {
  const tableOps = await loadTableOps();
  const row = {
    tableName: 'ERP_ORDER',
    triggers: [],
    usedBy: [usedByRow('ERP$P1', 'P1', { SELECT: [10, 20] })]
  };
  // No PA.api.getTableDetail stub provided at all — if _renderDetail tried to fetch, this would
  // throw synchronously (PA.api undefined) rather than silently pass.
  const html = tableOps._renderDetail(row, 0, (v) => v);
  includes(html, 'data-count="2"', 'must flatten both SELECT lines synchronously, no network call');
}

export async function test_renderUsageSummary_reads_usedBy_directly() {
  const tableOps = await loadTableOps();
  globalThis.PA._scope = 'full';
  globalThis.PA.context = { procId: null };
  const row = { usedBy: [usedByRow('ERP$P1', 'MY_PROC', { SELECT: [1], UPDATE: [2] })] };
  const html = tableOps._renderUsageSummary(row);
  includes(html, 'MY_PROC', 'must show the procedure name from usedBy');
  includes(html, '>1<', 'the usage count must be 1 (one distinct procedure)');
}
