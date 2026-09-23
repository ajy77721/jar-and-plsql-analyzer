/**
 * UI-030d — `plsql/js/summary.js`'s client conversion: fetch `PA.api.getProcedureReports()`
 * (UI-030c's endpoint, or UI-030-BUG's parser-backend equivalent — `getProcedureReports()` itself
 * dispatches by backend) instead of looping `getProcDetail` once per procedure, falling back to the
 * old per-procedure loop only when the fetch itself fails.
 *
 * `_reviveSetsPlsql` is this side's version of the JAR side's `_reviveSets` (UI-026g §76): checked
 * every `.size`/`.has` call site across `summary-tables.js`/`summary-export-excel.js` directly
 * rather than assuming, and only `schemaSliceReport`'s `procedures`/`tables`/`operations` and
 * `extReport`'s `procedures`/`callers`/`packages` actually need reviving — everything else reads
 * table-level `operations` only via spread/`for...of`, which behaves identically on arrays and Sets.
 */
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { ok, equal, includes, excludes } from './lib/assert.mjs';

const STATIC = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (rel) => readFile(path.join(STATIC, rel), 'utf8');

const code = (src) =>
  src.replace(/\/\*[\s\S]*?\*\//g, '').replace(/^\s*\/\/.*$/gm, '');

export async function test_render_prefers_the_server_endpoint_on_either_backend() {
  const src = code(await read('plsql/js/summary.js'));
  includes(src, 'PA.api.getProcedureReports()', '_render must try the precomputed endpoint');
  includes(src, 'this._renderLegacy(analysisData, container, esc)', 'the old per-procedure loop must remain as the fetch-failure fallback');
}

export async function test_api_dispatches_procedure_reports_by_backend() {
  const src = code(await read('plsql/js/api.js'));
  includes(src, 'this._parserName()', 'getProcedureReports must dispatch by backend, like every other dual-backend method here');
  includes(src, '/api/parser/analyses/${encodeURIComponent(n)}/reports', 'the parser backend must hit its own reports endpoint (UI-030-BUG\'s fix)');
  includes(src, "fetch('/api/plsql/analysis/reports')", 'the live-crawl backend must still hit its own endpoint (UI-030c)');
}

async function loadSummary() {
  globalThis.window = globalThis;
  globalThis.document = { getElementById: () => null, querySelectorAll: () => [] };
  globalThis.PA = globalThis.PA || {};
  globalThis.PA.esc = (s) => s;
  new Function(await read('plsql/js/summary.js'))();
  return globalThis.PA.summary;
}

export async function test_revive_sets_converts_only_the_documented_fields() {
  const summary = await loadSummary();
  const data = {
    schemaSliceReport: [{ schema: 'ERP', procedures: ['P1', 'P2'], tables: ['T1'], operations: ['SELECT'], callCount: 2 }],
    extReport: [{ schema: 'BILLING', procedures: ['EXT1'], callers: ['P1'], packages: ['PKG'], count: 1 }]
  };
  summary._reviveSetsPlsql(data);
  ok(data.schemaSliceReport[0].procedures instanceof Set, 'schemaSliceReport.procedures must become a real Set');
  equal(data.schemaSliceReport[0].procedures.size, 2);
  ok(data.schemaSliceReport[0].tables instanceof Set, 'schemaSliceReport.tables must become a real Set');
  ok(data.schemaSliceReport[0].operations instanceof Set, 'schemaSliceReport.operations must become a real Set');
  ok(data.extReport[0].procedures instanceof Set, 'extReport.procedures must become a real Set');
  ok(data.extReport[0].callers instanceof Set, 'extReport.callers must become a real Set');
  ok(data.extReport[0].packages instanceof Set, 'extReport.packages must become a real Set');
  equal(data.schemaSliceReport[0].callCount, 2, 'untouched scalar fields must be left alone');
}

export async function test_revive_sets_tolerates_missing_sections() {
  const summary = await loadSummary();
  // Must not throw when the server response omits a section entirely.
  summary._reviveSetsPlsql({});
  ok(true, 'no exception for an empty payload');
}

export async function test_finish_render_uses_precomputed_reports_when_given_not_the_client_builders() {
  const summary = await loadSummary();
  let builtClientSide = false;
  summary._buildTransactionReport = () => { builtClientSide = true; return []; };
  summary._buildSchemaSliceReport = () => { builtClientSide = true; return []; };
  summary._buildExternalDepsReport = () => { builtClientSide = true; return []; };
  summary._buildBatchReport = () => { builtClientSide = true; return []; };
  summary._renderProcedureTable = () => '';
  const container = { innerHTML: '' };
  summary._finishRender({}, [], container, (s) => s, {
    txnReport: [{ tag: 'server', transactionRequirement: 'READ ONLY' }],
    schemaSliceReport: [],
    extReport: [],
    batchReport: []
  });
  ok(!builtClientSide, 'when precomputed reports are supplied, the client-side builders must not run');
  equal(summary._txnReport[0].tag, 'server', 'the precomputed report must be the one actually cached/rendered');
}
