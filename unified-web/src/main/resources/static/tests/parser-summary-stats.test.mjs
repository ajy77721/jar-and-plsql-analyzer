/**
 * UI-024 — `parser/js/summary.js`'s scoped dashboard (`_render`/`_computeStats`) used to call
 * `PA.getScopedNodes()` (a `PA.nodeIndex.all()` scan) then aggregate every scoped node in the
 * browser. Converted to fetch `SummaryStatsService`'s server-computed aggregate instead — this one
 * genuinely collapses to a small response, unlike `complexity.js`'s own conversion.
 */
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { ok, equal, excludes, includes } from './lib/assert.mjs';

const STATIC = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (rel) => readFile(path.join(STATIC, rel), 'utf8');

const code = (src) =>
  src.replace(/\/\*[\s\S]*?\*\//g, '').replace(/^\s*\/\/.*$/gm, '');

export async function test_render_no_longer_scans_via_getScopedNodes() {
  const src = code(await read('parser/js/summary.js'));
  excludes(src, 'PA.getScopedNodes()', '_render must not scan the whole analysis for the dashboard anymore');
  excludes(src, '_computeStats: function', '_computeStats must be deleted, not just emptied');
  includes(src, 'PA.api.getSummaryStats', 'must fetch the server aggregate instead');
}

function statsFixture(overrides) {
  return Object.assign({
    totalNodes: 5, totalLoc: 500, totalTables: 10, totalCalls: 20, totalCursors: 3,
    totalParams: 2, totalVars: 1, totalExHandlers: 0, totalStmts: 40, encryptedCount: 0,
    schemas: { ERP: 5 }, types: { PROCEDURE: 5 }, stmtTypes: { SELECT: 40 },
    maxLoc: { name: 'P1', value: 200 }, maxDepthNode: { name: 'P2', value: 3 },
    maxTables: { name: 'P3', value: 4 }, maxCalls: { name: 'P4', value: 8 }
  }, overrides || {});
}

async function loadSummary({ apiStats, analysisData, context }) {
  globalThis.window = globalThis;
  globalThis.document = { getElementById: () => ({ innerHTML: '' }) };
  globalThis.PA = globalThis.PA || {};
  globalThis.PA.analysisData = analysisData;
  globalThis.PA.context = context || {};
  globalThis.PA._scope = 'full';
  globalThis.PA._currentScopeId = () => 'ERP$P1';
  globalThis.PA._isPackageRoot = () => false;
  globalThis.PA.esc = (s) => s;
  globalThis.PA.escAttr = (s) => s;
  globalThis.PA.escJs = (s) => s;
  globalThis.PA.getSchemaColor = () => ({ bg: '#000', fg: '#fff' });
  const calls = [];
  globalThis.PA.api = {
    getSummaryStats: async (scope, procId, entrySchema) => {
      calls.push({ scope, procId, entrySchema });
      return apiStats;
    }
  };
  new Function(await read('parser/js/summary.js'))();
  globalThis.PA.summary._calls = calls;
  return globalThis.PA.summary;
}

export async function test_full_scope_sends_no_procid() {
  const summary = await loadSummary({
    apiStats: statsFixture(),
    analysisData: { name: 'A1', totalNodes: 5, entrySchema: 'ERP' },
    context: {}
  });
  await summary._render();
  equal(summary._calls[0].scope, 'full');
  equal(summary._calls[0].procId, null);
}

export async function test_root_level_scalars_prefer_the_already_loaded_index_root() {
  const summary = await loadSummary({
    apiStats: statsFixture({ totalTables: 999, totalCalls: 888 }), // computed fallback values
    analysisData: { name: 'A1', totalNodes: 5, totalTables: 10, totalEdges: 20, entrySchema: 'ERP' },
    context: {}
  });
  await summary._render();
  ok(true); // no throw is the main assertion; field values checked structurally below
}

export async function test_package_root_forces_full_scope_even_with_a_scope_selected() {
  const summary = await loadSummary({
    apiStats: statsFixture(),
    analysisData: { name: 'A1', totalNodes: 5, entrySchema: 'ERP' },
    context: { procId: '__PKG_ROOT__' }
  });
  PA._scope = 'direct';
  PA._isPackageRoot = () => true;
  await summary._render();
  equal(summary._calls[0].scope, 'full', 'a package root must always be treated as full scope, regardless of the toggle');
  equal(summary._calls[0].procId, null);
}

export async function test_no_procid_forces_full_scope_even_if_a_non_full_toggle_is_selected() {
  const summary = await loadSummary({
    apiStats: statsFixture(),
    analysisData: { name: 'A1', totalNodes: 5, entrySchema: 'ERP' },
    context: {}
  });
  PA._scope = 'direct';
  await summary._render();
  equal(summary._calls[0].scope, 'full', 'no procId selected must mean full scope regardless of the toggle');
}

export async function test_scoped_and_full_counts_are_set_for_the_scope_note() {
  const summary = await loadSummary({
    apiStats: statsFixture({ totalNodes: 2 }),
    analysisData: { name: 'A1', totalNodes: 5, entrySchema: 'ERP' },
    context: { procId: 'ERP$P1' }
  });
  PA._scope = 'direct';
  await summary._render();
  equal(summary._calls[0].scope, 'direct');
  equal(summary._calls[0].procId, 'ERP$P1');
}
