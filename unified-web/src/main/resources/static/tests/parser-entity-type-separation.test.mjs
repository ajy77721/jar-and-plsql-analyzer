/**
 * Entity-type separation, frontend half. The procedures listing/counts are procedures + functions
 * only; triggers get their own figure and their own facet. The per-type breakdown rides the
 * analysis index as `typeCounts`; every surface falls back to the raw node total only when the
 * analysis predates that field, never silently labelling the lumped total "procs".
 */
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { equal, includes } from './lib/assert.mjs';

const STATIC = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (rel) => readFile(path.join(STATIC, rel), 'utf8');

async function loadLeftPanel(analysisData) {
  globalThis.window = globalThis;
  globalThis.document = {
    getElementById: () => ({ innerHTML: '', style: {}, querySelector: () => null }),
    querySelector: () => null,
    querySelectorAll: () => []
  };
  globalThis.PA = globalThis.PA || {};
  globalThis.PA.nodeIndex = { all: () => [], count: () => 0, ensureLoaded: async () => [] };
  globalThis.PA.analysisData = analysisData;
  globalThis.PA.escAttr = (s) => s;
  globalThis.PA.escJs = (s) => s;
  globalThis.PA.esc = (s) => s;
  globalThis.PA.getSchemaColor = () => ({ bg: '#fff', fg: '#000' });
  globalThis.PA.api = { searchProcedures: async () => [] };
  new Function(await read('parser/js/leftPanel.js'))();
  return globalThis.PA;
}

export async function test_left_panel_header_counts_routines_not_raw_nodes() {
  // 31 PROCEDURE + 17 FUNCTION + 2 TRIGGER + 2 TABLE = 52 raw nodes. The header must read 48.
  const PA = await loadLeftPanel({
    totalNodes: 52,
    typeCounts: { PROCEDURE: 31, FUNCTION: 17, TRIGGER: 2, TABLE: 2 },
    _packageAnalysis: false
  });
  equal(PA._lpRoutineCount(), 48, 'the Procedures header must be procedures + functions only');
}

export async function test_left_panel_falls_back_to_raw_total_when_breakdown_absent() {
  const PA = await loadLeftPanel({ totalNodes: 52, _packageAnalysis: false });
  equal(PA._lpRoutineCount(), 52, 'no typeCounts → the raw total is the honest fallback');
}

export async function test_topbar_and_history_label_procs_from_type_counts() {
  // Source-level: both surfaces derive procs from PROCEDURE+FUNCTION when the breakdown exists,
  // and carry a trigger figure separately — never the raw node total as "procs".
  const app = await read('parser/js/app.js');
  includes(app, 'tc.PROCEDURE || 0', 'app.js must derive procs from the type breakdown');
  includes(app, 'tc.FUNCTION || 0', 'app.js must include functions in the procs figure');

  const history = await read('parser/js/history.js');
  includes(history, 'typeCounts', 'the history card must read the per-type breakdown');
  includes(history, 'triggers', 'the history card must show the trigger count on its own');

  const perf = await read('parser/js/perfSummary.js');
  includes(perf, '_procsOf', 'perfSummary must use the same procs+funcs derivation');
}
