/**
 * UI-024 — `parser/js/tableDetail.js` step 4 ("merge triggers from call tree nodes / parsed source
 * triggers") used to scan `PA.nodeIndex.all()` for `TRIGGER`-type nodes matching one table.
 * Replaced with `PA.api.getTriggersForTable(tableName)` (server-filtered by table name), keeping
 * the `treeIds`/`triggerSeen` filtering exactly as it was — those weren't independently
 * re-verified, only the full-corpus scan they used to run against was.
 */
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { ok, equal, excludes, includes } from './lib/assert.mjs';

const STATIC = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (rel) => readFile(path.join(STATIC, rel), 'utf8');

const code = (src) =>
  src.replace(/\/\*[\s\S]*?\*\//g, '').replace(/^\s*\/\/.*$/gm, '');

export async function test_step_4_no_longer_scans_the_whole_node_index() {
  const src = code(await read('parser/js/tableDetail.js'));
  const start = src.indexOf('_loadTableData(tableName)');
  const end = src.indexOf('// --- 5. Joins', start);
  const fnSrc = src.slice(start, end);
  excludes(fnSrc, 'PA.nodeIndex.all()', 'step 4 must not scan the whole analysis for one table\'s triggers');
  includes(fnSrc, 'PA.api.getTriggersForTable(tableName)', 'must fetch server-filtered candidates instead');
}

function trigger(nodeId, name, schema, timing, event) {
  return { nodeId, name, objectName: name, schema, triggerTiming: timing, triggerEvent: event };
}

async function loadAndCallStep4({ apiTriggerCandidates, treeIds, tableOpsData }) {
  globalThis.window = globalThis;
  globalThis.document = { getElementById: () => null };
  globalThis.PA = globalThis.PA || {};
  globalThis.PA.context = { callTreeNodeIds: treeIds };
  globalThis.PA.tableOps = { data: tableOpsData || [] };
  // Isolate from any PA.analysisData a previous test file in this shared process left behind —
  // step 7's Claude-verification fetch only fires when an analysis name is present.
  globalThis.PA.analysisData = null;
  globalThis.PA.api = {
    _analysisName: null,
    getTableDetailByName: async () => null,
    getTablesIndex: async () => null,
    getTriggersForTable: async () => apiTriggerCandidates,
    claudeResult: async () => null
  };
  new Function(await read('parser/js/tableDetail.js'))();
  return globalThis.PA.tableDetail._loadTableData('ERP_SALES_ORDER');
}

export async function test_only_the_server_filtered_candidates_reach_the_result() {
  const result = await loadAndCallStep4({
    apiTriggerCandidates: [trigger('T1', 'TRG1', 'ANALYZER', 'AFTER', 'DELETE')],
    treeIds: null
  });
  equal(result.triggers.length, 1);
  equal(result.triggers[0].name, 'TRG1');
  equal(result.triggers[0].source, 'PARSED');
}

export async function test_treeids_filtering_is_preserved_unchanged() {
  const result = await loadAndCallStep4({
    apiTriggerCandidates: [
      trigger('T1', 'TRG1', 'ANALYZER', 'AFTER', 'DELETE'),
      trigger('T2', 'TRG2', 'ANALYZER', 'BEFORE', 'INSERT')
    ],
    treeIds: new Set(['T1'])
  });
  equal(result.triggers.length, 1, 'only the trigger inside the current scope\'s tree must survive');
  equal(result.triggers[0].nodeId, 'T1');
}

export async function test_dedup_against_step_3s_triggers_is_preserved_unchanged() {
  globalThis.window = globalThis;
  globalThis.document = { getElementById: () => null };
  globalThis.PA = globalThis.PA || {};
  globalThis.PA.context = { callTreeNodeIds: null };
  globalThis.PA.tableOps = { data: [{ tableName: 'ERP_SALES_ORDER', operations: [], accessDetails: [], triggers: [{ name: 'TRG1', nodeId: 'T1' }] }] };
  globalThis.PA.analysisData = null;
  globalThis.PA.api = {
    _analysisName: null,
    getTableDetailByName: async () => null,
    getTablesIndex: async () => null,
    getTriggersForTable: async () => [trigger('T1', 'TRG1', 'ANALYZER', 'AFTER', 'DELETE')],
    claudeResult: async () => null
  };
  new Function(await read('parser/js/tableDetail.js'))();
  const result = await globalThis.PA.tableDetail._loadTableData('ERP_SALES_ORDER');
  equal(result.triggers.length, 1, 'a trigger step 3 already found by name must not be duplicated by step 4');
  ok(result.triggers[0].source !== 'PARSED', 'the surviving copy must be step 3\'s (tables API), not step 4\'s parsed-source duplicate');
}
