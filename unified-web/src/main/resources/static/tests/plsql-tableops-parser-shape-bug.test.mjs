/**
 * MID-021 (§109) — BUG: the parser-backend's unpaged `/tables` endpoint returns the raw crawler
 * artifact shape (`{totalTables, tables:[{name, schema, objectType, allOperations, usedBy:[...]}]}`),
 * confirmed against `TableFacetReader.getTables` and real on-disk `api/tables/index.json` data — not
 * the `{tableName, schemaName, tableType, operations, accessDetails, accessCount}` array
 * `plsql/js/tableOps.js`'s renderer has always expected. `PA.api.getTableOperations()` now adapts
 * the shape client-side via `PA.api._adaptParserTables`.
 */
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { equal, ok } from './lib/assert.mjs';

const STATIC = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (rel) => readFile(path.join(STATIC, rel), 'utf8');

function loadApi() {
  globalThis.window = globalThis;
  globalThis.PA = globalThis.PA || {};
  return read('plsql/js/api.js').then(src => {
    new Function(src)();
    return globalThis.PA.api;
  });
}

const RAW_SAMPLE = {
  totalTables: 1,
  tables: [{
    name: 'ERP_ORDER',
    schema: 'ERP',
    objectType: 'TABLE',
    allOperations: ['SELECT', 'UPDATE'],
    usedBy: [
      { nodeId: 'ERP$P1', nodeName: 'USP_RUNMRPCYCLE', operations: { SELECT: [61, 66] } },
      { nodeId: 'ERP$P2', nodeName: 'USP_PROCESSSALESORDER', operations: { UPDATE: [99] } }
    ]
  }]
};

export async function test_adapt_produces_the_array_shape_tableOps_renderer_expects() {
  const api = await loadApi();
  const adapted = api._adaptParserTables(RAW_SAMPLE);
  ok(Array.isArray(adapted), 'must return an array, not the raw {tables:[...]} object');
  equal(adapted.length, 1);
  const t = adapted[0];
  equal(t.tableName, 'ERP_ORDER', 'tableName must come from raw.name');
  equal(t.schemaName, 'ERP', 'schemaName must come from raw.schema');
  equal(t.tableType, 'TABLE', 'tableType must come from raw.objectType');
  equal(t.accessCount, 3, 'accessCount must be the flattened per-line count, not usedBy.length');
  equal(t.accessDetails.length, 3);
  ok(t.accessDetails.some(d => d.operation === 'SELECT' && d.procedureId === 'ERP$P1' && d.lineNumber === 61),
    'each usedBy operations[op] line must flatten into its own accessDetails row');
}

/**
 * Calibrated by reverting `getTableOperations` to `return res.json()` (the pre-fix code) against a
 * stubbed fetch returning RAW_SAMPLE: confirmed the result was the raw `{tables:[...]}` object
 * (`Array.isArray` false, `.tableName` undefined) rather than the adapted array, before restoring.
 */
export async function test_getTableOperations_adapts_for_the_parser_backend() {
  const api = await loadApi();
  api._parserName = () => 'some-analysis';
  globalThis.fetch = async () => ({ ok: true, json: async () => RAW_SAMPLE });
  const result = await api.getTableOperations();
  ok(Array.isArray(result), 'getTableOperations must return an adapted array for the parser backend');
  equal(result[0].tableName, 'ERP_ORDER');
  equal(result[0].accessCount, 3);
}

/**
 * MID-021 (§138) — `usedBy[].predicates` is the artifact's new field (`ChunkedFlowWriter
 * .writeTableIndex`, once `TableOperationInfo.whereFilters` was actually carried through). This
 * pins the client half of that fix: grouped by `lineNumber` and attached as each matching
 * `accessDetails` entry's own `whereFilters` — the field `predicates.js`'s `_buildPredicateData`
 * has always read, and has always found empty for a parser-backed analysis until this fix.
 *
 * Calibrated by reverting the `whereFilters: predicatesByLine[lineNumber] || []` line back to no
 * such field at all: confirmed the test failed (`whereFilters` undefined), restored.
 */
export async function test_predicates_are_attached_to_the_matching_access_detail_by_line() {
  const api = await loadApi();
  const sample = {
    totalTables: 1,
    tables: [{
      name: 'REF_REGION',
      schema: 'ANALYZER_REF',
      objectType: 'TABLE',
      allOperations: ['SELECT'],
      usedBy: [{
        nodeId: 'ERP$FN_REGION_NAME',
        nodeName: 'FN_REGION_NAME',
        operations: { SELECT: [7, 20] },
        predicates: [{ columnName: 'REGION_ID', operator: '=', value: 'p_id', lineNumber: 7 }]
      }]
    }]
  };
  const adapted = api._adaptParserTables(sample);
  const details = adapted[0].accessDetails;
  const line7 = details.find(d => d.lineNumber === 7);
  const line20 = details.find(d => d.lineNumber === 20);
  equal(line7.whereFilters.length, 1, 'line 7 has a predicate attached to it');
  equal(line7.whereFilters[0].columnName, 'REGION_ID');
  equal(line20.whereFilters.length, 0, 'line 20 has no predicate — must not inherit line 7\'s');
}
