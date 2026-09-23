/**
 * MID-021 (§138) — end-to-end: the parser crawler's `api/tables/index.json` now carries
 * `usedBy[].predicates` (`ChunkedFlowWriter.writeTableIndex`, once `TableOperationInfo.whereFilters`
 * was actually carried through instead of dropped), `PA.api._adaptParserTableRow` attaches each
 * predicate to its matching `accessDetails` entry by line, and `predicates.js`'s array-based
 * fallback (`_buildPredicateData`) needed no changes at all — it already read `d.whereFilters`
 * correctly, it was simply always given none before this fix. This test proves the full chain: raw
 * artifact shape in, real predicate rows out, for a parser-backed analysis specifically (the case
 * that has been silently empty since this screen existed).
 */
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { equal, ok } from './lib/assert.mjs';

const STATIC = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (rel) => readFile(path.join(STATIC, rel), 'utf8');

async function loadApiAndPredicates() {
  globalThis.window = globalThis;
  globalThis.PA = globalThis.PA || {};
  new Function(await read('plsql/js/api.js'))();
  new Function(await read('plsql/js/predicates.js'))();
  return { api: globalThis.PA.api, predicates: globalThis.PA.predicates };
}

const RAW_PARSER_TABLES = {
  totalTables: 1,
  tables: [{
    name: 'REF_REGION',
    schema: 'ANALYZER_REF',
    objectType: 'TABLE',
    allOperations: ['SELECT'],
    usedBy: [{
      nodeId: 'ERP$FN_REGION_NAME',
      nodeName: 'FN_REGION_NAME',
      operations: { SELECT: [7] },
      predicates: [{ columnName: 'REGION_ID', operator: '=', value: 'p_id', lineNumber: 7 }]
    }]
  }]
};

export async function test_parser_backend_predicates_now_produces_real_rows_end_to_end() {
  const { api, predicates } = await loadApiAndPredicates();
  const tables = api._adaptParserTables(RAW_PARSER_TABLES);
  const rows = predicates._buildPredicateData(tables);

  equal(rows.length, 1, 'a table with a real predicate must produce one predicates row now');
  const row = rows[0];
  equal(row.tableName, 'REF_REGION');
  equal(row.predicateCount, 1);
  equal(row.predicates[0].columnName, 'REGION_ID');
  equal(row.predicates[0].operator, '=');
  equal(row.predicates[0].value, 'p_id');
  equal(row.predicates[0].procedureName, 'FN_REGION_NAME');
  ok(row.operations.includes('SELECT'));
}

export async function test_a_table_with_no_predicates_still_produces_no_row() {
  const { api, predicates } = await loadApiAndPredicates();
  const noPredicates = {
    totalTables: 1,
    tables: [{
      name: 'ERP_LOG',
      schema: 'ERP',
      objectType: 'TABLE',
      allOperations: ['INSERT'],
      usedBy: [{ nodeId: 'ERP$P1', nodeName: 'P1', operations: { INSERT: [3] } }]
    }]
  };
  const tables = api._adaptParserTables(noPredicates);
  const rows = predicates._buildPredicateData(tables);
  equal(rows.length, 0, 'a plain INSERT with no WHERE clause must still produce no predicates row');
}
