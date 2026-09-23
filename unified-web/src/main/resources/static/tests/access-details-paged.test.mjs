/**
 * UI-001 — `tableDetail.js`'s table-detail modal no longer falls back to the whole-corpus
 * `PA.api.getTableOperations()` flat-map (every table, every access line) just to find one table
 * by name when `PA.tableOps.data` has not loaded yet. It now asks `/tables/page?q=<name>` for that
 * one row via the new `PA.api.getTableDetailByName`.
 *
 * ## Why source-level
 *
 * Same reasoning as `tableops-paged.test.mjs`: the defect is structural (which endpoint gets
 * called), not a rendering difference — a fallback to the eager path would produce byte-identical
 * markup while quietly re-materialising ~1M objects, so this checks the call itself.
 */
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { includes, excludes } from './lib/assert.mjs';

const STATIC = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (rel) => readFile(path.join(STATIC, rel), 'utf8');

const code = (src) =>
  src.replace(/\/\*[\s\S]*?\*\//g, '').replace(/^\s*\/\/.*$/gm, '');

export async function test_table_detail_no_longer_falls_back_to_the_whole_corpus_read() {
  const src = code(await read('parser/js/tableDetail.js'));
  excludes(src, 'await PA.api.getTableOperations()',
    'the table-detail modal still falls back to the whole-corpus flat-map to find one table');
  includes(src, 'await PA.api.getTableDetailByName(tableName)',
    'the table-detail modal must ask for the one table it needs by name');
}

export async function test_get_table_detail_by_name_scopes_the_request_to_one_table() {
  const src = code(await read('parser/js/api.js'));
  const at = src.indexOf('async getTableDetailByName(tableName)');
  includes(src, 'async getTableDetailByName(tableName)',
    'PA.api.getTableDetailByName was not found');
  const body = src.slice(at, src.indexOf('\n    },', at));
  includes(body, "/tables/page", 'must query the paged tables endpoint, not the whole document');
  includes(body, "params.set('q', tableName)",
    'must search by table name rather than fetching every table');
}

export async function test_get_table_operations_the_eager_flatmap_is_still_used_only_for_export() {
  // getTableOperations() itself is not deleted: export.js has a legitimate need for every table's
  // every access line (the user asked to export everything). This pins that its only remaining
  // caller in this module is the export path, not a list-lookup path re-introducing the defect.
  const exportSrc = code(await read('parser/js/export.js'));
  includes(exportSrc, 'PA.api.getTableOperations()',
    'export.js should still use the eager read for a genuine whole-corpus export');

  const tableDetailSrc = code(await read('parser/js/tableDetail.js'));
  excludes(tableDetailSrc, 'PA.api.getTableOperations',
    'tableDetail.js must not call the eager whole-corpus read anywhere');
}
