/**
 * UI-024 (§112) — `parser/js/export.js` was the last real consumer forcing `PA.nodeIndex` to stay
 * fully resident (per §89's own finding); its Call Flow/Complexity/Summary sheets needed the full,
 * unprojected node shape `ParserFacetSpecs.PROCEDURES`'s paged grid deliberately doesn't carry.
 * Converted to a new bulk endpoint (`/procedures/export`, `PA.api.getProceduresForExport`),
 * following the same "always fetch fresh" pattern already used for joins/tables/cursors/sequences
 * in the same file, rather than reading `PA.nodeIndex.all()`.
 */
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { ok, excludes, includes } from './lib/assert.mjs';

const STATIC = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (rel) => readFile(path.join(STATIC, rel), 'utf8');

const code = (src) =>
  src.replace(/\/\*[\s\S]*?\*\//g, '').replace(/^\s*\/\/.*$/gm, '');

export async function test_export_js_no_longer_reads_the_client_side_node_index() {
  const src = code(await read('parser/js/export.js'));
  excludes(src, 'PA.nodeIndex',
    'export.js must not depend on PA.nodeIndex at all — it was the last consumer forcing full residency');
}

export async function test_export_js_fetches_the_bulk_endpoint_instead() {
  const src = code(await read('parser/js/export.js'));
  includes(src, 'PA.api.getProceduresForExport()',
    'export.js\'s _gather must fetch the full node array from the new bulk endpoint');
}

export async function test_api_exposes_the_bulk_export_endpoint() {
  const src = code(await read('parser/js/api.js'));
  includes(src, 'async getProceduresForExport()', 'api.js must expose getProceduresForExport');
  includes(src, '/procedures/export`', 'must hit the new unpaged, unprojected endpoint');
}
