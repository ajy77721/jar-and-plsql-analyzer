/**
 * UI-013 §60/§62/§64 — `parser/js/joinSummary.js:loadQueryOnly` no longer forces a full, unpaged
 * `/joins` fetch just to build the per-query grouping when `PA.joins.data` isn't already resident;
 * it reads the already-grouped rows from a dedicated summary endpoint instead.
 */
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { includes } from './lib/assert.mjs';

const STATIC = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (rel) => readFile(path.join(STATIC, rel), 'utf8');

const code = (src) =>
  src.replace(/\/\*[\s\S]*?\*\//g, '').replace(/^\s*\/\/.*$/gm, '');

export async function test_api_exposes_the_query_summary_call() {
  const src = code(await read('parser/js/api.js'));
  includes(src, 'getJoinQuerySummary',
    'api.js must expose the server-side query-grouping summary call');
  includes(src, '/joins/summary/queries',
    'the wrapper must call the dedicated query-summary path');
}

export async function test_load_query_only_uses_the_summary_when_joins_data_is_not_resident() {
  const src = code(await read('parser/js/joinSummary.js'));
  includes(src, 'PA.api.getJoinQuerySummary()',
    'loadQueryOnly must fall back to the server summary when PA.joins.data is empty');
}

export async function test_load_query_only_still_folds_locally_when_joins_data_is_already_loaded() {
  const src = code(await read('parser/js/joinSummary.js'));
  includes(src, 'PA.joinSummary._buildQueryItems(raw)',
    'when the Joins tab already holds the array, loadQueryOnly should reuse it rather than '
    + 'making a second request for the same data');
}
