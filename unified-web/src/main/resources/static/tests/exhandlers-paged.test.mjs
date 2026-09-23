/**
 * UI-029 — `exHandlers.js` no longer aggregates `PA.nodeIndex.all()` in the browser; it asks
 * `/procedures?cf=9:Y` for the same answer, with scope travelling as a request parameter the same
 * way `tableOps.js` already does it.
 */
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { ok, includes, excludes } from './lib/assert.mjs';

const STATIC = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (rel) => readFile(path.join(STATIC, rel), 'utf8');

const code = (src) =>
  src.replace(/\/\*[\s\S]*?\*\//g, '').replace(/^\s*\/\/.*$/gm, '');

export async function test_no_whole_corpus_aggregation_remains() {
  const src = code(await read('parser/js/exHandlers.js'));
  excludes(src, 'PA.nodeIndex.all()',
    'exHandlers.js still aggregates the whole node list in the browser');
  includes(src, 'PA.api.exHandlersPageSource',
    'exHandlers.js must be initialised from a page source, not a materialised array');
}

export async function test_the_exception_handler_filter_is_a_hidden_preset_not_removable_by_search() {
  const src = code(await read('parser/js/api.js'));
  const at = src.indexOf('exHandlersPageSource(params)');
  ok(at > 0, 'PA.api.exHandlersPageSource was not found');
  const body = src.slice(at, src.indexOf('\n    },', at));
  includes(body, "'9:Y'", "the preset filter (cf=9:Y) must always be sent");
  includes(body, "facetPageSource('procedures'",
    'must reuse the shared /procedures endpoint, not a new one');
}

export async function test_scope_travels_with_the_request() {
  const src = code(await read('parser/js/exHandlers.js'));
  includes(src, "src.setParam('scope'", 'scope is not sent to the server');
  includes(src, "src.setParam('procId'", 'the scoping procedure is not sent to the server');
  excludes(src, 'callTreeNodeIds',
    'the client-side scope filter survived; it scopes one page and reports it as the whole answer');
}

export async function test_columns_without_a_server_sort_key_are_not_claimed_sortable() {
  const src = code(await read('parser/js/exHandlers.js'));
  // Type and Handlers have no facet sort key — the file's own header explains why. A regression
  // here would silently reorder a fixed fifty-row page rather than the whole result set.
  includes(src, 'serverSorts:', 'the grid must declare its server-sortable columns explicitly');
  excludes(src, "2: { fn:", 'a client-side sortKeys entry for column 2 (Type) survived');
  excludes(src, "3: { fn:", 'a client-side sortKeys entry for column 3 (Handlers) survived');
}
