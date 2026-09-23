/**
 * UI-024 — `parser/js/joins.js`'s `_resolveDetailFile(procId)` used to scan the whole
 * `PA.nodeIndex.all()` just to find one procedure's `detailFile` before fetching it, shared by
 * three call sites (`joins.js`'s own `toggleSql`, `joinSummary.js`'s `_loadQuerySql`/
 * `_loadSqlForDetail`). Read `IndexFacetReader.getNodeDetail` directly and confirmed it already
 * falls back to a package analysis's own per-proc subfolders when the plain `{nodeId}.json` lookup
 * misses — so the id alone is enough; no `PA.nodeIndex` scan needed at all.
 */
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { equal, excludes, includes } from './lib/assert.mjs';

const STATIC = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (rel) => readFile(path.join(STATIC, rel), 'utf8');

const code = (src) =>
  src.replace(/\/\*[\s\S]*?\*\//g, '').replace(/^\s*\/\/.*$/gm, '');

export async function test_resolve_detail_file_no_longer_scans_the_whole_node_index() {
  const src = code(await read('parser/js/joins.js'));
  excludes(src, 'PA.nodeIndex.all()', '_resolveDetailFile must not scan the whole analysis for one procedure');
  includes(src, "_resolveDetailFile: function(procId) {", 'the shared helper must still exist for its three callers');
}

async function loadJoins() {
  globalThis.window = globalThis;
  globalThis.PA = globalThis.PA || {};
  new Function(await read('parser/js/joins.js'))();
  return globalThis.PA.joins;
}

export async function test_returns_the_nodes_prefixed_path_for_any_procid_without_looking_it_up() {
  const joins = await loadJoins();
  equal(joins._resolveDetailFile('ANALYZER$ERP_PKG_PLANNER$WALK_UP'), 'nodes/ANALYZER$ERP_PKG_PLANNER$WALK_UP.json');
}

export async function test_returns_null_for_an_empty_procid() {
  const joins = await loadJoins();
  equal(joins._resolveDetailFile(''), null);
  equal(joins._resolveDetailFile(null), null);
}

export async function test_the_nodes_prefix_still_strips_the_same_way_callers_expect() {
  const joins = await loadJoins();
  const detailFile = joins._resolveDetailFile('ERP$P1');
  const nodeFile = detailFile.replace(/^nodes\//, '');
  equal(nodeFile, 'ERP$P1.json', 'callers strip the "nodes/" prefix before calling PA.api.getNodeDetail — must still work unchanged');
}
