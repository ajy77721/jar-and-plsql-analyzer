/**
 * The node index — one place that answers "which node is this?".
 *
 * ## What it replaces
 *
 * `PA.analysisData.nodes` was scanned linearly in eleven places, and **two of those sit inside
 * loops over table accesses**. That makes the cost `accesses × nodes`: at 8,000 procedures and the
 * 2,435 accesses measured on one table, ~19.5M comparisons for a single render, growing in both
 * dimensions at once. Same defect as the `indexOf` in the JAR code tree (FE-017), different file.
 *
 * ## Why the tests are about precedence and staleness
 *
 * Speed is the easy part. The two ways an index of this kind is *wrong* are subtler: it resolves a
 * key by a different rule than the chain it replaced, or it answers from a previous analysis. Both
 * produce a plausible wrong answer rather than an error.
 */
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { installDom } from './lib/dom.mjs';
import { ok, equal, includes, atLeast } from './lib/assert.mjs';

const STATIC = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (rel) => readFile(path.join(STATIC, rel), 'utf8');

async function loadIndex(nodes) {
  installDom();
  globalThis.PA = { analysisData: { nodes } };
  new Function(await read('parser/js/nodeIndex.js'))();
  return globalThis.PA.nodeIndex;
}

const NODES = [
  { nodeId: 'PKG$A', name: 'PROC_A', sourceFile: 'S.PROC_A.sql', depth: 0 },
  { nodeId: 'PKG$B', name: 'PROC_B', sourceFile: 'S.PROC_B.sql', depth: 1 },
  { id: 'ALIAS$C', name: 'PROC_C', sourceFile: 'S.PROC_C.sql', depth: 2 },
];

export async function test_lookups_resolve_by_id_name_and_source_file() {
  const idx = await loadIndex(NODES);
  equal(idx.byId('PKG$A').name, 'PROC_A', 'lookup by nodeId failed');
  equal(idx.byName('PROC_B').nodeId, 'PKG$B', 'lookup by name failed');
  equal(idx.bySourceFile('S.PROC_C.SQL').name, 'PROC_C',
    'lookup by source file is not case-insensitive');
  equal(idx.byIdIgnoringCase('pkg$a').name, 'PROC_A', 'case-insensitive id lookup failed');
  equal(idx.byId('PKG$A').name, 'PROC_A', 'a repeated lookup returned something different');
}

export async function test_the_id_alias_is_indexed() {
  // Some payloads carry `id` rather than `nodeId`; the old chains checked both.
  const idx = await loadIndex(NODES);
  ok(idx.byId('ALIAS$C'), 'a node carrying only `id` is not reachable by id');
}

export async function test_resolve_prefers_an_exact_id_over_a_case_insensitive_name() {
  // The precedence the hand-written chains used. If it inverts, a procedure whose name matches
  // another node's lower-cased id resolves to the wrong node — silently.
  // The fixture has to make the orders disagree, which the first version did not: there must be
  // NO exact id match, but both a case-insensitive id match and an exact NAME match. Correct order
  // returns the exact name; inverted returns the case-insensitive id.
  const idx = await loadIndex([
    { nodeId: 'TARGET', name: 'BY_CI_ID' },
    { nodeId: 'zzz', name: 'target' },
  ]);
  equal(idx.resolve('target').name, 'target',
    'resolve() returned the case-insensitive id match ahead of the exact name match — an exact '
    + 'match must always beat an inexact one, or a procedure resolves to a different node');
}

export async function test_a_duplicate_resolves_the_way_find_did() {
  // `.find()` stopped at the first match, so a later duplicate was never visible. An index that
  // let the last one win would change answers on data that already exists — PL/SQL overloads share
  // a name (ANA-004).
  const idx = await loadIndex([
    { nodeId: 'DUP', name: 'FIRST' },
    { nodeId: 'DUP', name: 'SECOND' },
  ]);
  equal(idx.byId('DUP').name, 'FIRST', 'a duplicate id resolves to the later node');
}

export async function test_the_index_rebuilds_when_the_analysis_changes() {
  // The dangerous failure: a stale index answers with the PREVIOUS analysis's node. Invalidation
  // is by array identity rather than a flag, because a flag is something to forget to clear.
  const idx = await loadIndex(NODES);
  equal(idx.byId('PKG$A').name, 'PROC_A');

  globalThis.PA.analysisData = { nodes: [{ nodeId: 'PKG$A', name: 'RENAMED' }] };
  equal(idx.byId('PKG$A').name, 'RENAMED',
    'the index served a node from the previous analysis');
  equal(idx.count(), 1, 'the index did not rebuild over the new array');
}

export async function test_an_absent_analysis_is_not_an_error() {
  installDom();
  globalThis.PA = {};
  new Function(await read('parser/js/nodeIndex.js'))();
  const idx = globalThis.PA.nodeIndex;
  equal(idx.count(), 0, 'no analysis should mean an empty index, not a throw');
  equal(idx.byId('anything'), null, 'a lookup with no analysis should be null');
  equal(idx.resolve('anything'), null, 'resolve with no analysis should be null');
}

export async function test_lookups_do_not_scale_with_the_node_count() {
  // The property, not a threshold: 40,000 nodes must not make one lookup materially slower than
  // 400 do. A linear scan fails this by three orders of magnitude.
  const small = await loadIndex(Array.from({ length: 400 }, (_, i) => (
    { nodeId: 'N' + i, name: 'P' + i, sourceFile: 'f' + i })));
  let t = performance.now();
  for (let i = 0; i < 20000; i++) small.byId('N399');
  const smallMs = performance.now() - t;

  globalThis.PA.analysisData = { nodes: Array.from({ length: 40000 }, (_, i) => (
    { nodeId: 'N' + i, name: 'P' + i, sourceFile: 'f' + i })) };
  const big = globalThis.PA.nodeIndex;
  big.count();                                  // force the rebuild out of the timed section
  t = performance.now();
  for (let i = 0; i < 20000; i++) big.byId('N39999');
  const bigMs = performance.now() - t;

  ok(bigMs < smallMs * 5 + 50,
    `100x the nodes made lookups ${(bigMs / Math.max(smallMs, 0.01)).toFixed(1)}x slower ` +
    `(${smallMs.toFixed(1)}ms vs ${bigMs.toFixed(1)}ms) — this is still a scan`);
}

export async function test_no_identity_scan_over_the_node_array_remains() {
  // Asserted on the source, because a scan and a lookup produce identical output and differ only
  // in how long they take.
  const { readdir } = await import('node:fs/promises');
  const dir = 'parser/js';
  const offenders = [];
  for (const f of await readdir(path.join(STATIC, dir))) {
    if (!f.endsWith('.js')) continue;
    const src = (await read(`${dir}/${f}`))
      .replace(/\/\*[\s\S]*?\*\//g, '').replace(/^\s*\/\/.*$/gm, '');
    // An identity scan mentions nodeId or name inside a find over the node array.
    if (/nodes\.find\((?:function\s*\(\w+\)\s*\{[^}]*|\w+\s*=>[^;]*?)(?:nodeId|\.name|sourceFile)/.test(src)) {
      offenders.push(`${dir}/${f}`);
    }
  }
  equal(offenders.length, 0,
    `these still scan the node array for an identity: ${offenders.join(', ')}`);
}

export async function test_the_index_is_the_only_reader_of_the_raw_array() {
  // The point of the refactor. While 18 files reach into `analysisData.nodes` directly, the
  // transport cannot change — which is what blocked /index from shedding its node list (MID-002),
  // 53.1% of a response fetched on every analysis load. One reader means one thing to change.
  const { readdir } = await import('node:fs/promises');
  const offenders = [];
  for (const f of await readdir(path.join(STATIC, 'parser/js'))) {
    if (!f.endsWith('.js') || f === 'nodeIndex.js') continue;
    const src = (await read(`parser/js/${f}`))
      .replace(/\/\*[\s\S]*?\*\//g, '').replace(/^\s*\/\/.*$/gm, '');
    if (src.includes('analysisData.nodes')) offenders.push(`parser/js/${f}`);
  }
  equal(offenders.length, 0,
    `these read the raw node array instead of the index: ${offenders.join(', ')}`);
}

export async function test_the_index_is_loaded_before_its_consumers() {
  // A lookup against an undefined PA.nodeIndex throws at render time, on a page that worked before.
  const html = await read('parser/index.html');
  const idxAt = html.indexOf('js/nodeIndex.js');
  atLeast(idxAt, 0, 'the parser page does not load nodeIndex.js');
  for (const consumer of ['js/tableDetail.js', 'js/sourceView.js', 'js/callGraphViz.js',
    'js/triggerDetail.js']) {
    ok(html.indexOf(consumer) > idxAt,
      `${consumer} is loaded before nodeIndex.js, so its first lookup would throw`);
  }
}

/**
 * UI-024 (§161) — `ensureLoaded()`'s own cache-miss/cache-hit behavior, tested with a mocked,
 * artificially-delayed fetch: proving the mechanism works correctly does not require a corpus at
 * any particular scale — a miss-then-fetch-then-resolve sequence behaves identically whether the
 * fetched array has 1 node or 100,000. Only the *memory win* claim is scale-dependent, and this
 * session has no corpus to prove that half; this half is fully verifiable here and is.
 */
async function loadIndexLazy(fetchImpl) {
  installDom();
  globalThis.PA = { analysisData: { name: 'A1' }, api: { getProcedures: fetchImpl } };
  new Function(await read('parser/js/nodeIndex.js'))();
  return globalThis.PA.nodeIndex;
}

export async function test_ensureLoaded_fetches_once_on_a_cache_miss() {
  let calls = 0;
  const idx = await loadIndexLazy(async () => { calls++; return NODES; });
  ok(!Array.isArray(PA.analysisData.nodes), 'nodes must not be resident before ensureLoaded is called');
  const result = await idx.ensureLoaded();
  equal(calls, 1, 'exactly one fetch for the first ensureLoaded call');
  equal(result.length, NODES.length);
  ok(Array.isArray(PA.analysisData.nodes), 'analysisData.nodes must be populated after ensureLoaded resolves');
}

export async function test_ensureLoaded_does_not_refetch_once_resident() {
  let calls = 0;
  const idx = await loadIndexLazy(async () => { calls++; return NODES; });
  await idx.ensureLoaded();
  await idx.ensureLoaded();
  await idx.ensureLoaded();
  equal(calls, 1, 'a resident array must short-circuit every subsequent call, not fetch again');
}

export async function test_ensureLoaded_dedupes_concurrent_callers_against_one_in_flight_fetch() {
  let calls = 0;
  let resolveFetch;
  const idx = await loadIndexLazy(() => {
    calls++;
    return new Promise((resolve) => { resolveFetch = () => resolve(NODES); });
  });
  const p1 = idx.ensureLoaded();
  const p2 = idx.ensureLoaded();
  ok(p1 === p2, 'two callers racing before the fetch resolves must share the same in-flight promise');
  resolveFetch();
  await p1;
  equal(calls, 1, 'only one real fetch must have been made despite two concurrent callers');
}

export async function test_ensureLoaded_resolves_immediately_when_already_resident() {
  const idx = await loadIndex(NODES); // the existing eager-style loader — nodes already an array
  let calls = 0;
  PA.api = { getProcedures: async () => { calls++; return []; } };
  await idx.ensureLoaded();
  equal(calls, 0, 'must not fetch at all when analysisData.nodes is already an array');
}

/**
 * UI-024 (§161) — `_findNodeInData` is now genuinely lazy, awaiting `ensureLoaded()` before its
 * lookup rather than assuming `PA.loadAnalysis`'s own eager fetch already ran. Calibrated by
 * reverting the function to its old synchronous body (no `ensureLoaded()` call) and confirming the
 * exact expected failure — a lookup made before anything else populated the array returns `null`
 * instead of the real node — before restoring.
 */
export async function test_findNodeInData_awaits_ensureLoaded_before_looking_up() {
  installDom();
  globalThis.PA = {
    analysisData: { name: 'A1' },
    api: { getProcedures: async () => NODES },
    nodeIndex: null
  };
  new Function(await read('parser/js/nodeIndex.js'))();
  new Function((await read('parser/js/app.js')).match(
    /PA\._findNodeInData = async function[\s\S]*?\n};/)[0])();

  ok(!Array.isArray(PA.analysisData.nodes), 'nodes must not be resident before the lookup');
  const found = await PA._findNodeInData('PKG$A');
  ok(found && found.name === 'PROC_A', 'must resolve the real node once the lazy fetch completes');
}
