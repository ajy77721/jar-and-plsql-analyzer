/**
 * UI-024 (§164) — `parser/js/callGraph.js`'s `render()` awaits `PA.nodeIndex.ensureLoaded()` once,
 * at the single entry point every one of its 6 callers already funnels through, rather than
 * requiring `PA.nodeIndex` to already be resident. The recursive `renderNode`/`_getNodeMetrics`
 * walk itself stays fully synchronous — every node id it will ever ask for is already inside the
 * tree `render()` was called with, so one preload before the walk starts is enough.
 *
 * This is the real proof the design in §164's own doc comment claims: a lookup that would have
 * happened before the fetch resolved (the whole point of a delayed-fetch test, since the 52-node
 * real corpus resolves near-instantly and would never expose a race) must still see real data by
 * the time the recursive walk runs.
 */
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { installDom } from './lib/dom.mjs';
import { ok, equal } from './lib/assert.mjs';

const STATIC = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (rel) => readFile(path.join(STATIC, rel), 'utf8');

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function loadTrace(getProceduresImpl) {
  const dom = installDom();
  dom.add('ctContainer');
  dom.add('exploreSearch');
  dom.add('breadcrumb');
  dom.add('btnAnalysisRoot');
  globalThis.PA = {
    esc: (s) => String(s == null ? '' : s),
    escAttr: (s) => String(s == null ? '' : s),
    escJs: (s) => String(s == null ? '' : s),
    showProcedure: () => {},
    analysisData: {}, // deliberately no `nodes` yet — forces ensureLoaded() to fetch
    api: { getProcedures: getProceduresImpl },
  };
  new Function(await read('parser/js/nodeIndex.js'))();
  new Function(await read('parser/js/callGraph.js'))();
  return globalThis.PA.callTrace;
}

const TREE = {
  id: 'ROOT$PROC', name: 'ROOT_PROC', schemaName: 'APP', packageName: 'PKG', children: [
    { id: 'CHILD$PROC', name: 'CHILD_PROC', schemaName: 'APP', packageName: 'PKG', children: [] },
  ],
};

const REAL_NODES = [
  { nodeId: 'ROOT$PROC', name: 'ROOT_PROC', linesOfCode: 500, depth: 0, counts: { tables: 3, callsOut: 1 } },
  { nodeId: 'CHILD$PROC', name: 'CHILD_PROC', linesOfCode: 50, depth: 1, counts: { tables: 1, callsOut: 0 } },
];

export async function test_render_waits_for_a_delayed_fetch_before_the_walk_reads_node_data() {
  let fetchStarted = false;
  const trace = await loadTrace(async () => {
    fetchStarted = true;
    await delay(20); // real async gap — long enough that a missing await would show up as null data
    return REAL_NODES;
  });

  await trace.render(TREE);

  ok(fetchStarted, 'ensureLoaded() never triggered the fetch — render() did not await it at all');
  const metrics = trace._getNodeMetrics('ROOT$PROC');
  ok(metrics, 'render() finished before the delayed fetch resolved — _getNodeMetrics saw no data');
  equal(metrics.linesOfCode, 500, 'the resolved node data does not match what the fetch actually returned');
}

export async function test_a_second_render_does_not_refetch() {
  let fetchCount = 0;
  const trace = await loadTrace(async () => {
    fetchCount++;
    return REAL_NODES;
  });

  await trace.render(TREE);
  await trace.render(TREE);
  equal(fetchCount, 1, 'a second render() re-fetched instead of reusing the already-resolved node data');
}
