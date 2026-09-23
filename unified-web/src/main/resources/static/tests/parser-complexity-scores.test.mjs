/**
 * UI-024 (user decision: server-side, accept the debounce) — `parser/js/complexity.js`'s
 * `_recompute`/`_computeMetrics` used to scan `PA.nodeIndex.all()` and score every node in the
 * browser. Converted to fetch `ComplexityScoringService`'s server-computed scores instead, sending
 * the client's own already-resolved weights/thresholds/algorithm as the request.
 *
 * `scoreRaw`/`_metricContribution` are deliberately untouched — `callGraph.js`'s call-tree risk
 * badge still calls them client-side, one node at a time, during tree rendering.
 */
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { ok, equal, excludes, includes } from './lib/assert.mjs';

const STATIC = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (rel) => readFile(path.join(STATIC, rel), 'utf8');

const code = (src) =>
  src.replace(/\/\*[\s\S]*?\*\//g, '').replace(/^\s*\/\/.*$/gm, '');

export async function test_recompute_no_longer_scans_the_whole_node_index() {
  const src = code(await read('parser/js/complexity.js'));
  const start = src.indexOf('_recompute: async function');
  const end = src.indexOf('_initRiskPills: function', start);
  const fnSrc = src.slice(start, end);
  excludes(fnSrc, 'PA.nodeIndex.all()', '_recompute must not scan the whole analysis anymore');
  includes(fnSrc, 'PA.api.getComplexityScores', 'must fetch the server-computed scores instead');
}

export async function test_score_raw_and_metric_contribution_are_untouched_for_callgraph_js() {
  const src = code(await read('parser/js/complexity.js'));
  includes(src, 'scoreRaw: function(m)', 'the call-tree risk badge still needs this client-side, unconverted');
  includes(src, '_metricContribution: function(value, weight)', 'must remain for the same reason');
}

async function loadComplexity(scoresToReturn) {
  globalThis.window = globalThis;
  globalThis.document = { getElementById: () => null };
  globalThis.PA = globalThis.PA || {};
  const calls = [];
  globalThis.PA.api = {
    getComplexityScores: async (weights, thresholds, algorithm, exponent) => {
      calls.push({ weights, thresholds, algorithm, exponent });
      return scoresToReturn;
    }
  };
  new Function(await read('parser/js/complexity.js'))();
  const complexity = globalThis.PA.complexity;
  complexity._calls = calls;
  return complexity;
}

export async function test_recompute_sends_the_currently_resolved_config() {
  const complexity = await loadComplexity([{ nodeId: 'P1', score: 42, risk: 'MEDIUM' }]);
  complexity._weights = { loc: 1, tables: 2, callsOut: 3, cursors: 4, dynamicSql: 5, depth: 6 };
  complexity._thresholds = { medium: 10, high: 20 };
  complexity._algorithm = 'balanced';
  complexity._exponent = 2.5;
  await complexity._recompute();

  equal(complexity._calls.length, 1);
  const call = complexity._calls[0];
  equal(call.weights.loc, 1);
  equal(call.thresholds.high, 20);
  equal(call.algorithm, 'balanced');
  equal(call.exponent, 2.5);
  equal(complexity.data.length, 1);
  equal(complexity.data[0].score, 42);
}

export async function test_a_failed_fetch_leaves_data_empty_rather_than_throwing() {
  globalThis.window = globalThis;
  globalThis.document = { getElementById: () => null };
  globalThis.PA = globalThis.PA || {};
  globalThis.PA.api = { getComplexityScores: async () => { throw new Error('network error'); } };
  new Function(await read('parser/js/complexity.js'))();
  const complexity = globalThis.PA.complexity;
  await complexity._recompute();
  equal(complexity.data.length, 0);
}
