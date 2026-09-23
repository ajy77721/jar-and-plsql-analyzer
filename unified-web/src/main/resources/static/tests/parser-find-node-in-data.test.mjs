/**
 * UI-024 — `parser/js/app.js`'s `PA._findNodeInData(procId)` used to be a `PA.nodeIndex.all()`
 * linear scan checking `nodeId === procId || name === procId` per node. `nodeIndex.js`'s own O(1)
 * maps (`byId`/`byName`) already exist for exactly this class of lookup — this converts the scan
 * to use them instead, real execution against a small node-index fixture.
 *
 * UI-024 (§161): `_findNodeInData` is now `async`, awaiting `PA.nodeIndex.ensureLoaded()` before
 * its lookup — see `node-index.test.mjs`'s own `ensureLoaded`/`findNodeInData` tests for that
 * mechanism's own coverage. This file's tests updated to `await` the now-async function.
 */
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { equal, excludes, includes } from './lib/assert.mjs';

const STATIC = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (rel) => readFile(path.join(STATIC, rel), 'utf8');

const code = (src) =>
  src.replace(/\/\*[\s\S]*?\*\//g, '').replace(/^\s*\/\/.*$/gm, '');

export async function test_find_node_in_data_no_longer_scans() {
  const src = code(await read('parser/js/app.js'));
  const start = src.indexOf('PA._findNodeInData = async function');
  const fnSrc = src.slice(start, src.indexOf('PA._loadNodeDetailTimer', start));
  excludes(fnSrc, 'PA.nodeIndex.all()', '_findNodeInData must use the O(1) index, not scan the whole array');
  includes(fnSrc, 'PA.nodeIndex.byId(procId)', 'must use the existing O(1) byId lookup');
  includes(fnSrc, 'PA.nodeIndex.byName(procId)', 'must fall back to the existing O(1) byName lookup');
}

export async function test_find_node_in_data_awaits_ensureLoaded() {
  const src = code(await read('parser/js/app.js'));
  const start = src.indexOf('PA._findNodeInData = async function');
  const fnSrc = src.slice(start, src.indexOf('PA._loadNodeDetailTimer', start));
  includes(fnSrc, 'await PA.nodeIndex.ensureLoaded()',
    'must await ensureLoaded before looking up — UI-024 (§161)');
}

async function loadFindNodeInData(nodes) {
  globalThis.window = globalThis;
  globalThis.document = { addEventListener: () => {} };
  globalThis.PA = globalThis.PA || {};
  globalThis.PA.analysisData = { nodes };
  globalThis.PA.home = null;
  globalThis.PA.claude = null;
  globalThis.PA.onScopeChange = () => {};
  globalThis.PA.nodeIndex = {
    _source: null, _byId: null, _byName: null,
    ensureLoaded: async function () { return (PA.analysisData && PA.analysisData.nodes) || []; },
    all: function () {
      var ns = (PA.analysisData && PA.analysisData.nodes) || [];
      if (this._source !== ns) {
        this._source = ns;
        this._byId = new Map();
        this._byName = new Map();
        for (var i = 0; i < ns.length; i++) {
          var n = ns[i];
          if (n.nodeId && !this._byId.has(n.nodeId)) this._byId.set(n.nodeId, n);
          if (n.name && !this._byName.has(n.name)) this._byName.set(n.name, n);
        }
      }
      return ns;
    },
    byId: function (id) { this.all(); return this._byId.get(id) || null; },
    byName: function (name) { this.all(); return this._byName.get(name) || null; }
  };
  new Function(await read('parser/js/app.js'))();
  return globalThis.PA._findNodeInData;
}

export async function test_resolves_by_exact_nodeid() {
  const findNode = await loadFindNodeInData([
    { nodeId: 'ERP$P1', name: 'P1' },
    { nodeId: 'ERP$P2', name: 'P2' }
  ]);
  const node = await findNode('ERP$P2');
  equal(node.name, 'P2');
}

export async function test_falls_back_to_exact_name_when_id_does_not_match() {
  const findNode = await loadFindNodeInData([
    { nodeId: 'ERP$P1', name: 'SPECIAL_NAME' }
  ]);
  const node = await findNode('SPECIAL_NAME');
  equal(node.nodeId, 'ERP$P1');
}

export async function test_returns_null_for_an_unknown_id() {
  const findNode = await loadFindNodeInData([{ nodeId: 'ERP$P1', name: 'P1' }]);
  equal(await findNode('NO_SUCH_ID'), null);
}

export async function test_returns_null_for_an_empty_procid_without_touching_the_index() {
  const findNode = await loadFindNodeInData([{ nodeId: 'ERP$P1', name: 'P1' }]);
  equal(await findNode(''), null);
  equal(await findNode(null), null);
}
