/**
 * UI-024 (§131) — two confirmed-dead `PA.nodeIndex` read sites removed while investigating whether
 * `PA.nodeIndex` could be made lazy: `callGraphModal.js`'s whole render chain (`show`/`navigate`/
 * `focusNode`/`_findNodeData`/`_deriveCallersCallees`/`_findGraphNode`/`_buildList`/`_getTypeLabel`)
 * had no reachable caller anywhere (§91 found this first for `_findNodeData` alone; re-confirmed here
 * for the whole chain), and `refs.js`'s `_assignCallTypes` had no caller at all. Both scans never
 * actually ran; removing them is a real (if small) reduction in `PA.nodeIndex` residency
 * dependents, independent of whether the larger laziness redesign proceeds.
 */
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { excludes, includes } from './lib/assert.mjs';

const STATIC = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (rel) => readFile(path.join(STATIC, rel), 'utf8');

const code = (src) =>
  src.replace(/\/\*[\s\S]*?\*\//g, '').replace(/^\s*\/\/.*$/gm, '');

export async function test_callGraphModal_dead_render_chain_is_gone() {
  const src = code(await read('parser/js/callGraphModal.js'));
  excludes(src, 'PA.nodeIndex', 'callGraphModal.js must no longer read PA.nodeIndex at all');
  excludes(src, '_findNodeData', 'the dead render chain must be gone, not just its nodeIndex call');
  excludes(src, 'show:', 'the dead show() entry point must be gone');
  includes(src, 'close:', 'close() is the one real external dependency (callGraphFullscreen.js) and must remain');
}

export async function test_callGraphFullscreen_still_finds_close() {
  const modal = await read('parser/js/callGraphModal.js');
  const fullscreen = await read('parser/js/callGraphFullscreen.js');
  includes(fullscreen, 'PA.callGraphModal.close()', 'callGraphFullscreen.js\'s real dependency must still resolve');
  includes(modal, 'close: function()', 'and callGraphModal.js must still define it');
}

export async function test_refs_js_dead_assignCallTypes_is_gone() {
  const src = code(await read('parser/js/refs.js'));
  excludes(src, '_assignCallTypes', 'the confirmed-dead function must be removed, not just its body');
}
