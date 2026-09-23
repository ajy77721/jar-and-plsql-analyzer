/**
 * UI-025 — `jar/js/endpointList.js` is server-paged and flat (decision recorded 2026-09-08:
 * controller-grouping dropped, not kept behind a client-side re-group). It used to hold
 * `analysis.endpoints` whole, cap the rendered cards at 200, and offer a "show all" escape.
 */
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { includes, excludes } from './lib/assert.mjs';

const STATIC = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (rel) => readFile(path.join(STATIC, rel), 'utf8');

const code = (src) =>
  src.replace(/\/\*[\s\S]*?\*\//g, '').replace(/^\s*\/\/.*$/gm, '');

export async function test_render_no_longer_takes_the_whole_endpoints_array() {
  const src = code(await read('jar/js/endpointList.js'));
  excludes(src, 'render(endpoints)',
    'render() must not take the whole endpoints array as a parameter');
  includes(src, 'JA.api.endpointListPages',
    'render() must fetch a page source, not receive a materialised array');
}

export async function test_the_cap_and_show_all_escape_are_gone() {
  const src = code(await read('jar/js/endpointList.js'));
  excludes(src, '_cap', 'the 200-card cap is a client-side concept that should no longer exist');
  excludes(src, 'showAll', 'the "show all" escape has no meaning once the list is server-paged');
}

export async function test_search_travels_to_the_server_via_pa_tf() {
  const src = code(await read('jar/js/endpointList.js'));
  includes(src, "PA.tf.setSearch('endpoints'",
    'the search box must go through PA.tf, which pushes q to the server on the src path');
}

export async function test_select_reads_endpointIndex_not_an_array_position() {
  const src = code(await read('jar/js/endpointList.js'));
  includes(src, 'ep.endpointIndex',
    'each card must carry the server-provided endpointIndex, not a client-computed _origIdx');
  excludes(src, '_origIdx',
    'the array-position field only made sense when the whole array was held client-side');
}

export async function test_app_js_calls_render_with_no_argument() {
  const src = code(await read('jar/js/app.js'));
  includes(src, 'JA.endpointList.render();',
    'app.js must not still pass analysis.endpoints to render()');
}
