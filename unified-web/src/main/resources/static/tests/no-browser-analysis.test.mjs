/**
 * UI-026g — `jar/js/summary.js`'s live `_render` path fetches UI-026f's precomputed reports
 * instead of running `_analyzeEndpoint` in a `requestAnimationFrame` batch loop over every
 * endpoint (§71: that loop was CPU-bound and synchronous, not a per-procedure network call — this
 * replaces the freeze-and-progress-bar it needed).
 *
 * <p>The old loop (`_renderLegacy`) is kept as the fallback for a summary cache that isn't built
 * yet, so these checks are about which path is <em>live</em>, not about deleting the fallback.
 */
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { includes, excludes } from './lib/assert.mjs';

const STATIC = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (rel) => readFile(path.join(STATIC, rel), 'utf8');

const code = (src) =>
  src.replace(/\/\*[\s\S]*?\*\//g, '').replace(/^\s*\/\/.*$/gm, '');

export async function test_render_fetches_the_precomputed_reports_endpoint() {
  const src = code(await read('jar/js/summary.js'));
  includes(src, 'JA.api.getSummaryReports',
    '_render must fetch the precomputed reports rather than only running the in-browser loop');
}

export async function test_the_live_render_path_no_longer_drives_analyzeEndpoint_directly() {
  const src = code(await read('jar/js/summary.js'));
  // _analyzeEndpoint must still exist (the fallback calls it), but the live async _render
  // function itself must not call it — only _renderLegacy may.
  const renderFn = src.slice(src.indexOf('async _render(analysis, container)'),
      src.indexOf('_renderLegacy(analysis, container, esc) {'));
  excludes(renderFn, 'this._analyzeEndpoint(',
    '_render itself must not run the per-endpoint analysis loop directly — only the fallback should');
}

export async function test_a_failed_fetch_falls_back_to_the_in_browser_loop() {
  const src = code(await read('jar/js/summary.js'));
  includes(src, '_renderLegacy(analysis, container, esc)',
    'a failed /summary/reports fetch must fall back to the old in-browser analysis, not a blank tab');
}

export async function test_cross_reports_are_set_from_the_server_not_left_null() {
  const src = code(await read('jar/js/summary.js'));
  includes(src, 'this._vertReport = data.vertReport',
    'the server-provided cross-reports must be assigned directly, not left for lazy client rebuild');
  includes(src, 'this._extReport = data.extReport',
    'extReport must come from the server response too');
}

export async function test_api_exposes_the_summary_reports_call() {
  const src = code(await read('jar/js/api.js'));
  includes(src, 'getSummaryReports',
    'api.js must expose the UI-026f endpoint wrapper');
  includes(src, '/summary/reports',
    'the wrapper must call the dedicated reports path');
}
