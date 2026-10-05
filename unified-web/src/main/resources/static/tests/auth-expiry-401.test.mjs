/**
 * An expired AWS session must open the sign-in screen, never a scary error.
 *
 * The contract, asserted against the shipped sources:
 *   1. ONE decision point — page-envelope.js's PA.handleUnauthorized — that every page loads and
 *      the universal fetch wrapper (api-activity-panel.js) also calls.
 *   2. A 401 never shows the red "a request failed" banner: an expired session is a normal event.
 *   3. The home page tries a silent resume first, and only then opens the code + "Open AWS login"
 *      screen with a calm note — not the red error strip the old handler showed.
 */
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { includes, excludes } from './lib/assert.mjs';

const STATIC = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (rel) => readFile(path.join(STATIC, rel), 'utf8');

export async function test_shared_router_routes_sso_401s() {
  const js = await read('shared/page-envelope.js');
  includes(js, 'function handleUnauthorized(res)',
    'page-envelope.js must define the single 401 router');
  includes(js, 'function isSsoGate401(res)',
    'the router must scope itself to the AWS SSO gate, not any 401');
  includes(js, "path.indexOf('/api/admin/') === 0",
    'the admin portal keeps its own 401 — the router must not touch it');
  includes(js, 'PAsafe.handleUnauthorized = handleUnauthorized',
    'the router must be reachable as PA.handleUnauthorized');
  // apiFail routes 401 BEFORE the banner, so the red strip never appears for an expired session.
  includes(js, 'if (handleUnauthorized(res)) {',
    'apiFail must route a 401 to sign-in instead of showing the banner');
}

export async function test_universal_fetch_wrapper_routes_401s() {
  const js = await read('shared/api-activity-panel.js');
  includes(js, 'window.PA.handleUnauthorized(res)',
    'the universal fetch wrapper must route a 401 to the shared handler');
}

export async function test_home_page_resumes_then_prompts_without_an_error() {
  const html = await read('index.html');
  // The 401 handler delegates to the shared router — one decision point.
  includes(html, 'window.PA.handleUnauthorized(res)',
    'awsSsoHandle401 must delegate to the shared router');
  includes(html, 'window.PA._onUnauthorized = function ()',
    'the home page must register its re-auth flow with the shared router');
  // Resume first, prompt only if that fails.
  includes(html, 'async function awsSsoTrySilentResume()',
    'the page must try to resume the session silently before prompting');
  includes(html, 'async function awsSsoOnSessionExpired()',
    'the page must define the expired-session flow');
  // The gentle note, and NOT the red error strip, for an expired session.
  includes(html, 'id="awsSsoExpiredNote"',
    'the modal must carry a calm expired-session note');
  includes(html, 'Your session expired — one click to re-authenticate.',
    'the note must say exactly what the user can do');
  // The old handler showed the raw SSO-gate message as a red error; that must be gone.
  excludes(html, 'awsSsoShowError(1, msg)',
    'an expired session must not be rendered as a red error strip');
}
