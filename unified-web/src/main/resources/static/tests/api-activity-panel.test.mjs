/**
 * shared/api-activity-panel.js — the "is something still loading right now" panel.
 *
 * Scope, deliberately narrow (see the file's own header comment for the "why"):
 *   - in-flight calls only, no history of completed ones.
 *   - Queue/analysis-job endpoints (/api/queue, /api/parser/analyze,
 *     /api/plsql/analysis/analyze[-fast], and the upload+analyze POSTs to /api/jar/jars and
 *     /api/war/wars) never enter the registry at all — that's the separate job-tracking UI's job.
 *
 * The module is a self-installing IIFE keyed off `window`/`document`/`fetch` globals, so it is run
 * here via `vm` against a tiny fake window rather than imported — there is no export surface to
 * import, the effect IS the global installation.
 */
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import vm from 'node:vm';
import { ok, equal } from './lib/assert.mjs';
import { makeElement } from './lib/dom.mjs';

const STATIC = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const SOURCE = await readFile(path.join(STATIC, 'shared/api-activity-panel.js'), 'utf8');

/** A deferred fetch: resolves only when the test calls `.resolve()`, so a call can be observed mid-flight. */
function makeDeferredFetch() {
  const pending = [];
  const fetchFn = (url, init) => new Promise((resolve) => {
    pending.push({ url, init, resolve: () => resolve({ ok: true, status: 200 }) });
  });
  fetchFn.pending = pending;
  return fetchFn;
}

function makeSandbox(fetchFn) {
  const listeners = { DOMContentLoaded: [] };
  const document = {
    readyState: 'complete',
    body: { appendChild() {} },
    addEventListener(evt, fn) { (listeners[evt] || (listeners[evt] = [])).push(fn); },
    getElementById() { return null; }, // no button/panel DOM needed for registry-level assertions
    createElement() { return makeElement(); }, // for ensureButton()'s one-time bottom-left button
  };
  const window = {
    location: { origin: 'http://localhost', pathname: '/jar/index.html' },
    fetch: fetchFn,
    document,
  };
  window.window = window;
  window.setInterval = () => 0;
  window.clearInterval = () => {};
  const ctx = vm.createContext(window);
  vm.runInContext(SOURCE, ctx, { filename: 'api-activity-panel.js' });
  return window;
}

export async function test_in_flight_call_is_tracked_and_dropped_on_completion() {
  const fetchFn = makeDeferredFetch();
  const win = makeSandbox(fetchFn);

  const p = win.fetch('/api/jar/jars/42/summary');
  equal(win.__apiActivity.inFlight.size, 1, 'call should be registered while pending');

  fetchFn.pending[0].resolve();
  await p;
  equal(win.__apiActivity.inFlight.size, 0, 'call must be dropped the instant it settles');
  ok(!('recent' in win.__apiActivity), 'registry must not keep a completed-call history at all');
}

export async function test_queue_endpoint_is_never_tracked() {
  const fetchFn = makeDeferredFetch();
  const win = makeSandbox(fetchFn);

  const p = win.fetch('/api/queue');
  equal(win.__apiActivity.inFlight.size, 0, '/api/queue must never enter the registry');
  fetchFn.pending[0].resolve();
  await p;
}

export async function test_parser_analyze_endpoint_is_never_tracked() {
  const fetchFn = makeDeferredFetch();
  const win = makeSandbox(fetchFn);

  const p = win.fetch('/api/parser/analyze', { method: 'POST' });
  equal(win.__apiActivity.inFlight.size, 0, '/api/parser/analyze must never enter the registry');
  fetchFn.pending[0].resolve();
  await p;
}

export async function test_plsql_analyze_and_analyze_fast_are_never_tracked() {
  for (const url of ['/api/plsql/analysis/analyze', '/api/plsql/analysis/analyze-fast']) {
    const fetchFn = makeDeferredFetch();
    const win = makeSandbox(fetchFn);
    const p = win.fetch(url, { method: 'POST' });
    equal(win.__apiActivity.inFlight.size, 0, `${url} must never enter the registry`);
    fetchFn.pending[0].resolve();
    await p;
  }
}

export async function test_upload_and_analyze_posts_are_excluded_but_the_list_get_is_not() {
  const fetchFn = makeDeferredFetch();
  const win = makeSandbox(fetchFn);

  const uploadPost = win.fetch('/api/jar/jars', { method: 'POST' });
  equal(win.__apiActivity.inFlight.size, 0, 'POST /api/jar/jars (upload+analyze) must be excluded');
  fetchFn.pending[fetchFn.pending.length - 1].resolve();
  await uploadPost;

  const listGet = win.fetch('/api/jar/jars');
  equal(win.__apiActivity.inFlight.size, 1, 'ordinary GET /api/jar/jars (the list call) must still be tracked');
  fetchFn.pending[fetchFn.pending.length - 1].resolve();
  await listGet;

  const warUploadPost = win.fetch('/api/war/wars', { method: 'POST' });
  equal(win.__apiActivity.inFlight.size, 0, 'POST /api/war/wars (upload+analyze) must be excluded');
  fetchFn.pending[fetchFn.pending.length - 1].resolve();
  await warUploadPost;
}

export async function test_non_api_calls_pass_through_untouched() {
  const fetchFn = makeDeferredFetch();
  const win = makeSandbox(fetchFn);
  const p = win.fetch('/static/app.js');
  equal(win.__apiActivity.inFlight.size, 0, 'non-/api/ URLs are not this panel\'s concern');
  fetchFn.pending[0].resolve();
  await p;
}
