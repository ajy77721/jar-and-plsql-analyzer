/** FE-028 / FE-029 — the small correctness defects, each pinned by the behaviour it broke. */
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { ok, equal, includes, excludes } from './lib/assert.mjs';

const STATIC = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (f) => readFile(path.join(STATIC, f), 'utf8');

export async function test_export_caches_are_reset_when_the_analysis_changes() {
  // PA.export._procedures/_tables are module-level. Cleared only after a successful export, they
  // survived an analysis switch and showed analysis A's counts while B was loaded.
  const app = await read('plsql/js/app.js');
  const reset = app.indexOf('PA.export._procedures = null');
  ok(reset > 0, 'the export caches are not reset on analysis switch');
  const flags = app.indexOf('PA.joins._loaded = false');
  ok(flags > 0 && Math.abs(reset - flags) < 800,
    'the export reset should sit with the other per-analysis resets, so the next one added is seen');
}

export async function test_a_failed_load_does_not_report_success() {
  // Promise.allSettled never rejects, so every load produced "Loaded: …" regardless.
  const app = await read('plsql/js/app.js');
  includes(app, "r.status === 'rejected'",
    'nothing inspects the settled results, so failures still show a success toast');
  const guard = app.indexOf("r.status === 'rejected'");
  const toast = app.indexOf("PA.toast('Loaded: '");
  ok(guard > 0 && toast > guard, 'the success toast must be guarded by the failure check');
}

export async function test_the_targeted_poll_uses_its_intended_interval() {
  // `every(key, fn)` takes two arguments; a third was being passed and silently dropped, so a
  // deliberate 2s poll ran at jobStatusMs (10s).
  const poll = await read('shared/poll-config.js');
  includes(poll, 'targetedJobMs', 'the fast interval has no named key');
  const targeted = await read('jar/js/targeted.js');
  includes(targeted, "PollConfig.every('targetedJobMs'", 'targeted.js still polls on the slow key');
  excludes(targeted, '}, 2000);', 'the discarded third argument is still there');
}

export async function test_no_control_is_wired_to_a_no_op() {
  // A button that does nothing is worse than an absent one: the user cannot tell broken from
  // unsupported.
  const html = await read('plsql/index.html');
  excludes(html, 'PA.trace.expandAll', 'a button is still wired to a removed no-op');
  excludes(html, 'PA.trace.collapseAll', 'a button is still wired to a removed no-op');
  const app = await read('plsql/js/app.js');
  excludes(app, 'flat view — no-op', 'the no-op implementations are still present');
}

export async function test_the_working_expand_controls_are_untouched() {
  // The call-graph view has real implementations; removing the flat view's no-ops must not have
  // taken those with them.
  const html = await read('plsql/index.html');
  includes(html, 'PA.callTrace.expandAll', 'the working expand control was removed by mistake');
  const cg = await read('plsql/js/callGraph.js');
  includes(cg, 'expandAll()', 'callGraph lost its real implementation');
}
