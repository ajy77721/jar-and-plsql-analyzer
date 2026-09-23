/**
 * Regression for the History "Load" 404 error banner (docs/TASK-REGISTRY §273).
 *
 * `PA.autoSelectEntryProc` (and `loadSidebarData`'s equivalent probe loop) deliberately try
 * several *candidate* node ids in a row — `schema.obj.proc`, `schema.obj`, `obj` — and expect most
 * of them to 404 before the real one hits. That is normal, silently-handled control flow: each
 * call is wrapped in its own try/catch and the loop moves on to the next candidate. But
 * `PA.api.getProcDetail` used to call the *shared* `PA.apiFail(res)` on every non-OK response
 * unconditionally, which raises the global "A request returned 404 (nothing loaded)" banner for a
 * miss the caller was always going to recover from — even on an analysis that goes on to load
 * correctly a moment later. Confirmed live: loading any History entry whose entry id needs more
 * than one candidate produced the scary top-of-page banner while the analysis itself loaded fine.
 *
 * The fix threads a `{ silentOn404: true }` flag from each *probing* call site into
 * `getProcDetail`, which then skips `PA.apiFail` for a 404 specifically (any other status, or any
 * non-probing caller that omits the flag, still raises the banner as before).
 */
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { includes, ok } from './lib/assert.mjs';

const STATIC = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (rel) => readFile(path.join(STATIC, rel), 'utf8');

export async function test_getProcDetail_accepts_a_silentOn404_option_and_skips_apiFail_for_a_404() {
    const src = await read('plsql/js/api.js');
    includes(src, 'async getProcDetail(procName, opts)', 'getProcDetail must accept an opts param');
    includes(src, 'silent404 && res.status === 404', 'a 404 must be skippable when the caller marks the probe silent');
}

export async function test_autoSelectEntryProc_probes_are_marked_silent() {
    const src = await read('plsql/js/app.js');
    // Isolate the autoSelectEntryProc function body.
    const start = src.indexOf('PA.autoSelectEntryProc = async function');
    ok(start >= 0, 'PA.autoSelectEntryProc must still exist');
    const end = src.indexOf('\n};', start);
    const body = src.slice(start, end);

    const calls = body.match(/PA\.api\.getProcDetail\([^)]*\)/g) || [];
    ok(calls.length >= 2, 'expected at least the rootId probe and the schema/obj/proc candidate-loop probe');
    for (const call of calls) {
        includes(call, 'silentOn404: true', 'every candidate probe inside autoSelectEntryProc must suppress the 404 banner: ' + call);
    }
}

export async function test_loadSidebarData_candidate_probe_is_marked_silent() {
    const src = await read('plsql/js/app.js');
    const idx = src.indexOf("const idsToTry = [];");
    // The second idsToTry loop (loadSidebarData's) follows the one already covered above; find the
    // occurrence after autoSelectEntryProc's own so this test is pinned to a *different* call site.
    const afterAutoSelect = src.indexOf('PA.autoSelectEntryProc = async function');
    const secondLoopStart = src.indexOf('const idsToTry = [];', afterAutoSelect + 1);
    ok(secondLoopStart >= 0, 'loadSidebarData\'s own candidate-id loop must still exist');
    const loopBody = src.slice(secondLoopStart, secondLoopStart + 800);
    includes(loopBody, 'getProcDetail(entryId, { silentOn404: true })',
        'loadSidebarData\'s candidate probe must also suppress the 404 banner');
}
