/**
 * The accurate progress renderer — the stage list, the per-stage N/M, the current-stage indicator
 * and the ETA. These are the frontend half of the progress engine: the server ships `stages`,
 * `etaMs`, `remainingStages` and `readyToBrowse`, and this is what the queue panels render from.
 *
 * Loaded through `new Function` with the real escaper, the same technique the other screen tests
 * use, so the escaping under test is the escaping the app runs.
 */
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { ok, equal, includes, excludes } from './lib/assert.mjs';

const STATIC = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (rel) => readFile(path.join(STATIC, rel), 'utf8');

async function load() {
    globalThis.window = globalThis;
    globalThis.PA = globalThis.PA || {};
    if (!globalThis.Esc) new Function(await read('shared/escape.js'))();
    PA.esc = (s) => Esc.html(s);
    new Function(await read('shared/progress-stages.js'))();
    return globalThis.PA;
}

function jarJob(overrides) {
    return Object.assign({
        id: 'j1', type: 'JAR_UPLOAD', status: 'RUNNING', progressPercent: 42, etaMs: 125000,
        remainingStages: 4, readyToBrowse: true,
        stages: [
            { id: 'parse-classes', label: 'Parse classes', state: 'COMPLETE', done: 800, total: 800, etaMs: 0 },
            { id: 'config-extract', label: 'Extract config', state: 'COMPLETE', done: 0, total: 0, etaMs: 0 },
            { id: 'call-graph', label: 'Call graph & endpoints', state: 'RUNNING', done: 300, total: 800, etaMs: 60000 },
            { id: 'decompile', label: 'Decompile sources', state: 'PENDING', done: 0, total: 0, etaMs: 30000 }
        ]
    }, overrides || {});
}

export async function test_eta_text_formats_seconds_and_minutes() {
    const PA = await load();
    equal(PA.progressStages.etaText(0), '', 'zero ETA is empty');
    equal(PA.progressStages.etaText(500), '<1s remaining', 'sub-second');
    includes(PA.progressStages.etaText(45000), '~45s remaining', 'seconds');
    includes(PA.progressStages.etaText(125000), '~3 min remaining', 'minutes (rounded up)');
    includes(PA.progressStages.etaText(3 * 3600 * 1000), '~3h', 'hours');
}

export async function test_render_shows_every_stage_with_counts_and_current_indicator() {
    const PA = await load();
    const html = PA.progressStages.render(jarJob(), {});
    includes(html, 'Parse classes', 'a completed stage is listed');
    includes(html, 'Call graph &amp; endpoints', 'the running stage is listed (escaped label)');
    includes(html, '800/800', 'a sized stage shows its N/M count');
    includes(html, '300/800', 'the running stage shows live N/M progress');
    includes(html, 'data-state="RUNNING"', 'the current-stage indicator is present');
    includes(html, 'Stage 2/4', 'the stage summary counts completed stages');
    includes(html, '~3 min remaining', 'the total ETA is shown');
    includes(html, 'ready to browse', 'the honest early-access hint is shown while stages remain');
}

export async function test_render_is_empty_without_stages() {
    const PA = await load();
    equal(PA.progressStages.render({ id: 'x', status: 'RUNNING' }, {}), '',
        'a job with no modelled stages renders nothing (legacy jobs unaffected)');
}

export async function test_render_escapes_hostile_stage_labels() {
    const PA = await load();
    const job = jarJob({ stages: [
        { id: 'x', label: '<img src=x onerror=alert(1)>', state: 'RUNNING', done: 0, total: 0, etaMs: 0 }
    ] });
    const html = PA.progressStages.render(job, {});
    excludes(html, '<img', 'a hostile label must not be emitted raw');
    includes(html, '&lt;img', 'the label is escaped');
}

export async function test_summary_hides_eta_when_not_running() {
    const PA = await load();
    const done = jarJob({ status: 'COMPLETE', etaMs: 0, remainingStages: 0, readyToBrowse: true });
    const sum = PA.progressStages.summary(done);
    excludes(sum, 'remaining', 'a complete job shows no remaining time');
    excludes(sum, 'ready to browse', 'a complete job shows no polishing hint');
}
