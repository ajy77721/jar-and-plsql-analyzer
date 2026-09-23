/**
 * FE-013 — the Summary must not pull the whole analysis document.
 *
 * `/summary/headers` is ~120 KB. `/summary` is the entire analysis — **47 MB** on the measured jar,
 * and it must be parsed into JS heap before a single number renders. Seven call sites had a
 * `try { headers } catch { full }` fallback, so any hiccup on the small endpoint silently traded a
 * 120 KB request for a 47 MB one, with nothing shown to the user but a slow page.
 */
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { equal, ok, includes } from './lib/assert.mjs';

const STATIC = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (f) => readFile(path.join(STATIC, f), 'utf8');

export async function test_only_one_place_can_fetch_the_whole_document() {
  // The task's definition of done. Every other caller goes through loadSummary().
  const { readdir } = await import('node:fs/promises');
  const files = (await readdir(path.join(STATIC, 'jar/js'))).filter(f => f.endsWith('.js'));
  const sites = [];
  for (const f of files) {
    const src = await read(`jar/js/${f}`);
    for (const m of src.matchAll(/JA\.api\.getSummary\(/g)) {
      sites.push(`${f}:${src.slice(0, m.index).split('\n').length}`);
    }
  }
  equal(sites.length, 1, `whole-document fetches outside the helper: ${sites.join(', ')}`);
  includes(await read('jar/js/api.js'), 'return JA.api.getSummary(id, version);',
    'the single remaining call should be the helper\'s own fallback');
}

export async function test_the_fallback_is_not_silent() {
  // The point of consolidating: a ~400x transfer must not happen without telling anyone.
  const api = await read('jar/js/api.js');
  const helper = api.slice(api.indexOf('async loadSummary('), api.indexOf('async getSummaryHeaders('));
  ok(helper.includes('onDegraded') || helper.includes('toast'),
    'the fallback reports nothing to the user');
  includes(helper, 'console.warn', 'the fallback leaves no trace for a developer either');
}

export async function test_no_call_site_reimplements_the_fallback() {
  // If the pattern reappears, this task has been undone one paste at a time.
  const { readdir } = await import('node:fs/promises');
  const files = (await readdir(path.join(STATIC, 'jar/js'))).filter(f => f.endsWith('.js'));
  for (const f of files) {
    const src = await read(`jar/js/${f}`);
    const reimplemented = /getSummaryHeaders\([^)]*\)[^}]{0,120}catch[^}]{0,120}getSummary\(/s.test(src);
    ok(!reimplemented, `${f} reimplements the headers→full fallback instead of using loadSummary`);
  }
}
