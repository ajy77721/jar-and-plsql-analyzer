/** FE-001 smoke test: the home page ships its tool cards and every script it references exists. */
import { readFile, access } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { ok, atLeast, includes } from './lib/assert.mjs';

const STATIC = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');

export async function test_home_page_renders_its_tool_cards() {
  const html = await readFile(path.join(STATIC, 'index.html'), 'utf8');
  // `class="card"`, not `tool-card` — checked against the markup rather than assumed, after the
  // first guess asserted a class this page never had and failed for the wrong reason.
  const cards = html.match(/class="card"/g) || [];
  atLeast(cards.length, 3, 'the home page should offer at least three tool cards');
}

export async function test_every_script_the_home_page_loads_exists() {
  // A 404 script is silent in the browser until something it defined is called — which is exactly
  // how the User Manual shipped broken.
  const html = (await readFile(path.join(STATIC, 'index.html'), 'utf8'))
    .replace(/<!--[\s\S]*?-->/g, '');
  const srcs = [...html.matchAll(/<script[^>]*\ssrc\s*=\s*"([^"]+)"/g)].map(m => m[1].split('?')[0]);
  atLeast(srcs.length, 5, 'expected the home page to load several scripts');
  for (const src of srcs) {
    if (/^https?:/.test(src)) continue;
    const p = src.startsWith('/') ? path.join(STATIC, src.slice(1)) : path.join(STATIC, src);
    await access(p).catch(() => { throw new Error(`index.html loads ${src}, which does not exist`); });
  }
}

export async function test_the_user_manual_has_its_dependency() {
  // The concrete regression: user-manual.js calls SearchQuery, defined in ui-busy.js.
  const html = await readFile(path.join(STATIC, 'index.html'), 'utf8');
  const manual = await readFile(path.join(STATIC, 'shared/user-manual.js'), 'utf8');
  if (manual.includes('SearchQuery')) {
    includes(html, 'ui-busy.js',
      'user-manual.js uses SearchQuery but index.html does not load ui-busy.js, so every use throws');
  }
  ok(true);
}
