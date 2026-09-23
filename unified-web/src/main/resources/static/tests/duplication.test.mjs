/**
 * UI-011 / UI-015 — what is actually duplicated between the page trees, and what is not.
 *
 * <h2>Both rows' premises were wrong, in opposite directions</h2>
 *
 * `UI-015` asks to "consolidate the three duplicate pagers (parser, plsql, jar)". Measured: there
 * is **one** shared page source (`shared/page-envelope.js`, which all three pages load) and one
 * parser-specific `PA.pager`. Three pagers do not exist.
 *
 * `UI-011` treats all of `plsql/js` as a duplicate framework. Measured across the 18 basenames
 * `jar/js` and `plsql/js` share, **none is byte-identical**, and most are not copies at all:
 * `app.js` is 0.7% similar, `summary-tables.js` 2.9%, `callGraph.js` 3.1%. They are different
 * implementations that happen to share a filename.
 *
 * <h2>The two that are real duplicates</h2>
 *
 * `summary-col-filter.js` and `summary-export-style.js` are the same code twice. The plsql copies
 * say so — "Domain-agnostic — identical to JAR version but uses PA namespace" — and once the
 * namespace and the escaper name are normalised away, the only remaining differences are comments
 * the plsql copies dropped.
 *
 * <h2>Why a test and not a merge</h2>
 *
 * Merging them into `shared/` needs an escaper indirection (`JA.utils.escapeHtml` against
 * `PA.esc`), and these files drive column filtering and Excel export on two pages with no
 * behavioural tests over them. That is real risk for a cosmetic gain.
 *
 * The actual hazard of a duplicate is not that it exists — it is that it **drifts**, so a fix lands
 * in one copy and the other keeps the bug. This file removes that hazard without touching the
 * product: the pair must stay code-identical modulo namespace, and a new shared basename has to be
 * declared. Managed duplication instead of unmanaged.
 */
import { readFile, readdir } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { ok, equal, atLeast } from './lib/assert.mjs';

const STATIC = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (rel) => readFile(path.join(STATIC, rel), 'utf8');

/**
 * Source reduced to the code the two copies are meant to share.
 *
 * Namespace object, escaper name and comments are the three things they are *allowed* to differ
 * in — the first two because the page frameworks genuinely name them differently, the third
 * because the plsql copies were made by stripping doc comments. Anything else is drift.
 */
function normalise(src) {
  return stripComments(src)
    .replace(/\bJA\.utils\.escapeHtml\b/g, 'NS.ESC')
    .replace(/\bPA\.esc\b/g, 'NS.ESC')
    .replace(/\b(JA|PA)\b/g, 'NS')
    .replace(/\s+/g, '');
}

/**
 * Removes block and line comments, including comments that trail code.
 *
 * <p>Scanned rather than matched with a regex, because both shortcuts are wrong here. Stripping
 * only lines that *begin* with `//` left `headerFill: {...},   // strong blue` in place, and the
 * two copies then differed by four trailing comments the plsql side had dropped — a diff that
 * reads as drift and is nothing. Stripping `//.*$` unconditionally would eat the `//` in a
 * `'http://…'` literal and could make two genuinely different files look the same.
 *
 * <p>Quote and template-literal state is tracked; regex literals are not, since neither file
 * contains one and a scanner that handled them would be a parser.
 */
function stripComments(src) {
  let out = '';
  let quote = null;
  for (let i = 0; i < src.length; i++) {
    const c = src[i];
    const next = src[i + 1];
    if (quote) {
      out += c;
      if (c === '\\') {
        out += next === undefined ? '' : next;
        i++;
      } else if (c === quote) {
        quote = null;
      }
      continue;
    }
    if (c === '"' || c === "'" || c === '`') {
      quote = c;
      out += c;
      continue;
    }
    if (c === '/' && next === '/') {
      while (i < src.length && src[i] !== '\n') i++;
      out += '\n';
      continue;
    }
    if (c === '/' && next === '*') {
      i += 2;
      while (i < src.length && !(src[i] === '*' && src[i + 1] === '/')) i++;
      i++;
      continue;
    }
    out += c;
  }
  return out;
}

/** Pairs that are the same code twice, and must stay that way. */
const PARALLEL = ['summary-col-filter.js', 'summary-export-style.js'];

export async function test_the_parallel_copies_have_not_drifted() {
  for (const f of PARALLEL) {
    const a = normalise(await read(`jar/js/${f}`));
    const b = normalise(await read(`plsql/js/${f}`));
    equal(a, b,
      `jar/js/${f} and plsql/js/${f} have drifted apart in code, not just in namespace. `
      + 'One of them has a change the other does not — which is the whole failure mode of a '
      + 'duplicated file. Apply it to both, or consolidate them into shared/.');
  }
  atLeast(PARALLEL.length, 2, 'a parallel pair went missing from this list');
}

export async function test_no_new_basename_is_shared_between_the_page_trees() {
  // A ratchet, in the shape this codebase already uses for DirectFileIoGateTest. The 18 below are
  // not a target — most are distinct implementations and merging them is not the goal. The point
  // is that a *new* shared filename is a decision someone should make deliberately, because a
  // reader seeing two files with one name will reasonably assume they are the same thing.
  const jar = new Set((await readdir(path.join(STATIC, 'jar/js'))).filter((f) => f.endsWith('.js')));
  const plsql = (await readdir(path.join(STATIC, 'plsql/js'))).filter((f) => f.endsWith('.js'));
  const shared = plsql.filter((f) => jar.has(f)).sort();

  ok(shared.length <= 18,
    `${shared.length} basenames are shared between jar/js and plsql/js, measured at 18: `
    + `${shared.join(', ')}`);
}

export async function test_there_is_one_page_source_not_three() {
  // UI-015's premise, asserted so the row cannot be "completed" by building what already exists.
  const roots = ['shared', 'parser/js', 'plsql/js', 'jar/js'];
  const definers = [];
  for (const root of roots) {
    for (const f of await readdir(path.join(STATIC, root))) {
      if (!f.endsWith('.js')) continue;
      const code = normalise(await read(`${root}/${f}`));
      // The definition, not a use: `NS.pageSource=function` after normalisation.
      if (/NS\.pageSource=function/.test(code)) definers.push(`${root}/${f}`);
    }
  }
  equal(definers.length, 1,
    `${definers.length} files define a page source: ${definers.join(', ')} — there should be one, `
    + 'in shared/');
  ok(definers[0].startsWith('shared/'),
    `the page source lives in ${definers[0]}, not in shared/`);
}

export async function test_every_page_loads_the_shared_page_source() {
  // Consolidation only counts if the pages actually use the shared one.
  for (const page of ['parser', 'plsql', 'jar']) {
    const html = await readFile(path.join(STATIC, page, 'index.html'), 'utf8');
    ok(html.includes('page-envelope.js'),
      `${page}/index.html does not load shared/page-envelope.js, so it cannot be using the `
      + 'shared page source');
  }
}
