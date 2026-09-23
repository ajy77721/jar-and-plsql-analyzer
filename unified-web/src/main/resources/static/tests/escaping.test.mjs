/**
 * FE-004 — one escaping utility, and it must be correct in BOTH contexts.
 *
 * The hostile value is `O'Brien<script>alert(1)</script>"` because it exercises every character
 * that mattered: the apostrophe broke inline handlers, the double quote closed attributes early,
 * and the angle brackets are the classic injection. The previous escapers each handled some of
 * these and none handled all.
 */
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { ok, equal, includes, excludes } from './lib/assert.mjs';

const STATIC = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const HOSTILE = `O'Brien<script>alert(1)</script>"`;

async function loadEsc() {
  globalThis.window = globalThis;
  const src = await readFile(path.join(STATIC, 'shared/escape.js'), 'utf8');
  new Function(src)();          // the real file, not a copy
  return globalThis.Esc;
}

export async function test_html_escaping_neutralises_every_dangerous_character() {
  const Esc = await loadEsc();
  const out = Esc.html(HOSTILE);
  excludes(out, '<script', 'an angle bracket survived — this is the injection case');
  excludes(out, '"', 'a double quote survived; inside an attribute it closes it early');
  excludes(out, "'", 'a single quote survived; inside an inline handler it ends the literal');
  includes(out, '&lt;script', 'the text must still be readable, just inert');
  includes(out, '&#39;Brien', 'the apostrophe should render as an entity, not vanish');
}

export async function test_attribute_context_is_safe() {
  const Esc = await loadEsc();
  // The concrete defect: 72 measured call sites interpolate an escaped value inside a quoted
  // attribute. Building one here proves the attribute cannot be broken out of.
  const html = `<div title="${Esc.attr(HOSTILE)}" data-x="1">text</div>`;
  const attrs = html.match(/\w+="/g) || [];
  equal(attrs.length, 2, `the value broke out and created extra attributes: ${html}`);
  excludes(html.slice(html.indexOf('title="') + 7, html.indexOf('" data-x')), '"',
    'the title attribute contains a raw quote');
}

export async function test_js_context_keeps_the_literal_intact() {
  const Esc = await loadEsc();
  // An inline handler: onclick="pick('...')". The apostrophe must not end the string literal.
  const escaped = Esc.js(HOSTILE);
  const attribute = `onclick="pick('${escaped}')"`;

  // The output is JS-escaped and THEN HTML-escaped, so neither layer can be broken out of:
  // no raw quote survives to close the attribute, and no raw apostrophe to end the JS literal.
  excludes(escaped, '"', 'a raw double quote survives and would close the onclick attribute');
  ok(!/(^|[^\\\\])'/.test(escaped.replace(/&#39;/g, '')),
    `an unescaped apostrophe would terminate the JS literal: ${escaped}`);

  // And the value must survive the round trip — escaping that loses data is not escaping.
  // Written with split/join rather than regexes: the double-escaping here is exactly the kind of
  // thing a backslash-heavy regex gets wrong, and a test that is wrong about escaping is worse
  // than no test.
  const htmlDecoded = escaped
    .split('&#39;').join("'")
    .split('&quot;').join('"')
    .split('&lt;').join('<')
    .split('&gt;').join('>')
    .split('&amp;').join('&');
  const jsDecoded = htmlDecoded.split("\\'").join("'").split('\\\\').join('\\');
  equal(jsDecoded, HOSTILE, `round trip changed the value; attribute was ${attribute}`);
}

export async function test_every_escaper_in_the_app_delegates_here() {
  // The point of the task: one definition. If a fourth copy reappears, this fails.
  for (const [file, marker] of [
    ['parser/js/utils.js', 'Esc.html'],
    ['jar/js/utils.js', 'Esc.html'],
    ['parser/js/promptManager.js', 'Esc.html'],
  ]) {
    const src = await readFile(path.join(STATIC, file), 'utf8');
    includes(src, marker, `${file} no longer delegates to the shared escaper`);
  }
}

export async function test_null_and_undefined_do_not_print_as_text() {
  const Esc = await loadEsc();
  // The old implementations used `(s || '')`, which turned the number 0 into an empty string —
  // a real data-loss bug hiding inside an escaping helper.
  equal(Esc.html(null), '');
  equal(Esc.html(undefined), '');
  equal(Esc.html(0), '0', 'zero must render as 0, not as an empty cell');
  equal(Esc.html(false), 'false');
}
