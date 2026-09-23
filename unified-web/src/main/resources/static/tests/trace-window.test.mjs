/**
 * FE-016 — the Trace view must render a window, not the whole call tree.
 *
 * Two defects, both invisible on a small trace:
 *   1. every flattened node was concatenated into one string and assigned to `innerHTML`;
 *   2. `Math.max(...nodes.map(...))` spreads the array into a call, which throws RangeError once
 *      the array is large enough — a hard failure on exactly the deep traces this view is for.
 */
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { ok, equal, includes, excludes, atLeast } from './lib/assert.mjs';

const STATIC = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');

export async function test_no_view_spreads_an_array_into_math_max() {
  // Spreading passes one argument per element; a large enough array overflows the call stack.
  // Checked across every page, not just the trace: the first fix found two more instances of the
  // same pattern elsewhere in the same file, which a narrower assertion would have left in place.
  const { readdir } = await import('node:fs/promises');
  const roots = ['plsql/js', 'parser/js', 'jar/js', 'shared'];
  const offenders = [];
  for (const root of roots) {
    for (const f of await readdir(path.join(STATIC, root))) {
      if (!f.endsWith('.js') || f.includes('xlsx')) continue;
      const src = await readFile(path.join(STATIC, root, f), 'utf8');
      // Ignore comments: the fix's own explanation naturally names the pattern it removed.
      const code = src.replace(/\/\*[\s\S]*?\*\//g, '').replace(/^\s*\/\/.*$/gm, '');
      if (/Math\.(max|min)\(\.\.\./.test(code)) offenders.push(`${root}/${f}`);
    }
  }
  equal(offenders.length, 0, `these spread an array into Math.max/min: ${offenders.join(', ')}`);
}

export async function test_the_render_is_bounded_by_a_window() {
  const src = await readFile(path.join(STATIC, 'plsql/js/app.js'), 'utf8');
  const render = src.slice(src.indexOf('    render() {\n        const container = document.getElementById(\'traceContainer\')'));
  includes(render, 'Math.min(nodes.length, PA.trace._shown)',
    'the loop is not bounded by the window');
  excludes(render.slice(0, render.indexOf('container.innerHTML')), 'i < nodes.length;',
    'the row loop still runs over every node');
}

export async function test_truncation_is_visible_to_the_user() {
  // A view that silently stops is a wrong answer: the reader cannot tell "the trace ends here"
  // from "the trace was cut here".
  const src = await readFile(path.join(STATIC, 'plsql/js/app.js'), 'utf8');
  includes(src, 'Showing ', 'nothing tells the user the view is truncated');
  includes(src, 'PA.trace.showMore()', 'there is no way to see the rest');
}

export async function test_a_new_trace_resets_the_window() {
  // Otherwise opening a large trace, expanding it, then opening a small one leaves the window
  // wide — and the next large trace renders unbounded again.
  const src = await readFile(path.join(STATIC, 'plsql/js/app.js'), 'utf8');
  const from = src.indexOf('renderFromTree(tree) {');
  const body = src.slice(from, from + 300);
  includes(body, '_shown = PA.trace.WINDOW', 'the window is not reset for a new trace');
}

export async function test_the_window_is_a_sane_size() {
  const src = await readFile(path.join(STATIC, 'plsql/js/app.js'), 'utf8');
  const m = src.match(/WINDOW:\s*(\d+)/);
  ok(m, 'no WINDOW constant');
  const n = Number(m[1]);
  atLeast(n, 100, 'too small a window makes the view useless');
  ok(n <= 2000, `a window of ${n} defeats the purpose of windowing`);
}
