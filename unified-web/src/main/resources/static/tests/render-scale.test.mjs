/**
 * Scale gates: what a view renders when the corpus is the size the product is aimed at.
 *
 * These use the 50,000-row fixture rather than today's 15,824, because the failure being guarded
 * against is invisible at current volume. A render whose node count tracks the corpus is fine on a
 * demo schema and unusable on a real one.
 */
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { assertNodeBudget, countTags } from './lib/budget.mjs';
import { fixture } from './lib/fixtures.mjs';
import { renderWith } from './lib/tf-golden.mjs';
import { ok, atLeast } from './lib/assert.mjs';

const STATIC = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const SHARED = path.join(STATIC, 'shared/tableFramework.js');

const rowFn = (d, i, e) =>
  `<tr><td>${e(d.tableName)}</td><td>${e(d.operation)}</td><td>${d.line}</td></tr>`;

export async function test_the_paging_framework_renders_a_page_not_the_corpus() {
  // The property that makes the shared framework safe at scale: 50,000 rows in, one page out.
  const { statements } = await fixture('large');
  atLeast(statements.length, 50000, 'fixture too small to prove anything');

  globalThis.PA = { tf: undefined };
  const out = await renderWith(SHARED, statements, rowFn, { pageSize: 50 });
  assertNodeBudget(out.tbody, 'tr', 50, statements.length, 'PA.tf table');
}

export async function test_the_page_size_is_honoured_at_every_size() {
  const { statements } = await fixture('large');
  for (const size of [25, 100, 500]) {
    globalThis.PA = { tf: undefined };
    const out = await renderWith(SHARED, statements, rowFn, { pageSize: size });
    assertNodeBudget(out.tbody, 'tr', size, statements.length, `PA.tf table @${size}/page`);
  }
  ok(true);
}

export async function test_rendering_a_page_of_fifty_thousand_rows_is_fast() {
  // Not a microbenchmark — a guard against an O(n) or worse pass over the whole array on every
  // render. Generous enough not to be flaky, tight enough that a full-corpus scan would breach it.
  const { statements } = await fixture('large');
  globalThis.PA = { tf: undefined };
  const t0 = performance.now();
  await renderWith(SHARED, statements, rowFn, { pageSize: 50 });
  const ms = performance.now() - t0;
  ok(ms < 2000, `rendering one page of ${statements.length} rows took ${Math.round(ms)}ms`);
}
