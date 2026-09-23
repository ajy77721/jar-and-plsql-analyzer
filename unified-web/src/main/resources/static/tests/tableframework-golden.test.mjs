/**
 * FE-009 — one `tableFramework.js`, and both pages still render what they rendered before.
 *
 * The two forks (752 lines parser, 457 plsql, 357 differing) merged into `shared/`. The parser copy
 * was a strict superset by API, but API compatibility is not behavioural compatibility, so this
 * compares rendered markup rather than trusting the reading.
 *
 * The goldens below were captured from the ORIGINAL plsql fork before it was deleted. They are the
 * point of the test: without them, "it renders fine" means only that it renders.
 */
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { equal, ok, includes, excludes } from './lib/assert.mjs';
import { renderWith } from './lib/tf-golden.mjs';

const STATIC = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const SHARED = path.join(STATIC, 'shared/tableFramework.js');

const ROWS = [
  { name: "O'Brien<x>", op: 'SELECT', line: 10, n: 0 },
  { name: 'ERP_PRODUCT', op: 'INSERT', line: 250, n: 42 },
  { name: 'ERP_ORDER', op: 'SELECT', line: 3, n: 7 },
];
const rowFn = (d, i, e) => `<tr><td>${e(d.name)}</td><td>${e(d.op)}</td></tr>`;

/** Captured from plsql/js/tableFramework.js before deletion. */
const PLSQL_GOLDEN_TBODY =
  "<tr><td>O&#39;Brien&lt;x&gt;</td><td>SELECT</td></tr>"
  + "<tr><td>ERP_PRODUCT</td><td>INSERT</td></tr>"
  + "<tr><td>ERP_ORDER</td><td>SELECT</td></tr>";

export async function test_there_is_exactly_one_framework() {
  const { readdir } = await import('node:fs/promises');
  const found = [];
  async function walk(dir) {
    for (const e of await readdir(dir, { withFileTypes: true })) {
      if (e.name === 'node_modules') continue;
      const p = path.join(dir, e.name);
      if (e.isDirectory()) await walk(p);
      else if (e.name === 'tableFramework.js') found.push(path.relative(STATIC, p));
    }
  }
  await walk(STATIC);
  equal(found.length, 1, `expected one framework, found: ${found.join(', ')}`);
  equal(found[0], 'shared/tableFramework.js');
}

export async function test_rows_render_exactly_as_the_plsql_fork_did() {
  globalThis.PA = { tf: undefined };
  const out = await renderWith(SHARED, ROWS, rowFn);
  equal(out.tbody, PLSQL_GOLDEN_TBODY, 'row markup changed when the forks were merged');
}

export async function test_plsql_still_hides_the_pager_on_small_tables() {
  // The one behavioural difference between the forks. plsql suppressed the pager at <= 20 rows;
  // if the merge lost that, every small plsql table grows a pager it never had.
  globalThis.PA = { tf: undefined };
  const plsqlLike = await renderWith(SHARED, ROWS, rowFn, {}, (tf) => { tf.hidePagerBelow = 20; });
  equal(plsqlLike.pager, '', 'a 3-row table on the plsql page must render no pager');
}

export async function test_parser_still_shows_the_pager() {
  globalThis.PA = { tf: undefined };
  const parserLike = await renderWith(SHARED, ROWS, rowFn);   // default hidePagerBelow = 0
  includes(parserLike.pager, '3 items', 'the parser page lost its item count');
  includes(parserLike.pager, 'setPageSize', 'the parser page lost its page-size selector');
}

export async function test_the_server_paging_layer_survived() {
  // The capability plsql never had, and the reason the parser copy was the one promoted. Losing it
  // silently would undo the only reason a grid can show more rows than the browser can hold.
  const { readFile } = await import('node:fs/promises');
  const src = await readFile(SHARED, 'utf8');
  for (const member of ['isServerPaged', 'setServerSort', '_fetch', 'pageCache', 'serverTotal']) {
    includes(src, member, `the server-paging layer lost ${member}`);
  }
}
