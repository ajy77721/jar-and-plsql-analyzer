/**
 * The shared page-size policy: default 10, selector 10/25/50/100/250/All, and the remembered choice
 * in localStorage 'ampPageSize' — including on the server-paged tables, where the size is the
 * `limit` sent to the server.
 */
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { ok, equal, includes, excludes } from './lib/assert.mjs';
import { installDom } from './lib/dom.mjs';

const STATIC = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (rel) => readFile(path.join(STATIC, rel), 'utf8');

function installStorage(initial) {
  const m = Object.assign({}, initial || {});
  globalThis.localStorage = {
    getItem: (k) => (Object.prototype.hasOwnProperty.call(m, k) ? m[k] : null),
    setItem: (k, v) => { m[k] = String(v); },
    removeItem: (k) => { delete m[k]; },
    clear: () => { for (const k of Object.keys(m)) delete m[k]; }
  };
  return m;
}

async function loadFramework() {
  installDom();
  globalThis.window = globalThis;
  globalThis.PA = { esc: (v) => String(v == null ? '' : v), escAttr: (v) => String(v), escJs: (v) => String(v) };
  new Function(await read('shared/tableFramework.js'))();
  return globalThis.PA;
}

const ROWS = [{ name: 'A' }, { name: 'B' }, { name: 'C' }];
const rowFn = (d, i, e) => '<tr><td>' + e(d.name) + '</td></tr>';

export async function test_page_size_options_and_default() {
  installStorage();
  const PA = await loadFramework();
  equal(PA.pageSize.DEFAULT, 10, 'the first-time default must be 10');
  equal(JSON.stringify(PA.pageSize.OPTIONS), JSON.stringify([10, 25, 50, 100, 250]),
    'the numeric options must be 10/25/50/100/250');
  equal(PA.pageSize.resolve(false), 10, 'with nothing remembered, the size is 10');
  delete globalThis.localStorage;
}

export async function test_selector_offers_all_and_drops_20() {
  installStorage();
  const PA = await loadFramework();
  const client = PA.pageSize.optionsHtml(10, false);
  for (const n of [10, 25, 50, 100, 250]) includes(client, '>' + n + '/page<', `${n}/page must be offered`);
  includes(client, '>All<', 'the client selector must offer All');
  excludes(client, '20/page', 'the 20 option must be dropped');
  const server = PA.pageSize.optionsHtml(10, true);
  excludes(server, '>All<', 'a server-paged table must not offer All');
  includes(server, '>250/page<', 'a server-paged table still offers up to 250');
  delete globalThis.localStorage;
}

export async function test_the_choice_is_remembered() {
  installStorage();
  const PA = await loadFramework();
  PA.pageSize.set(100);
  equal(globalThis.localStorage.getItem('ampPageSize'), '100', 'the choice must persist under ampPageSize');
  equal(PA.pageSize.resolve(false), 100, 'the remembered size must be used');
  PA.pageSize.set('all');
  equal(PA.pageSize.resolve(false), 1000000, 'All resolves to the whole set on a client table');
  equal(PA.pageSize.resolve(true), 250, 'All is capped to 250 on a server-paged table');
  delete globalThis.localStorage;
}

export async function test_tables_open_at_the_remembered_size_else_ten() {
  installStorage();
  const PA = await loadFramework();
  const a = PA.tf.init('t1', ROWS, 50, rowFn);   // caller says 50; policy says 10 first time
  equal(a.pageSize, 10, 'a table must open at the default 10, not the caller\'s 50');
  installStorage({ ampPageSize: '250' });
  const b = PA.tf.init('t2', ROWS, 50, rowFn);
  equal(b.pageSize, 250, 'a table must open at the remembered size');
  delete globalThis.localStorage;
}

export async function test_server_paged_table_honors_the_policy_as_its_limit() {
  installStorage({ ampPageSize: '100' });
  const PA = await loadFramework();
  PA.isPageSource = (d) => !!(d && d.__pageSource);
  const src = { __pageSource: true, reset() {}, fetchPage() { return Promise.resolve({ items: [], page: { hasMore: false, total: 0, totalIsExact: true } }); } };
  const s = PA.tf.init('sp', src, 50, rowFn);
  equal(s.pageSize, 100, 'the server-paged table must send the remembered size as its limit');
  equal(PA.tf.isServerPaged('sp'), true, 'the table must be recognised as server-paged');
  delete globalThis.localStorage;
}

export async function test_set_page_size_persists_and_all_expands() {
  installStorage();
  const PA = await loadFramework();
  const s = PA.tf.init('t3', ROWS, 10, rowFn);
  PA.tf.setPageSize('t3', '250');
  equal(s.pageSize, 250, 'setPageSize must apply the chosen size');
  equal(globalThis.localStorage.getItem('ampPageSize'), '250', 'setPageSize must remember the choice');
  PA.tf.setPageSize('t3', 'all');
  equal(s.pageSize, 3, 'All expands a client table to its row count');
  equal(globalThis.localStorage.getItem('ampPageSize'), 'all', 'All is remembered too');
  delete globalThis.localStorage;
}

export async function test_the_legacy_pager_follows_the_same_policy() {
  installStorage();
  const PA = await loadFramework();
  new Function(await read('parser/js/pagination.js'))();
  equal(PA.pager.defaultSize, 10, 'the legacy pager default must be 10');
  equal(PA.pager.get('k').size, 10, 'a fresh legacy pager state must use the default 10');
  installStorage({ ampPageSize: '250' });
  PA.pager._state = {};
  equal(PA.pager.get('k2').size, 250, 'the legacy pager must honor the remembered size');
  delete globalThis.localStorage;
}
