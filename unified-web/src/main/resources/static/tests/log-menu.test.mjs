/**
 * The top-bar "Logs" dropdown on the JAR/WAR screens: it must sit next to Sessions, offer the
 * three log categories, and route each to the right existing API — escaping anything the server
 * returns (a run-log or session name is data).
 */
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { ok, equal, includes, excludes } from './lib/assert.mjs';
import { installDom } from './lib/dom.mjs';

const STATIC = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (rel) => readFile(path.join(STATIC, rel), 'utf8');
const tick = () => new Promise((r) => setTimeout(r, 0));

async function loadLogMenu() {
  const d = installDom();
  globalThis.window = globalThis;
  // The module prefers the page's escapers when present; a stray JA/PA from another test file
  // must not change what it renders, so clear them and exercise the module's own escaper.
  globalThis.JA = undefined;
  globalThis.PA = undefined;
  const bar = d.add('topbar');
  bar.appendChild(d.add('sessions-btn'));
  new Function(await read('shared/log-menu.js'))();
  return { d, LM: globalThis.LogMenu };
}

export async function test_logs_control_is_injected_next_to_sessions_with_three_items() {
  const { d, LM } = await loadLogMenu();
  LM.init({});
  ok(d.get('log-menu'), 'the Logs control must be injected next to Sessions');
  includes(d.get('log-menu-btn').innerHTML, 'Logs', 'the button must be labelled Logs');
  const items = d.get('log-menu-dropdown').innerHTML;
  includes(items, 'Application log', 'the app-log item is missing');
  includes(items, 'Analysis logs', 'the analysis-log item is missing');
  includes(items, 'Session chat logs', 'the chat-log item is missing');
}

export async function test_toggle_shows_and_hides_the_dropdown() {
  const { d, LM } = await loadLogMenu();
  LM.init({});
  const dd = d.get('log-menu-dropdown');
  LM.toggle();
  equal(dd.style.display, 'block', 'the dropdown must open on toggle');
  LM.toggle();
  equal(dd.style.display, 'none', 'the dropdown must close on the second toggle');
}

export async function test_analysis_logs_lists_run_logs_and_escapes_names() {
  const { d, LM } = await loadLogMenu();
  LM.init({
    currentId: () => 'JAR1',
    name: (id) => id,
    runLogs: async () => [{ name: 'run_<img src=x onerror=alert(1)>.log', size: 2048 }]
  });
  LM.showAnalysis();
  await tick();
  const list = d.get('log-menu-list');
  includes(list.innerHTML, 'KB', 'the run-log size is missing');
  excludes(list.innerHTML, '<img', 'a hostile run-log name reached the DOM');
  includes(list.innerHTML, '&lt;img', 'the run-log name must render escaped');
  ok(d.get('log-menu-overlay'), 'the overlay must open');
}

export async function test_analysis_log_content_loads() {
  const { d, LM } = await loadLogMenu();
  LM.init({
    currentId: () => 'JAR1',
    runLogs: async () => [{ name: 'run_1.log', size: 10 }],
    getRunLog: async () => 'LINE-A\nLINE-B'
  });
  LM.showAnalysis();
  await tick();
  LM.loadRunLog('run_1.log');
  await tick();
  equal(d.get('log-menu-pre').textContent, 'LINE-A\nLINE-B', 'the selected run log must render');
}

export async function test_session_chat_logs_list_sessions_and_transcript() {
  const { d, LM } = await loadLogMenu();
  LM.init({
    chatSessions: async () => [{ id: 'S1', name: 'Session <b>x</b>', messageCount: 2 }],
    // Newest-first, as getMessages returns them.
    chatMessages: async () => [{ role: 'assistant', content: 'yo' }, { role: 'user', content: 'hi' }]
  });
  LM.showChat();
  await tick();
  const list = d.get('log-menu-list');
  includes(list.innerHTML, 'S1', 'the session id is missing');
  excludes(list.innerHTML, '<b>x</b>', 'a hostile session name reached the DOM');
  LM.loadChat('S1');
  await tick();
  equal(d.get('log-menu-pre').textContent, '[user] hi\n\n[assistant] yo',
    'the transcript must render oldest-first');
}

export async function test_a_missing_source_says_so_instead_of_opening_empty() {
  const { d, LM } = await loadLogMenu();
  LM.init({});   // no chatSessions configured
  LM.showChat();
  await tick();
  includes(d.get('log-menu-body').innerHTML, 'not available',
    'a screen without chat logs must say so');
}

export async function test_logs_menu_is_wired_into_jar_and_war() {
  for (const page of ['jar/index.html', 'war/index.html']) {
    const html = await read(page);
    includes(html, 'log-menu.js', `${page} must load the Logs menu module`);
    includes(html, 'LogMenu.init', `${page} must initialise the Logs menu`);
    includes(html, 'listRunLogs', `${page} must wire the analysis run logs`);
    includes(html, 'ChatAPI.listSessions', `${page} must wire the session chat logs`);
  }
}
