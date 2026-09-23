/**
 * Renders a table through a real tableFramework.js and returns the markup.
 *
 * Used to prove the de-fork changed nothing: the same rows, through the old plsql fork and the
 * promoted shared version, must produce identical HTML. Comparing implementations by reading them
 * would not have caught a behavioural difference; comparing their output does.
 */
import { readFile } from 'node:fs/promises';
import { installDom, esc } from './dom.mjs';

export async function renderWith(frameworkPath, rows, rowFn, opts = {}, configure) {
  const dom = installDom();
  dom.add('t-tbody');
  dom.add('t-pager');
  dom.add('t-shown');

  globalThis.PA = globalThis.PA || {};
  globalThis.PA.esc = esc;
  globalThis.PA.escAttr = esc;
  globalThis.PA.escJs = (s) => String(s == null ? '' : s).replace(/'/g, "\\'");
  globalThis.JA = globalThis.JA || {};

  const src = await readFile(frameworkPath, 'utf8');
  new Function(src)();

  const tf = globalThis.PA.tf;
  // Page-level configuration is applied before init, exactly as a page's startup code does.
  if (configure) configure(tf);
  tf.init('t', rows, opts.pageSize || 50, rowFn, opts);
  tf.render('t');
  return {
    tbody: dom.get('t-tbody').innerHTML,
    pager: dom.get('t-pager').innerHTML,
    state: tf.state ? tf.state('t') : null,
  };
}
