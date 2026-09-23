/**
 * UI-013 §60/§62 — `parser/js/claudeEnrichment.js`'s coverage-overview table no longer folds
 * `result.tables`'s full `claudeVerifications` arrays into per-table counts client-side; it reads
 * the counts from a dedicated summary endpoint, mirroring `ClaudeController.summarizeTables`.
 */
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { includes, excludes } from './lib/assert.mjs';

const STATIC = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (rel) => readFile(path.join(STATIC, rel), 'utf8');

const code = (src) =>
  src.replace(/\/\*[\s\S]*?\*\//g, '').replace(/^\s*\/\/.*$/gm, '');

export async function test_api_has_the_dedicated_table_summary_call() {
  const src = code(await read('parser/js/api.js'));
  includes(src, 'claudeVerificationSummaryTables',
    'api.js must expose the per-table summary endpoint wrapper');
  includes(src, '/claude/verification-summary/tables',
    'the wrapper must call the dedicated summary path, not /claude/result');
}

export async function test_overview_no_longer_folds_claudeVerifications_itself() {
  const src = code(await read('parser/js/claudeEnrichment.js'));
  excludes(src, 'vfs.length === 0',
    'the per-table fold (confirmed/removed/newOps/accepted/rejected/pending tallying) moved '
    + 'server-side; this file must not still loop over claudeVerifications to build it');
  includes(src, 'PA.api.claudeVerificationSummaryTables',
    '_renderOverviewBody\'s rows must come from the dedicated summary call');
}

export async function test_render_overview_body_takes_tableRows_as_a_parameter() {
  const src = code(await read('parser/js/claudeEnrichment.js'));
  includes(src, '_renderOverviewBody: function(body, result, analysisName, tableRows)',
    '_renderOverviewBody must receive the already-summarized rows, not the whole result');
}
