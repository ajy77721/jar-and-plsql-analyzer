/**
 * The transaction FLOW (runs) view — the UI over the backend's interprocedural transaction
 * resolver (`/txn-runs/{nodeId}`): one timeline per execution path (guarded conditional
 * commits/rollbacks enumerate paths), the resolver's own plain-English outcome sentence, and a
 * per-write survival chip (survived / discarded / open) — the "what survives the later
 * ROLLBACK" answer the per-proc statement views could not give.
 */
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { equal, ok, includes } from './lib/assert.mjs';
import { installDom, esc } from './lib/dom.mjs';

const STATIC = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (p) => readFile(path.join(STATIC, p), 'utf8');

const RUNS_DOC = {
  entry: 'T1', dialect: 'ORACLE', pathCountExceeded: false,
  runs: [
    {
      runId: 1, kind: 'main', outcome: 'PARTIAL',
      assumptions: [{ nodeId: 'CHILD', line: 30, condition: "v_mode = 'FULL'", taken: true }],
      events: [
        { seq: 1, kind: 'WRITE', nodeId: 'T1', proc: 'ERP_T1', depth: 0, line: 12, table: 'ORDERS', op: 'UPDATE', survival: 'survived' },
        { seq: 2, kind: 'CALL', nodeId: 'CHILD', proc: 'ERP_CHILD', depth: 1, line: 20 },
        { seq: 3, kind: 'WRITE', nodeId: 'CHILD', proc: 'ERP_CHILD', depth: 1, line: 22, table: 'ORDER_LINES', op: 'UPDATE', survival: 'survived' },
        { seq: 4, kind: 'COMMIT', nodeId: 'CHILD', proc: 'ERP_CHILD', depth: 1, line: 30,
          guard: { kind: 'IF', line: 29, condition: "v_mode = 'FULL'" } },
        { seq: 5, kind: 'WRITE', nodeId: 'T1', proc: 'ERP_T1', depth: 0, line: 40, table: 'ORDERS', op: 'DELETE', survival: 'discarded' },
        { seq: 6, kind: 'ROLLBACK', nodeId: 'T1', proc: 'ERP_T1', depth: 0, line: 50 },
        { seq: 7, kind: 'COMMIT', nodeId: 'T1', proc: 'ERP_T1', depth: 0, line: 60 }
      ],
      writes: [],
      summary: "On this path the work is PARTIALLY saved: the ROLLBACK at line 50 (in ERP_T1) undoes the uncommitted work up to that point; the COMMIT at line 30 (in ERP_CHILD) makes the work before it permanent. Writes: 2 survive, 1 discarded."
    }
  ]
};

async function loadTxnTrace(runsDoc) {
  const dom = installDom();
  dom.add('txnContainerTrace');
  globalThis.window = globalThis;
  globalThis.PA = {
    esc, escJs: (s) => String(s == null ? '' : s).replace(/'/g, "\\'"),
    escAttr: esc,
    api: { _analysisName: 'ORACLE~DEMO~LOCAL~T1' },
    context: { procId: 'T1', callTreeRootId: 'T1' },
    analysisData: { transactionStatements: [] },
    isInScope: () => true,
    sourceView: {},
    callTrace: null,
  };
  globalThis.fetch = async () => ({ ok: true, json: async () => runsDoc });
  new Function(await read('parser/js/txnTrace.js'))();
  return dom;
}

export async function test_runs_view_renders_the_runtime_timeline_with_survival() {
  const dom = await loadTxnTrace(RUNS_DOC);
  const c = dom.get('txnContainerTrace');
  PA.txnTrace.setView('runs');
  PA.txnTrace.render();
  await new Promise((r) => setTimeout(r, 5));   // the fetch resolves

  const html = c.innerHTML;
  includes(html, 'execution path', 'the runs view did not render');
  includes(html, 'PARTIALLY saved', 'the resolver’s own outcome sentence must be shown, not a fixed template');
  includes(html, 'v_mode = ', 'the guard assumption must be stated (escaped — DB-sourced text)');
  includes(html, '&#39;FULL&#39;', 'the guard condition is escaped like any DB-sourced text');
  ok((html.match(/txn-survival/g) || []).length >= 3, 'every write carries a survival chip');
  includes(html, 'discarded', 'the write undone by the rollback is marked discarded');
  includes(html, 'survived', 'the writes the callee’s commit persisted are marked survived');
  includes(html, 'CALLS', 'the cross-procedure call edge appears on the timeline');
  includes(html, 'COMMIT', 'commit points appear on the timeline');
}

export async function test_runs_view_needs_a_rooted_procedure() {
  const dom = await loadTxnTrace(RUNS_DOC);
  const c = dom.get('txnContainerTrace');
  PA.context.procId = '';
  PA.context.callTreeRootId = '';
  PA.txnTrace.setView('runs');
  PA.txnTrace.render();
  await new Promise((r) => setTimeout(r, 5));
  includes(c.innerHTML, 'rooted procedure',
    'the runs view silently rendering the whole analysis would misstate the semantics — it needs an entry');
}

export async function test_statements_facet_gives_transaction_types_their_own_family() {
  const src = (await read('parser/js/statements.js')).replace(/\/\*[\s\S]*?\*\//g, '').replace(/^\s*\/\/.*$/gm, '');
  includes(src, "COMMIT: 'txn'", 'COMMIT must join the txn pill family');
  includes(src, 'BEGIN_TRAN', 'the SQL Server begin-transaction type must be classified');
  includes(src, "COMMIT: 'var(--badge-green)'", 'COMMIT must render green (persists)');
  includes(src, "ROLLBACK: 'var(--badge-red)'", 'ROLLBACK must render red (discards)');
}

export async function test_source_view_marks_transaction_lines() {
  const src = (await read('parser/js/sourceView.js')).replace(/\/\*[\s\S]*?\*\//g, '').replace(/^\s*\/\/.*$/gm, '');
  includes(src, '_txnLineMap', 'the source view has no transaction-line map');
  includes(src, 'src-txn-mark', 'the gutter badge is not emitted');
  includes(src, 'detail.transactionScope', 'the txn lines must come from the loaded node’s transactionScope');
}
