/**
 * UI-026g/§76 — `JA.summary._reviveSets`, a real regression caught mid-conversion, not by any
 * source-text check: JSON has no `Set` type, so every field `_analyzeEndpoint`/
 * `_buildVerticalisation`/`_buildExternalDeps` built as a `Set` — read everywhere downstream with
 * `.has()`/`.size`/`[...x]` — arrives from `/summary/reports` as a plain array. Every renderer in
 * `summary-tables.js`/`summary-popups.js`/`summary-export*.js` was written against the `Set` shape
 * and left unchanged by UI-026g/h, so the server path silently breaks them without this revival
 * step. Executed for real (not just checked as source text), since a shape bug like this is
 * exactly what a text-only check cannot see.
 */
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { ok, equal } from './lib/assert.mjs';

const STATIC = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (rel) => readFile(path.join(STATIC, rel), 'utf8');

async function loadSummary() {
  globalThis.window = globalThis;
  globalThis.JA = globalThis.JA || {};
  new Function(await read('jar/js/summary.js'))();
  return globalThis.JA.summary;
}

export async function test_per_collection_set_fields_are_revived_on_every_epReport() {
  const summary = await loadSummary();
  const data = {
    epReports: [{
      collections: {
        ORDERS: { operations: ['READ', 'WRITE'], sources: ['OrderRepo'], detectedVia: ['REPOSITORY_MAPPING'], type: 'COLLECTION' }
      }
    }]
  };
  summary._reviveSets(data);
  const c = data.epReports[0].collections.ORDERS;
  ok(c.operations instanceof Set, 'operations must become a real Set');
  ok(c.operations.has('WRITE'), 'the revived Set must contain the original values');
  ok(c.sources instanceof Set, 'sources must become a real Set');
  ok(c.detectedVia instanceof Set, 'detectedVia must become a real Set');
  equal(c.type, 'COLLECTION', 'fields not in _SET_FIELDS must be left exactly as they arrived');
}

export async function test_vertReport_rows_get_all_nine_set_fields_revived() {
  const summary = await loadSummary();
  const data = {
    vertReport: [{
      name: 'ORDERS', domain: 'Orders', usageCount: 3,
      endpoints: ['A.x', 'B.y'], operations: ['READ'], sources: ['S1'], detectedVia: ['X'],
      procNames: ['P1'], readOps: ['READ'], writeOps: [], aggOps: [], domainSet: ['Orders']
    }]
  };
  summary._reviveSets(data);
  const row = data.vertReport[0];
  for (const f of summary._SET_FIELDS.vertRow) {
    ok(row[f] instanceof Set, `vertReport.${f} must become a real Set`);
  }
  equal(row.usageCount, 3, 'usageCount (not a Set field) must be untouched');
  equal(row.name, 'ORDERS', 'name (not a Set field) must be untouched');
}

export async function test_extReport_crossModule_rows_get_their_three_set_fields_revived() {
  const summary = await loadSummary();
  const data = {
    extReport: {
      crossModule: [{ jar: 'billing.jar', classes: ['BillClient'], endpoints: ['A.x'], methods: ['BillClient.charge'], count: 5 }]
    }
  };
  summary._reviveSets(data);
  const row = data.extReport.crossModule[0];
  ok(row.classes instanceof Set, 'classes must become a real Set');
  ok(row.endpoints instanceof Set, 'endpoints must become a real Set');
  ok(row.methods instanceof Set, 'methods must become a real Set');
  equal(row.count, 5, 'count (not a Set field) must be untouched');
}

export async function test_missing_sections_do_not_throw() {
  const summary = await loadSummary();
  // A response with no epReports/vertReport/extReport at all — e.g. an error shape — must not
  // throw partway through reviving the sections that ARE present.
  summary._reviveSets({});
  summary._reviveSets({ epReports: [{ collections: null }] });
  ok(true, 'reviveSets must tolerate a response missing any of its sections');
}

export async function test_a_field_already_revived_or_absent_is_left_alone() {
  const summary = await loadSummary();
  const already = new Set(['x']);
  const data = { vertReport: [{ endpoints: already }] };
  summary._reviveSets(data);
  ok(data.vertReport[0].endpoints === already,
    'an already-a-Set field must not be re-wrapped (Array.isArray guards this)');
}
