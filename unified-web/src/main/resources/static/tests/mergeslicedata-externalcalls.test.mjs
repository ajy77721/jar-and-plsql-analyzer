/**
 * §278 — a real regression in `JA.summary._mergeSliceData`'s `externalCalls` branch, caught by
 * executing the code (not by reading it): "95 Cross-Module" on the stat card was real (built from
 * `externalScopeCalls`, a lightweight int the endpoint list already carries), but the External
 * Dependencies sub-tab under it always said "No cross-module dependencies detected".
 *
 * Root cause: `_epReports` (from `/summary/reports`) and `analysis.endpoints` (from the main
 * analysis fetch) are two independently JSON-parsed object graphs — `r.endpoint` on a report row is
 * NEVER the same object as the `eps` entry `_mergeSliceData` mutates just above with the real
 * `externalCalls` array. Reading `r.endpoint.externalCalls` after that mutation therefore always
 * read an object nothing had written to, and `_buildExternalDeps(this._epReports)` always saw empty
 * arrays. Made worse by `r.endpoint` also being a stripped-down copy (only `controllerClass`/
 * `methodName`, no `httpMethod`/`fullPath`/`endpointId`), so even keying off it directly couldn't
 * reproduce the real identity key.
 *
 * The fix: recompute the same composite key `JA.utils.endpointKey` falls back to (httpMethod +
 * fullPath + controllerClass#methodName) from `r`'s own top-level fields plus `r.endpoint`'s two
 * fields, and look the real, mutated `ep` up in `byKey` — the exact map the slice-merge loop above
 * already built. This test drives `_mergeSliceData` for real, with two distinct object graphs for
 * `_epReports` vs `_analysis.endpoints` (mirroring the real two-fetch shape), so it fails against
 * the old `r.endpoint.externalCalls` read and passes against the `byKey` lookup.
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
  new Function(await read('jar/js/utils.js'))();
  new Function(await read('jar/js/summary.js'))();
  // `_mergeSliceData`'s externalCalls branch calls `_buildExternalDeps`, which lives in
  // summary-analyzer.js — stub it so this test exercises only the join, not that builder.
  globalThis.JA.summary._buildExternalDeps = (epReports) => ({ __built: true, epReports });
  return globalThis.JA.summary;
}

function buildFixture() {
  // Two SEPARATE object graphs for the same two endpoints — exactly what two independent JSON
  // fetches (`/summary/reports` vs the main analysis payload) produce. Deliberately not sharing
  // any object references, so a bug that relies on reference identity (the real one) reproduces.
  const analysisEndpoints = [
    {
      httpMethod: 'POST', fullPath: '/api/war/wars',
      controllerClass: 'com.waranalyzer.controller.WarAnalyzerController',
      methodName: 'uploadAndAnalyze'
    },
    {
      httpMethod: 'GET', fullPath: '/api/jar/jars',
      controllerClass: 'com.jaranalyzer.controller.AnalyzerController',
      methodName: 'listJars'
    }
  ];
  const epReports = [
    {
      httpMethod: 'POST', fullPath: '/api/war/wars',
      endpoint: { controllerClass: 'com.waranalyzer.controller.WarAnalyzerController', methodName: 'uploadAndAnalyze' },
      externalCalls: [], httpCalls: [], externalScopeCalls: 0
    },
    {
      httpMethod: 'GET', fullPath: '/api/jar/jars',
      endpoint: { controllerClass: 'com.jaranalyzer.controller.AnalyzerController', methodName: 'listJars' },
      externalCalls: [], httpCalls: [], externalScopeCalls: 0
    }
  ];
  return { analysisEndpoints, epReports };
}

export async function test_external_calls_join_to_the_right_epReport_across_two_separate_object_graphs() {
  const summary = await loadSummary();
  const { analysisEndpoints, epReports } = buildFixture();
  summary._analysis = { endpoints: analysisEndpoints };
  summary._epReports = epReports;

  const sliceArray = [
    {
      endpointId: undefined, httpMethod: 'POST', fullPath: '/api/war/wars',
      controllerClass: 'com.waranalyzer.controller.WarAnalyzerController', methodName: 'uploadAndAnalyze',
      externalCalls: [{ className: 'com.plsqlanalyzer.config.PlsqlConfig', simpleClassName: 'PlsqlConfig',
        methodName: 'resolveEnvironment', sourceJar: 'plsql-config-1.0.0-SNAPSHOT.jar', module: 'Plsql Config' }],
      httpCalls: []
    },
    {
      httpMethod: 'GET', fullPath: '/api/jar/jars',
      controllerClass: 'com.jaranalyzer.controller.AnalyzerController', methodName: 'listJars',
      externalCalls: [], httpCalls: []
    }
  ];

  summary._mergeSliceData(sliceArray, 'externalCalls');

  const uploadReport = summary._epReports[0];
  equal(uploadReport.externalScopeCalls, 1,
    'the endpoint with a real external call must show a nonzero externalScopeCalls after the merge');
  ok(uploadReport.externalCalls.length === 1, 'the report row must carry the actual external call, not an empty array');
  equal(uploadReport.externalCalls[0].sourceJar, 'plsql-config-1.0.0-SNAPSHOT.jar',
    'the merged call must be the real one, not a default/blank stand-in');

  const listJarsReport = summary._epReports[1];
  equal(listJarsReport.externalScopeCalls, 0, 'an endpoint with no external calls must stay at zero');

  ok(summary._extReport && summary._extReport.__built, '_extReport must be rebuilt from the now-populated epReports');
}

export async function test_an_endpoint_absent_from_the_slice_is_left_untouched() {
  const summary = await loadSummary();
  const { analysisEndpoints, epReports } = buildFixture();
  summary._analysis = { endpoints: analysisEndpoints };
  summary._epReports = epReports;

  // Slice only covers the first endpoint.
  summary._mergeSliceData([{
    httpMethod: 'POST', fullPath: '/api/war/wars',
    controllerClass: 'com.waranalyzer.controller.WarAnalyzerController', methodName: 'uploadAndAnalyze',
    externalCalls: [{ sourceJar: 'x.jar' }], httpCalls: []
  }], 'externalCalls');

  equal(summary._epReports[1].externalScopeCalls, 0, 'an endpoint missing from the slice must not gain calls');
}
