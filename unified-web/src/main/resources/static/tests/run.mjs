/**
 * Test runner. Discovers `*.test.mjs` under tests/ and runs every exported case.
 *
 * Exits non-zero on any failure, and — deliberately — also when it finds no tests at all. A runner
 * that reports success for an empty suite is indistinguishable from one whose tests all passed,
 * which is how a broken discovery glob goes unnoticed for months.
 */
import { readdir } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';

const here = path.dirname(fileURLToPath(import.meta.url));

async function findTests(dir) {
  const out = [];
  for (const e of await readdir(dir, { withFileTypes: true })) {
    const p = path.join(dir, e.name);
    if (e.isDirectory()) out.push(...await findTests(p));
    else if (e.name.endsWith('.test.mjs')) out.push(p);
  }
  return out.sort();
}

const files = await findTests(here);
if (files.length === 0) {
  console.error('no *.test.mjs found — the discovery glob is broken, not the suite empty');
  process.exit(2);
}

let pass = 0, fail = 0;
const failures = [];

for (const file of files) {
  const mod = await import(file);
  const rel = path.relative(here, file);
  for (const [name, fn] of Object.entries(mod)) {
    if (typeof fn !== 'function' || !name.startsWith('test')) continue;
    const label = `${rel} :: ${name.replace(/^test_?/, '').replace(/_/g, ' ')}`;
    try {
      await fn();
      pass++;
      console.log(`  ok    ${label}`);
    } catch (e) {
      fail++;
      failures.push({ label, e });
      console.log(`  FAIL  ${label}`);
    }
  }
}

if (failures.length) {
  console.log('\nFailures:\n');
  for (const f of failures) console.log(`  ${f.label}\n    ${(f.e.message || f.e).split('\n').join('\n    ')}\n`);
}
console.log(`\n  ${pass} passed, ${fail} failed, ${files.length} file(s)`);
process.exit(fail ? 1 : 0);
