/**
 * Fails when a `fetch` response is parsed as JSON without first checking `res.ok`.
 *
 * ## Why this is worth a lint rule
 *
 * A 500 from this backend returns an HTML error page. Calling `.json()` on it throws
 * `SyntaxError: Unexpected token '<'`, which is what the user sees — a message about JSON syntax
 * for what is actually a server error. It is the least useful possible rendering of a failure, and
 * it looks like a frontend bug, so it gets investigated in the wrong place.
 *
 * `jar/js/api.js` already checks at all of its fetch sites, so the house style existed; 25 sites
 * across seven other files had simply skipped it. A rule is what stops the twenty-sixth.
 */
import { readdir, readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';

const STATIC = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const SKIP = ['xlsx.full.min.js'];

async function jsFiles(dir) {
  const out = [];
  for (const e of await readdir(dir, { withFileTypes: true })) {
    if (e.name === 'tests' || e.name === 'node_modules') continue;
    const p = path.join(dir, e.name);
    if (e.isDirectory()) out.push(...await jsFiles(p));
    else if (e.name.endsWith('.js') && !SKIP.includes(e.name)) out.push(p);
  }
  return out;
}

const problems = [];
for (const file of await jsFiles(STATIC)) {
  const src = await readFile(file, 'utf8');
  for (const m of src.matchAll(/(?:const|let|var)\s+(\w+)\s*=\s*await\s+fetch\(/g)) {
    const varName = m[1];
    // The check must appear BETWEEN the fetch and the .json() call — not merely somewhere
    // nearby. A fixed look-ahead window was the first attempt and it was wrong: the window ran
    // past the end of the function and found the NEXT function's `res.ok`, so removing a real
    // check still linted clean. The calibration caught that, which is the entire reason for
    // calibrating a rule instead of trusting it.
    const after = src.slice(m.index + m[0].length);
    const jsonAt = after.search(new RegExp(`\\b${varName}\\.json\\s*\\(`));
    if (jsonAt === -1) continue;                       // response never parsed as JSON
    const between = after.slice(0, jsonAt);
    const checksOk = new RegExp(`\\b${varName}\\.ok\\b`).test(between);
    if (!checksOk) {
      const line = src.slice(0, m.index).split('\n').length;
      problems.push(`${path.relative(STATIC, file)}:${line}  ${varName}.json() with no ${varName}.ok check`);
    }
  }
}

if (problems.length) {
  console.error(`\n  ${problems.length} unchecked fetch response(s):\n`);
  problems.forEach(p => console.error('    ' + p));
  console.error('\n  A non-2xx response returns HTML; .json() then throws "Unexpected token \'<\'"');
  console.error("  at the user. Guard with `if (!res.ok) ...` before parsing.\n");
  process.exit(1);
}
console.log('  lint:fetch — every fetch response is checked before parsing');
