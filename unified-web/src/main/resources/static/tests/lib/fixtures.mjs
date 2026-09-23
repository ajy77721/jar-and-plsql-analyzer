/**
 * Loads a fixture, generating it first if it is not on disk.
 *
 * The large fixture is 17.7 MB. Generating on demand keeps it out of the source tree while making
 * the tests self-sufficient — nobody has to remember a setup step, and a stale committed blob
 * cannot drift away from the generator that is supposed to describe it.
 */
import { readFile, access } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';

const run = promisify(execFile);
const HERE = path.dirname(fileURLToPath(import.meta.url));
const DIR = path.join(HERE, '..', 'fixtures');
const cache = new Map();

export async function fixture(size) {
  if (cache.has(size)) return cache.get(size);
  const file = path.join(DIR, `${size}.json`);
  try {
    await access(file);
  } catch {
    await run('node', [path.join(DIR, 'generate.mjs')], { cwd: path.join(HERE, '..', '..') });
  }
  const data = JSON.parse(await readFile(file, 'utf8'));
  cache.set(size, data);
  return data;
}
