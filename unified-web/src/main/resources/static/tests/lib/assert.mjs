/** Assertions that report the offending value, because "expected true, got false" explains nothing. */

export class AssertionError extends Error {}

export function ok(cond, msg) {
  if (!cond) throw new AssertionError(msg || 'expected a truthy value');
}

export function equal(actual, expected, msg) {
  if (actual !== expected) {
    throw new AssertionError(`${msg || 'values differ'}\n    expected: ${JSON.stringify(expected)}\n    actual:   ${JSON.stringify(actual)}`);
  }
}

export function atLeast(actual, min, msg) {
  if (!(actual >= min)) {
    throw new AssertionError(`${msg || 'below minimum'}: expected >= ${min}, got ${actual}`);
  }
}

export function includes(haystack, needle, msg) {
  if (!String(haystack).includes(needle)) {
    const s = String(haystack);
    throw new AssertionError(`${msg || 'substring missing'}: ${JSON.stringify(needle)}\n    in: ${s.slice(0, 300)}${s.length > 300 ? '…' : ''}`);
  }
}

export function excludes(haystack, needle, msg) {
  if (String(haystack).includes(needle)) {
    const s = String(haystack);
    const at = s.indexOf(needle);
    throw new AssertionError(`${msg || 'substring should be absent'}: ${JSON.stringify(needle)}\n    found at ${at}: …${s.slice(Math.max(0, at - 60), at + 80)}…`);
  }
}
