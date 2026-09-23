/**
 * Asserts a render stays within a DOM-node budget.
 *
 * ## What this converts from opinion into measurement
 *
 * "This view renders an unbounded list" is a code review comment that can be argued with. "This
 * view produced 50,000 `<tr>` elements against the large fixture" is not. Several remaining tasks
 * are about virtualising views whose node count currently tracks the corpus size, and each of them
 * needs a number to pass or fail against.
 *
 * The budget is a count of *rendered nodes*, not of input rows: a virtualised view given 50,000
 * rows should still produce a page-sized handful. That is the property being asserted — the
 * relationship between input size and DOM size — and it is why the helper takes the fixture rather
 * than a fixed expectation.
 */
import { AssertionError } from './assert.mjs';

/** Counts occurrences of an element tag in a markup string. */
export function countTags(html, tag) {
  const m = html.match(new RegExp(`<${tag}\\b`, 'g'));
  return m ? m.length : 0;
}

/**
 * @param html   the rendered markup
 * @param tag    the element to count, e.g. 'tr'
 * @param max    the budget
 * @param inputSize how many rows were supplied, for a message that explains the failure
 */
export function assertNodeBudget(html, tag, max, inputSize, what = 'view') {
  const n = countTags(html, tag);
  if (n > max) {
    throw new AssertionError(
      `${what} rendered ${n} <${tag}> nodes for ${inputSize} input rows, over the budget of ${max}.\n` +
      `    A render whose node count tracks the corpus is the failure mode this budget exists to catch:\n` +
      `    it is imperceptible on today's data and fatal on the target volume.`);
  }
  return n;
}
