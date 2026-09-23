/** FE-003 — the budget helper itself, including that it can fail. */
import { assertNodeBudget, countTags } from './lib/budget.mjs';
import { fixture } from './lib/fixtures.mjs';
import { ok, equal, atLeast } from './lib/assert.mjs';

export async function test_the_large_fixture_is_actually_large() {
  const data = await fixture('large');
  atLeast(data.statements.length, 50000,
    'the large fixture must be at scale, or every budget assertion below is meaningless');
  atLeast(data.endpoints.length, 300, 'endpoints in the large fixture');
}

export function test_the_budget_helper_counts_what_was_rendered() {
  equal(countTags('<tr><td>a</td></tr><tr><td>b</td></tr>', 'tr'), 2);
  equal(countTags('', 'tr'), 0);
}

export function test_the_budget_helper_fails_when_the_budget_is_exceeded() {
  // A helper that cannot fail would let every virtualisation task pass without virtualising
  // anything. This is the self-test the task asks for.
  const html = '<tr></tr>'.repeat(120);
  let threw = false;
  try {
    assertNodeBudget(html, 'tr', 50, 50000, 'test view');
  } catch (e) {
    threw = true;
    ok(String(e.message).includes('120'), 'the failure must state what was actually rendered');
    ok(String(e.message).includes('50000'), 'and how many rows produced it');
  }
  ok(threw, 'the helper did not fail when the budget was exceeded');
}

export function test_the_budget_helper_passes_a_virtualised_render() {
  const html = '<tr></tr>'.repeat(50);
  equal(assertNodeBudget(html, 'tr', 50, 50000, 'virtualised view'), 50);
}
