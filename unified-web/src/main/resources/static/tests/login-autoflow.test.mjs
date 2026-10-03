/**
 * The login flow's UX contract, asserted against the shipped index.html.
 *
 * Three regressions this locks down:
 *   1. The device flow auto-starts once per browser session (a refresh must not register a second
 *      device code) and resumes an in-flight flow instead of restarting it.
 *   2. Step 2 is ONE big "Open AWS login →" button next to the big code — no Copy, no link. The
 *      click is the user gesture that makes window.open reliable; the auto-open attempt is gone
 *      (it always popup-blocks). The handle is kept so the AWS tab can be auto-closed on success.
 *   3. The approval→usable-app path polls every 2 s and shows a live status while the server
 *      finishes.
 */
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { ok, includes, excludes } from './lib/assert.mjs';

const STATIC = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (rel) => readFile(path.join(STATIC, rel), 'utf8');

export async function test_login_auto_starts_the_device_flow() {
  const html = await read('index.html');
  // The auto-start entry point exists and is what the page-lock landing calls.
  includes(html, 'function awsSsoAutoStartLogin()',
    'the login page must define the auto-start helper');
  includes(html, 'awsSsoAutoStartLogin();',
    'loadAwsSsoStatus / the 401 handler must call awsSsoAutoStartLogin');
  // It registers the device flow (POST /api/auth/aws/start) and lands on Step 2 — but it must NOT
  // try to open the AWS tab itself: without a gesture that is always popup-blocked.
  includes(html, 'awsSsoContinue({ startUrl: startUrl })',
    'auto-start must register the device flow without auto-opening a tab');
  excludes(html, 'autoRedirect',
    'the popup-blocked auto-open attempt must be gone — the button is the gesture');
}

export async function test_auto_start_is_guarded_once_per_session() {
  const html = await read('index.html');
  includes(html, "AWS_SSO_AUTOSTART_KEY = 'awsSsoAutoStarted'",
    'a sessionStorage guard key must gate auto-start');
  includes(html, 'sessionStorage.getItem(AWS_SSO_AUTOSTART_KEY)',
    'auto-start must read the session guard before registering');
  includes(html, 'sessionStorage.setItem(AWS_SSO_AUTOSTART_KEY',
    'auto-start must set the session guard when it registers');
  // Unauthenticated-only: bail out when a live session is already known.
  includes(html, 'if (_awsSsoSession) return;',
    'auto-start must be a no-op when already authenticated');
}

export async function test_step2_is_one_big_open_button() {
  const html = await read('index.html');
  // ONE big button, wired to the click-gesture opener.
  includes(html, 'id="awsSsoOpenAwsBtn"',
    'Step 2 must have the big Open AWS login button');
  includes(html, 'onclick="awsSsoOpenVerifyTab()"',
    'the big button must open the AWS tab');
  includes(html, 'Open AWS login',
    'the button label must be "Open AWS login →"');
  // No Copy button and no link below: the URL is pre-filled with the code.
  excludes(html, 'awsSsoCopyCode',
    'the Copy button must be gone — the code is pre-filled on the AWS page');
  excludes(html, 'id="awsSsoVerifyLink"',
    'the fallback link below must be gone — one button only');
}

export async function test_aws_tab_opens_by_gesture_and_closes_on_success() {
  const html = await read('index.html');
  // The click gesture is what makes the new tab reliable.
  includes(html, "window.open(href, '_blank')",
    'the button must open the AWS portal in a new tab');
  includes(html, 'function awsSsoCloseVerifyTab(',
    'the app must try to close the AWS tab once the login completes');
  includes(html, 'w.close()',
    'closing the AWS tab must use the window handle');
  includes(html, 'you can close the AWS tab',
    'when the tab cannot be closed, the app must say so as a hint');
  // Last-resort fallback only: same-tab redirect, flow still persisted.
  includes(html, 'window.location.href = href',
    'a blocked popup must fall back to the same-tab redirect');
  includes(html, "AWS_SSO_FLOW_KEY = 'awsSsoFlow'",
    'the in-flight flow must be persisted across the AWS redirect');
  includes(html, 'function awsSsoReadFlow()',
    'returning to the app must be able to resume the persisted flow');
  includes(html, 'awsSsoShowStep(2)',
    'the resumed flow must restore the device-verification step');
}

export async function test_poll_interval_is_two_seconds() {
  const html = await read('index.html');
  includes(html, 'const AWS_SSO_POLL_MS = 2000;',
    'the device-flow poll must run every 2 s for a fast post-approval hand-off');
  includes(html, 'setInterval(awsSsoPoll, AWS_SSO_POLL_MS)',
    'the poll timer must use the 2 s interval');
  excludes(html, 'AWS_SSO_POLL_FALLBACK_MS',
    'the retired 5 s poll fallback must be gone');
}

export async function test_post_approval_shows_live_status() {
  const html = await read('index.html');
  includes(html, 'id="awsSsoStatusLine"',
    'the modal must render a shared live-status line');
  includes(html, 'Approved — entering…',
    'the post-approval step must show the entering status');
  includes(html, 'function awsSsoAwaitReconnect(',
    'the UI must wait briefly for the server-side auto-reconnect');
  includes(html, 'awsSsoEnterWorkspace()',
    'the UI must admit the user once the session is connected');
}
