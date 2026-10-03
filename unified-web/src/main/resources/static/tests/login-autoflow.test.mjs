/**
 * The login flow's UX contract, asserted against the shipped index.html.
 *
 * Two regressions this locks down:
 *   1. The login must open the AWS SSO portal with no intermediate click. The home page used to
 *      auto-open a modal that waited for "Continue with AWS SSO"; now it auto-starts the device
 *      flow, shows the code, and redirects — but only ONCE per browser session (a refresh must not
 *      register a second device code), and it must resume an in-flight flow after the AWS redirect.
 *   2. The approval→usable-app path must not wait on the old 5 s poll: the UI polls every 2 s and
 *      shows a live status while the server finishes.
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
  // It starts the device flow itself (POST /api/auth/aws/start), rather than waiting for a click.
  includes(html, "awsSsoContinue({ startUrl: startUrl, autoRedirect: true })",
    'auto-start must invoke the device flow with autoRedirect');
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

export async function test_aws_portal_opens_in_a_new_tab_and_is_closed_on_success() {
  const html = await read('index.html');
  // The AWS portal opens in a NEW TAB, so the app tab stays open and keeps polling.
  includes(html, "window.open(d.verificationUriComplete, '_blank')",
    'the auto-start path must open the AWS portal in a new tab');
  includes(html, 'function awsSsoCloseVerifyTab(',
    'the app must try to close the AWS tab once the login completes');
  includes(html, 'w.close()',
    'closing the AWS tab must use the window handle');
  includes(html, 'you can close the AWS tab',
    'when the tab cannot be closed, the app must say so as a hint');
  // Popup-blocked fallback: same-tab redirect, with the flow still persisted.
  includes(html, 'window.location.href = d.verificationUriComplete',
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
  includes(html, 'Approved — loading your workspace…',
    'the post-approval step must show the loading status');
  includes(html, 'function awsSsoAwaitReconnect(',
    'the UI must wait briefly for the server-side auto-reconnect');
  includes(html, 'awsSsoEnterWorkspace()',
    'the UI must admit the user once the session is connected');
}
