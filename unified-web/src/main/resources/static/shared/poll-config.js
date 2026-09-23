/*
 * Polling intervals, fetched once from /api/config/polling.
 *
 * Three defects made the configuration mechanism decorative, and every other polling change in the
 * migration depends on the interval actually being configurable:
 *
 *   1. load() was fired but never awaited, so any timer registered during initial script execution
 *      captured the hard-coded defaults and the fetched values arrived too late to matter.
 *   2. Nothing under parser/** referenced this file at all — six intervals were literals, every one
 *      of them faster than the configured value (2 s where the config said 30 s).
 *   3. parser/index.html loaded shared/log-fab.js without loading this file, so log-fab.js threw
 *      ReferenceError: PollConfig is not defined and its Auto checkbox was dead; conversion loaded
 *      neither.
 *
 * `every()` is the fix for (1) and the shape that makes (2) grep-detectable: a literal-ms timer in
 * polling code is now a lint failure, because there is a correct alternative to point at.
 */
var PollConfig = {
    sessionPollMs: 30000,
    claudeProgressMs: 30000,
    jobStatusMs: 10000,
    // Targeted analysis polls faster than a background job because a user is watching it finish.
    // This existed only as a literal third argument to every(), which takes two — so the value was
    // discarded and the poll silently ran at jobStatusMs, five times slower than intended.
    targetedJobMs: 2000,
    logRefreshMs: 10000,
    correctionMs: 30000,

    _ready: null,

    /** The in-flight (or completed) config fetch. Idempotent — safe to await from anywhere. */
    ready: function () {
        return this._ready || (this._ready = this._load());
    },

    /** Retained under its old name: existing call sites do `PollConfig.load()` fire-and-forget. */
    load: function () {
        return this.ready();
    },

    _load: async function () {
        try {
            var res = await fetch('/api/config/polling');
            if (res.ok) {
                var cfg = await res.json();
                if (cfg.sessionPollMs) this.sessionPollMs = cfg.sessionPollMs;
                if (cfg.claudeProgressMs) this.claudeProgressMs = cfg.claudeProgressMs;
                if (cfg.jobStatusMs) this.jobStatusMs = cfg.jobStatusMs;
                if (cfg.logRefreshMs) this.logRefreshMs = cfg.logRefreshMs;
                if (cfg.correctionMs) this.correctionMs = cfg.correctionMs;
            }
        } catch (e) { /* defaults above are the fallback; a missing endpoint must not break a page */ }
        return this;
    },

    /**
     * The only correct way to start a poll: awaits the configured interval, then schedules.
     *
     * Fires `fn` once immediately, matching what every hand-rolled call site did, so converting one
     * means deleting its explicit first call rather than keeping both.
     *
     * Returns a promise, because the interval id does not exist until the config has landed. Store
     * the promise and hand it to `clear()`; never unwrap it just to get a number, or the window
     * between the two is a timer that cannot be stopped.
     */
    every: function (key, fn) {
        var self = this;
        return this.ready().then(function () {
            fn();
            return setInterval(fn, self[key]);
        });
    },

    /** Cancels a handle from `every()` (a promise) or a plain setInterval id. Null-safe. */
    clear: function (handle) {
        if (handle === null || handle === undefined) return null;
        if (typeof handle.then === 'function') {
            handle.then(function (id) { clearInterval(id); });
        } else {
            clearInterval(handle);
        }
        return null;
    }
};

PollConfig.ready();   // start the fetch on load; do not block script execution on it
