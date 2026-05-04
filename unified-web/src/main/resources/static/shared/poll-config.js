var PollConfig = {
    sessionPollMs: 30000,
    claudeProgressMs: 30000,
    jobStatusMs: 10000,
    logRefreshMs: 10000,
    correctionMs: 30000,
    _loaded: false,

    async load() {
        if (this._loaded) return;
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
        } catch (e) { /* use defaults */ }
        this._loaded = true;
    }
};
PollConfig.load();
