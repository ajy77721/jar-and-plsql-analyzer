/**
 * WAR-specific overrides applied before jar/js/app.js loads.
 * Redirects the hardcoded /api/jar/progress SSE URL to /api/war/progress.
 */
(function () {
    const _NativeES = window.EventSource;
    function PatchedEventSource(url, opts) {
        if (url === '/api/jar/progress') url = '/api/war/progress';
        return new _NativeES(url, opts);
    }
    PatchedEventSource.prototype = _NativeES.prototype;
    PatchedEventSource.CONNECTING = _NativeES.CONNECTING;
    PatchedEventSource.OPEN       = _NativeES.OPEN;
    PatchedEventSource.CLOSED     = _NativeES.CLOSED;
    window.EventSource = PatchedEventSource;
})();
