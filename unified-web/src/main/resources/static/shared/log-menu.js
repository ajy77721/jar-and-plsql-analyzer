/**
 * LogMenu — the top-bar "Logs ▾" control, next to Sessions, on the JAR and WAR screens.
 *
 * One click from that corner reaches the three log categories a reader wants:
 *   - Application log  — the live app log via the existing LogController `/logs` endpoint
 *                        (opened by the page's own app-log viewer, `JA.logviewer`).
 *   - Analysis logs    — this analysis's per-run logs (`…/run-logs` list + `…/run-logs/{name}`).
 *   - Session chat logs — recent chat sessions and their messages (`/api/chat/sessions…`).
 *
 * Everything reuses the existing APIs and the existing overlay look (`.session-*`, `.btn-sm`,
 * `.top-bar-btn`) so it reads as the same control family as Sessions. If a page cannot supply a
 * source, that item says so rather than opening an empty panel.
 */
window.LogMenu = (function () {
    'use strict';

    var cfg = {};
    var open = false;

    function esc(v) {
        if (window.JA && JA.utils && JA.utils.escapeHtml) return JA.utils.escapeHtml(v);
        if (window.PA && PA.esc) return PA.esc(v);
        return String(v == null ? '' : v)
            .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
    }
    function escJs(v) {
        if (window.JA && JA.utils && JA.utils.escapeJs) return JA.utils.escapeJs(v);
        if (window.PA && PA.escJs) return PA.escJs(v);
        // JS-escape for the inline handler's string literal, then HTML-escape because this lands
        // inside a quoted attribute — the same double-escape shared/escape.js uses.
        var s = String(v == null ? '' : v)
            .replace(/\\/g, '\\\\').replace(/'/g, "\\'").replace(/\n/g, '\\n').replace(/\r/g, '\\r');
        return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
    }

    function init(options) {
        cfg = options || {};
        installStyle();
        inject();
    }

    /** Puts the control where the old top-bar Logs button was, or right after Sessions. */
    function inject() {
        if (document.getElementById('log-menu')) return;
        var wrap = document.createElement('div');
        wrap.className = 'log-menu';
        wrap.id = 'log-menu';

        var btn = document.createElement('button');
        btn.className = 'top-bar-btn';
        btn.id = 'log-menu-btn';
        btn.title = 'View logs';
        btn.setAttribute('onclick', 'LogMenu.toggle(event)');
        btn.innerHTML = '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="vertical-align:middle;margin-right:4px">'
            + '<path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/>'
            + '<polyline points="14 2 14 8 20 8"/><line x1="16" y1="13" x2="8" y2="13"/><line x1="16" y1="17" x2="8" y2="17"/>'
            + '</svg>Logs <span class="log-menu-caret">&#9662;</span>';

        var dd = document.createElement('div');
        dd.className = 'log-menu-dropdown';
        dd.id = 'log-menu-dropdown';
        dd.setAttribute('role', 'menu');
        dd.innerHTML =
            '<button class="log-menu-item" role="menuitem" onclick="LogMenu.showApp()">Application log</button>'
            + '<button class="log-menu-item" role="menuitem" onclick="LogMenu.showAnalysis()">Analysis logs</button>'
            + '<button class="log-menu-item" role="menuitem" onclick="LogMenu.showChat()">Session chat logs</button>';

        wrap.appendChild(btn);
        wrap.appendChild(dd);
        var old = document.getElementById('logviewer-btn');
        if (old && old.parentNode && old.parentNode.replaceChild) {
            old.parentNode.replaceChild(wrap, old);   // takes the old button's place in the bar
        } else {
            var s = document.getElementById('sessions-btn');
            if (s && s.parentNode) s.parentNode.insertBefore(wrap, s.nextSibling);
            else document.body.appendChild(wrap);
        }
        document.addEventListener('click', function (e) {
            var m = document.getElementById('log-menu');
            if (m && !m.contains(e.target)) hide();
        });
    }

    function toggle(e) {
        if (e && e.stopPropagation) e.stopPropagation();
        open ? hide() : show();
    }
    function show() { var d = document.getElementById('log-menu-dropdown'); if (d) d.style.display = 'block'; open = true; }
    function hide() { var d = document.getElementById('log-menu-dropdown'); if (d) d.style.display = 'none'; open = false; }

    function showApp() {
        hide();
        if (cfg.appLog) cfg.appLog();
        else if (window.LogFab) LogFab.open();
    }

    function showAnalysis() {
        hide();
        var id = cfg.currentId ? cfg.currentId() : '';
        if (!id || !cfg.runLogs) {
            var emptyBody = overlay('Analysis logs');
            emptyBody.innerHTML = '<div class="log-menu-empty">Select an analysis first — run logs belong to one analysis.</div>';
            return;
        }
        var body = overlay('Analysis logs' + (cfg.name ? ' — ' + esc(cfg.name(id)) : ''));
        var list = document.createElement('div');
        list.className = 'log-menu-list';
        list.id = 'log-menu-list';
        list.textContent = 'Loading…';
        body.appendChild(list);
        var pre = document.createElement('pre');
        pre.className = 'log-menu-pre';
        pre.id = 'log-menu-pre';
        body.appendChild(pre);

        Promise.resolve(cfg.runLogs(id)).then(function (logs) {
            if (!document.getElementById('log-menu-list')) return;
            if (!logs || !logs.length) {
                list.innerHTML = '<div class="log-menu-empty">No run logs yet. Run a full scan to generate logs.</div>';
                return;
            }
            var h = '<div class="log-menu-sub">Click a log to view it:</div>';
            logs.forEach(function (l) {
                h += '<button class="log-menu-logitem" onclick="LogMenu.loadRunLog(\'' + escJs(l.name) + '\')">'
                    + esc(l.name) + ' <span class="log-menu-dim">(' + Math.round((l.size || 0) / 1024) + 'KB)</span></button>';
            });
            list.innerHTML = h;
        }).catch(function (e) {
            if (list) list.textContent = 'Failed to load run logs: ' + (e && e.message ? e.message : e);
        });
    }

    function loadRunLog(name) {
        var pre = document.getElementById('log-menu-pre');
        if (!pre) return;
        var id = cfg.currentId ? cfg.currentId() : '';
        pre.textContent = 'Loading ' + name + '…';
        Promise.resolve(cfg.getRunLog(id, name)).then(function (content) {
            pre.textContent = content == null ? '(empty log)' : content;
        }).catch(function (e) {
            pre.textContent = 'Failed to load: ' + (e && e.message ? e.message : e);
        });
    }

    function showChat() {
        hide();
        if (!cfg.chatSessions) {
            var emptyBody = overlay('Session chat logs');
            emptyBody.innerHTML = '<div class="log-menu-empty">Chat logs are not available on this screen.</div>';
            return;
        }
        var body = overlay('Session chat logs');
        var list = document.createElement('div');
        list.className = 'log-menu-list';
        list.id = 'log-menu-list';
        list.textContent = 'Loading…';
        body.appendChild(list);
        var pre = document.createElement('pre');
        pre.className = 'log-menu-pre';
        pre.id = 'log-menu-pre';
        body.appendChild(pre);

        Promise.resolve(cfg.chatSessions()).then(function (sessions) {
            if (!document.getElementById('log-menu-list')) return;
            sessions = sessions || [];
            if (!sessions.length) {
                list.innerHTML = '<div class="log-menu-empty">No chat sessions yet.</div>';
                return;
            }
            var h = '<div class="log-menu-sub">Recent sessions — click to view the transcript:</div>';
            sessions.slice(0, 20).forEach(function (s) {
                var sid = s.id || s.sessionId || '';
                h += '<button class="log-menu-logitem" onclick="LogMenu.loadChat(\'' + escJs(sid) + '\')">'
                    + esc(s.name || sid || '(session)')
                    + ' <span class="log-menu-dim">' + (s.messageCount || 0) + ' msgs</span></button>';
            });
            list.innerHTML = h;
        }).catch(function (e) {
            if (list) list.textContent = 'Failed to load chat sessions: ' + (e && e.message ? e.message : e);
        });
    }

    function loadChat(sessionId) {
        var pre = document.getElementById('log-menu-pre');
        if (!pre) return;
        pre.textContent = 'Loading transcript…';
        Promise.resolve(cfg.chatMessages(sessionId)).then(function (env) {
            var items = (env && env.items) || env || [];
            // getMessages returns newest-first; show the transcript oldest-first.
            var lines = items.slice().reverse().map(function (m) {
                return '[' + (m.role || '?') + '] ' + (m.content || m.message || '');
            });
            pre.textContent = lines.length ? lines.join('\n\n') : '(no messages)';
        }).catch(function (e) {
            pre.textContent = 'Failed to load: ' + (e && e.message ? e.message : e);
        });
    }

    /**
     * Opens the overlay and returns its body element, so callers can build their content from real
     * elements (and tests can reach them) rather than from a parsed innerHTML string. The overlay
     * reuses the Sessions modal classes, so it looks like the same control family.
     */
    function overlay(title) {
        var old = document.getElementById('log-menu-overlay');
        if (old) old.remove();
        var ov = document.createElement('div');
        ov.id = 'log-menu-overlay';
        ov.className = 'session-overlay';
        var panel = document.createElement('div');
        panel.className = 'session-panel';
        var header = document.createElement('div');
        header.className = 'session-header';
        header.innerHTML = '<span class="session-title">' + esc(title) + '</span>'
            + '<div class="session-header-actions"><button class="btn-sm" onclick="LogMenu.closeOverlay()">Close</button></div>';
        var body = document.createElement('div');
        body.className = 'session-body';
        body.id = 'log-menu-body';
        panel.appendChild(header);
        panel.appendChild(body);
        ov.appendChild(panel);
        document.body.appendChild(ov);
        ov.addEventListener('click', function (e) { if (e.target === ov) closeOverlay(); });
        return body;
    }

    function closeOverlay() {
        var ov = document.getElementById('log-menu-overlay');
        if (ov) ov.remove();
    }

    function installStyle() {
        if (document.getElementById('log-menu-style')) return;
        var st = document.createElement('style');
        st.id = 'log-menu-style';
        st.textContent = [
            '.log-menu{position:relative;display:inline-block}',
            '.log-menu-caret{font-size:9px;opacity:.7;margin-left:2px}',
            '.log-menu-dropdown{position:absolute;top:calc(100% + 4px);right:0;z-index:1000;min-width:190px;',
            '  display:none;background:var(--bg-card,#fff);border:1px solid var(--border,#ccc);',
            '  border-radius:var(--radius-sm,4px);box-shadow:0 6px 18px rgba(0,0,0,.18);padding:4px;text-align:left}',
            '.log-menu-item{display:block;width:100%;text-align:left;background:transparent;border:none;',
            '  color:var(--text,#111);font-size:12px;padding:7px 10px;border-radius:4px;cursor:pointer;white-space:nowrap}',
            '.log-menu-item:hover{background:var(--bg-hover,rgba(128,128,128,.12))}',
            '.log-menu-list{max-height:60vh;overflow:auto}',
            '.log-menu-sub{font-size:11px;color:var(--text-muted,#888);margin:2px 0 6px}',
            '.log-menu-logitem{display:block;width:100%;text-align:left;background:transparent;border:none;',
            '  color:var(--accent,#3aa);font-size:12px;padding:5px 6px;border-radius:4px;cursor:pointer;font-family:var(--font-mono,monospace)}',
            '.log-menu-logitem:hover{background:var(--bg-hover,rgba(128,128,128,.12));text-decoration:underline}',
            '.log-menu-dim{color:var(--text-muted,#888);font-size:10px;font-family:inherit}',
            '.log-menu-empty{color:var(--text-muted,#888);font-size:12px;font-style:italic;padding:6px 2px}',
            '.log-menu-pre{font-family:var(--font-mono,monospace);font-size:11px;line-height:1.45;background:var(--bg-inset,#1112);',
            '  border:1px solid var(--border,#ccc);border-radius:4px;padding:8px;margin:8px 0 0;max-height:50vh;overflow:auto;',
            '  white-space:pre-wrap;word-break:break-word;color:var(--text,#111)}'
        ].join('');
        document.head.appendChild(st);
    }

    return { init: init, toggle: toggle, showApp: showApp, showAnalysis: showAnalysis,
             showChat: showChat, loadRunLog: loadRunLog, loadChat: loadChat, closeOverlay: closeOverlay };
})();
