/**
 * "Upload analysis ZIP" — one dialog shared by the JAR page and the parser page.
 *
 * <p>The owner uploads a ZIP of ANALYSIS RESULTS produced locally; the server detects the type
 * (JAR/WAR analysis, PL/SQL parse, or a raw .jar/.war) and stores it in their own ArtifactStore
 * without re-running anything. This module is deliberately page-agnostic: each page calls
 * `ZipUpload.init({ notify, onStored })` once, supplying its own toast and its own "reload the
 * list" callback, and gets identical markup and behaviour.
 *
 * <p>Flow: selecting a file calls the endpoint with `dryRun=true`, which writes nothing and
 * returns the detection; that renders the "Detected: …" line. Confirm uploads for real. Changing
 * one of the optional fields re-detects, so a ZIP that does not self-describe (no project/env)
 * can be completed without reloading the page.
 */
(function (global) {
    'use strict';

    var ENDPOINT = '/api/jar/zip-upload';
    var OVERLAY_ID = 'zip-upload-overlay';

    function esc(s) {
        if (global.Esc && Esc.html) return Esc.html(s);
        return String(s === null || s === undefined ? '' : s)
            .replace(/[&<>"']/g, function (c) {
                return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c];
            });
    }

    var ZipUpload = {
        _cfg: null,

        init: function (cfg) {
            this._cfg = cfg || {};
        },

        _notify: function (msg, kind) {
            var fn = this._cfg && this._cfg.notify;
            if (typeof fn === 'function') fn(msg, kind || 'info');
            else console.log('[ZipUpload] ' + kind + ': ' + msg);
        },

        open: function () {
            var self = this;
            if (document.getElementById(OVERLAY_ID)) return;

            var overlay = document.createElement('div');
            overlay.id = OVERLAY_ID;
            overlay.style.cssText = 'position:fixed;inset:0;background:rgba(0,0,0,.55);z-index:9999;'
                + 'display:flex;align-items:center;justify-content:center;padding:16px;box-sizing:border-box';
            overlay.innerHTML = ''
                + '<div style="background:var(--bg-elevated,#1e2229);color:var(--text,#e6e6e6);'
                + 'border:1px solid var(--border,#333);border-radius:var(--radius,8px);'
                + 'width:min(580px,96vw);max-height:90vh;overflow:auto;box-shadow:0 12px 40px rgba(0,0,0,.5)">'
                + '  <div style="display:flex;justify-content:space-between;align-items:center;'
                + '       padding:12px 16px;border-bottom:1px solid var(--border,#333);font-weight:600">'
                + '    <span>Upload analysis ZIP</span>'
                + '    <button data-zu-close style="background:none;border:none;color:inherit;'
                + '         font-size:16px;cursor:pointer" title="Close">&#x2715;</button>'
                + '  </div>'
                + '  <div style="padding:14px 16px;display:flex;flex-direction:column;gap:12px">'
                + '    <div style="font-size:12px;color:var(--text-muted,#999);line-height:1.5">'
                + '      A ZIP of results already produced locally — a JAR/WAR analysis, a PL/SQL parse '
                + 'tree, or a single raw .jar/.war. It is stored as-is; no analysis is re-run unless the '
                + 'ZIP holds a raw archive.'
                + '    </div>'
                + '    <div style="display:flex;gap:8px">'
                + '      <input id="zu-file-name" readonly placeholder="No file selected" '
                + '        style="flex:1;min-width:0;background:var(--bg-inset,#14171c);color:inherit;'
                + '          border:1px solid var(--border,#333);border-radius:6px;padding:6px 8px;font-size:13px">'
                + '      <button id="zu-browse" style="background:var(--bg-inset,#14171c);color:inherit;'
                + '          border:1px solid var(--border,#333);border-radius:6px;padding:6px 12px;cursor:pointer">Browse</button>'
                + '      <input type="file" id="zu-file" accept=".zip" hidden>'
                + '    </div>'
                + '    <details style="font-size:12px;color:var(--text-muted,#999)">'
                + '      <summary style="cursor:pointer">Optional fields (used only when the ZIP does not self-describe)</summary>'
                + '      <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-top:8px">'
                + '        <label style="display:flex;flex-direction:column;gap:4px">Analysis name'
                + '          <input id="zu-name" style="background:var(--bg-inset,#14171c);color:inherit;'
                + '            border:1px solid var(--border,#333);border-radius:6px;padding:6px 8px" placeholder="e.g. mongo-flow-demo"></label>'
                + '        <label style="display:flex;flex-direction:column;gap:4px">Dialect'
                + '          <input id="zu-dialect" style="background:var(--bg-inset,#14171c);color:inherit;'
                + '            border:1px solid var(--border,#333);border-radius:6px;padding:6px 8px" placeholder="ORACLE | MYSQL | ..."></label>'
                + '        <label style="display:flex;flex-direction:column;gap:4px">Project'
                + '          <input id="zu-project" style="background:var(--bg-inset,#14171c);color:inherit;'
                + '            border:1px solid var(--border,#333);border-radius:6px;padding:6px 8px"></label>'
                + '        <label style="display:flex;flex-direction:column;gap:4px">Env'
                + '          <input id="zu-env" style="background:var(--bg-inset,#14171c);color:inherit;'
                + '            border:1px solid var(--border,#333);border-radius:6px;padding:6px 8px"></label>'
                + '      </div>'
                + '    </details>'
                + '    <div id="zu-detect" style="display:none;padding:8px 10px;border-radius:6px;font-size:13px;'
                + '         background:var(--bg-inset,#14171c);border:1px solid var(--border,#333)"></div>'
                + '    <div id="zu-error" style="display:none;padding:8px 10px;border-radius:6px;font-size:13px;'
                + '         background:rgba(220,60,60,.12);border:1px solid rgba(220,60,60,.5);color:#ff8a8a"></div>'
                + '  </div>'
                + '  <div style="display:flex;justify-content:flex-end;gap:8px;padding:12px 16px;'
                + '       border-top:1px solid var(--border,#333)">'
                + '    <button data-zu-close style="background:var(--bg-inset,#14171c);color:inherit;'
                + '         border:1px solid var(--border,#333);border-radius:6px;padding:6px 14px;cursor:pointer">Cancel</button>'
                + '    <button id="zu-confirm" disabled style="background:var(--accent,#3b82f6);color:#fff;'
                + '         border:none;border-radius:6px;padding:6px 14px;cursor:pointer;opacity:.5">Confirm upload</button>'
                + '  </div>'
                + '</div>';
            document.body.appendChild(overlay);

            var $ = function (id) { return overlay.querySelector(id); };
            var fileInput = $('#zu-file');
            var confirmBtn = $('#zu-confirm');
            var detectBox = $('#zu-detect');
            var errorBox = $('#zu-error');
            var confirmed = false;

            function close() {
                overlay.remove();
                document.removeEventListener('keydown', onKey);
            }
            function onKey(e) { if (e.key === 'Escape') close(); }

            function setError(msg) {
                detectBox.style.display = 'none';
                if (!msg) { errorBox.style.display = 'none'; errorBox.textContent = ''; return; }
                errorBox.style.display = '';
                errorBox.textContent = msg;
            }
            function setConfirmEnabled(on) {
                confirmBtn.disabled = !on;
                confirmBtn.style.opacity = on ? '1' : '.5';
                confirmBtn.style.cursor = on ? 'pointer' : 'not-allowed';
            }
            function setBusy(msg) {
                detectBox.style.display = '';
                detectBox.innerHTML = '<span style="opacity:.8">' + esc(msg) + '</span>';
            }

            function fieldParams() {
                return {
                    analysisName: $('#zu-name').value.trim(),
                    dialect: $('#zu-dialect').value.trim(),
                    project: $('#zu-project').value.trim(),
                    env: $('#zu-env').value.trim()
                };
            }

            function renderDetected(res) {
                var text;
                if (res.type === 'jar-analysis') {
                    var kind = res.war ? 'WAR analysis' : 'JAR analysis';
                    text = 'Detected: ' + kind + " '" + esc(res.analysisName) + "' &rarr; " + esc(res.target);
                } else if (res.type === 'plsql-parse') {
                    var scope = [res.dialect, res.project, res.env].filter(Boolean).map(esc).join(' / ');
                    text = 'Detected: PL/SQL parse ' + scope + ' &rarr; ' + esc(res.target);
                } else if (res.type === 'raw-jar') {
                    text = "Detected: raw archive '" + esc(res.analysisName)
                        + "' — it will be stored and the analysis pipeline started.";
                } else {
                    text = 'Detected: ' + esc(res.type || 'unknown');
                }
                detectBox.style.display = '';
                detectBox.innerHTML = text;
            }

            async function call(dryRun) {
                var file = fileInput.files[0];
                if (!file) throw new Error('Please select a .zip file');
                var p = fieldParams();
                var fd = new FormData();
                fd.append('file', file);
                if (p.analysisName) fd.append('analysisName', p.analysisName);
                if (p.dialect) fd.append('dialect', p.dialect);
                if (p.project) fd.append('project', p.project);
                if (p.env) fd.append('env', p.env);
                fd.append('dryRun', dryRun ? 'true' : 'false');

                var res = await fetch(ENDPOINT, { method: 'POST', body: fd });
                var text = await res.text();
                var data;
                try { data = JSON.parse(text); } catch (e) { data = { ok: false, error: text }; }
                if (!res.ok) {
                    var err = new Error(data.message || data.error || res.statusText || 'Upload failed');
                    err.status = res.status;
                    throw err;
                }
                return data;
            }

            async function detect() {
                if (!fileInput.files[0]) return;
                setError(null);
                setConfirmEnabled(false);
                setBusy('Detecting…');
                try {
                    var res = await call(true);
                    renderDetected(res);
                    setConfirmEnabled(true);
                } catch (err) {
                    setError(err.message);
                }
            }

            async function confirm() {
                if (confirmed) return;
                setError(null);
                setBusy('Uploading…');
                setConfirmEnabled(false);
                try {
                    var res = await call(false);
                    confirmed = true;
                    var summary = res.type === 'raw-jar'
                        ? 'Queued: ' + (res.analysisName || 'archive') + ' — analysis started'
                        : 'Stored ' + (res.storedCount != null ? res.storedCount : 0) + ' file(s) as '
                          + (res.analysisName || [res.dialect, res.project, res.env].filter(Boolean).join('/'));
                    self._notify(summary, 'success');
                    close();
                    if (self._cfg && typeof self._cfg.onStored === 'function') self._cfg.onStored(res);
                } catch (err) {
                    setError(err.message);
                    setConfirmEnabled(true);
                }
            }

            $('#zu-browse').addEventListener('click', function () { fileInput.click(); });
            fileInput.addEventListener('change', function () {
                $('#zu-file-name').value = fileInput.files[0] ? fileInput.files[0].name : '';
                detect();
            });
            ['#zu-name', '#zu-dialect', '#zu-project', '#zu-env'].forEach(function (sel) {
                $(sel).addEventListener('change', detect);
            });
            confirmBtn.addEventListener('click', confirm);
            overlay.querySelectorAll('[data-zu-close]').forEach(function (b) {
                b.addEventListener('click', close);
            });
            overlay.addEventListener('click', function (e) { if (e.target === overlay) close(); });
            document.addEventListener('keydown', onKey);

            fileInput.click();
        }
    };

    global.ZipUpload = ZipUpload;
})(window);
