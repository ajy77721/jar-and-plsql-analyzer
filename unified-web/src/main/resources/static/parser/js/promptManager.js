window.PA = window.PA || {};

/**
 * Prompt Manager (PIECE 1) + Analysis Context modal (PIECE 2).
 *
 * Backend contract (base /api/prompt-config):
 *   GET  /templates                 -> [{name, current, defaultText, overridden}]
 *   POST /templates/{name} {text}   -> save permanent
 *   POST /templates/{name}/reset    -> reset to default
 *   GET  /extras                    -> {globalAppend, globalContext}
 *   POST /extras {globalAppend, globalContext}
 *   GET  /analysis/{name}           -> {context, append, templates:{name:text}}
 *   POST /analysis/{name} {context, append, templates:{name:text}}
 *
 * Everything degrades gracefully if an endpoint 404s.
 */
PA.promptManager = {
    _BASE: '/api/prompt-config',
    _templates: [],
    _selected: null,

    // ======================= small helpers =======================

    /** Delegated: a private fourth escaper is a fourth set of edge cases to get wrong. */
    _esc: function (s) {
        return Esc.html(s);
    },

    _toast: function (msg, type) { if (PA.toast) PA.toast(msg, type); },

    _get: async function (path) {
        var r = await fetch(this._BASE + path);
        if (!r.ok) throw new Error(r.status + '');
        return r.json();
    },

    _post: async function (path, body) {
        var r = await fetch(this._BASE + path, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(body || {})
        });
        if (!r.ok) throw new Error(r.status + '');
        // response body is optional
        try { return await r.json(); } catch (e) { return {}; }
    },

    // ======================= PIECE 1: Prompt Manager =======================

    open: async function () {
        var ov = document.getElementById('promptMgrOverlay');
        if (!ov) {
            ov = document.createElement('div');
            ov.id = 'promptMgrOverlay';
            ov.className = 'cfg-overlay';
            ov.onclick = function (e) { if (e.target === ov) PA.promptManager.close(); };
            document.body.appendChild(ov);
        }
        ov.style.display = '';
        if (PA.modalStack) ov.style.zIndex = PA.modalStack.nextZ();
        ov.innerHTML =
            '<div class="cfg-modal pm-modal" style="width:960px;max-width:95vw;height:auto;max-height:86vh" onclick="event.stopPropagation()">'
            + '<div class="cfg-header"><h2>&#128221; Prompt Manager</h2>'
            + '<button class="btn btn-sm" onclick="PA.promptManager.close()" title="Close">&times;</button></div>'
            + '<div class="cfg-body pm-body">'
            + '  <div class="pm-col-list"><div class="pm-col-title">Templates</div>'
            + '    <div id="pmTemplateList" class="pm-list"><div class="empty-msg">Loading…</div></div>'
            + '  </div>'
            + '  <div class="pm-col-edit">'
            + '    <div id="pmEditor"><div class="empty-msg">Select a template to edit</div></div>'
            + '    <div class="pm-extras">'
            + '      <div class="pm-col-title">Global extras</div>'
            + '      <label class="pm-lbl">Global context</label>'
            + '      <textarea id="pmGlobalContext" class="form-input pm-ta" placeholder="Applied to every analysis…"></textarea>'
            + '      <label class="pm-lbl">Global append (additional instructions)</label>'
            + '      <textarea id="pmGlobalAppend" class="form-input pm-ta" placeholder="Appended to every prompt…"></textarea>'
            + '      <div class="pm-actions"><button class="btn btn-sm btn-primary" onclick="PA.promptManager.saveExtras(this)">Save global extras</button>'
            + '      <span id="pmExtrasStatus" class="pm-status"></span></div>'
            + '    </div>'
            + '  </div>'
            + '</div></div>';
        await this._loadTemplates();
        this._loadExtras();
    },

    close: function () {
        var ov = document.getElementById('promptMgrOverlay');
        if (ov) ov.style.display = 'none';
    },

    _loadTemplates: async function () {
        var list = document.getElementById('pmTemplateList');
        try {
            this._templates = await this._get('/templates') || [];
        } catch (e) {
            this._templates = [];
            if (list) list.innerHTML = '<div class="empty-msg">Templates unavailable</div>';
            return;
        }
        this._renderTemplateList();
    },

    // Friendly labels for the per-dialect DB prompts.
    _DB_LABELS: {
        'plsql-oracle-verification': 'Oracle',
        'plsql-mysql-verification': 'MySQL',
        'plsql-sqlserver-verification': 'SQL Server',
        'plsql-db2-verification': 'Db2'
    },

    _groupOf: function (name) {
        if (this._DB_LABELS[name]) return 'db';
        if (name.indexOf('java-') === 0) return 'java';
        return 'other';
    },

    _renderTemplateList: function () {
        var list = document.getElementById('pmTemplateList');
        if (!list) return;
        if (!this._templates.length) {
            list.innerHTML = '<div class="empty-msg">No templates</div>';
            return;
        }
        var self = this;
        // Parser screen = DATABASE prompts only. Java (and the legacy single
        // plsql-verification) belong to the JAR analyzer, not this screen.
        var db = this._templates.filter(function (t) { return self._groupOf(t.name) === 'db'; });

        var itemHtml = function (t) {
            var active = (t.name === self._selected) ? ' active' : '';
            var badge = t.overridden ? '<span class="pm-badge">edited</span>' : '';
            var label = self._DB_LABELS[t.name] || t.name;
            return '<div class="pm-item' + active + '" onclick="PA.promptManager.select(\''
                + self._esc(t.name).replace(/'/g, "\\'") + '\')">'
                + '<span class="pm-item-name">' + self._esc(label) + '</span>' + badge + '</div>';
        };
        if (!db.length) { list.innerHTML = '<div class="empty-msg">No database prompts</div>'; return; }
        list.innerHTML = '<div class="pm-group-title">Database Prompts</div>' + db.map(itemHtml).join('');
    },

    select: function (name) {
        this._selected = name;
        this._renderTemplateList();
        var t = this._templates.filter(function (x) { return x.name === name; })[0];
        if (!t) return;
        var ed = document.getElementById('pmEditor');
        if (!ed) return;
        ed.innerHTML =
            '<div class="pm-col-title">' + this._esc(name)
            + (t.overridden ? ' <span class="pm-badge">overridden</span>' : '') + '</div>'
            + '<textarea id="pmTemplateText" class="form-input pm-ta pm-ta-tall"></textarea>'
            + '<div class="pm-actions">'
            + '<button class="btn btn-sm btn-primary" onclick="PA.promptManager.saveTemplate(this)">Save (permanent)</button>'
            + '<button class="btn btn-sm" onclick="PA.promptManager.resetTemplate(this)">Reset to default</button>'
            + '<span id="pmTemplateStatus" class="pm-status"></span></div>';
        document.getElementById('pmTemplateText').value = (t.current != null ? t.current : t.defaultText) || '';
    },

    saveTemplate: async function (btn) {
        if (!this._selected) return;
        var done = UIBusy.begin(btn, 'Saving…');
        if (!done) return;                       // a save is already in flight
        var ta = document.getElementById('pmTemplateText');
        var st = document.getElementById('pmTemplateStatus');
        try {
            await this._post('/templates/' + encodeURIComponent(this._selected), { text: ta.value });
            if (st) st.textContent = 'Saved';
            this._toast('Template saved (permanent)', 'success');
            var t = this._templates.filter(function (x) { return x.name === PA.promptManager._selected; })[0];
            if (t) { t.current = ta.value; t.overridden = true; }
            this._renderTemplateList();
        } catch (e) {
            if (st) st.textContent = 'Save failed';
            this._toast('Save failed (' + e.message + ')', 'error');
        } finally {
            done();
        }
    },

    resetTemplate: async function (btn) {
        if (!this._selected) return;
        var done = UIBusy.begin(btn, 'Resetting…');
        if (!done) return;
        var st = document.getElementById('pmTemplateStatus');
        try {
            await this._post('/templates/' + encodeURIComponent(this._selected) + '/reset', {});
            this._toast('Reset to default', 'success');
            await this._loadTemplates();
            this.select(this._selected);
        } catch (e) {
            if (st) st.textContent = 'Reset failed';
            this._toast('Reset failed (' + e.message + ')', 'error');
        } finally {
            // select() re-renders these actions, so this releases a node that may already be gone.
            // Release is a no-op in that case, and the fresh button starts un-busy.
            done();
        }
    },

    _loadExtras: async function () {
        var ctx = document.getElementById('pmGlobalContext');
        var app = document.getElementById('pmGlobalAppend');
        try {
            var d = await this._get('/extras') || {};
            if (ctx) ctx.value = d.globalContext || '';
            if (app) app.value = d.globalAppend || '';
        } catch (e) { /* leave empty */ }
    },

    saveExtras: async function (btn) {
        var done = UIBusy.begin(btn, 'Saving…');
        if (!done) return;
        var ctx = document.getElementById('pmGlobalContext');
        var app = document.getElementById('pmGlobalAppend');
        var st = document.getElementById('pmExtrasStatus');
        try {
            await this._post('/extras', {
                globalContext: ctx ? ctx.value : '',
                globalAppend: app ? app.value : ''
            });
            if (st) st.textContent = 'Saved';
            this._toast('Global extras saved', 'success');
        } catch (e) {
            if (st) st.textContent = 'Save failed';
            this._toast('Save failed (' + e.message + ')', 'error');
        } finally {
            done();
        }
    },

    // ======================= PIECE 2: Analysis Context modal =======================

    /**
     * Called by the "Enable Claude" button. Opens the per-analysis context
     * modal, pre-loaded from GET /analysis/{name}. If the modal can't be built
     * for any reason, we fall back to running the verification directly so the
     * existing trigger never breaks.
     */
    openAnalysisContext: function (resume) {
        var name = (PA.claude && PA.claude._analysisName && PA.claude._analysisName())
            || (PA.analysisData && PA.analysisData.name) || null;
        if (!name) {
            // No analysis — defer to the original trigger (it will toast a warning).
            if (PA.claude) PA.claude.startVerification(!!resume);
            return;
        }
        this._ctxAnalysis = name;
        this._ctxResume = !!resume;

        var dialect = (PA.analysisData && (PA.analysisData.dbType || '')) || '';
        dialect = dialect ? dialect.toUpperCase() : '';

        var ov = document.getElementById('analysisCtxOverlay');
        if (!ov) {
            ov = document.createElement('div');
            ov.id = 'analysisCtxOverlay';
            ov.className = 'cfg-overlay';
            ov.onclick = function (e) { if (e.target === ov) PA.promptManager.closeAnalysisContext(); };
            document.body.appendChild(ov);
        }
        ov.style.display = '';
        if (PA.modalStack) ov.style.zIndex = PA.modalStack.nextZ();

        var dialectRow = dialect
            ? '<div class="pm-dialect">Dialect: <b>' + this._esc(dialect) + '</b></div>' : '';

        ov.innerHTML =
            '<div class="cfg-modal pm-modal" style="width:760px;max-width:95vw;height:auto;max-height:86vh" onclick="event.stopPropagation()">'
            + '<div class="cfg-header"><h2>&#129504; Analysis Context — ' + this._esc(name) + '</h2>'
            + '<button class="btn btn-sm" onclick="PA.promptManager.closeAnalysisContext()" title="Close">&times;</button></div>'
            + '<div class="cfg-body pm-ctx-body">'
            + dialectRow
            + '<label class="pm-lbl">Context (free text)</label>'
            + '<textarea id="pmCtxContext" class="form-input pm-ta pm-ta-tall" placeholder="Extra context for this analysis…"></textarea>'
            + '<label class="pm-lbl">Additional instructions (append)</label>'
            + '<textarea id="pmCtxAppend" class="form-input pm-ta" placeholder="Appended to the prompt for this analysis…"></textarea>'
            + '<label class="pm-lbl">Template override (optional)</label>'
            + '<select id="pmCtxTemplateSel" class="form-input" onchange="PA.promptManager._onCtxTemplatePick()">'
            + '<option value="">— none —</option></select>'
            + '<textarea id="pmCtxTemplateText" class="form-input pm-ta pm-ta-tall" style="display:none" '
            + 'placeholder="Override template text for this analysis…"></textarea>'
            + '<div class="pm-actions pm-ctx-actions">'
            + '<button class="btn btn-sm btn-primary" onclick="PA.promptManager.ctxUseAndRun(this)">Use for this analysis &amp; Run</button>'
            + '<button class="btn btn-sm" onclick="PA.promptManager.ctxRunOnly()">Run without saving</button>'
            + '<button class="btn btn-sm" onclick="PA.promptManager.ctxSavePermanent(this)" title="Save the edited template override permanently">Save permanent</button>'
            + '<span id="pmCtxStatus" class="pm-status"></span></div>'
            + '</div></div>';

        this._loadAnalysisContext(name);
    },

    closeAnalysisContext: function () {
        var ov = document.getElementById('analysisCtxOverlay');
        if (ov) ov.style.display = 'none';
    },

    _ctxTemplates: {},

    _loadAnalysisContext: async function (name) {
        // Populate the per-template picker from GET /templates (names), then
        // overlay any per-analysis values from GET /analysis/{name}.
        var sel = document.getElementById('pmCtxTemplateSel');
        try {
            var tpls = await this._get('/templates') || [];
            var self = this;
            if (sel) {
                tpls.filter(function (t) { return self._groupOf(t.name) === 'db'; }).forEach(function (t) {
                    var o = document.createElement('option');
                    o.value = t.name; o.textContent = self._DB_LABELS[t.name] || t.name;
                    sel.appendChild(o);
                });
            }
        } catch (e) { /* picker optional */ }

        try {
            var d = await this._get('/analysis/' + encodeURIComponent(name)) || {};
            var ctx = document.getElementById('pmCtxContext');
            var app = document.getElementById('pmCtxAppend');
            if (ctx) ctx.value = d.context || '';
            if (app) app.value = d.append || '';
            this._ctxTemplates = d.templates || {};
        } catch (e) {
            this._ctxTemplates = {};
        }
    },

    _onCtxTemplatePick: function () {
        var sel = document.getElementById('pmCtxTemplateSel');
        var ta = document.getElementById('pmCtxTemplateText');
        if (!sel || !ta) return;
        var name = sel.value;
        if (!name) { ta.style.display = 'none'; ta.value = ''; return; }
        ta.style.display = '';
        // Prefer an existing per-analysis override; otherwise fetch current template text.
        if (this._ctxTemplates && this._ctxTemplates[name] != null) {
            ta.value = this._ctxTemplates[name];
            return;
        }
        var self = this;
        this._get('/templates').then(function (tpls) {
            var t = (tpls || []).filter(function (x) { return x.name === name; })[0];
            ta.value = t ? (t.current != null ? t.current : t.defaultText) || '' : '';
        }).catch(function () { ta.value = ''; });
    },

    _collectCtxTemplates: function () {
        var sel = document.getElementById('pmCtxTemplateSel');
        var ta = document.getElementById('pmCtxTemplateText');
        var out = {};
        // keep any previously-set overrides
        for (var k in this._ctxTemplates) {
            if (Object.prototype.hasOwnProperty.call(this._ctxTemplates, k)) out[k] = this._ctxTemplates[k];
        }
        if (sel && sel.value && ta) out[sel.value] = ta.value;
        return out;
    },

    ctxUseAndRun: async function (btn) {
        var done = UIBusy.begin(btn, 'Saving…');
        if (!done) return;
        var name = this._ctxAnalysis;
        var st = document.getElementById('pmCtxStatus');
        var ctx = document.getElementById('pmCtxContext');
        var app = document.getElementById('pmCtxAppend');
        try {
            await this._post('/analysis/' + encodeURIComponent(name), {
                context: ctx ? ctx.value : '',
                append: app ? app.value : '',
                templates: this._collectCtxTemplates()
            });
            this._toast('Context saved for this analysis', 'success');
            this.closeAnalysisContext();
            this._runVerification();
        } catch (e) {
            if (st) st.textContent = 'Save failed';
            this._toast('Save failed (' + e.message + ')', 'error');
        } finally {
            done();
        }
    },

    ctxRunOnly: function () {
        this.closeAnalysisContext();
        this._runVerification();
    },

    ctxSavePermanent: async function (btn) {
        var sel = document.getElementById('pmCtxTemplateSel');
        var ta = document.getElementById('pmCtxTemplateText');
        var st = document.getElementById('pmCtxStatus');
        if (!sel || !sel.value) {
            this._toast('Pick a template to save permanently', 'warn');
            return;
        }
        var done = UIBusy.begin(btn, 'Saving…');
        if (!done) return;
        try {
            await this._post('/templates/' + encodeURIComponent(sel.value), { text: ta ? ta.value : '' });
            if (st) st.textContent = 'Saved permanently';
            this._toast('Template saved (permanent)', 'success');
        } catch (e) {
            if (st) st.textContent = 'Save failed';
            this._toast('Save failed (' + e.message + ')', 'error');
        } finally {
            done();
        }
    },

    _runVerification: function () {
        if (PA.claude && PA.claude.startVerification) {
            PA.claude.startVerification(this._ctxResume);
        }
    }
};
