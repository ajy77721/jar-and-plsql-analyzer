/**
 * The left panel: procedures and tables for the current analysis.
 *
 * <h2>What was wrong</h2>
 *
 * The list rendered 50 at a time behind a "Show more" button, which was fine — until you typed in
 * the filter. Filtering called {@code _expandAllLeftItems}, which injected <b>every remaining item</b>
 * into the DOM and then set {@code display:none} on the ones that did not match. So the one action a
 * user takes when the list is too long was also the one action that materialised all of it. The
 * batching protected everything except the case it existed for.
 *
 * <p>Two further problems, both of the kind this project treats as defects rather than polish:
 *
 * <ul>
 *   <li>The header read {@code Procedures (114)} whether or not a filter was active. Narrow the list
 *       to three matches and it still said 114, so a filtered view was indistinguishable from the
 *       whole set.</li>
 *   <li>A flat list of 114 procedures across several schemas, ordered by depth, gave nothing to
 *       navigate by.</li>
 * </ul>
 *
 * <h2>What changed</h2>
 *
 * Filtering now narrows the <i>data</i> and re-renders the first batch of matches, so the DOM never
 * holds more than one batch per group regardless of how large the analysis is. Items are grouped by
 * schema under collapsible headers carrying their own counts, and the header states
 * {@code showing N of M} whenever a filter is hiding anything.
 *
 * <p>Measured on this machine: the largest analysis has 114 procedures and 85 tables. The batching
 * is kept because that is the number this was sized against — it is not a guess, and it is also not
 * evidence about the thousand-procedure case, which no analysis here exercises.
 */

/** Batch size, and the point at which schema grouping starts paying for itself. */
PA._LP_BATCH = 50;
PA._LP_GROUP_MIN = 12;

/**
 * MID-002 — the sidebar-filter decision (threshold-based, resolved 2026-09-07): below this many
 * procedures, filtering stays an instant client-side scan over `PA._lp.proc.all` exactly as before.
 * At or above it, each keystroke instead queries `/procedures?q=...` (debounced), the same route
 * `PA.api.getProcedures()` already fetches the unfiltered list from. 1,000 is chosen as an order of
 * magnitude above the 114-procedure analysis this file's own measurements are based on (see the
 * file doc comment) — comfortably past where an in-memory `.filter()` stops being "instant", while
 * staying well under the counts (8,000+ procedures) this registry treats as its scale targets
 * elsewhere, where every keystroke scanning the whole list in the browser is the wrong default.
 */
PA._LP_SERVER_FILTER_THRESHOLD = 1000;
PA._LP_FILTER_DEBOUNCE_MS = 200;
PA._lpFilterTimer = null;

/** Per-list state. `all` is never mutated by filtering; `shown` is what the DOM reflects. */
PA._lp = {
    proc:  { all: [], shown: [], rendered: 0, collapsed: {} },
    table: { all: [], shown: [], rendered: 0, collapsed: {} }
};

PA.switchLeftTab = function(tab) {
    document.querySelectorAll('.ltab').forEach(function(t) { t.classList.remove('active'); });
    var btn = document.querySelector('.ltab[data-tab="' + tab + '"]');
    if (btn) btn.classList.add('active');
    document.querySelectorAll('.left-panel-list').forEach(function(p) { p.style.display = 'none'; });
    var map = { procedures: 'lpProcedures', tables: 'lpTables' };
    var panel = document.getElementById(map[tab]);
    if (panel) panel.style.display = '';
};

/**
 * Narrows both lists to what matches, then re-renders.
 *
 * <p>Filters the arrays rather than walking the DOM. The old version had to have every item present
 * to hide it, which is why it expanded the list first; nothing needs to exist here in order not to
 * be shown.
 */
PA.filterLeft = function() {
    var el = document.getElementById('leftFilter');
    var q = SearchQuery.norm(el && el.value);
    if (el) SearchQuery.markPadding(el, el.value);
    PA._lpApplyFilter('table', q);
    PA._lpRender('table');

    // Procedures: below the threshold, filter the in-memory copy exactly as before, synchronously.
    // At or above it, debounce and ask the server. UI-024 (§160): checked against
    // `PA.analysisData.totalNodes`, not `PA._lp.proc.all.length` — that array is now deliberately
    // left empty above the threshold (the initial render no longer populates it, see
    // `_renderLeftProcedures`), so its length stopped being a valid proxy for "how many procedures
    // does this analysis have" the moment this fix landed.
    var totalKnown = (PA.analysisData && PA.analysisData.totalNodes) || 0;
    if (totalKnown < PA._LP_SERVER_FILTER_THRESHOLD) {
        PA._lpApplyFilter('proc', q);
        PA._lpRender('proc');
    } else {
        PA._lpServerFilterProc(q);
    }
};

PA._lpApplyFilter = function(kind, q) {
    var st = PA._lp[kind];
    st.shown = !q ? st.all.slice() : st.all.filter(function(x) { return x._q.indexOf(q) !== -1; });
    st.rendered = 0;
};

/**
 * The over-threshold branch of `PA.filterLeft`, and (UI-024, §160) the initial render's own
 * over-threshold branch too: server search, debounced, replacing `shown`.
 *
 * <p>The empty-query short-circuit only fires when `all` is a genuine, already-fully-fetched
 * resident copy — true below the threshold (the array path never left this file), never true above
 * it since §160: `_renderLeftProcedures` no longer populates `all` from `PA.nodeIndex` once the
 * analysis is large enough to cross `PA._LP_SERVER_FILTER_THRESHOLD`, so an empty query up there
 * still has to ask the server for its first page, the same as any other query.
 */
PA._lpServerFilterProc = function(q) {
    if (PA._lpFilterTimer) clearTimeout(PA._lpFilterTimer);
    PA._lpFilterTimer = setTimeout(async function() {
        var st = PA._lp.proc;
        if (!q && st.all.length > 0) {
            st.shown = st.all.slice();
            st.rendered = 0;
            PA._lpRender('proc');
            return;
        }
        var rows = await PA.api.searchProcedures(q, PA._LP_BATCH * 4);
        // PA.api returns the PA.FAILED sentinel on a non-ok response instead of throwing, so
        // without this check a failed search renders as "No matches" — indistinguishable from a
        // query that genuinely matched nothing. Same treatment as statements.js/complexity.js.
        if (PA.failed && PA.failed(rows)) {
            st.shown = [];
            st.rendered = 0;
            var errEl = document.getElementById('lpProcItems');
            if (errEl) {
                errEl.innerHTML = '<div class="empty-msg" style="padding:10px;font-size:11px;color:var(--badge-red,#c0392b)">Search failed</div>';
            }
            return;
        }
        st.shown = rows.map(function(n) {
            n._group = n.schema || '';
            return n;
        });
        st.rendered = 0;
        PA._lpRender('proc');
    }, PA._LP_FILTER_DEBOUNCE_MS);
};

/** Groups by schema, but only once there are enough items for grouping to help. */
PA._lpGroups = function(kind) {
    var st = PA._lp[kind];
    if (st.shown.length < PA._LP_GROUP_MIN) return null;
    var seen = {}, order = [];
    for (var i = 0; i < st.shown.length; i++) {
        var g = st.shown[i]._group || '';
        if (!seen[g]) { seen[g] = []; order.push(g); }
        seen[g].push(st.shown[i]);
    }
    // One group is not a grouping; it would only add a header the user must click past.
    if (order.length < 2) return null;
    order.sort();
    return order.map(function(g) { return { name: g, items: seen[g] }; });
};

PA._lpRender = function(kind) {
    var st = PA._lp[kind];
    var itemsEl = document.getElementById(kind === 'proc' ? 'lpProcItems' : 'lpTableItems');
    var countEl = document.getElementById(kind === 'proc' ? 'lpProcCount' : 'lpTableCount');
    if (!itemsEl) return;

    if (countEl) {
        // The count must say when it is not the whole set. A bare smaller number reads as the total.
        countEl.textContent = st.shown.length === st.all.length
            ? String(st.all.length)
            : st.shown.length + ' of ' + st.all.length;
    }

    if (st.shown.length === 0) {
        itemsEl.innerHTML = '<div class="empty-msg" style="padding:10px;font-size:11px">No matches</div>';
        return;
    }

    var groups = PA._lpGroups(kind);
    if (!groups) { itemsEl.innerHTML = ''; st.rendered = 0; PA._lpRenderBatch(kind); return; }

    var html = '';
    for (var g = 0; g < groups.length; g++) {
        var grp = groups[g];
        var isCollapsed = !!st.collapsed[grp.name];
        html += '<div class="lp-group-head" onclick="PA._lpToggleGroup(\'' + kind + '\',\''
              + PA.escJs(grp.name) + '\')" title="' + PA.escAttr(grp.name || 'no schema') + '">'
              + '<span class="lp-group-caret">' + (isCollapsed ? '▸' : '▾') + '</span>'
              + '<span class="lp-group-name">' + PA.esc(grp.name || '(no schema)') + '</span>'
              + '<span class="lp-group-n">' + grp.items.length + '</span></div>';
        html += '<div class="lp-group-body"' + (isCollapsed ? ' style="display:none"' : '') + '>';
        // Each group gets its own batch, so one huge schema cannot force the others to render.
        var cap = Math.min(grp.items.length, PA._LP_BATCH);
        for (var i = 0; i < cap; i++) html += PA._lpItemHtml(kind, grp.items[i]);
        if (cap < grp.items.length) {
            html += '<div class="lp-show-more" onclick="event.stopPropagation();PA._lpMoreGroup(this,\''
                  + kind + '\',\'' + PA.escJs(grp.name) + '\',' + cap + ')">Show more ('
                  + (grp.items.length - cap) + ' remaining)</div>';
        }
        html += '</div>';
    }
    itemsEl.innerHTML = html;
    st.rendered = st.shown.length;
};

PA._lpToggleGroup = function(kind, name) {
    var st = PA._lp[kind];
    st.collapsed[name] = !st.collapsed[name];
    PA._lpRender(kind);
};

/** Appends the next batch inside one group, leaving the other groups untouched. */
PA._lpMoreGroup = function(btn, kind, name, from) {
    var groups = PA._lpGroups(kind) || [];
    var grp = null;
    for (var i = 0; i < groups.length; i++) if (groups[i].name === name) { grp = groups[i]; break; }
    if (!grp) return;
    var to = Math.min(grp.items.length, from + PA._LP_BATCH);
    var html = '';
    for (var j = from; j < to; j++) html += PA._lpItemHtml(kind, grp.items[j]);
    btn.insertAdjacentHTML('beforebegin', html);
    if (to < grp.items.length) {
        btn.setAttribute('onclick', 'event.stopPropagation();PA._lpMoreGroup(this,\'' + kind
            + '\',\'' + PA.escJs(name) + '\',' + to + ')');
        btn.textContent = 'Show more (' + (grp.items.length - to) + ' remaining)';
    } else {
        btn.remove();
    }
};

/** Flat (ungrouped) batch, for lists too small to be worth grouping. */
PA._lpRenderBatch = function(kind) {
    var st = PA._lp[kind];
    var container = document.getElementById(kind === 'proc' ? 'lpProcItems' : 'lpTableItems');
    if (!container) return;
    var start = st.rendered;
    var end = Math.min(start + PA._LP_BATCH, st.shown.length);
    var html = '';
    for (var i = start; i < end; i++) html += PA._lpItemHtml(kind, st.shown[i]);

    var existing = container.querySelector('.lp-show-more');
    if (existing) existing.remove();
    container.insertAdjacentHTML('beforeend', html);
    st.rendered = end;
    if (end < st.shown.length) {
        container.insertAdjacentHTML('beforeend',
            '<div class="lp-show-more" onclick="PA._lpRenderBatch(\'' + kind + '\')">Show more ('
            + (st.shown.length - end) + ' remaining)</div>');
    }
};

PA._lpItemHtml = function(kind, x) {
    return kind === 'proc' ? PA._renderLeftProcItem(x) : PA._renderLeftTableItem(x);
};

/**
 * UI-024 (§160): threshold-aware, matching the filter path MID-002 (§39) already made so. Below
 * `PA._LP_SERVER_FILTER_THRESHOLD`, unchanged — the array path this file's own header comment was
 * measured against. At or above it, this no longer touches `PA.nodeIndex.all()` at all: painting
 * the first screen used to force the whole node array resident before a single keystroke, the same
 * defect the filter path itself had before MID-002, just one call earlier in the sequence. The
 * total procedure count for the header comes from `PA.analysisData.totalNodes` (already known
 * cheaply since MID-002 dropped `nodes[]` from `/index` — see `api.js`'s own doc comment), never
 * from the array whose residency this fix removes.
 *
 * <p>UI-024 (§165): `async` now, awaiting `PA.nodeIndex.ensureLoaded()` at the top of the
 * below-threshold branch only — the above-threshold branch never touches the array at all, so it
 * has nothing to await. This is what let `app.js`'s own eager `PA.api.getProcedures()` fetch (this
 * function's one real caller, on every analysis load) finally be removed: below threshold, this
 * function is now the thing that triggers the fetch, on first paint, instead of `PA.loadAnalysis`
 * triggering it unconditionally before this function ever runs.
 */
PA._renderLeftProcedures = async function() {
    var panel = document.getElementById('lpProcedures');
    if (!panel) return;
    var totalKnown = (PA.analysisData && PA.analysisData.totalNodes) || 0;

    if (totalKnown >= PA._LP_SERVER_FILTER_THRESHOLD) {
        PA._lp.proc.all = [];      // deliberately never populated at this scale — see doc comment above
        PA._lp.proc.shown = [];
        PA._lp.proc.rendered = 0;

        var htmlOver = '<div class="lp-head">Procedures (<span id="lpProcCount">' + totalKnown + '</span>)</div>';
        if (PA.analysisData._packageAnalysis && PA.analysisData.entryPoint) {
            htmlOver += PA._lpPackageRootHtml();
        }
        htmlOver += '<div id="lpProcItems"></div>';
        panel.innerHTML = htmlOver;
        // Same server round trip an empty filter query already takes over the threshold — the first
        // screen is a page fetch, not free, and that is the honest cost of not holding everything
        // resident. `_lpServerFilterProc('')` skips its own debounce delay having no timer to clear.
        PA._lpServerFilterProc('');
        return;
    }

    await PA.nodeIndex.ensureLoaded();
    if (!PA.nodeIndex.count()) {
        panel.innerHTML = '<div class="empty-msg">No procedures found</div>';
        return;
    }
    var nodes = PA.nodeIndex.all();

    // The filter key is precomputed once per item rather than per keystroke: with 114 procedures and
    // a filter that fires on every input event, rebuilding it each time is work for nothing.
    PA._lp.proc.all = nodes.slice()
        .sort(function(a, b) { return (a.depth || 0) - (b.depth || 0) || (a.name || '').localeCompare(b.name || ''); })
        .map(function(n) {
            n._q = ((n.nodeId || '') + ' ' + (n.name || '') + ' ' + (n.schema || '')).toLowerCase();
            n._group = n.schema || '';
            return n;
        });
    PA._lp.proc.shown = PA._lp.proc.all.slice();
    PA._lp.proc.rendered = 0;

    var html = '<div class="lp-head">Procedures (<span id="lpProcCount">' + nodes.length + '</span>)</div>';

    // Package analysis: a clickable package root so the user can get back to the package-level view
    // after drilling into a procedure.
    if (PA.analysisData._packageAnalysis && PA.analysisData.entryPoint) {
        html += PA._lpPackageRootHtml();
    }

    html += '<div id="lpProcItems"></div>';
    panel.innerHTML = html;
    PA._lpRender('proc');
};

/** Factored out of `_renderLeftProcedures` (UI-024, §160) so both the array and server-page paths
 *  render the identical package-root markup rather than keeping two copies in sync by hand. */
PA._lpPackageRootHtml = function() {
    var pkgId = PA.analysisData.entryPoint;
    var pkgSchema = PA.analysisData.entrySchema || '';
    var pkgColor = PA.getSchemaColor(pkgSchema);
    var html = '<div class="lp-item lp-pkg-root" data-id="' + PA.escAttr(pkgId) + '"'
        + ' data-filter="' + PA.escAttr(pkgId + ' ' + pkgSchema) + '"'
        + ' onclick="PA.showProcedure(\'' + PA.escJs(pkgId) + '\')"'
        + ' title="' + PA.escAttr('Package root: ' + (pkgSchema ? pkgSchema + '.' : '') + pkgId) + '"'
        + ' style="background:var(--bg-hover);border-bottom:2px solid var(--border)">';
    if (pkgSchema) {
        html += '<span class="ct-schema-badge" style="background:' + pkgColor.bg + ';color:' + pkgColor.fg + ';font-size:7px;padding:0 4px;margin-right:2px">' + PA.esc(pkgSchema.substring(0, 4)) + '</span>';
    }
    html += '<span class="lp-icon PKG" style="width:16px;height:16px;font-size:8px" title="PKG=Package">PKG</span>';
    html += '<span class="lp-name" style="font-size:11px;font-weight:600;color:var(--accent)">' + PA.esc(pkgId) + '</span>';
    html += '</div>';
    return html;
};

PA._renderLeftTables = async function() {
    var panel = document.getElementById('lpTables');
    if (!panel) return;
    if (!PA.analysisData) { panel.innerHTML = '<div class="empty-msg">No tables found</div>'; return; }
    try {
        // The list shape, not the full table-ops document: this panel reads four fields and the
        // full document's size is driven by access lines it never looks at. See PA.api.getTableList.
        var tables = await PA.api.getTableList();
        if (!tables || tables.length === 0) {
            panel.innerHTML = '<div class="empty-msg">No tables found</div>';
            return;
        }
        PA._lp.table.all = tables.slice()
            .sort(function(a, b) { return (a.tableName || '').localeCompare(b.tableName || ''); })
            .map(function(t) {
                t._q = ((t.tableName || '') + ' ' + (t.tableSchema || '')).toLowerCase();
                t._group = t.tableSchema || '';
                return t;
            });
        PA._lp.table.shown = PA._lp.table.all.slice();
        PA._lp.table.rendered = 0;

        panel.innerHTML = '<div class="lp-head">Tables (<span id="lpTableCount">'
            + PA._lp.table.all.length + '</span>)</div><div id="lpTableItems"></div>';
        PA._lpRender('table');
    } catch (e) {
        panel.innerHTML = '<div class="empty-msg">Failed to load tables</div>';
    }
};

PA._renderLeftProcItem = function(n) {
    var nodeId = n.nodeId || n.name;
    var name = n.name || nodeId || '?';
    var schema = n.schema || '';
    var objType = n.objectType || '';
    var typeLabel = objType === 'FUNCTION' ? 'F' : objType === 'TRIGGER' ? 'T' : 'P';
    var depth = n.depth != null ? n.depth : 0;
    var loc = n.linesOfCode || 0;
    var colorObj = PA.getSchemaColor(schema);

    var tip = name;
    if (schema) tip = schema + '.' + tip;
    if (n.lineStart) tip += ' | Lines ' + n.lineStart + '-' + (n.lineEnd || '?');
    if (loc) tip += ' | ' + loc + ' LOC';
    tip += ' | Depth ' + depth;

    var html = '<div class="lp-item" data-id="' + PA.escAttr(nodeId) + '" data-filter="' + PA.escAttr(nodeId + ' ' + name + ' ' + schema) + '"';
    html += ' onclick="PA.showProcedure(\'' + PA.escJs(nodeId) + '\')" title="' + PA.escAttr(tip) + '">';
    if (schema) {
        html += '<span class="ct-schema-badge" style="background:' + colorObj.bg + ';color:' + colorObj.fg + ';font-size:7px;padding:0 4px;margin-right:2px" data-tip="Owner schema" title="Schema/owner of this object">' + PA.esc(schema.substring(0, 4)) + '</span>';
    }
    var nodeTypeTooltip = typeLabel === 'P' ? 'P=Procedure' : typeLabel === 'F' ? 'F=Function' : typeLabel === 'T' ? 'T=Trigger' : typeLabel;
    html += '<span class="lp-icon ' + typeLabel + '" style="width:16px;height:16px;font-size:8px" title="' + nodeTypeTooltip + '">' + typeLabel + '</span>';
    html += '<span class="lp-name" style="font-size:11px">' + PA.esc(name) + '</span>';
    html += '<span class="lp-badge" data-tip="Call depth level">d' + depth + '</span>';
    if (loc > 0) html += '<span class="lp-badge" data-tip="Lines of code" style="color:var(--text-muted)">' + loc + '</span>';
    // "scheduled" badge — this procedure is the target of a discovered scheduled job.
    if (n.scheduled) html += '<span class="lp-badge" data-tip="Scheduled job target" title="This procedure is run by a scheduled job" style="background:var(--badge-purple-bg);color:var(--badge-purple)">&#9200;</span>';
    html += '</div>';
    return html;
};PA._renderLeftTableItem = function(t) {
    var ops = (t.operations || []).map(function(op) {
        return '<span class="op-badge ' + op + '" style="font-size:7px;padding:0 3px">' + op[0] + '</span>';
    }).join('');
    var count = t.accessCount || 0;

    // Same objectType the Tables tab badge uses (tableOps.js _renderRow) — kept in sync so a
    // view/materialized view is never mislabeled "T" (table) in this list.
    var objType = t.objectType || 'TABLE';
    var isMv = objType === 'MATERIALIZED VIEW' || objType === 'MATERIALIZED_VIEW';
    // TEMP (the fourth table kind) takes precedence over the objectType badge: a temp table is
    // session-scoped and not persisted, which the reader should see at a glance.
    var isTemp = t.kind === 'TEMP' || t.tableKind === 'TEMP';
    var typeCls = isTemp ? 'temp' : objType === 'VIEW' ? 'view' : isMv ? 'mv' : 'table';
    var typeLabel = isTemp ? 'TMP' : objType === 'VIEW' ? 'V' : isMv ? 'MV' : 'T';
    var typeTip = isTemp
        ? 'TEMP' + (t.tempScope ? ' — ' + t.tempScope : '')
            + ' — session-scoped, not persisted. Modernization note: replace with application-memory '
            + 'or the target stack\'s own temp handling.'
        : objType === 'VIEW' ? 'VIEW' : isMv ? 'MATERIALIZED VIEW' : 'TABLE';

    var html = '<div class="lp-item" data-filter="' + PA.escAttr(t.tableName || '') + '" onclick="PA.tableDetail.open(\'' + PA.escJs(t.tableName || '') + '\', \'\')" style="cursor:pointer">';
    html += '<span class="to-type-badge ' + typeCls + '" style="font-size:7px;min-width:12px;text-align:center" title="' + typeTip + '">' + typeLabel + '</span>';
    html += '<span class="lp-name" style="color:var(--teal,var(--badge-teal));font-size:11px">' + PA.esc(t.tableName || '') + '</span>';
    // var html = '<div class="lp-item" data-filter="' + PA.escAttr(t.tableName || '') + '" onclick="PA.tableDetail.open(\'' + PA.escJs(t.tableName || '') + '\', \'\')" style="cursor:pointer">';
    // html += '<span style="font-size:7px;font-weight:700;color:var(--text-muted);min-width:12px;text-align:center">T</span>';
    // html += '<span class="lp-name" style="color:var(--teal,var(--badge-teal));font-size:11px">' + PA.esc(t.tableName || '') + '</span>';
    html += '<span style="display:flex;gap:1px;align-items:center">' + ops + '</span>';
    html += '<span class="lp-badge">' + count + '</span>';
    html += '</div>';
    return html;
};