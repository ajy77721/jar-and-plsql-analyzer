window.PA = window.PA || {};

/*
 * Scheduled Jobs — the estate facet's fourth screen.
 *
 * One row per scheduled job discovered by the crawler from the dialect's scheduler catalog:
 * Oracle DBMS_SCHEDULER / DBMS_JOB, SQL Server Agent, pg_cron or MySQL EVENT. Each row carries the
 * schedule, the enabled flag and the procedure the job runs — the target is clickable and opens
 * that procedure, because a scheduled job is the root of the call tree, not a leaf.
 *
 * Server-paged over `/jobs` (search `q`, sort by name/schema/type/schedule/target/enabled). An
 * analysis with no scheduler catalog returns an empty page, which the empty state explains.
 */
PA.estateJobs = {
    data: [],
    _src: null,

    async load() {
        PA.estateJobs._src = PA.api.jobsPageSource({});
        PA.estateJobs._src.setParam('sort', 'NAME_ASC');
        PA.estateJobs._initTable();
    },

    reset() {
        PA.estateJobs.data = [];
    },

    /** Cross-estate link target: open Details → Scheduled and filter to this job. */
    showJob: function(job) {
        if (!job) return;
        if (PA.switchRightTab) PA.switchRightTab('details');
        if (PA.estateTab && PA.estateTab.switchSub) PA.estateTab.switchSub('jobs');
        var el = document.getElementById('jbSearch');
        if (el) el.value = job;
        var src = PA.estateJobs._src;
        if (src) { src.setParam('q', job); if (PA.tf) PA.tf._restart('jb'); }
    },

    _initTable: function() {
        var container = document.getElementById('jbContainer');
        if (!container) return;

        var html = '<table class="to-table"><thead><tr>';
        html += '<th data-sort-col="0" onclick="PA.tf.sort(\'jb\',0)">Job</th>';
        html += '<th data-sort-col="1" onclick="PA.tf.sort(\'jb\',1)">Type</th>';
        html += '<th data-sort-col="2" onclick="PA.tf.sort(\'jb\',2)">Schedule</th>';
        html += '<th data-sort-col="3" onclick="PA.tf.sort(\'jb\',3)">Enabled</th>';
        html += '<th data-sort-col="4" onclick="PA.tf.sort(\'jb\',4)">Target procedure</th>';
        html += '</tr></thead><tbody id="jb-tbody"></tbody></table>';
        container.innerHTML = html;

        PA.tf.init('jb', PA.estateJobs._src, 50, PA.estateJobs._renderRow, {
            serverSorts: {
                0: { asc: 'NAME_ASC', desc: 'NAME_DESC' },
                1: { asc: 'TYPE_ASC', desc: 'TYPE_DESC' },
                2: { asc: 'SCHEDULE_ASC', desc: 'SCHEDULE_DESC' },
                3: { asc: 'ENABLED_ASC', desc: 'ENABLED_DESC' },
                4: { asc: 'TARGET_ASC', desc: 'TARGET_DESC' }
            },
            onFilter: function() {
                PA.estateJobs._updateCounts();
                PA.estateJobs._maybeEmpty();
            }
        });
        var st = PA.tf.state('jb');
        if (st) { st.sortCol = 0; st.sortDir = 'asc'; }
        PA.tf._updateSortIndicators('jb');
    },

    onSearch: function() {
        var el = document.getElementById('jbSearch');
        PA.estateJobs._src.setParam('q', el ? el.value : '');
        PA.tf._restart('jb');
    },

    clearFilters: function() {
        var el = document.getElementById('jbSearch');
        if (el) el.value = '';
        var src = PA.estateJobs._src;
        if (src) src.setParam('q', '');
        PA.tf._restart('jb');
    },

    _isFiltered: function() {
        var src = PA.estateJobs._src;
        return !!(src && src.param('q'));
    },

    _updateCounts: function() {
        var st = PA.tf.state('jb');
        var totalEl = document.getElementById('jbTotalCount');
        if (!totalEl || !st) return;
        var total = st.totalIsExact ? st.serverTotal : -1;
        totalEl.textContent = (total >= 0 ? total : (st.filtered ? st.filtered.length : 0))
            + (PA.estateJobs._isFiltered() ? ' matched' : ' jobs');
    },

    _maybeEmpty: function() {
        var st = PA.tf.state('jb');
        var tb = document.getElementById('jb-tbody');
        if (!st || !tb) return;
        if (st.loading || st.loadError) return;
        if (st.totalIsExact && st.serverTotal === 0) {
            tb.innerHTML = '<tr><td colspan="20" class="empty-msg" style="padding:24px;text-align:center">'
                + (PA.estateJobs._isFiltered()
                    ? 'No scheduled jobs match the current filter.'
                    : 'No scheduled jobs in this analysis. Scheduled jobs are discovered from the '
                      + 'scheduler catalog (DBMS_SCHEDULER / DBMS_JOB, SQL Server Agent, pg_cron, MySQL EVENT).')
                + '</td></tr>';
        }
    },

    _renderRow: function(j, idx, esc) {
        var type = j.type || 'JOB';
        var enabled = (j.enabled === true) ? 'YES' : (j.enabled === false ? 'NO' : '—');
        var target = j.target
            ? '<span class="est-usedby-proc" onclick="event.stopPropagation();PA.showProcedure(\''
              + PA.escJs(j.targetNodeId || '') + '\')">' + esc(j.target) + '</span>'
            : '<span style="color:var(--text-muted)">—</span>';
        var html = '<tr class="to-row">';
        html += '<td><span style="font-weight:600;color:var(--badge-purple);font-family:var(--font-mono)">' + esc(j.name || '') + '</span>'
            + (j.schema ? ' <span style="color:var(--text-muted);font-size:11px">' + esc(j.schema) + '</span>' : '') + '</td>';
        html += '<td><span class="est-badge">' + esc(type) + '</span></td>';
        html += '<td style="font-family:var(--font-mono);font-size:11px">' + esc(j.schedule || '—') + '</td>';
        html += '<td>' + enabled + '</td>';
        html += '<td>' + target + '</td>';
        html += '</tr>';
        return html;
    }
};
