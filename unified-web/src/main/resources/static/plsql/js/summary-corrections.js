/**
 * Summary Corrections — Claude Corrections sub-tab for PL/SQL analyzer.
 * Shows Claude verification results: CONFIRMED, REMOVED (false positive), NEW (missed).
 * Loads data from PA.claude.result or fetches from API.
 */
window.PA = window.PA || {};
PA.summary = PA.summary || {};

Object.assign(PA.summary, {

    _correctionData: null,

    async _loadCorrectionData() {
        if (PA.claude && PA.claude.result) {
            this._correctionData = PA.claude.result;
        } else {
            try {
                const name = PA.analysisData ? PA.analysisData.name : null;
                if (name) {
                    this._correctionData = await PA.api.getClaudeResultByName(name);
                } else {
                    this._correctionData = await PA.api.getClaudeResult();
                }
            } catch (e) {
                this._correctionData = null;
            }
        }
    },

    _renderCorrectionsTab() {
        const esc = PA.esc;
        const data = this._correctionData;

        let html = '<div style="padding:16px 24px">';

        // Status header
        html += '<div style="display:flex;align-items:center;gap:8px;margin-bottom:16px">';
        html += '<span class="badge" style="background:var(--badge-purple-bg);color:var(--badge-purple);font-size:11px;padding:4px 10px">CORRECTIONS</span>';
        html += '<span style="font-weight:700;font-size:14px">Table & Operation Corrections</span>';
        html += '<button class="sum-btn" onclick="PA.summary._refreshCorrections()" style="margin-left:auto;font-size:11px">Refresh</button>';
        html += '</div>';

        if (!data || !data.tables || !data.tables.length) {
            html += '<div style="padding:24px;background:var(--bg-card);border:1px solid var(--border);border-radius:12px;text-align:center">';
            html += '<div style="font-size:40px;margin-bottom:12px;opacity:0.3">&#128269;</div>';
            html += '<div style="font-size:16px;font-weight:600;color:var(--text);margin-bottom:8px">No Correction Data Available</div>';
            html += '<div style="font-size:13px;color:var(--text-muted);max-width:500px;margin:0 auto;line-height:1.6">';
            html += 'Run Claude AI verification from the "Claude AI" tab first, then come back here to see corrections summary.';
            html += '</div></div>';
            html += '</div>';
            return html;
        }

        // Compute stats from VerificationResult structure
        let confirmed = data.confirmedCount || 0;
        let removed = data.removedCount || 0;
        let newOps = data.newCount || 0;
        let total = confirmed + removed + newOps;
        const tableStats = [];
        for (const t of data.tables) {
            const ops = t.claudeVerifications || [];
            let tConfirm = 0, tRemoved = 0, tNew = 0;
            for (const op of ops) {
                if (op.status === 'CONFIRMED') tConfirm++;
                else if (op.status === 'REMOVED') tRemoved++;
                else if (op.status === 'NEW') tNew++;
            }
            if (total === 0) { total += ops.length; confirmed += tConfirm; removed += tRemoved; newOps += tNew; }
            tableStats.push({
                tableName: t.tableName,
                overallStatus: t.overallStatus || '',
                confirmed: tConfirm, removed: tRemoved, new: tNew,
                total: ops.length, operations: ops
            });
        }

        // Summary stats bar
        html += '<div style="display:flex;gap:12px;margin-bottom:16px;flex-wrap:wrap">';
        html += `<span class="dh-stat"><span class="dh-stat-value">${total}</span><span class="dh-stat-label">Total Operations</span></span>`;
        html += `<span class="dh-stat" style="border-left:3px solid var(--green)"><span class="dh-stat-value" style="color:var(--green)">${confirmed}</span><span class="dh-stat-label">Confirmed</span></span>`;
        html += `<span class="dh-stat" style="border-left:3px solid var(--red)"><span class="dh-stat-value" style="color:var(--red)">${removed}</span><span class="dh-stat-label">Removed (False +)</span></span>`;
        html += `<span class="dh-stat" style="border-left:3px solid var(--accent)"><span class="dh-stat-value" style="color:var(--accent)">${newOps}</span><span class="dh-stat-label">New (Missed)</span></span>`;

        const accuracy = total > 0 ? Math.round((confirmed / (confirmed + removed + newOps)) * 100) : 0;
        html += `<span class="dh-stat" style="border-left:3px solid var(--purple,#a855f7)"><span class="dh-stat-value">${accuracy}%</span><span class="dh-stat-label">Accuracy</span></span>`;
        html += '</div>';

        // Filter pills
        html += '<div style="display:flex;align-items:center;gap:6px;margin-bottom:12px">';
        html += '<span style="font-size:11px;color:var(--text-muted);font-weight:600">FILTER:</span>';
        html += '<span class="op-filter-pill active" data-corr-filter="ALL" onclick="PA.summary._corrFilter(\'ALL\')" style="cursor:pointer">ALL</span>';
        if (removed > 0) html += '<span class="op-filter-pill active" data-corr-filter="REMOVED" onclick="PA.summary._corrFilter(\'REMOVED\')" style="cursor:pointer;background:#fee2e2;color:#b91c1c">REMOVED</span>';
        if (newOps > 0) html += '<span class="op-filter-pill active" data-corr-filter="NEW" onclick="PA.summary._corrFilter(\'NEW\')" style="cursor:pointer;background:#dbeafe;color:#1d4ed8">NEW</span>';
        html += '<span class="op-filter-pill active" data-corr-filter="CONFIRMED" onclick="PA.summary._corrFilter(\'CONFIRMED\')" style="cursor:pointer;background:#dcfce7;color:#15803d">CONFIRMED</span>';
        html += '</div>';

        // Paginated corrections table
        html += this._buildFilterBar('sum-corr', tableStats, () => null);
        html += '<div class="pagination-bar" id="sum-corr-pager-top"></div>';
        html += '<div style="overflow:auto;flex:1">';
        html += '<table class="to-table">';
        html += '<thead><tr>';
        html += '<th data-sort-col="0" onclick="PA.summary._pageSort(\'sum-corr\',0)">Table</th>';
        html += '<th data-sort-col="1" onclick="PA.summary._pageSort(\'sum-corr\',1)">Status</th>';
        html += '<th data-sort-col="2" onclick="PA.summary._pageSort(\'sum-corr\',2)">Confirmed</th>';
        html += '<th data-sort-col="3" onclick="PA.summary._pageSort(\'sum-corr\',3)">Removed</th>';
        html += '<th data-sort-col="4" onclick="PA.summary._pageSort(\'sum-corr\',4)">New</th>';
        html += '<th data-sort-col="5" onclick="PA.summary._pageSort(\'sum-corr\',5)">Total Ops</th>';
        html += '<th>Operations</th>';
        html += '</tr></thead>';
        html += '<tbody id="sum-corr-tbody"></tbody>';
        html += '</table></div>';
        html += '<div class="pagination-bar" id="sum-corr-pager"></div>';

        // Init pagination
        this._initPage('sum-corr', tableStats, 25,
            (ts, i, esc) => {
                const statusBg = ts.overallStatus === 'CONFIRMED' ? 'var(--badge-green-bg)' : ts.removed > 0 ? 'var(--badge-red-bg)' : 'var(--badge-orange-bg)';
                const statusColor = ts.overallStatus === 'CONFIRMED' ? 'var(--badge-green)' : ts.removed > 0 ? 'var(--badge-red)' : 'var(--badge-orange)';
                let row = '<tr class="to-row">';
                row += `<td><strong style="font-family:var(--font-mono)">${esc(ts.tableName)}</strong></td>`;
                row += `<td><span class="badge" style="background:${statusBg};color:${statusColor}">${esc(ts.overallStatus || (ts.removed > 0 ? 'MODIFIED' : 'CONFIRMED'))}</span></td>`;
                row += `<td style="color:var(--green);font-weight:700">${ts.confirmed}</td>`;
                row += `<td style="color:var(--red);font-weight:700">${ts.removed}</td>`;
                row += `<td style="color:var(--accent);font-weight:700">${ts.new}</td>`;
                row += `<td>${ts.total}</td>`;
                row += '<td>';
                for (const op of ts.operations.slice(0, 6)) {
                    row += `<span class="op-badge ${op.operation || ''}" style="font-size:9px">${esc(op.operation || '?')}: ${esc(op.status || '?')}</span> `;
                }
                if (ts.operations.length > 6) row += '<span style="font-size:9px;color:var(--text-muted)">+' + (ts.operations.length - 6) + ' more</span>';
                row += '</td>';
                row += '</tr>';
                return row;
            },
            () => null,
            null,
            {
                sortKeys: [
                    { fn: ts => ts.tableName },
                    { fn: ts => ts.overallStatus },
                    { fn: ts => ts.confirmed },
                    { fn: ts => ts.removed },
                    { fn: ts => ts.new },
                    { fn: ts => ts.total }
                ]
            }
        );
        setTimeout(() => {
            this._pageRender('sum-corr');
            this._initColFilters('sum-corr', {
                1: { label: 'Status', valueFn: ts => ts.overallStatus || (ts.removed > 0 ? 'MODIFIED' : 'CONFIRMED') }
            });
        }, 0);

        // Summary text
        if (data.summary) {
            html += `<div style="margin-top:12px;padding:10px 14px;background:var(--bg-card);border:1px solid var(--border);border-radius:8px;font-size:12px;color:var(--text-muted);line-height:1.5">${esc(data.summary)}</div>`;
        }

        html += '</div>';
        return html;
    },

    async _refreshCorrections() {
        await this._loadCorrectionData();
        const el = document.getElementById('sum-corr');
        if (el) el.innerHTML = this._renderCorrectionsTab();
    }
});
