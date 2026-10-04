/*
 * The accurate progress renderer — one place that turns a QueueJob's `stages` into the stage list,
 * the per-stage N/M, the current-stage indicator and the "~2 min remaining" ETA.
 *
 * The server's progress engine (QueueJob/ProgressMath/ProgressEstimator) ships every stage of a
 * job — including the ones that used to hide in the background: sidecar warm-up, estate facets,
 * entity ingest, decompile. Before this file each screen scraped `currentStep` and a single
 * `progressPercent`; now they all render the same honest stage list.
 *
 * Pure string building (no DOM access) so it is trivially unit-testable and safe on any screen.
 */
(function () {
    'use strict';

    var PA = window.PA = window.PA || {};

    function esc(v) {
        if (PA.esc) return PA.esc(v);
        return String(v == null ? '' : v)
            .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
    }

    /** A short, human ETA: "~2 min remaining", "~45s remaining", "<1s remaining". */
    function etaText(ms) {
        if (ms == null || !isFinite(ms) || ms <= 0) return '';
        var s = ms / 1000;
        if (s < 1) return '<1s remaining';
        if (s < 60) return '~' + Math.ceil(s) + 's remaining';
        var m = s / 60;
        if (m < 60) return '~' + Math.ceil(m) + ' min remaining';
        var h = Math.floor(m / 60), rem = Math.round(m % 60);
        return '~' + h + 'h' + (rem ? ' ' + rem + 'm' : '') + ' remaining';
    }

    function stageIcon(state) {
        switch (state) {
            case 'COMPLETE': return '✓';
            case 'RUNNING': return '▶';
            case 'SKIPPED': return '⊘';
            case 'FAILED': return '✕';
            default: return '○';
        }
    }

    function stageColor(state) {
        switch (state) {
            case 'COMPLETE': return 'var(--badge-green, #2e7d32)';
            case 'RUNNING': return 'var(--blue, #1565c0)';
            case 'FAILED': return 'var(--badge-red, #c62828)';
            case 'SKIPPED': return 'var(--text-muted, #888)';
            default: return 'var(--text-muted, #888)';
        }
    }

    /** "N/M" for a sized stage, or '' when the stage has no unit count. */
    function countText(stage) {
        if (!stage || !stage.total || stage.total <= 0) return '';
        return stage.done + '/' + stage.total;
    }

    /**
     * A one-line status: "Stage 4/9 · ~2 min remaining", plus the honest early-access hint while
     * background stages still run.
     */
    function summary(job) {
        if (!job) return '';
        var stages = job.stages || [];
        var total = stages.length;
        var done = 0;
        for (var i = 0; i < total; i++) {
            if (stages[i].state === 'COMPLETE' || stages[i].state === 'SKIPPED') done++;
        }
        var parts = [];
        if (total > 0) parts.push('Stage ' + done + '/' + total);
        var eta = etaText(job.etaMs);
        if (eta && job.status === 'RUNNING') parts.push(eta);
        if (job.readyToBrowse && job.status === 'RUNNING' && job.remainingStages > 0) {
            parts.push('ready to browse — polishing in background (' + job.remainingStages
                + ' stage' + (job.remainingStages === 1 ? '' : 's') + ' left)');
        }
        return parts.join(' · ');
    }

    /**
     * The full stage list. `opts.compact` drops the per-stage ETA (for a dense queue row);
     * otherwise each running/pending stage shows its own remaining estimate.
     */
    function render(job, opts) {
        opts = opts || {};
        var stages = (job && job.stages) || [];
        if (!stages.length) return '';
        var html = '<div class="pa-stages" style="margin-top:4px;display:flex;flex-direction:column;gap:2px">';
        var sum = summary(job);
        if (sum) {
            html += '<div class="pa-stages-summary" style="font-size:.68rem;color:var(--text-muted)">'
                + esc(sum) + '</div>';
        }
        for (var i = 0; i < stages.length; i++) {
            var s = stages[i];
            var color = stageColor(s.state);
            var weight = (s.state === 'RUNNING') ? '600' : '400';
            var count = countText(s);
            var stageEta = (!opts.compact && s.etaMs > 0 && s.state !== 'COMPLETE' && s.state !== 'SKIPPED')
                ? etaText(s.etaMs) : '';
            html += '<div class="pa-stage" data-stage="' + esc(s.id) + '" data-state="' + esc(s.state)
                + '" style="display:flex;align-items:center;gap:6px;font-size:.68rem;color:'
                + (s.state === 'PENDING' ? 'var(--text-muted)' : 'var(--text)') + '">'
                + '<span style="color:' + color + ';flex-shrink:0;width:12px;text-align:center">'
                + stageIcon(s.state) + '</span>'
                + '<span style="flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-weight:'
                + weight + '">' + esc(s.label || s.id) + '</span>'
                + (count ? '<span style="flex-shrink:0;color:var(--text-muted)">' + esc(count) + '</span>' : '')
                + (stageEta ? '<span style="flex-shrink:0;color:var(--blue,#1565c0)">' + esc(stageEta) + '</span>' : '')
                + '</div>';
        }
        html += '</div>';
        return html;
    }

    PA.progressStages = {
        etaText: etaText,
        summary: summary,
        render: render,
        stageIcon: stageIcon
    };
})();
