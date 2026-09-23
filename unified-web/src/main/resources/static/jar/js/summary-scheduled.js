/**
 * Summary — Scheduled Jobs sub-tab.
 * Scans analysis.classes for methods annotated with @Scheduled
 * and displays cron/fixedRate/fixedDelay configuration.
 */
window.JA = window.JA || {};
JA.summary = JA.summary || {};

Object.assign(JA.summary, {

    _renderScheduledTab(analysis, esc) {
        const jobs = [];

        if (analysis.scheduledJobs && analysis.scheduledJobs.length) {
            for (const sj of analysis.scheduledJobs) {
                const attrs = sj.attributes || {};
                jobs.push({
                    className: sj.className,
                    simpleName: sj.simpleName,
                    packageName: sj.packageName || '',
                    methodName: sj.methodName,
                    stereotype: sj.stereotype || 'OTHER',
                    returnType: sj.returnType,
                    cron: attrs.cron || null,
                    fixedRate: attrs.fixedRate != null ? String(attrs.fixedRate) : null,
                    fixedDelay: attrs.fixedDelay != null ? String(attrs.fixedDelay) : null,
                    initialDelay: attrs.initialDelay != null ? String(attrs.initialDelay) : null,
                    zone: attrs.zone || null,
                    sourceJar: sj.sourceJar
                });
            }
        } else {
            for (const cls of (analysis.classes || [])) {
                for (const method of (cls.methods || [])) {
                    for (const ann of (method.annotations || [])) {
                        if (ann.name === 'Scheduled') {
                            const attrs = ann.attributes || {};
                            jobs.push({
                                className: cls.fullyQualifiedName,
                                simpleName: cls.simpleName,
                                packageName: cls.packageName || '',
                                methodName: method.name,
                                stereotype: cls.stereotype || 'OTHER',
                                returnType: method.returnType,
                                cron: attrs.cron || null,
                                fixedRate: attrs.fixedRate != null ? String(attrs.fixedRate) : null,
                                fixedDelay: attrs.fixedDelay != null ? String(attrs.fixedDelay) : null,
                                initialDelay: attrs.initialDelay != null ? String(attrs.initialDelay) : null,
                                zone: attrs.zone || null,
                                sourceJar: cls.sourceJar
                            });
                        }
                    }
                }
            }
        }

        const enabledClasses = analysis.enableSchedulingClasses || [];
        if (!enabledClasses.length) {
            for (const cls of (analysis.classIndex || analysis.classes || [])) {
                for (const ann of (cls.annotations || [])) {
                    if (ann.name === 'EnableScheduling') {
                        enabledClasses.push(cls.simpleName);
                    }
                }
            }
        }

        if (!jobs.length) {
            let html = '<div class="sum-section" style="padding:30px">';
            html += '<div class="sum-section-title">Scheduled Jobs</div>';
            html += '<p class="sum-muted">No @Scheduled methods detected in this JAR.</p>';
            if (enabledClasses.length) {
                html += '<p class="sum-muted">@EnableScheduling found on: ' + enabledClasses.map(c => '<code>' + esc(c) + '</code>').join(', ') + '</p>';
            }
            html += '</div>';
            return html;
        }

        const byClass = {};
        for (const job of jobs) {
            (byClass[job.simpleName] = byClass[job.simpleName] || []).push(job);
        }

        const cronJobs = jobs.filter(j => j.cron);
        const rateJobs = jobs.filter(j => j.fixedRate);
        const delayJobs = jobs.filter(j => j.fixedDelay);

        let html = '<div class="sum-section">';
        html += '<div class="sum-section-title">Scheduled Jobs (' + jobs.length + ')</div>';
        html += '<div class="sum-section-desc">Methods annotated with <code>@Scheduled</code> &mdash; cron, fixed-rate, and fixed-delay tasks.</div>';
        html += '<div class="sum-tip-bar">';
        html += '<span class="sum-tip" title="Cron jobs run on a schedule (e.g., every midnight). After verticalisation, each cron job must be owned by exactly one service — duplicate execution across replicas needs @SchedulerLock or similar.">Cron jobs need single-instance execution after splitting (use @SchedulerLock)</span>';
        html += '<span class="sum-tip" title="fixedRate/fixedDelay tasks run continuously at intervals. In a multi-instance deployment, these execute on EVERY instance unless guarded. May cause duplicate processing or resource contention.">fixedRate/fixedDelay run on every instance — guard against duplicates</span>';
        html += '<span class="sum-tip" title="Scheduled tasks that access collections from other domains create hidden coupling. These should either be moved to the owning domain or converted to event-driven patterns.">Cross-domain collection access in scheduled tasks = hidden coupling</span>';
        html += '</div>';

        // Stats
        html += '<div class="sched-stats">';
        html += '<span class="sum-chip"><b>' + jobs.length + '</b> Total Jobs</span>';
        if (cronJobs.length) html += '<span class="sum-chip sched-chip-cron"><b>' + cronJobs.length + '</b> Cron</span>';
        if (rateJobs.length) html += '<span class="sum-chip sched-chip-rate"><b>' + rateJobs.length + '</b> Fixed Rate</span>';
        if (delayJobs.length) html += '<span class="sum-chip sched-chip-delay"><b>' + delayJobs.length + '</b> Fixed Delay</span>';
        html += '<span class="sum-chip"><b>' + Object.keys(byClass).length + '</b> Classes</span>';
        if (enabledClasses.length) html += '<span class="sum-chip sched-chip-enabled"><b>' + enabledClasses.length + '</b> @EnableScheduling</span>';
        html += '</div>';

        // Grouped by class
        const classNames = Object.keys(byClass).sort();
        for (const className of classNames) {
            const classJobs = byClass[className];
            const first = classJobs[0];
            const badgeCls = JA.utils.stereotypeBadgeClass(first.stereotype);

            html += '<div class="sched-class-card">';
            html += '<div class="sched-class-header">';
            html += '<span class="badge ' + badgeCls + '" style="font-size:9px">' + first.stereotype.replace('REST_', '') + '</span> ';
            html += '<strong>' + esc(className) + '</strong>';
            html += '<span class="sched-pkg">' + esc(first.packageName) + '</span>';
            if (first.sourceJar) html += '<span class="sum-module-tag">' + esc(first.sourceJar.replace(/\.jar$/i, '').replace(/-\d+\.\d+.*$/, '')) + '</span>';
            html += '</div>';

            html += '<div class="sched-method-list">';
            for (const job of classJobs) {
                const type = job.cron ? 'CRON' : job.fixedRate ? 'FIXED_RATE' : job.fixedDelay ? 'FIXED_DELAY' : 'TRIGGER';
                const schedule = job.cron || (job.fixedRate ? this._formatMs(job.fixedRate) : (job.fixedDelay ? this._formatMs(job.fixedDelay) : ''));
                const initDelay = job.initialDelay ? this._formatMs(job.initialDelay) : null;

                html += '<div class="sched-method-row">';
                html += '<span class="sched-type sched-type-' + type.toLowerCase() + '">' + type.replace('_', ' ') + '</span>';
                html += '<code class="sched-method-name">' + esc(job.methodName) + '()</code>';
                if (schedule) {
                    html += '<code class="sched-expr">' + esc(schedule) + '</code>';
                }
                if (job.cron) {
                    const desc = this._describeCron(job.cron);
                    if (desc) html += '<span class="sched-cron-desc">' + esc(desc) + '</span>';
                }
                if (initDelay) html += '<span class="sched-init-delay">initial: ' + esc(initDelay) + '</span>';
                if (job.zone) html += '<span class="sched-zone">' + esc(job.zone) + '</span>';
                html += '</div>';
            }
            html += '</div></div>';
        }

        html += '</div>';
        return html;
    },

    _formatMs(val) {
        const ms = parseInt(val, 10);
        if (isNaN(ms)) return val + 'ms';
        if (ms < 1000) return ms + 'ms';
        if (ms < 60000) return (ms / 1000) + 's';
        if (ms < 3600000) return (ms / 60000) + 'min';
        return (ms / 3600000) + 'h';
    },

    _describeCron(expr) {
        if (!expr || expr.startsWith('$')) return null;
        const parts = expr.trim().split(/\s+/);
        if (parts.length < 5) return null;

        // Spring cron: sec min hour day month weekday
        const [sec, min, hour, day, month, weekday] = parts;

        if (sec === '0' && min === '0' && hour === '0' && day === '*' && month === '*') return 'Every day at midnight';
        if (sec === '0' && min === '0' && day === '*' && month === '*' && weekday === '*') return 'Every day at ' + hour + ':00';
        if (sec === '0' && day === '*' && month === '*' && weekday === '*') return 'Daily at ' + hour + ':' + min.padStart(2, '0');
        if (min.includes('/')) return 'Every ' + min.split('/')[1] + ' minutes';
        if (sec.includes('/')) return 'Every ' + sec.split('/')[1] + ' seconds';
        if (hour.includes('/')) return 'Every ' + hour.split('/')[1] + ' hours';
        return null;
    }
});
