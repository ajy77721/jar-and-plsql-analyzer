/**
 * Error detail modal — shows parse errors from analysis index.
 * PA.errorModal.show() / PA.errorModal.close()
 */
PA.errorModal = (function() {
    var overlayEl = null;

    function getErrors() {
        var data = PA.analysisData;
        return (data && data.errors && data.errors.length) ? data.errors : [];
    }

    /** Extract procedure name from error text — typically in brackets like [PKG.PROC] */
    function extractProc(text) {
        var m = text.match(/\[([^\]]+)\]/);
        return m ? m[1] : '';
    }

    /** Detect severity from text keywords */
    function detectSeverity(text) {
        var lower = (text || '').toLowerCase();
        if (lower.indexOf('warn') !== -1) return 'warn';
        return 'error';
    }

    /** Group errors by source procedure */
    function groupByProc(errors) {
        var groups = {};
        var order = [];
        for (var i = 0; i < errors.length; i++) {
            var err = typeof errors[i] === 'string' ? errors[i] : (errors[i].message || String(errors[i]));
            var proc = extractProc(err) || '(General)';
            if (!groups[proc]) { groups[proc] = []; order.push(proc); }
            groups[proc].push(err);
        }
        return { groups: groups, order: order };
    }

    function renderCard(errText) {
        var sev = detectSeverity(errText);
        var proc = extractProc(errText);
        var msg = errText;
        var cls = sev === 'warn' ? 'err-card err-warn' : 'err-card';
        var icon = sev === 'warn' ? '!' : '✘';

        var html = '<div class="' + cls + '">';
        html += '<span class="err-card-icon">' + icon + '</span>';
        html += '<div class="err-card-body">';
        if (proc) html += '<div class="err-card-proc">' + PA.esc(proc) + '</div>';
        html += '<div class="err-card-msg">' + PA.esc(msg) + '</div>';
        html += '</div></div>';
        return html;
    }

    function show() {
        var errors = getErrors();
        if (!errors.length) { PA.toast('No errors to display', 'success'); return; }

        // Remove existing overlay if open
        close();

        var grouped = groupByProc(errors);
        var bodyHtml = '';

        if (grouped.order.length === 1 && grouped.order[0] === '(General)') {
            // No grouping needed — all general errors
            for (var i = 0; i < errors.length; i++) {
                var err = typeof errors[i] === 'string' ? errors[i] : (errors[i].message || String(errors[i]));
                bodyHtml += renderCard(err);
            }
        } else {
            for (var g = 0; g < grouped.order.length; g++) {
                var proc = grouped.order[g];
                var items = grouped.groups[proc];
                bodyHtml += '<div class="err-group-header">' + PA.esc(proc) +
                    '<span class="err-group-count">(' + items.length + ')</span></div>';
                for (var j = 0; j < items.length; j++) {
                    bodyHtml += renderCard(items[j]);
                }
            }
        }

        var html = '<div class="err-modal-overlay" id="errModalOverlay">';
        html += '<div class="err-modal" onclick="event.stopPropagation()">';
        html += '<div class="err-modal-header">';
        html += '<span class="err-modal-title">Parse Errors (' + errors.length + ')</span>';
        html += '<button class="err-modal-close" onclick="PA.errorModal.close()" title="Close (Esc)">&times;</button>';
        html += '</div>';
        html += '<div class="err-modal-body">' + bodyHtml + '</div>';
        html += '</div></div>';

        var wrapper = document.createElement('div');
        wrapper.innerHTML = html;
        overlayEl = wrapper.firstChild;

        // Close on overlay click
        overlayEl.addEventListener('click', function(e) {
            if (e.target === overlayEl) close();
        });

        document.body.appendChild(overlayEl);

        // Close on Escape
        document.addEventListener('keydown', onKeydown);
    }

    function close() {
        if (overlayEl && overlayEl.parentNode) {
            overlayEl.parentNode.removeChild(overlayEl);
        }
        overlayEl = null;
        document.removeEventListener('keydown', onKeydown);
    }

    function onKeydown(e) {
        if (e.key === 'Escape') close();
    }

    return { show: show, close: close };
})();
