/**
 * PA.sourceView — Source code viewer with PL/SQL syntax highlighting.
 * Supports: full package view, focused proc view, clickable line numbers, copy proc section.
 */
window.PA = window.PA || {};

const PLSQL_KEYWORDS = new Set([
    'CREATE', 'OR', 'REPLACE', 'PACKAGE', 'BODY', 'PROCEDURE', 'FUNCTION',
    'IS', 'AS', 'BEGIN', 'END', 'DECLARE', 'EXCEPTION', 'WHEN', 'THEN',
    'ELSE', 'ELSIF', 'IF', 'LOOP', 'WHILE', 'FOR', 'IN', 'OUT', 'NOCOPY',
    'RETURN', 'RETURNING', 'INTO', 'BULK', 'COLLECT', 'FORALL',
    'CURSOR', 'OPEN', 'FETCH', 'CLOSE', 'EXIT',
    'SELECT', 'INSERT', 'UPDATE', 'DELETE', 'MERGE', 'FROM', 'WHERE',
    'AND', 'NOT', 'NULL', 'SET', 'VALUES', 'JOIN', 'LEFT',
    'RIGHT', 'INNER', 'OUTER', 'ON', 'GROUP', 'BY', 'ORDER', 'HAVING',
    'UNION', 'ALL', 'EXISTS', 'BETWEEN', 'LIKE', 'DISTINCT',
    'COMMIT', 'ROLLBACK', 'SAVEPOINT', 'PRAGMA', 'AUTONOMOUS_TRANSACTION',
    'RAISE', 'RAISE_APPLICATION_ERROR', 'EXECUTE', 'IMMEDIATE',
    'TYPE', 'SUBTYPE', 'RECORD', 'TABLE', 'INDEX', 'OF', 'REF',
    'VARCHAR2', 'NUMBER', 'INTEGER', 'BOOLEAN', 'DATE', 'TIMESTAMP',
    'CLOB', 'BLOB', 'ROWTYPE', 'NOTFOUND', 'FOUND', 'ROWCOUNT',
    'NO_DATA_FOUND', 'TOO_MANY_ROWS', 'OTHERS', 'SQLERRM', 'SQLCODE',
    'TRUE', 'FALSE', 'DEFAULT', 'CONSTANT', 'TRIGGER', 'BEFORE', 'AFTER',
    'INSTEAD', 'EACH', 'ROW', 'STATEMENT', 'USING', 'CONTINUE', 'GOTO',
    'PIPE', 'CASE', 'LIMIT', 'SAVE', 'EXCEPTIONS', 'EDITIONABLE', 'NONEDITIONABLE'
]);

PA.sourceView = {
    _currentSourceFile: null,
    _currentContent: null,

    /**
     * Open source in the Source tab.
     * If PA.currentDetail has startLine/endLine, shows focused proc view + package-level vars.
     */
    async open(sourceFile, lineNumber) {
        // Clean up any previous chunked renderer scroll listener
        if (PA.sourceView._chunkCleanup) { PA.sourceView._chunkCleanup(); PA.sourceView._chunkCleanup = null; }
        // Don't call switchRightTab here — caller already did or this is called FROM switchRightTab
        const parts = (sourceFile || '').split('.');
        let data = null;

        if (parts.length >= 2) {
            data = await PA.api.getSource(parts[0], parts[1]);
        }

        if (!data || !data.content) {
            document.getElementById('srcPath').textContent = sourceFile || 'Unknown';
            document.getElementById('srcContainer').innerHTML =
                '<div class="empty-msg">Source not available for: ' + PA.esc(sourceFile || '') + '</div>';
            return;
        }

        PA.sourceView._currentSourceFile = sourceFile;
        PA.sourceView._currentContent = data.content;

        const displayName = (data.owner || '') + '.' + (data.objectName || sourceFile);

        // Check if we have proc boundaries to show focused view
        const detail = PA.currentDetail;
        const startLine = detail ? (detail.startLine || 0) : 0;
        const endLine = detail ? (detail.endLine || 0) : 0;
        const procName = detail ? (detail.name || '') : '';

        if (startLine > 0 && endLine > 0 && endLine > startLine) {
            // Focused view: show proc section + package-level variables
            document.getElementById('srcPath').innerHTML =
                `<span style="color:var(--text-muted)">${PA.esc(displayName)}</span>` +
                `<span style="margin:0 6px;opacity:0.5">&rsaquo;</span>` +
                `<span style="color:var(--accent);font-weight:700">${PA.esc(procName)}</span>` +
                `<span style="margin-left:8px;font-size:10px;color:var(--text-muted)">Lines ${startLine}-${endLine}</span>` +
                `<button class="btn btn-sm" style="margin-left:12px;font-size:10px" onclick="PA.sourceView.showFullPackage()" title="Switch to full package source code view">Show Full Package</button>` +
                `<button class="btn btn-sm" style="margin-left:4px;font-size:10px" onclick="PA.sourceView.copyProcSource()" title="Copy only this procedure's source code to clipboard">Copy Proc</button>`;
            PA.sourceView.renderFocused(data.content, startLine, endLine, sourceFile);
        } else {
            // Full package view
            document.getElementById('srcPath').innerHTML =
                `<span>${PA.esc(displayName)}</span>` +
                `<button class="btn btn-sm" style="margin-left:12px;font-size:10px" onclick="PA.sourceView.copyFullSource()" title="Copy the entire package source code to clipboard">Copy All</button>`;
            PA.sourceView.renderContent(data.content, lineNumber);
        }
    },

    /** Render focused proc view: package-level declarations + proc body */
    renderFocused(content, startLine, endLine, sourceFile) {
        const container = document.getElementById('srcContainer');
        const lines = content.split('\n');
        const prefix = 'src-ln-' + Date.now() + '-';

        // Extract package-level variable declarations (lines before first PROCEDURE/FUNCTION)
        let pkgVarEnd = 0;
        for (let i = 0; i < lines.length && i < startLine - 1; i++) {
            const trimmed = lines[i].trim().toUpperCase();
            if (trimmed.match(/^\s*(PROCEDURE|FUNCTION)\s+/)) {
                pkgVarEnd = i;
                break;
            }
            pkgVarEnd = i + 1;
        }

        // Find actual package header end (after IS/AS)
        let headerEnd = 0;
        for (let i = 0; i < Math.min(pkgVarEnd, 30); i++) {
            const trimmed = lines[i].trim().toUpperCase();
            if (trimmed === 'IS' || trimmed === 'AS' || trimmed.endsWith(' IS') || trimmed.endsWith(' AS')) {
                headerEnd = i + 1;
                break;
            }
        }

        let html = '<div class="source-code">';

        // Show package-level declarations if they exist and are meaningful
        const hasVars = pkgVarEnd > headerEnd + 1;
        if (hasVars) {
            html += `<div class="src-section-label">Package Variables & Types</div>`;
            for (let i = headerEnd; i < pkgVarEnd; i++) {
                const ln = i + 1;
                const highlighted = PA.sourceView.highlightLine(lines[i]);
                html += `<div class="src-line dim" id="${prefix}${ln}">`;
                html += `<span class="src-ln clickable" onclick="PA.codeModal.openAtLine('${PA.escJs(sourceFile)}', ${ln})">${ln}</span>`;
                html += `<span class="src-code">${highlighted}</span>`;
                html += '</div>';
            }
            html += `<div class="src-section-divider"></div>`;
        }

        // Show procedure body (the focused section)
        const procLineCount = endLine - startLine + 1;
        html += `<div class="src-section-label" style="color:var(--accent)">Procedure Body (Lines ${startLine}-${endLine}, ${procLineCount.toLocaleString()} lines)</div>`;

        // For large procs (2000+ lines), use chunked rendering for the body
        const from = Math.max(0, startLine - 1);
        const to = Math.min(lines.length, endLine);
        if (procLineCount > PA.sourceView.CHUNK_THRESHOLD) {
            // Render initial chunk around startLine, expand on scroll
            const chunkSize = PA.sourceView.CHUNK_SIZE;
            const initTo = Math.min(to, from + chunkSize);
            const lineHeight = 19;

            html += `<div class="src-chunked-body" style="position:relative">`;
            html += `<div class="src-chunk-info">${procLineCount.toLocaleString()} lines — chunked rendering</div>`;
            html += `<div id="${prefix}body-top" style="height:0"></div>`;
            html += `<div id="${prefix}body-lines"></div>`;
            html += `<div id="${prefix}body-bot" style="height:${(to - initTo) * lineHeight}px"></div>`;
            html += `</div></div>`;
            container.innerHTML = html;

            // Render initial lines
            const bodyDiv = document.getElementById(prefix + 'body-lines');
            const bodyState = {
                lines, sf: sourceFile, prefix, lineNumber: startLine, lineHeight, totalLines: lines.length,
                rangeFrom: from, rangeTo: to,
                renderedFrom: from, renderedTo: initTo,
            };
            let bodyHtml = '';
            for (let i = from; i < initTo; i++) {
                const ln = i + 1;
                const isStart = ln === startLine;
                const highlighted = PA.sourceView.highlightLine(lines[i]);
                bodyHtml += `<div class="src-line${isStart ? ' highlight' : ''}" id="${prefix}${ln}" style="height:${lineHeight}px">`;
                bodyHtml += `<span class="src-ln clickable${isStart ? ' active' : ''}" onclick="PA.codeModal.openAtLine('${PA.escJs(sourceFile)}', ${ln})">${ln}</span>`;
                bodyHtml += `<span class="src-code">${highlighted}</span>`;
                bodyHtml += '</div>';
            }
            bodyDiv.innerHTML = bodyHtml;

            // Scroll handler for focused chunked body
            const botSpacer = document.getElementById(prefix + 'body-bot');
            const scrollParent = container.closest('.right-content') || container.parentElement || container;
            let scrollTick = false;
            const buffer = PA.sourceView.CHUNK_BUFFER;

            const onScroll = () => {
                if (scrollTick) return;
                scrollTick = true;
                requestAnimationFrame(() => {
                    scrollTick = false;
                    if (bodyState.renderedTo >= to) {
                        scrollParent.removeEventListener('scroll', onScroll);
                        return;
                    }
                    const bodyRect = bodyDiv.getBoundingClientRect();
                    const parentRect = scrollParent.getBoundingClientRect();
                    const distToBottom = bodyRect.bottom - parentRect.bottom;
                    if (distToBottom < 500) {
                        // Expand downward
                        const expandTo = Math.min(to, bodyState.renderedTo + chunkSize);
                        let moreHtml = '';
                        for (let i = bodyState.renderedTo; i < expandTo; i++) {
                            const ln = i + 1;
                            const highlighted = PA.sourceView.highlightLine(lines[i]);
                            moreHtml += `<div class="src-line" id="${prefix}${ln}" style="height:${lineHeight}px">`;
                            moreHtml += `<span class="src-ln clickable" onclick="PA.codeModal.openAtLine('${PA.escJs(sourceFile)}', ${ln})">${ln}</span>`;
                            moreHtml += `<span class="src-code">${highlighted}</span>`;
                            moreHtml += '</div>';
                        }
                        bodyDiv.insertAdjacentHTML('beforeend', moreHtml);
                        bodyState.renderedTo = expandTo;
                        botSpacer.style.height = ((to - expandTo) * lineHeight) + 'px';
                    }
                });
            };
            scrollParent.addEventListener('scroll', onScroll, { passive: true });
            PA.sourceView._chunkCleanup = () => scrollParent.removeEventListener('scroll', onScroll);
        } else {
            // Small proc: render all lines at once
            for (let i = from; i < to; i++) {
                const ln = i + 1;
                const isStart = ln === startLine;
                const highlighted = PA.sourceView.highlightLine(lines[i]);
                html += `<div class="src-line${isStart ? ' highlight' : ''}" id="${prefix}${ln}">`;
                html += `<span class="src-ln clickable${isStart ? ' active' : ''}" onclick="PA.codeModal.openAtLine('${PA.escJs(sourceFile)}', ${ln})">${ln}</span>`;
                html += `<span class="src-code">${highlighted}</span>`;
                html += '</div>';
            }
            html += '</div>';
            container.innerHTML = html;
        }

        // Scroll to start
        setTimeout(() => {
            const el = document.getElementById(prefix + startLine);
            if (el) el.scrollIntoView({ behavior: 'smooth', block: 'start' });
        }, 150);
    },

    /** Switch to full package view */
    showFullPackage() {
        if (PA.sourceView._currentContent) {
            const sf = PA.sourceView._currentSourceFile || '';
            const displayName = sf;
            document.getElementById('srcPath').innerHTML =
                `<span>${PA.esc(displayName)}</span>` +
                `<button class="btn btn-sm" style="margin-left:12px;font-size:10px" onclick="PA.sourceView.copyFullSource()" title="Copy the entire package source code to clipboard">Copy All</button>`;
            const startLine = PA.currentDetail ? (PA.currentDetail.startLine || 0) : 0;
            PA.sourceView.renderContent(PA.sourceView._currentContent, startLine);
        }
    },

    /** Copy only the current proc's source lines */
    copyProcSource() {
        if (!PA.sourceView._currentContent || !PA.currentDetail) return;
        const lines = PA.sourceView._currentContent.split('\n');
        const from = Math.max(0, (PA.currentDetail.startLine || 1) - 1);
        const to = Math.min(lines.length, PA.currentDetail.endLine || lines.length);
        const procText = lines.slice(from, to).join('\n');
        navigator.clipboard.writeText(procText).then(() => PA.toast('Proc source copied', 'success'));
    },

    /** Copy full package source */
    copyFullSource() {
        if (!PA.sourceView._currentContent) return;
        navigator.clipboard.writeText(PA.sourceView._currentContent).then(() => PA.toast('Full source copied', 'success'));
    },

    renderContent(content, lineNumber) {
        const container = document.getElementById('srcContainer');
        PA.sourceView.renderContentTo(container, content, lineNumber);
    },

    /**
     * Threshold: files with more lines than this use chunked rendering.
     * Below this threshold we render all at once (faster for small files).
     */
    CHUNK_THRESHOLD: 2000,
    /** Lines to render per chunk in chunked mode */
    CHUNK_SIZE: 500,
    /** Extra lines above/below viewport to pre-render */
    CHUNK_BUFFER: 200,

    /** Render source into any container element (used by code modal too) */
    renderContentTo(container, content, lineNumber, sourceFile) {
        const lines = content.split('\n');
        const sf = sourceFile || PA.sourceView._currentSourceFile || '';

        // Small files: render all at once (fast path)
        if (lines.length <= PA.sourceView.CHUNK_THRESHOLD) {
            PA.sourceView._renderAllLines(container, lines, lineNumber, sf);
            return;
        }

        // Large files: chunked rendering — only render visible window
        PA.sourceView._renderChunked(container, lines, lineNumber, sf);
    },

    /** Fast path: render all lines at once for small files */
    _renderAllLines(container, lines, lineNumber, sf) {
        const prefix = 'src-ln-' + Date.now() + '-';
        let html = '<div class="source-code">';
        for (let i = 0; i < lines.length; i++) {
            const ln = i + 1;
            const isHL = lineNumber && ln === lineNumber;
            const highlighted = PA.sourceView.highlightLine(lines[i]);
            html += `<div class="src-line${isHL ? ' highlight' : ''}" id="${prefix}${ln}">`;
            html += `<span class="src-ln clickable${isHL ? ' active' : ''}" onclick="PA.codeModal.openAtLine('${PA.escJs(sf)}', ${ln})">${ln}</span>`;
            html += `<span class="src-code">${highlighted}</span>`;
            html += '</div>';
        }
        html += '</div>';
        container.innerHTML = html;

        if (lineNumber && lineNumber > 0) {
            setTimeout(() => {
                const el = document.getElementById(prefix + lineNumber);
                if (el) {
                    el.scrollIntoView({ behavior: 'smooth', block: 'center' });
                    el.classList.add('highlight-pulse');
                    setTimeout(() => el.classList.remove('highlight-pulse'), 2000);
                }
            }, 150);
        }
    },

    /**
     * Chunked rendering for large files (2000+ lines).
     * Renders an initial window around the target line, then expands on scroll.
     * Uses a spacer div to maintain correct scroll position/height.
     */
    _renderChunked(container, lines, lineNumber, sf) {
        const totalLines = lines.length;
        const lineHeight = 19; // px per line (matches .src-line height)
        const prefix = 'src-chn-' + Date.now() + '-';
        const chunkSize = PA.sourceView.CHUNK_SIZE;
        const buffer = PA.sourceView.CHUNK_BUFFER;

        // State for this chunked view
        const state = {
            lines, sf, prefix, lineNumber, lineHeight, totalLines,
            renderedFrom: 0,  // first rendered line index (0-based)
            renderedTo: 0,    // last rendered line index (exclusive)
        };

        // Determine initial render window: center on lineNumber or start
        const targetIdx = (lineNumber && lineNumber > 0) ? lineNumber - 1 : 0;
        const initFrom = Math.max(0, targetIdx - chunkSize);
        const initTo = Math.min(totalLines, targetIdx + chunkSize);

        // Build container: spacer-top + rendered lines + spacer-bottom
        const totalHeight = totalLines * lineHeight;
        container.innerHTML =
            `<div class="source-code src-chunked" style="position:relative">` +
            `<div class="src-chunk-info">${totalLines.toLocaleString()} lines — chunked rendering</div>` +
            `<div id="${prefix}top" style="height:${initFrom * lineHeight}px"></div>` +
            `<div id="${prefix}lines"></div>` +
            `<div id="${prefix}bot" style="height:${(totalLines - initTo) * lineHeight}px"></div>` +
            `</div>`;

        const linesDiv = document.getElementById(prefix + 'lines');
        const topSpacer = document.getElementById(prefix + 'top');
        const botSpacer = document.getElementById(prefix + 'bot');

        // Render initial chunk
        linesDiv.innerHTML = PA.sourceView._buildLinesHtml(state, initFrom, initTo);
        state.renderedFrom = initFrom;
        state.renderedTo = initTo;

        // Scroll to target line
        if (lineNumber && lineNumber > 0) {
            setTimeout(() => {
                const el = document.getElementById(prefix + lineNumber);
                if (el) {
                    el.scrollIntoView({ behavior: 'smooth', block: 'center' });
                    el.classList.add('highlight-pulse');
                    setTimeout(() => el.classList.remove('highlight-pulse'), 2000);
                }
            }, 100);
        }

        // Scroll handler: expand rendered range when user scrolls near edges
        const scrollParent = container.closest('.right-content') || container.parentElement || container;
        let scrollTick = false;

        const onScroll = () => {
            if (scrollTick) return;
            scrollTick = true;
            requestAnimationFrame(() => {
                scrollTick = false;
                const rect = container.getBoundingClientRect();
                const viewTop = scrollParent.scrollTop || 0;
                const viewHeight = scrollParent.clientHeight || rect.height;

                // Which line range is visible?
                const codeEl = container.querySelector('.source-code');
                if (!codeEl) return;
                const codeTop = codeEl.getBoundingClientRect().top - rect.top + viewTop;
                const visibleFromPx = Math.max(0, viewTop - codeTop);
                const visibleFrom = Math.floor(visibleFromPx / lineHeight);
                const visibleTo = Math.ceil((visibleFromPx + viewHeight) / lineHeight);

                // Need to expand?
                const needFrom = Math.max(0, visibleFrom - buffer);
                const needTo = Math.min(totalLines, visibleTo + buffer);

                let changed = false;

                // Expand upward
                if (needFrom < state.renderedFrom) {
                    const expandFrom = Math.max(0, needFrom);
                    const expandTo = state.renderedFrom;
                    const html = PA.sourceView._buildLinesHtml(state, expandFrom, expandTo);
                    linesDiv.insertAdjacentHTML('afterbegin', html);
                    state.renderedFrom = expandFrom;
                    topSpacer.style.height = (expandFrom * lineHeight) + 'px';
                    changed = true;
                }

                // Expand downward
                if (needTo > state.renderedTo) {
                    const expandFrom = state.renderedTo;
                    const expandTo = Math.min(totalLines, needTo);
                    const html = PA.sourceView._buildLinesHtml(state, expandFrom, expandTo);
                    linesDiv.insertAdjacentHTML('beforeend', html);
                    state.renderedTo = expandTo;
                    botSpacer.style.height = ((totalLines - expandTo) * lineHeight) + 'px';
                    changed = true;
                }
            });
        };

        scrollParent.addEventListener('scroll', onScroll, { passive: true });

        // Store cleanup ref so we can remove listener on next render
        PA.sourceView._chunkCleanup = () => {
            scrollParent.removeEventListener('scroll', onScroll);
        };
    },

    /** Build HTML for a range of lines [from, to) — used by chunked renderer */
    _buildLinesHtml(state, from, to) {
        let html = '';
        for (let i = from; i < to; i++) {
            const ln = i + 1;
            const isHL = state.lineNumber && ln === state.lineNumber;
            const highlighted = PA.sourceView.highlightLine(state.lines[i]);
            html += `<div class="src-line${isHL ? ' highlight' : ''}" id="${state.prefix}${ln}" style="height:${state.lineHeight}px">`;
            html += `<span class="src-ln clickable${isHL ? ' active' : ''}" onclick="PA.codeModal.openAtLine('${PA.escJs(state.sf)}', ${ln})">${ln}</span>`;
            html += `<span class="src-code">${highlighted}</span>`;
            html += '</div>';
        }
        return html;
    },

    highlightLine(line) {
        let s = line.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
        if (s.trimStart().startsWith('--')) return `<span class="cmt">${s}</span>`;
        s = s.replace(/'([^']*)'/g, '<span class="str">\'$1\'</span>');
        s = s.replace(/\b(\d+\.?\d*)\b/g, '<span class="num">$1</span>');
        s = s.replace(/\b([A-Z_]+)\b/g, (m) => PLSQL_KEYWORDS.has(m) ? `<span class="kw">${m}</span>` : m);
        return s;
    }
};
