const UserManual = (() => {

    let _overlay = null;
    let _contentArea = null;
    let _navContainer = null;
    let _docs = [];
    let _activeDoc = null;
    let _cache = {};

    // Curated metadata (icon/title/desc/order) for known docs. Any *other* .md returned by
    // /api/docs is appended automatically (see _mergedSections), so new docs bind with no code change.
    const SECTIONS = [
        { id: 'manual-home', icon: '⌂', title: 'Home Dashboard', desc: 'Tool cards, quick submit, queue, progress' },
        { id: 'manual-feature-guide', icon: '🖼️', title: 'Feature Guide (Screenshots)', desc: 'Every screen/tab annotated with numbered button callouts' },
        { id: 'manual-jar-analyzer', icon: '⚙', title: 'JAR Analyzer', desc: 'Endpoints, code structure, summary tabs' },
        { id: 'manual-parser', icon: '⚒', title: 'PL/SQL Parser', desc: 'Call flow, table ops, complexity, source' },
        { id: 'manual-properties', icon: '☰', title: 'Properties', desc: 'Configuration reference & impact' },
        { id: 'manual-architecture', icon: '⊞', title: 'Architecture', desc: 'Modules, pipeline, data flow' },
        { id: 'tech', icon: '📦', title: 'Technology Stack', desc: 'All libraries used — versions, purpose, module' },
        { id: 'jar-parse', icon: '🔬', title: 'JAR Parse — Internal Flow', desc: 'How a JAR upload is parsed, graphed & saved' },
        { id: 'plsql-parse', icon: '🗄️', title: 'PL/SQL Parse — Internal Flow', desc: 'How PL/SQL procedures are fetched, parsed & analyzed' },
        { id: 'databases', icon: '🗃️', title: 'Databases Overview', desc: 'Supported dialects & how source is fetched' },
        { id: 'db-oracle', icon: '🔶', title: 'Oracle Setup', desc: 'Connection, schemas & Oracle-specific notes' },
        { id: 'db-mysql', icon: '🐬', title: 'MySQL Setup', desc: 'Connection & MySQL routine notes' },
        { id: 'db-sqlserver', icon: '🟦', title: 'SQL Server Setup', desc: 'Connection & T-SQL notes' },
        { id: 'db-db2', icon: '🔷', title: 'Db2 Setup', desc: 'Connection & Db2 SQL PL notes' },
        { id: 'tips', icon: '💡', title: 'Tips', desc: 'Usage tips & good-to-know details' }
    ];

    // Built once from SECTIONS + any extra docs the server lists — used for nav & welcome cards.
    function _mergedSections() {
        var known = {};
        var merged = SECTIONS.slice();
        for (var i = 0; i < SECTIONS.length; i++) known[SECTIONS[i].id] = true;
        for (var j = 0; j < _docs.length; j++) {
            var d = _docs[j];
            if (d && d.id && !known[d.id]) {
                merged.push({ id: d.id, icon: '📄', title: d.title || d.id, desc: '' });
                known[d.id] = true;
            }
        }
        return merged;
    }

    // Fetch the server's doc list once so any doc not in SECTIONS still shows up.
    async function _ensureDocs() {
        if (_docs.length) return;
        try {
            var res = await fetch('/api/docs');
            if (res.ok) _docs = await res.json();
        } catch (e) { _docs = []; }
    }

    function init() {
        _buildModal();
    }

    async function open(docId) {
        if (!_overlay) _buildModal();
        _overlay.classList.add('open');
        await _ensureDocs();      // pull the full doc list so all docs are bound in the nav
        _renderNav();
        if (docId) {
            _loadDoc(docId);
        } else {
            _showWelcome();
        }
    }

    function close(e) {
        if (e && e.target !== e.currentTarget && !e.target.classList.contains('um-close-btn')) return;
        if (_overlay) _overlay.classList.remove('open');
    }

    // Full-screen toggle: expands .um-modal to fill the whole viewport (no browser Fullscreen
    // API involved — that would require a user gesture per-element and loses control over the
    // close/Escape flow already wired up here, so this is a pure CSS-class expand instead).
    function toggleFullscreen() {
        if (!_overlay) return;
        var modal = _overlay.querySelector('.um-modal');
        if (!modal) return;
        var isFull = modal.classList.toggle('um-fullscreen');
        var btn = modal.querySelector('.um-fullscreen-btn');
        if (btn) {
            btn.textContent = isFull ? '⤢' : '⛶';
            btn.title = isFull ? 'Exit full screen' : 'Toggle full screen';
        }
    }

    // Download as PDF: uses the browser's native print-to-PDF (no external library / CDN
    // dependency — jspdf/html2pdf were never present in this codebase, see feature audit). A
    // scoped @media print stylesheet (user-manual.css) hides everything except the currently
    // loaded doc's rendered content, so "Save as PDF" in the print dialog produces a clean,
    // readable document rather than a screenshot of the whole modal chrome/sidebar.
    function downloadPdf() {
        // Real bug found live: this used to silently no-op ("return") whenever _activeDoc was
        // null (e.g. user clicks the PDF button straight from the welcome screen, before picking
        // a section) — no feedback at all, which looked exactly like "PDF download not working".
        // Always print whatever is currently shown in .um-content instead (welcome cards included).
        var titleEl = _contentArea.querySelector('h1');
        var docTitle = titleEl ? titleEl.textContent : (_activeDoc || 'User Manual');
        var prevTitle = document.title;
        document.title = docTitle;
        setTimeout(function() {
            window.print();
            document.title = prevTitle;
        }, 50); // let the title swap flush before print grabs it (some browsers snapshot title early)
    }

    function _buildModal() {
        _overlay = document.createElement('div');
        _overlay.className = 'um-overlay';
        _overlay.onclick = function(e) { close(e); };

        var modal = document.createElement('div');
        modal.className = 'um-modal';
        modal.onclick = function(e) { e.stopPropagation(); };

        modal.innerHTML =
            '<div class="um-header">' +
                '<div class="um-header-left">' +
                    '<span class="um-icon">📖</span>' +
                    '<h2>User Manual</h2>' +
                '</div>' +
                '<div class="um-header-actions">' +
                    '<button class="um-action-btn" onclick="UserManual.downloadPdf()" title="Download as PDF">⤓ PDF</button>' +
                    '<button class="um-action-btn um-fullscreen-btn" onclick="UserManual.toggleFullscreen()" title="Toggle full screen">⛶</button>' +
                    '<button class="um-close-btn" onclick="UserManual.close()" title="Close (Esc)">&times;</button>' +
                '</div>' +
            '</div>' +
            '<div class="um-body">' +
                '<div class="um-sidebar">' +
                    '<div class="um-search"><input type="text" placeholder="Search docs..." oninput="UserManual._onSearch(this.value)"></div>' +
                    '<div id="umNav"></div>' +
                '</div>' +
                '<div class="um-content" id="umContent"></div>' +
            '</div>';

        _overlay.appendChild(modal);
        document.body.appendChild(_overlay);

        _contentArea = document.getElementById('umContent');
        _navContainer = document.getElementById('umNav');

        _renderNav();
        _showWelcome();

        document.addEventListener('keydown', function(e) {
            if (e.key === 'Escape' && _overlay.classList.contains('open')) {
                close();
            }
        });
    }

    function _renderNav(filter) {
        var html = '';
        var lf = SearchQuery.norm(filter);
        var list = _mergedSections();
        for (var i = 0; i < list.length; i++) {
            var s = list[i];
            if (lf && s.title.toLowerCase().indexOf(lf) === -1 && s.desc.toLowerCase().indexOf(lf) === -1) continue;
            var active = _activeDoc === s.id ? ' active' : '';
            html += '<button class="um-nav-btn' + active + '" onclick="UserManual._loadDoc(\'' + s.id + '\')">' +
                '<span class="um-nav-icon">' + s.icon + '</span>' + _esc(s.title) +
                '</button>';
        }
        _navContainer.innerHTML = html;
    }

    function _showWelcome() {
        _activeDoc = null;
        _renderNav();
        var html = '<div class="um-welcome">' +
            '<h3>User Manual</h3>' +
            '<p>Complete documentation for the Code Analyzer platform. Select a section from the sidebar or click a card below to get started.</p>' +
            '<div class="um-welcome-cards">';
        var wlist = _mergedSections();
        for (var i = 0; i < wlist.length; i++) {
            var s = wlist[i];
            html += '<div class="um-welcome-card" onclick="UserManual._loadDoc(\'' + s.id + '\')">' +
                '<div class="um-wc-icon">' + s.icon + '</div>' +
                '<div class="um-wc-title">' + _esc(s.title) + '</div>' +
                '<div class="um-wc-desc">' + _esc(s.desc) + '</div>' +
                '</div>';
        }
        html += '</div></div>';
        _contentArea.innerHTML = html;
    }

    async function _loadDoc(id) {
        _activeDoc = id;
        _renderNav();
        _contentArea.innerHTML = '<div class="um-loading">Loading...</div>';

        try {
            var md;
            if (_cache[id]) {
                md = _cache[id];
            } else {
                var res = await fetch('/api/docs/' + id);
                if (!res.ok) throw new Error('Not found');
                md = await res.text();
                _cache[id] = md;
            }
            _contentArea.innerHTML = _renderMarkdown(md);
            _contentArea.scrollTop = 0;
        } catch (e) {
            _contentArea.innerHTML = '<div class="um-loading">Document not available: ' + _esc(e.message) + '</div>';
        }
    }

    function _onSearch(val) {
        _renderNav(val);
        if (val && _activeDoc) {
            var content = _contentArea;
            if (content && val.length >= 2) {
                _highlightText(content, val);
            }
        }
    }

    function _highlightText(el, term) {
        // simple: no DOM highlight, leave as-is for now
    }

    function _renderMarkdown(md) {
        var html = md;

        // raw HTML blocks — lines starting with <div (or <section/figure) at column 0
        // Use depth tracking to handle nested divs correctly
        var htmlBlocks = [];
        var lines = html.split('\n');
        var outLines = [];
        var i = 0;
        while (i < lines.length) {
            if (/^<(div|section|figure)\b/.test(lines[i])) {
                var depth = 0;
                var blockLines = [];
                while (i < lines.length) {
                    var l = lines[i];
                    depth += (l.match(/<(div|section|figure)\b[^>]*>/g) || []).length;
                    depth -= (l.match(/<\/(div|section|figure)>/g) || []).length;
                    blockLines.push(l);
                    i++;
                    if (depth <= 0) break;
                }
                htmlBlocks.push(blockLines.join('\n'));
                outLines.push('\x00HTMLBLOCK' + (htmlBlocks.length - 1) + '\x00');
            } else {
                outLines.push(lines[i]);
                i++;
            }
        }
        html = outLines.join('\n');

        // code blocks (fenced)
        html = html.replace(/```(\w*)\n([\s\S]*?)```/g, function(m, lang, code) {
            return '<pre><code>' + _esc(code.trim()) + '</code></pre>';
        });

        // inline code
        html = html.replace(/`([^`]+)`/g, '<code>$1</code>');

        // headers
        html = html.replace(/^#### (.+)$/gm, '<h4>$1</h4>');
        html = html.replace(/^### (.+)$/gm, '<h3>$1</h3>');
        html = html.replace(/^## (.+)$/gm, '<h2>$1</h2>');
        html = html.replace(/^# (.+)$/gm, '<h1>$1</h1>');

        // hr
        html = html.replace(/^---+$/gm, '<hr>');

        // blockquotes
        html = html.replace(/^> (.+)$/gm, '<blockquote>$1</blockquote>');

        // bold and italic
        html = html.replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>');
        html = html.replace(/\*(.+?)\*/g, '<em>$1</em>');

        // images (before links to avoid conflict)
        html = html.replace(/!\[([^\]]*)\]\(([^)]+)\)/g, '<img src="$2" alt="$1" style="max-width:100%">');

        // links
        html = html.replace(/\[([^\]]+)\]\(([^)]+)\)/g, '<a href="$2">$1</a>');

        // tables
        html = html.replace(/^\|(.+)\|\s*\n\|[-| :]+\|\s*\n((?:\|.+\|\s*\n?)*)/gm, function(m, header, body) {
            var cols = header.split('|').map(function(c) { return c.trim(); }).filter(Boolean);
            var rows = body.trim().split('\n');
            var t = '<table><thead><tr>';
            for (var c = 0; c < cols.length; c++) {
                t += '<th>' + cols[c] + '</th>';
            }
            t += '</tr></thead><tbody>';
            for (var r = 0; r < rows.length; r++) {
                var cells = rows[r].split('|').map(function(c) { return c.trim(); }).filter(Boolean);
                t += '<tr>';
                for (var ci = 0; ci < cells.length; ci++) {
                    t += '<td>' + cells[ci] + '</td>';
                }
                t += '</tr>';
            }
            t += '</tbody></table>';
            return t;
        });

        // unordered lists
        html = html.replace(/^(\s*)-\s+(.+)$/gm, function(m, indent, content) {
            return '<li style="margin-left:' + (indent.length * 8) + 'px">' + content + '</li>';
        });
        html = html.replace(/((?:<li[^>]*>.*<\/li>\s*)+)/g, '<ul>$1</ul>');

        // ordered lists
        html = html.replace(/^(\s*)\d+\.\s+(.+)$/gm, function(m, indent, content) {
            return '<oli style="margin-left:' + (indent.length * 8) + 'px">' + content + '</oli>';
        });
        html = html.replace(/((?:<oli[^>]*>.*<\/oli>\s*)+)/g, function(m, items) {
            return '<ol>' + items.replace(/<\/?oli/g, function(t) { return t.replace('oli', 'li'); }) + '</ol>';
        });

        // paragraphs: wrap standalone lines
        html = html.replace(/^(?!<[a-z])((?!<).+)$/gm, function(m, line) {
            if (line.trim() === '') return '';
            return '<p>' + line + '</p>';
        });

        // clean up empty paragraphs
        html = html.replace(/<p>\s*<\/p>/g, '');

        // restore raw HTML blocks
        html = html.replace(/\x00HTMLBLOCK(\d+)\x00/g, function(m, idx) {
            return htmlBlocks[parseInt(idx)];
        });

        return html;
    }

    function _esc(s) {
        var d = document.createElement('div');
        d.textContent = s || '';
        return d.innerHTML;
    }

    return {
        init: init,
        open: open,
        close: close,
        toggleFullscreen: toggleFullscreen,
        downloadPdf: downloadPdf,
        _loadDoc: _loadDoc,
        _onSearch: _onSearch
    };

})();
