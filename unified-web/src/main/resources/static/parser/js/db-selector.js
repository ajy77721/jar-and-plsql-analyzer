/* ── PA.dbSelector ─────────────────────────────────────────────────────────
   Standalone DB-type pill selector component.
   Usage:
     PA.dbSelector.render(containerEl, currentDbType)  — inject pills into el
     PA.dbSelector.get()                               — returns 'ORACLE'|'MYSQL'|'SQLSERVER'
     PA.dbSelector.set(type)                           — programmatically switch
     PA.dbSelector.onChange(fn)                        — register a listener
────────────────────────────────────────────────────────────────────────── */
window.PA = window.PA || {};

PA.dbSelector = (function () {

    var _selected = 'ORACLE';
    var _listeners = [];
    var _containerId = null;

    var DB_DEFS = [
        {
            key: 'ORACLE',
            label: 'Oracle',
            color: 'var(--red)',
            icon: 'data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iMTQiIGhlaWdodD0iMTQiIHZpZXdCb3g9IjAgMCAxNCAxNCIgZmlsbD0ibm9uZSIgeG1sbnM9Imh0dHA6Ly93d3cudzMub3JnLzIwMDAvc3ZnIj48cmVjdCB3aWR0aD0iMTQiIGhlaWdodD0iMTQiIHJ4PSIzIiBmaWxsPSIjYzAzOTJiIi8+PHBhdGggZD0iTTMgN2E0IDQgMCAxIDAgOCAwIDQgNCAwIDAgMC04IDB6IiBzdHJva2U9IiNmZmYiIHN0cm9rZS13aWR0aD0iMS41IiBmaWxsPSJub25lIi8+PC9zdmc+'
        },
        {
            key: 'MYSQL',
            label: 'MySQL',
            color: 'var(--badge-teal)',
            icon: 'data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iMTQiIGhlaWdodD0iMTQiIHZpZXdCb3g9IjAgMCAxNCAxNCIgZmlsbD0ibm9uZSIgeG1sbnM9Imh0dHA6Ly93d3cudzMub3JnLzIwMDAvc3ZnIj48cmVjdCB3aWR0aD0iMTQiIGhlaWdodD0iMTQiIHJ4PSIzIiBmaWxsPSIjMjE5NmEwIi8+PHBhdGggZD0iTTMgNWg4TTMgN2g4TTMgOWg4IiBzdHJva2U9IiNmZmYiIHN0cm9rZS13aWR0aD0iMS41IiBzdHJva2UtbGluZWNhcD0icm91bmQiLz48L3N2Zz4='
        },
        {
            key: 'SQLSERVER',
            label: 'SQL Server',
            color: 'var(--badge-orange)',
            icon: 'data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iMTQiIGhlaWdodD0iMTQiIHZpZXdCb3g9IjAgMCAxNCAxNCIgZmlsbD0ibm9uZSIgeG1sbnM9Imh0dHA6Ly93d3cudzMub3JnLzIwMDAvc3ZnIj48cmVjdCB3aWR0aD0iMTQiIGhlaWdodD0iMTQiIHJ4PSIzIiBmaWxsPSIjZDA0YTAyIi8+PHBhdGggZD0iTTMgNS41aDhhMSAxIDAgMCAxIDAgM0gzYTEgMSAwIDAgMSAwLTN6IiBzdHJva2U9IiNmZmYiIHN0cm9rZS13aWR0aD0iMS4yIiBmaWxsPSJub25lIi8+PC9zdmc+'
        },
        {
            key: 'DB2',
            label: 'IBM Db2',
            color: 'var(--badge-blue)',
            icon: 'data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iMTQiIGhlaWdodD0iMTQiIHZpZXdCb3g9IjAgMCAxNCAxNCIgZmlsbD0ibm9uZSIgeG1sbnM9Imh0dHA6Ly93d3cudzMub3JnLzIwMDAvc3ZnIj48cmVjdCB3aWR0aD0iMTQiIGhlaWdodD0iMTQiIHJ4PSIzIiBmaWxsPSIjMWY3MGMxIi8+PHBhdGggZD0iTTMuNSA0aDMuNWEzIDMgMCAwIDEgMCA2SDMuNXoiIHN0cm9rZT0iI2ZmZiIgc3Ryb2tlLXdpZHRoPSIxLjIiIGZpbGw9Im5vbmUiLz48L3N2Zz4='
        },
        {
            key: 'POSTGRES',
            label: 'PostgreSQL',
            color: 'var(--badge-blue)',
            icon: 'data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iMTQiIGhlaWdodD0iMTQiIHZpZXdCb3g9IjAgMCAxNCAxNCIgZmlsbD0ibm9uZSIgeG1sbnM9Imh0dHA6Ly93d3cudzMub3JnLzIwMDAvc3ZnIj48cmVjdCB3aWR0aD0iMTQiIGhlaWdodD0iMTQiIHJ4PSIzIiBmaWxsPSIjMzM2NzkxIi8+PGVsbGlwc2UgY3g9IjciIGN5PSI3IiByeD0iMy4yIiByeT0iNCIgc3Ryb2tlPSIjZmZmIiBzdHJva2Utd2lkdGg9IjEuMiIgZmlsbD0ibm9uZSIvPjxwYXRoIGQ9Ik03IDN2OCIgc3Ryb2tlPSIjZmZmIiBzdHJva2Utd2lkdGg9IjEuMiIvPjwvc3ZnPg=='
        }
    ];

    function _notify(newType) {
        for (var i = 0; i < _listeners.length; i++) {
            try { _listeners[i](newType); } catch (e) { /* ignore */ }
        }
    }

    function _selectPill(type) {
        if (_selected === type) return;
        _selected = type;
        if (_containerId) _refresh(_containerId);
        _notify(type);
    }

    function _refresh(containerId) {
        var el = document.getElementById(containerId);
        if (!el) return;
        var pills = el.querySelectorAll('.db-pill');
        for (var i = 0; i < pills.length; i++) {
            var pill = pills[i];
            var isActive = pill.getAttribute('data-db') === _selected;
            pill.classList.toggle('active', isActive);
        }
    }

    function _buildHtml(containerId) {
        var html = '<div class="db-selector" id="' + containerId + '">';
        html += '<span class="db-selector-label">Database</span>';
        for (var i = 0; i < DB_DEFS.length; i++) {
            var d = DB_DEFS[i];
            var active = d.key === _selected ? ' active' : '';
            html += '<button type="button" class="db-pill' + active + '" data-db="' + d.key + '"';
            html += ' onclick="PA.dbSelector.set(\'' + d.key + '\')">';
            html += '<img class="db-pill-icon" src="' + d.icon + '" alt="">';
            html += d.label;
            html += '</button>';
        }
        html += '</div>';
        return html;
    }

    return {
        render: function (el, currentDbType, locked) {
            if (currentDbType) _selected = currentDbType.toUpperCase();
            var id = 'dbSelectorPills';
            _containerId = id;
            if (locked) {
                // Datasource is fixed by the project — show a static badge, no switching.
                el.innerHTML = '<div class="db-selector"><span class="db-selector-label">Database</span>'
                    + PA.dbSelector.badgeHtml(_selected)
                    + '<span class="db-locked-note" title="This project is tied to one datasource">&#128274; fixed by project</span></div>';
            } else {
                el.innerHTML = _buildHtml(id);
            }
        },

        get: function () { return _selected; },

        set: function (type) {
            var t = type ? type.toUpperCase() : 'ORACLE';
            _selectPill(t);
        },

        onChange: function (fn) {
            if (typeof fn === 'function') _listeners.push(fn);
        },

        badgeHtml: function (dbType) {
            var cls = (dbType || 'ORACLE').toLowerCase();
            if (cls === 'sqlserver') cls = 'sqlserver';
            var label = { oracle: 'Oracle', mysql: 'MySQL', sqlserver: 'SQL Server', db2: 'IBM Db2', postgres: 'PostgreSQL' }[cls] || dbType;
            return '<span class="cfg-db-badge ' + cls + '">' + label + '</span>';
        }
    };
}());
