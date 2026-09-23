/**
 * Injects a fixed "Sign Out" button into any page that loads this script.
 * Also redirects to /login if any API response comes back 401.
 */
(function () {
    function logout() {
        fetch('/api/auth/logout', { method: 'POST' })
            .catch(() => {})
            .finally(() => { window.location.href = '/login'; });
    }

    function injectBtn() {
        const btn = document.createElement('button');
        btn.textContent = 'Sign Out';
        btn.title = 'Sign out';
        Object.assign(btn.style, {
            position: 'fixed',
            top: '12px',
            right: '16px',
            zIndex: '9998',
            background: 'var(--sidebar-hover)',
            border: 'none',
            color: 'var(--sidebar-text)',
            padding: '5px 14px',
            borderRadius: '6px',
            fontSize: '12px',
            fontWeight: '600',
            cursor: 'pointer',
            fontFamily: "'Segoe UI', system-ui, sans-serif",
            letterSpacing: '0.02em',
            transition: 'background 0.15s',
        });
        btn.onmouseover = () => { btn.style.background = 'var(--sidebar-hover)'; };
        btn.onmouseout  = () => { btn.style.background = 'var(--sidebar-hover)'; };
        btn.onclick = logout;
        document.body.appendChild(btn);
    }

    // Monkey-patch fetch to catch global 401 responses and redirect to login
    const _origFetch = window.fetch;
    window.fetch = function (...args) {
        return _origFetch.apply(this, args).then(res => {
            if (res.status === 401) {
                window.location.href = '/login?next=' + encodeURIComponent(location.pathname);
            }
            return res;
        });
    };

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', injectBtn);
    } else {
        injectBtn();
    }
})();
