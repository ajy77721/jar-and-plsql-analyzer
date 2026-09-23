/**
 * §276 — the floating top-right Export+Log cluster kept colliding with real page content
 * (§274/§275 tried hardcoded pixel offsets, then a runtime-measured dynamic offset — both still
 * `position: fixed`, both still capable of landing on something if the page's own header ever
 * grows past whatever was measured). The user asked for something structurally simpler instead:
 * put Export/Log as ordinary buttons INSIDE the loaded screen itself, not floating over it at all.
 *
 * This reparents the two existing elements (`.global-export-fab`, created once already loaded
 * inline in each page's own topbar/header markup; `.log-fab`, created by `shared/log-fab.js` via
 * `document.body.appendChild`) into a real container in normal document flow, and strips their
 * `position: fixed` via `.fab-inline` (see the matching CSS in each page's own stylesheet). Once
 * they're real flow children of the header again, the browser lays out everything else around
 * them automatically — the whole class of "which pixel value doesn't collide today" bug is gone
 * by construction, not by a better guess.
 */
window.MdbInlineFab = {
    /** @param containerSelector CSS selector for the element to move Export+Log into. */
    init(containerSelector) {
        var container = document.querySelector(containerSelector);
        if (!container) return;
        var exportBtn = document.querySelector('.global-export-fab');
        var logBtn = document.querySelector('.log-fab');
        [exportBtn, logBtn].forEach(function(btn) {
            if (!btn) return;
            btn.classList.add('fab-inline');
            container.appendChild(btn);
        });
    }
};
