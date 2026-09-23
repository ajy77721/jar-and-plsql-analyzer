window.PA = window.PA || {};

/**
 * The one place that answers "which node is this?".
 *
 * ## Why this exists
 *
 * `PA.analysisData.nodes` is read in 47 places across 18 files, and eleven of those are
 * `.find(n => n.nodeId === x)` — a linear scan. Two of them sit **inside loops over table
 * accesses**, so the cost is `accesses × nodes`: at 8,000 procedures and the 2,435 accesses measured
 * on one table that is ~19.5M comparisons for one render, and it grows with both dimensions at once.
 * The same defect as the `indexOf` in the JAR code tree (FE-017), in a different file.
 *
 * ## Why an index rather than 47 tidier scans
 *
 * The lookups are O(1) here, which is the immediate win. The larger one is that `analysisData.nodes`
 * stops being the interface: every consumer asking this object instead means the *transport* can
 * change later — paged, projected, fetched on demand — without touching 47 call sites. That
 * coupling is what currently blocks `/index` from shedding its node list (MID-002), which is 53.1%
 * of a response fetched on every analysis load.
 *
 * ## Built lazily, invalidated by identity
 *
 * Rebuilding is triggered by the array being a different object, not by a flag someone has to
 * remember to clear. A stale index that looks fresh resolves a name to the *previous* analysis's
 * node — which renders a plausible wrong answer rather than failing.
 */
PA.nodeIndex = {
    _source: null,
    _byId: null,
    _byUpperId: null,
    _byName: null,
    _byUpperName: null,
    _bySourceFile: null,
    _loadPromise: null,
    _loadPromiseFor: null,

    /**
     * UI-024 (§161) — the async half of "block render until fetched", for the handful of call
     * sites that are genuinely leaf-level and already inside an async caller (measured, not
     * assumed — see this section's own doc comment for the two foundational, high-fan-in
     * synchronous utilities, `PA.getScopedNodes`/`PA._buildNodeIdMap`, deliberately NOT converted
     * because dozens of their own callers are themselves synchronous, a materially bigger blast
     * radius than the ~19 sites this row was originally scoped against).
     *
     * <p>Resolves immediately if `PA.analysisData.nodes` is already an array (the common case,
     * once anything has triggered a fetch for the current analysis) — this is a cache-miss guard,
     * not a network call on every use. Keyed to the current `PA.analysisData` object identity so a
     * new analysis load starts a fresh fetch rather than resolving with the previous one's stale
     * promise (the same identity-based invalidation `all()`'s own `_source` check already uses).
     */
    ensureLoaded: function () {
        var ad = PA.analysisData;
        if (!ad) return Promise.resolve([]);
        if (Array.isArray(ad.nodes)) return Promise.resolve(ad.nodes);
        if (this._loadPromise && this._loadPromiseFor === ad) return this._loadPromise;
        this._loadPromiseFor = ad;
        this._loadPromise = PA.api.getProcedures().then(function (nodes) {
            ad.nodes = Array.isArray(nodes) ? nodes : [];
            return ad.nodes;
        });
        return this._loadPromise;
    },

    /** The node array this index is built over, or an empty array when no analysis is loaded. */
    all: function () {
        var nodes = (PA.analysisData && PA.analysisData.nodes) || [];
        if (this._source !== nodes) this._build(nodes);
        return nodes;
    },

    count: function () {
        return this.all().length;
    },

    _build: function (nodes) {
        this._source = nodes;
        this._byId = new Map();
        this._byUpperId = new Map();
        this._byName = new Map();
        this._byUpperName = new Map();
        this._bySourceFile = new Map();

        for (var i = 0; i < nodes.length; i++) {
            var n = nodes[i];
            var id = n.nodeId || n.id || '';
            var name = n.name || '';
            var sf = n.sourceFile || '';
            // First-write-wins throughout, matching what `.find()` returned: it stopped at the
            // first match, so a later duplicate was never visible and must stay invisible.
            if (id && !this._byId.has(id)) this._byId.set(id, n);
            if (id && !this._byUpperId.has(id.toUpperCase())) this._byUpperId.set(id.toUpperCase(), n);
            if (name && !this._byName.has(name)) this._byName.set(name, n);
            if (name && !this._byUpperName.has(name.toUpperCase())) {
                this._byUpperName.set(name.toUpperCase(), n);
            }
            if (sf && !this._bySourceFile.has(sf.toUpperCase())) {
                this._bySourceFile.set(sf.toUpperCase(), n);
            }
        }
    },

    /** Exact match on `nodeId` (or the `id` alias some payloads use). */
    byId: function (id) {
        if (!id) return null;
        this.all();
        return this._byId.get(id) || null;
    },

    /** Case-insensitive `nodeId`. Several callers upper-case before comparing. */
    byIdIgnoringCase: function (id) {
        if (!id) return null;
        this.all();
        return this._byUpperId.get(String(id).toUpperCase()) || null;
    },

    byName: function (name) {
        if (!name) return null;
        this.all();
        return this._byName.get(name) || null;
    },

    byNameIgnoringCase: function (name) {
        if (!name) return null;
        this.all();
        return this._byUpperName.get(String(name).toUpperCase()) || null;
    },

    bySourceFile: function (sourceFile) {
        if (!sourceFile) return null;
        this.all();
        return this._bySourceFile.get(String(sourceFile).toUpperCase()) || null;
    },

    /**
     * Resolves whatever a caller happens to hold — a node id, a name, or a source file — trying
     * exact matches before case-insensitive ones.
     *
     * <p>The order matters and is the order the hand-written chains used: an exact id beats a
     * case-insensitive name, so a procedure named like another's lower-cased id still resolves to
     * itself.
     */
    resolve: function (key) {
        if (!key) return null;
        return this.byId(key)
            || this.byName(key)
            || this.byIdIgnoringCase(key)
            || this.byNameIgnoringCase(key)
            || this.bySourceFile(key);
    }
};
