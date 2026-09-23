/* ==========================================================================
   Runtime token resolver.

   Canvas and any other API that needs a REAL colour string (ctx.fillStyle,
   ctx.strokeStyle, ctx.shadowColor) cannot take `var(--token)` — it silently
   fails to paint. This resolves a token to its computed value for the theme
   that is currently active, and clears the cache when the theme flips so the
   next repaint picks up the new palette.

   Usage:
       MdbColor.get('--accent')            -> "#00684A"
       MdbColor.alpha('--accent', 0.25)    -> "rgba(0,104,74,0.25)"
       MdbColor.cat(i)                     -> i-th categorical series colour

   Canvas consumers must repaint on the 'themechange' event; this module only
   guarantees the values it hands out are current.

   Load after theme-toggle.js.
   ========================================================================== */
(function () {
    'use strict';

    var cache = {};
    var probe = null;

    function computed(token) {
        var v = getComputedStyle(document.documentElement)
                    .getPropertyValue(token).trim();
        if (!v) return '';
        // A token may alias another token (--accent: var(--mdb-green-base)).
        // getPropertyValue already resolves that, but a var() can survive when
        // the custom property is registered late — resolve it via a probe node.
        if (v.indexOf('var(') === 0) {
            if (!probe) {
                probe = document.createElement('span');
                probe.style.display = 'none';
                document.documentElement.appendChild(probe);
            }
            probe.style.color = '';
            probe.style.color = v;
            v = getComputedStyle(probe).color || '';
        }
        return v;
    }

    function toRgb(value) {
        if (!value) return null;
        if (value.charAt(0) === '#') {
            var h = value.slice(1);
            if (h.length === 3) h = h[0]+h[0]+h[1]+h[1]+h[2]+h[2];
            if (h.length < 6) return null;
            return [parseInt(h.slice(0,2),16), parseInt(h.slice(2,4),16), parseInt(h.slice(4,6),16)];
        }
        var m = value.match(/rgba?\(\s*([\d.]+)[,\s]+([\d.]+)[,\s]+([\d.]+)/i);
        return m ? [+m[1], +m[2], +m[3]] : null;
    }

    var MdbColor = {
        /* Resolved colour string for a token, e.g. '--accent'. */
        get: function (token, fallback) {
            if (token in cache) return cache[token];
            var v = computed(token) || fallback || '#000000';
            cache[token] = v;
            return v;
        },

        /* Same colour at a given alpha — for glows, fills and shadows. */
        alpha: function (token, a) {
            var rgb = toRgb(this.get(token));
            if (!rgb) return 'rgba(0,0,0,' + a + ')';
            return 'rgba(' + rgb[0] + ',' + rgb[1] + ',' + rgb[2] + ',' + a + ')';
        },

        /* Categorical series colour, wrapping at the end of the ramp. */
        cat: function (i) {
            return this.get('--cat-' + ((i % 8) + 1));
        },

        clear: function () { cache = {}; }
    };

    document.addEventListener('themechange', function () { MdbColor.clear(); });

    window.MdbColor = MdbColor;
})();
