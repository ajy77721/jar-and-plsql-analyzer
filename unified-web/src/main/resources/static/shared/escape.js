/**
 * The one escaping utility. Every other escape entry point in this app delegates here.
 *
 * ## Why this file exists
 *
 * There were four implementations with three different behaviours:
 *
 *   - `PA.esc`               escaped `& < >`      — used 340 times in parser, 163 in plsql
 *   - `PA.escAttr`           escaped `" ' <`      — missed `&` and `>`
 *   - `JA.utils.escapeHtml`  escaped `& < >`      — via textContent, so quotes survived
 *   - `promptManager._esc`   escaped `& < > "`    — a private fourth copy
 *
 * None escaped both quote characters, and **72 measured call sites interpolate one of them inside
 * a quoted HTML attribute**. A value containing a double quote closes the attribute early: at best
 * the markup breaks, at worst the remainder is parsed as further attributes. Table names, SQL text
 * and Claude output all reach these paths, and none of them is guaranteed quote-free.
 *
 * ## The rule applied here
 *
 * `html()` escapes `& < > " '` — all five. Escaping quotes in text context is harmless (a browser
 * renders `&#39;` as `'`), and it is what makes the same function safe in an attribute. One
 * function that is always correct beats two that are each correct in one context and silently
 * wrong in the other, because the call site is where the context is known and the call site is
 * exactly what nobody checks.
 *
 * `js()` is genuinely different and stays separate: it escapes for a JavaScript string literal
 * inside an inline handler, where the danger is a quote or backslash ending the literal, not an
 * angle bracket.
 *
 * ## Ordering
 *
 * This must load before any script that uses it. The delegating shims below are written so that a
 * page which somehow loads them first still works — they call through at invocation time, not at
 * definition time — but the load order is asserted by a gate rather than left to chance.
 */
(function (global) {
    'use strict';

    var HTML = { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' };

    var Esc = {
        /**
         * Safe in both text and attribute context.
         * @param {*} s any value; null and undefined become the empty string
         */
        html: function (s) {
            if (s === null || s === undefined) return '';
            return String(s).replace(/[&<>"']/g, function (c) { return HTML[c]; });
        },

        /**
         * Attribute context. Identical to {@link html} by design.
         *
         * <p>Kept as a separate name because call sites read better when they say which context
         * they are in, and because the previous `escAttr` had different behaviour — an alias makes
         * the equivalence explicit rather than leaving two functions that might drift apart.
         */
        attr: function (s) {
            return Esc.html(s);
        },

        /**
         * A JavaScript string literal inside an inline handler, e.g. `onclick="f('...')"`.
         *
         * <p>Backslash first: escaping quotes before backslashes would double-escape the escape
         * character and corrupt the value.
         */
        js: function (s) {
            if (s === null || s === undefined) return '';
            var jsEscaped = String(s)
                .replace(/\\/g, '\\\\')
                .replace(/'/g, "\\'")
                .replace(/\n/g, '\\n')
                .replace(/\r/g, '\\r');
            // Then HTML-escape the result, because this string is nearly always written into an
            // HTML attribute: onclick="f('...')". JS-escaping alone turns a double quote into
            // \" — which still contains a literal `"` and still closes the attribute. The browser
            // decodes entities before the JS parser sees the text, so escaping twice is what makes
            // both layers correct; escaping once is correct for whichever layer you had in mind
            // and silently wrong for the other.
            return Esc.html(jsEscaped);
        },
    };

    global.Esc = Esc;
})(window);
