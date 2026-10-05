package com.analyzer.web;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;
import org.springframework.web.util.ContentCachingResponseWrapper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Rewrites the {@code <script src>}/{@code <link href>} tags of the plain-HTML pages so every local
 * JS/CSS reference carries a content hash ({@code ?v=<hash>}), and owns the Cache-Control policy for
 * static resources. This is the piece that makes "no hard refresh after a deploy" permanent.
 *
 * <h2>The problem, precisely</h2>
 *
 * <p>The pages are static HTML with hardcoded asset tags, so Spring's built-in content-versioning
 * (which only rewrites URLs generated through {@code ResourceUrlProvider}) never touches them. The
 * tags kept a fixed URL, the browser kept serving its cached copy, and users saw the previous
 * deploy's — sometimes broken — JS until they hard-refreshed. {@link StaticResourceVersionIndex}
 * knows each file's content hash; this filter substitutes it into the served HTML at request time.
 *
 * <h2>Mechanics</h2>
 *
 * <ol>
 *   <li>Only {@code GET} page requests (HTML entry routes) are buffered — via Spring's
 *       {@link ContentCachingResponseWrapper}, so the wrapped resource handler cannot commit the
 *       response before we rewrite it. Everything else streams through untouched.</li>
 *   <li>In the buffered HTML, every local {@code .js} {@code <script src>} and {@code .css}
 *       {@code <link href>} has its {@code v} query parameter set to the file's content hash.
 *       Relative ({@code ../shared/x.js}, {@code css/x.css}) and absolute ({@code /shared/x.js})
 *       references both resolve against the page's own directory before lookup; the emitted URL
 *       keeps the original form, only the {@code v} value changes. External URLs (CDN,
 *       protocol-relative, {@code data:}) and non-{@code .js}/{@code .css} tags are left alone.</li>
 *   <li>Cache-Control is decided per request:
 *       <ul>
 *         <li>a {@code .js}/{@code .css} requested with its <b>current</b> {@code ?v=} hash is
 *             {@code public, max-age=31536000, immutable} — the URL changes whenever the bytes do,
 *             so it is safe to cache forever;</li>
 *         <li>a {@code .js}/{@code .css} without it (a direct/bookmarked URL, or an old hash) is
 *             {@code no-cache, must-revalidate} — never served stale, never cached forever;</li>
 *         <li>an HTML entry page is {@code no-store}. It deliberately does <b>not</b> rely on
 *             {@code no-cache} revalidation: the page file itself does not change when a referenced
 *             JS/CSS changes, so its ETag/Last-Modified would 304 and the browser would keep an old
 *             {@code ?v=}. Forcing a full fetch each navigation is what keeps the references
 *             current;</li>
 *         <li>other static files (the runtime-fetched JSON configs, icons, …) are
 *             {@code no-cache, must-revalidate} — they are not versioned, so they must revalidate.</li>
 *       </ul></li>
 * </ol>
 *
 * <p>{@code spring.web.resources.cache.*} is intentionally left unset: it is global and would also
 * cache the two runtime-fetched JSON config files forever. This filter is the single place that
 * decides static-resource caching, which is what lets it distinguish the versioned assets from
 * everything else.
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE + 100)
public class PageHtmlTransformer extends OncePerRequestFilter {

    static final String IMMUTABLE = "public, max-age=31536000, immutable";
    static final String NO_CACHE = "no-cache, must-revalidate";
    static final String NO_STORE = "no-store, no-cache, must-revalidate";

    /** {@code <script ... src="VALUE">} — captures prefix, quote, value; the value never spans a tag. */
    private static final Pattern SCRIPT_SRC = Pattern.compile(
            "(<script\\b[^>]*?\\bsrc\\s*=\\s*)([\"'])([^\"']*)\\2", Pattern.CASE_INSENSITIVE);

    /** {@code <link ... href="VALUE">} — the {@code .css} guard below keeps icons/etc. untouched. */
    private static final Pattern LINK_HREF = Pattern.compile(
            "(<link\\b[^>]*?\\bhref\\s*=\\s*)([\"'])([^\"']*)\\2", Pattern.CASE_INSENSITIVE);

    /** Non-versioned static file extensions that must revalidate rather than cache forever. */
    private static final String[] REVALIDATED_EXTENSIONS = {
            ".json", ".ico", ".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp",
            ".woff", ".woff2", ".ttf", ".eot", ".map", ".txt", ".xml", ".webmanifest"
    };

    private final StaticResourceVersionIndex index;

    public PageHtmlTransformer(StaticResourceVersionIndex index) {
        this.index = index;
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response,
                                    FilterChain filterChain) throws ServletException, IOException {
        String path = pathOf(request);
        if (path.startsWith("/api/")) {
            filterChain.doFilter(request, response);
            return;
        }

        String lower = path.toLowerCase(Locale.ROOT);
        if (lower.endsWith(".js") || lower.endsWith(".css")) {
            response.setHeader("Cache-Control",
                    isCurrentVersionedAsset(path, request) ? IMMUTABLE : NO_CACHE);
        } else if (isRevalidatedStatic(lower)) {
            response.setHeader("Cache-Control", NO_CACHE);
        }

        if (!"GET".equalsIgnoreCase(request.getMethod()) || !isHtmlPage(path)) {
            filterChain.doFilter(request, response);
            return;
        }

        ContentCachingResponseWrapper wrapper = new ContentCachingResponseWrapper(response);
        filterChain.doFilter(request, wrapper);

        byte[] body = wrapper.getContentAsByteArray();
        String contentType = wrapper.getContentType();
        if (wrapper.getStatus() == 200 && body.length > 0 && isHtml(contentType)) {
            byte[] rewritten = rewrite(new String(body, StandardCharsets.UTF_8),
                    StaticResourceVersionIndex.baseDir(path)).getBytes(StandardCharsets.UTF_8);
            response.setContentType(contentType);
            response.setContentLength(rewritten.length);
            response.setHeader("Cache-Control", NO_STORE);
            response.getOutputStream().write(rewritten);
        } else {
            wrapper.copyBodyToResponse();
        }
    }

    /** Rewrites local JS/CSS references to their content-versioned form. Package-visible for tests. */
    String rewrite(String html, String baseDir) {
        String out = SCRIPT_SRC.matcher(html).replaceAll(mr -> replaceRef(mr, baseDir, ".js"));
        return LINK_HREF.matcher(out).replaceAll(mr -> replaceRef(mr, baseDir, ".css"));
    }

    private String replaceRef(MatchResult match, String baseDir, String extension) {
        String versioned = versionedRef(match.group(3), baseDir, extension);
        if (versioned == null) {
            return Matcher.quoteReplacement(match.group(0));
        }
        return Matcher.quoteReplacement(match.group(1) + match.group(2) + versioned + match.group(2));
    }

    /**
     * The versioned form of a local reference, or {@code null} to leave it unchanged (external URL,
     * wrong extension, or a file this app does not serve).
     */
    private String versionedRef(String ref, String baseDir, String extension) {
        if (ref == null) {
            return null;
        }
        String value = ref.trim();
        if (value.isEmpty()) {
            return null;
        }
        String lower = value.toLowerCase(Locale.ROOT);
        if (lower.startsWith("http://") || lower.startsWith("https://") || lower.startsWith("//")
                || lower.startsWith("data:") || lower.startsWith("javascript:")) {
            return null;
        }

        String fragment = "";
        int hash = value.indexOf('#');
        if (hash >= 0) {
            fragment = value.substring(hash);
            value = value.substring(0, hash);
        }
        String query = "";
        int mark = value.indexOf('?');
        if (mark >= 0) {
            query = value.substring(mark + 1);
            value = value.substring(0, mark);
        }
        if (!value.toLowerCase(Locale.ROOT).endsWith(extension)) {
            return null;
        }

        String resolved = StaticResourceVersionIndex.resolve(baseDir, value);
        String version = index.versionFor(resolved);
        if (version == null) {
            return null;
        }

        List<String> params = new ArrayList<>();
        for (String param : query.split("&")) {
            if (param.isEmpty()) {
                continue;
            }
            int equals = param.indexOf('=');
            String name = equals >= 0 ? param.substring(0, equals) : param;
            if (!name.equals("v")) {
                params.add(param);
            }
        }
        params.add("v=" + version);
        return value + "?" + String.join("&", params) + fragment;
    }

    private boolean isCurrentVersionedAsset(String path, HttpServletRequest request) {
        String resolved = StaticResourceVersionIndex.resolve(StaticResourceVersionIndex.baseDir(path), path);
        String current = index.versionFor(resolved);
        return current != null && current.equals(request.getParameter("v"));
    }

    private static boolean isRevalidatedStatic(String lowerPath) {
        if (lowerPath.endsWith(".html")) {
            return true;
        }
        for (String extension : REVALIDATED_EXTENSIONS) {
            if (lowerPath.endsWith(extension)) {
                return true;
            }
        }
        return false;
    }

    /** The HTML entry routes: the root, any {@code *.html}, the app roots ({@code /parser/}, …), {@code /login}. */
    static boolean isHtmlPage(String path) {
        return path.equals("/") || path.equals("/login") || path.endsWith(".html") || path.endsWith("/");
    }

    private static boolean isHtml(String contentType) {
        return contentType != null && contentType.toLowerCase(Locale.ROOT).contains("text/html");
    }

    /** Request path with the context path stripped (empty here, but correct if one is ever set). */
    private static String pathOf(HttpServletRequest request) {
        String uri = request.getRequestURI();
        String context = request.getContextPath();
        if (context != null && !context.isEmpty() && uri.startsWith(context)) {
            uri = uri.substring(context.length());
        }
        return uri.isEmpty() ? "/" : uri;
    }
}
