package com.analyzer.web;

import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Maps every local static JS/CSS file to a short hash of its own content, so a URL can be
 * content-versioned ({@code /parser/js/app.js?v=<hash>}) and a deploy that changes a file changes
 * its URL — the browser then has no choice but to fetch the new bytes.
 *
 * <h2>Why this exists (the stale-static problem)</h2>
 *
 * <p>The parser / jar / plsql pages are <b>plain HTML</b> served as static resources, with
 * hardcoded {@code <script src>}/{@code <link href>} tags. Spring's own content strategy
 * ({@code spring.web.resources.chain.strategy.content.*}) can only version URLs produced through
 * {@code ResourceUrlProvider} (Thymeleaf {@code @{...}}), so it does nothing for those hardcoded
 * tags. The pages therefore kept referencing a fixed URL, and the browser served its cached copy
 * after a deploy — the user saw the old, sometimes broken, JS until a manual hard refresh. This
 * index is the source of truth {@link PageHtmlTransformer} rewrites those tags with.
 *
 * <h2>The map</h2>
 *
 * <p>{@link #buildIndex()} walks the classpath {@code static/} tree once at startup and hashes
 * every {@code .js}/{@code .css} (a few hundred small files — bounded, one pass). {@link
 * #versionFor(String)} then serves from that map with no per-request hashing. A lazy, cached
 * fallback resolves a resource on first request if the startup walk somehow missed it (for example
 * a file added to the running classpath), so the mechanism degrades to correct rather than silent.
 */
@Component
public class StaticResourceVersionIndex {

    private static final Logger log = LoggerFactory.getLogger(StaticResourceVersionIndex.class);

    /**
     * URL-path (e.g. {@code /shared/escape.js}) → content hash. Replaced wholesale by
     * {@link #buildIndex()} and only read afterwards, so a {@code volatile} reference is enough to
     * publish it safely to the request threads.
     */
    private volatile Map<String, String> versions = Map.of();

    /** Lazy fallback for a path the startup walk did not see; memoised so it is hashed at most once. */
    private final Map<String, Optional<String>> resolved = new ConcurrentHashMap<>();

    private final ResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();

    /** Hash the whole {@code static/} JS/CSS tree once, at startup. */
    @PostConstruct
    void buildIndex() {
        Map<String, String> built = new HashMap<>();
        for (String ext : new String[]{"js", "css"}) {
            try {
                Resource[] found = resolver.getResources("classpath*:/static/**/*." + ext);
                for (Resource resource : found) {
                    if (!resource.isReadable()) {
                        continue;
                    }
                    String urlPath = urlPathOf(resource);
                    if (urlPath != null) {
                        built.put(urlPath, contentHash(readBytes(resource)));
                    }
                }
            } catch (IOException e) {
                log.warn("Could not index static .{} resources at startup (per-resource fallback still "
                        + "applies): {}", ext, e.toString());
            }
        }
        versions = Map.copyOf(built);
        log.info("Static resource version index: {} files hashed", versions.size());
    }

    /**
     * The content hash for a local static resource, or {@code null} when {@code urlPath} is not a
     * local static file (external URL, missing file, path escape). Callers must pass an
     * already-normalised, leading-slash path such as {@code /parser/js/app.js}.
     */
    public String versionFor(String urlPath) {
        if (urlPath == null || urlPath.isEmpty() || urlPath.contains("..")) {
            return null;
        }
        String known = versions.get(urlPath);
        if (known != null) {
            return known;
        }
        return resolved.computeIfAbsent(urlPath, this::resolveAndHash).orElse(null);
    }

    private Optional<String> resolveAndHash(String urlPath) {
        try {
            Resource resource = resolver.getResource("classpath:/static" + urlPath);
            if (resource.isReadable()) {
                return Optional.of(contentHash(readBytes(resource)));
            }
        } catch (Exception e) {
            log.debug("No readable static resource at {}: {}", urlPath, e.toString());
        }
        return Optional.empty();
    }

    /** Derives {@code /shared/escape.js} from a classpath resource's URL (works in a fat jar too). */
    private static String urlPathOf(Resource resource) {
        try {
            String url = resource.getURL().toString();
            int marker = url.lastIndexOf("/static/");
            if (marker < 0) {
                return null;
            }
            String relative = url.substring(marker + "/static/".length());
            int nestedJar = relative.indexOf("!/");
            if (nestedJar >= 0) {
                relative = relative.substring(0, nestedJar);
            }
            int query = relative.indexOf('?');
            if (query >= 0) {
                relative = relative.substring(0, query);
            }
            return "/" + URLDecoder.decode(relative, StandardCharsets.UTF_8);
        } catch (IOException e) {
            return null;
        }
    }

    private static byte[] readBytes(Resource resource) throws IOException {
        try (InputStream in = resource.getInputStream()) {
            return in.readAllBytes();
        }
    }

    /** First 8 bytes of SHA-256 as 16 lowercase hex characters — short, collision-safe enough here. */
    static String contentHash(byte[] content) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(content);
            StringBuilder hex = new StringBuilder(16);
            for (int i = 0; i < 8; i++) {
                hex.append(Character.forDigit((digest[i] >> 4) & 0xF, 16));
                hex.append(Character.forDigit(digest[i] & 0xF, 16));
            }
            return hex.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is required but unavailable", e);
        }
    }

    /**
     * The directory a relative reference resolves against: everything up to and including the last
     * slash. {@code /parser/index.html → /parser/}, {@code /jar/ → /jar/}, {@code / → /}.
     */
    static String baseDir(String path) {
        int slash = path.lastIndexOf('/');
        return slash < 0 ? "/" : path.substring(0, slash + 1);
    }

    /**
     * Resolves {@code ref} against {@code baseDir} and normalises {@code .}/{@code ..} segments,
     * yielding a rooted path. {@code resolve("/parser/", "../shared/x.js") = "/shared/x.js"}.
     */
    static String resolve(String baseDir, String ref) {
        String combined = ref.startsWith("/") ? ref : baseDir + ref;
        Deque<String> parts = new ArrayDeque<>();
        for (String segment : combined.split("/")) {
            if (segment.isEmpty() || segment.equals(".")) {
                continue;
            }
            if (segment.equals("..")) {
                if (!parts.isEmpty()) {
                    parts.removeLast();
                }
            } else {
                parts.addLast(segment);
            }
        }
        return "/" + String.join("/", parts);
    }
}
