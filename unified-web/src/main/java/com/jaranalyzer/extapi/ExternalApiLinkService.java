package com.jaranalyzer.extapi;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jaranalyzer.service.JarDataPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import com.analyzer.storage.ArtifactEntry;
import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.ArtifactStore;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Links HTTP_CALL/FEIGN leaf nodes in one analyzed JAR's call trees to the endpoint they actually
 * target in ANOTHER already-analyzed JAR — without merging trees (see CrossRefService, which does
 * the same Java-to-PL/SQL link-not-splice for Oracle procs; this is the Java-to-Java analog).
 *
 * Reads only the lightweight per-endpoint files already written by EndpointOutputWriter
 * (endpoint.json for method+path, nodes.json for HTTP_CALL leaves) — never the full multi-hundred
 * MB analysis.json.
 */
@Service
public class ExternalApiLinkService {

    private static final Logger log = LoggerFactory.getLogger(ExternalApiLinkService.class);

    private final JarDataPaths jarDataPaths;
    private final ObjectMapper objectMapper;
    private final com.fasterxml.jackson.databind.ObjectWriter prettyWriter;
    private final ArtifactStore store;

    /** Required, never optional: an absent store would leave a silent local-disk fallback. */
    public ExternalApiLinkService(JarDataPaths jarDataPaths, ObjectMapper objectMapper,
                                  @Qualifier("jarArtifactStore") ArtifactStore store) {
        this.jarDataPaths = jarDataPaths;
        this.objectMapper = objectMapper;
        this.prettyWriter = objectMapper.writerWithDefaultPrettyPrinter();
        this.store = store;
    }

    /** Every read goes through the store; the key decides where the bytes come from. */
    private InputStream openRead(ArtifactKey key) throws IOException { return store.read(key); }

    /** Every write goes through the store. close() is the commit point. */
    private OutputStream openWrite(ArtifactKey key) throws IOException { return store.write(key); }

    /**
     * The key prefix every JAR of this instance lives under ({@code jar/}, or {@code jar/war/} for
     * the WAR flavour). Derived from {@link JarDataPaths#jarRootKey} so the WAR override is
     * honoured — hardcoding {@code "jar"} would key WAR artifacts into the JAR namespace.
     */
    private ArtifactKey jarsRootKey() {
        ArtifactKey parent = jarDataPaths.jarRootKey("probe").parent();
        if (parent == null) throw new IllegalStateException("JarDataPaths key root is empty");
        return parent;
    }

    // ── Public API ────────────────────────────────────────────────────────────

    /**
     * Resolve every HTTP_CALL/FEIGN leaf in the given JAR's endpoints against the endpoint index
     * built from every OTHER already-analyzed JAR in the data store. Writes
     * external-links.json (resolved + ambiguous) and external-links-pending.json (unresolved).
     */
    public ExternalLinkResult resolve(String jarId) {
        Map<String, List<EndpointRef>> index = buildEndpointIndex(jarId);
        return resolveAgainstIndex(jarId, index);
    }

    /**
     * Called after any JAR finishes (re)analysis. Re-checks every OTHER JAR's pending list against
     * the now-larger endpoint index (which now includes jarId) — anything that newly resolves
     * moves from pending to linked, without needing to re-run the other JAR's full analysis.
     */
    public void reresolvePending(String jarId) {
        List<String> allJarIds = listAllJarIds();
        for (String otherId : allJarIds) {
            if (otherId.equals(jarId)) continue;
            ArtifactKey pendingFile = jarDataPaths.externalLinksPendingKey(otherId);
            if (!store.exists(pendingFile)) continue;
            try (InputStream in = openRead(pendingFile)) {
                JsonNode pending = objectMapper.readTree(in);
                if (!pending.has("pending") || pending.get("pending").isEmpty()) continue;
            } catch (IOException e) {
                log.debug("Failed to read pending links for {}: {}", otherId, e.getMessage());
                continue;
            }
            log.info("Re-resolving pending external links for {} after {} finished analysis", otherId, jarId);
            try {
                Map<String, List<EndpointRef>> index = buildEndpointIndex(otherId);
                resolveAgainstIndex(otherId, index);
            } catch (Exception e) {
                log.warn("Failed to re-resolve pending links for {}: {}", otherId, e.getMessage());
            }
        }
    }

    public JsonNode getLinks(String jarId) throws IOException {
        ArtifactKey f = jarDataPaths.externalLinksKey(jarId);
        if (!store.exists(f)) return emptyResultNode();
        try (InputStream in = openRead(f)) { return objectMapper.readTree(in); }
    }

    public JsonNode getPending(String jarId) throws IOException {
        ArtifactKey f = jarDataPaths.externalLinksPendingKey(jarId);
        if (!store.exists(f)) return emptyPendingNode();
        try (InputStream in = openRead(f)) { return objectMapper.readTree(in); }
    }

    // ── Endpoint index ────────────────────────────────────────────────────────

    private record EndpointRef(String jarId, String controllerSimpleName, String methodName, String httpMethod, String path) {}

    /**
     * Build "METHOD /normalized/path" -> [candidate endpoints] across every analyzed JAR EXCEPT
     * jarId itself (an endpoint never links to its own JAR's own controller).
     */
    private Map<String, List<EndpointRef>> buildEndpointIndex(String excludeJarId) {
        Map<String, List<EndpointRef>> index = new LinkedHashMap<>();
        for (String otherId : listAllJarIds()) {
            if (otherId.equals(excludeJarId)) continue;
            // ONE listing of the whole endpoints/ subtree, then a pure in-memory filter. The
            // directory scan this replaces did an exists() probe per endpoint folder — fine
            // locally, minutes of HEAD latency over object storage for a few thousand endpoints.
            for (ArtifactKey epJson : leafKeys(jarDataPaths.endpointsDirKey(otherId), "endpoint.json")) {
                try (InputStream in = openRead(epJson)) {
                    JsonNode ep = objectMapper.readTree(in);
                    String httpMethod = textOrNull(ep, "httpMethod");
                    String path = textOrNull(ep, "fullPath");
                    if (httpMethod == null || path == null) continue;
                    String key = matchKey(httpMethod, path);
                    index.computeIfAbsent(key, k -> new ArrayList<>()).add(new EndpointRef(
                            otherId, textOrNull(ep, "controllerSimpleName"), textOrNull(ep, "methodName"),
                            httpMethod, path));
                } catch (IOException e) {
                    log.debug("Failed to read {}: {}", epJson, e.getMessage());
                }
            }
        }
        return index;
    }

    /**
     * Every {@code <prefix>/<endpointFolder>/<leafName>} artifact, in ONE round trip.
     *
     * @return the matching keys, sorted for a stable order (a local directory listing was
     *         effectively ordered; an object listing must be made so explicitly)
     */
    private List<ArtifactKey> leafKeys(ArtifactKey prefix, String leafName) {
        int depth = prefix.segments().size();
        try {
            return store.statPrefix(prefix).keySet().stream()
                    .filter(k -> k.segments().size() == depth + 2 && leafName.equals(k.leaf()))
                    .sorted(Comparator.comparing(ArtifactKey::toString))
                    .collect(Collectors.toList());
        } catch (IOException e) {
            log.debug("Failed to list {} under {}: {}", leafName, prefix, e.getMessage());
            return List.of();
        }
    }

    // ── Resolution ────────────────────────────────────────────────────────────

    private ExternalLinkResult resolveAgainstIndex(String jarId, Map<String, List<EndpointRef>> index) {
        List<Map<String, Object>> resolved = new ArrayList<>();
        List<Map<String, Object>> ambiguous = new ArrayList<>();
        List<Map<String, Object>> pending = new ArrayList<>();

        for (ArtifactKey nodesFile : leafKeys(jarDataPaths.endpointsDirKey(jarId), "nodes.json")) {
            resolveOneEndpoint(nodesFile, index, resolved, ambiguous, pending);
        }

        writeResult(jarId, resolved, ambiguous, pending);
        log.info("External link resolution for {}: {} resolved, {} ambiguous, {} pending",
                jarId, resolved.size(), ambiguous.size(), pending.size());
        return new ExternalLinkResult(resolved, ambiguous, pending);
    }

    private void resolveOneEndpoint(ArtifactKey nodesFile, Map<String, List<EndpointRef>> index,
                                     List<Map<String, Object>> resolved, List<Map<String, Object>> ambiguous,
                                     List<Map<String, Object>> pending) {
        JsonNode nodesRoot;
        try (InputStream in = openRead(nodesFile)) {
            nodesRoot = objectMapper.readTree(in);
        } catch (IOException e) {
            log.debug("Failed to read {}: {}", nodesFile, e.getMessage());
            return;
        }
        String callerEndpoint = textOrNull(nodesRoot, "endpoint");
        JsonNode nodes = nodesRoot.path("nodes");
        if (!nodes.isArray()) return;

        for (JsonNode node : nodes) {
            String callType = textOrNull(node, "callType");
            if (!"HTTP_CALL".equals(callType)) continue;
            String httpMethod = textOrNull(node, "httpTargetMethod");
            String path = textOrNull(node, "httpTargetPath");
            String moduleHint = textOrNull(node, "httpTargetModuleHint");
            if (httpMethod == null || path == null) {
                // Can't even form a match key — nothing to resolve against, and not enough
                // information to record a useful pending entry either. Skip silently; this is
                // the (rare, dynamic-URL) case createHttpCallLeaf already couldn't resolve.
                continue;
            }

            Map<String, Object> callSite = new LinkedHashMap<>();
            callSite.put("callerEndpoint", callerEndpoint);
            callSite.put("callerClassName", textOrNull(node, "className"));
            callSite.put("callerMethodName", textOrNull(node, "methodName"));
            callSite.put("httpMethod", httpMethod);
            callSite.put("path", path);
            if (moduleHint != null) callSite.put("moduleHint", moduleHint);

            List<EndpointRef> candidates = index.get(matchKey(httpMethod, path));
            if (candidates == null || candidates.isEmpty()) {
                pending.add(callSite);
                continue;
            }
            if (candidates.size() == 1) {
                resolved.add(withTarget(callSite, candidates.get(0)));
                continue;
            }
            // Multiple JARs expose the same method+path — use the module hint as a tie-breaker.
            List<EndpointRef> narrowed = narrowByModuleHint(candidates, moduleHint);
            if (narrowed.size() == 1) {
                resolved.add(withTarget(callSite, narrowed.get(0)));
            } else {
                Map<String, Object> amb = new LinkedHashMap<>(callSite);
                amb.put("candidates", candidates.stream().map(this::targetOf).collect(Collectors.toList()));
                ambiguous.add(amb);
            }
        }
    }

    private List<EndpointRef> narrowByModuleHint(List<EndpointRef> candidates, String moduleHint) {
        if (moduleHint == null || moduleHint.isBlank()) return candidates;
        String hintLower = moduleHint.toLowerCase(Locale.ROOT);
        List<EndpointRef> narrowed = candidates.stream()
                .filter(c -> c.jarId().toLowerCase(Locale.ROOT).contains(hintLower)
                        || hintLower.contains(c.jarId().toLowerCase(Locale.ROOT)))
                .collect(Collectors.toList());
        return narrowed.size() == 1 ? narrowed : candidates;
    }

    private Map<String, Object> withTarget(Map<String, Object> callSite, EndpointRef target) {
        Map<String, Object> withTarget = new LinkedHashMap<>(callSite);
        withTarget.putAll(targetOf(target));
        return withTarget;
    }

    private Map<String, Object> targetOf(EndpointRef ref) {
        Map<String, Object> t = new LinkedHashMap<>();
        t.put("targetJarId", ref.jarId());
        t.put("targetControllerSimpleName", ref.controllerSimpleName());
        t.put("targetMethodName", ref.methodName());
        t.put("targetHttpMethod", ref.httpMethod());
        t.put("targetPath", ref.path());
        return t;
    }

    // ── Persistence ───────────────────────────────────────────────────────────

    private void writeResult(String jarId, List<Map<String, Object>> resolved,
                              List<Map<String, Object>> ambiguous, List<Map<String, Object>> pending) {
        try {
            // No ensureJarRoot: the store creates parents on write.
            Map<String, Object> linksDoc = new LinkedHashMap<>();
            linksDoc.put("jarId", jarId);
            linksDoc.put("resolved", resolved);
            linksDoc.put("ambiguous", ambiguous);
            // Streamed, not writeValueAsString: a jar with many resolved links produces a document
            // there is no reason to materialise as a String first.
            try (OutputStream out = openWrite(jarDataPaths.externalLinksKey(jarId))) {
                prettyWriter.writeValue(out, linksDoc);
            }

            Map<String, Object> pendingDoc = new LinkedHashMap<>();
            pendingDoc.put("jarId", jarId);
            pendingDoc.put("pending", pending);
            try (OutputStream out = openWrite(jarDataPaths.externalLinksPendingKey(jarId))) {
                prettyWriter.writeValue(out, pendingDoc);
            }
        } catch (IOException e) {
            log.warn("Failed to write external link results for {}: {}", jarId, e.getMessage());
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /**
     * Every analyzed JAR id — a jar folder holding {@code analysis.json} or
     * {@code analysis_corrected.json}, matching what {@code JarDataPaths.listJarRoots} used to
     * return.
     *
     * <p>Deliberately a shallow listing plus a per-jar existence check rather than one
     * {@code statPrefix} of the whole tree: a recursive stat of {@code jar/} would enumerate every
     * endpoint, resource and corrections file of every analysis — millions of entries — to answer
     * a question about a few dozen folders.
     */
    private List<String> listAllJarIds() {
        ArtifactKey root = jarsRootKey();
        List<String> ids = new ArrayList<>();
        try (Stream<ArtifactEntry> s = store.list(root, false)) {
            for (ArtifactKey k : s.filter(ArtifactEntry::directory).map(ArtifactEntry::key).toList()) {
                String id = k.leaf();
                if (store.exists(jarDataPaths.analysisKey(id))
                        || store.exists(jarDataPaths.correctedKey(id))) {
                    ids.add(id);
                }
            }
        } catch (IOException e) {
            log.debug("Failed to list jar roots: {}", e.getMessage());
        }
        return ids;
    }

    /** Normalize method + path into a stable lookup key: uppercase method, leading slash, no
     *  trailing slash, and every {pathVariable} segment collapsed to a single wildcard token so
     *  a declared template and a literal-looking call site both hash identically. */
    static String matchKey(String httpMethod, String path) {
        String m = httpMethod == null ? "" : httpMethod.trim().toUpperCase(Locale.ROOT);
        String p = path == null ? "" : path.trim();
        if (!p.startsWith("/")) p = "/" + p;
        while (p.length() > 1 && p.endsWith("/")) p = p.substring(0, p.length() - 1);
        p = p.replaceAll("\\{[^}]*}", "{}");
        return m + " " + p;
    }

    private static String textOrNull(JsonNode node, String field) {
        JsonNode v = node.get(field);
        return (v != null && !v.isNull()) ? v.asText() : null;
    }

    private JsonNode emptyResultNode() {
        var node = objectMapper.createObjectNode();
        node.putArray("resolved");
        node.putArray("ambiguous");
        return node;
    }

    private JsonNode emptyPendingNode() {
        var node = objectMapper.createObjectNode();
        node.putArray("pending");
        return node;
    }

    public record ExternalLinkResult(List<Map<String, Object>> resolved,
                                      List<Map<String, Object>> ambiguous,
                                      List<Map<String, Object>> pending) {}
}
