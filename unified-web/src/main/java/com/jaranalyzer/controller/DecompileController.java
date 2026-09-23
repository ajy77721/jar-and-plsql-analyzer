package com.jaranalyzer.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jaranalyzer.service.ClaudeAnalysisService;
import com.jaranalyzer.service.ClaudeEnrichmentTracker;
import com.jaranalyzer.service.DecompilerService;
import com.jaranalyzer.service.JarDataPaths;
import com.jaranalyzer.service.JarNameUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.ArtifactStore;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * <p><b>WAR screen</b>: also mapped under {@code /api/war/wars}, because the WAR page is a fork of
 * the JAR page and reuses these scripts verbatim. It was calling {@code /api/jar/jars/{warId}/...}
 * and getting a 404 on every decompile: a WAR's bytecode and analysis live under the
 * {@code jar/war/<id>/} key root ({@link com.jaranalyzer.service.WarDataPaths#keyRoot()}), not
 * {@code jar/<id>/}.
 *
 * <p>Note the deliberate asymmetry below. Only the routes that read the <em>artifacts the WAR
 * pipeline itself wrote</em> — {@code stored.war} and {@code analysis.json}, i.e. decompile —
 * switch key root. The Claude-derived routes ({@code claude-fragments}, {@code endpoints/*})
 * stay on {@code jarDataPaths} for BOTH prefixes, because WAR Claude runs go through the same
 * shared {@code ClaudeAnalysisService}/{@code FragmentStore} singletons, which hold the
 * JAR-rooted bean and therefore write WAR fragments to {@code jar/<warId>/claude} already.
 * Switching those to WAR paths would break the one set of endpoints that currently works.
 */
@RestController("jarDecompileController")
@RequestMapping({"/api/jar/jars", "/api/war/wars"})
public class DecompileController {

    private static final Logger log = LoggerFactory.getLogger(DecompileController.class);
    private static final Pattern VALID_CLASS_NAME = Pattern.compile("^[a-zA-Z0-9._$]+$");

    private final ClaudeAnalysisService claudeAnalysisService;
    private final DecompilerService decompilerService;
    private final ObjectMapper objectMapper;
    private final ClaudeEnrichmentTracker claudeTracker;
    private final JarDataPaths jarDataPaths;

    /** Key root for artifacts the WAR analysis pipeline wrote — see the class doc's asymmetry note. */
    private final JarDataPaths warDataPaths;

    /**
     * Required, not optional: an ArtifactStore that can be null leaves a filesystem fallback that
     * silently keeps working in local mode and then loses the data the day the backend changes.
     */
    private final ArtifactStore store;

    public DecompileController(ClaudeAnalysisService claudeAnalysisService,
                               DecompilerService decompilerService,
                               ObjectMapper objectMapper,
                               ClaudeEnrichmentTracker claudeTracker,
                               JarDataPaths jarDataPaths,
                               @org.springframework.beans.factory.annotation.Qualifier("warPersistenceService")
                               com.jaranalyzer.service.PersistenceService warPersistenceService,
                               @org.springframework.beans.factory.annotation.Qualifier("jarArtifactStore") ArtifactStore store) {
        this.claudeAnalysisService = claudeAnalysisService;
        this.decompilerService = decompilerService;
        this.objectMapper = objectMapper;
        this.claudeTracker = claudeTracker;
        this.jarDataPaths = jarDataPaths;
        this.warDataPaths = warPersistenceService.getPaths();
        this.store = store;
    }

    /**
     * The two class-level mappings are the only thing distinguishing a WAR call from a JAR one:
     * the id carries no marker, and a WAR may share a name with a JAR.
     */
    private JarDataPaths pathsFor(jakarta.servlet.http.HttpServletRequest request) {
        return request.getRequestURI().startsWith("/api/war/") ? warDataPaths : jarDataPaths;
    }

    private String readString(ArtifactKey key) throws IOException {
        try (InputStream in = store.read(key)) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    /** The endpoint output folder for a raw endpoint name, sanitised to one safe key segment. */
    private ArtifactKey endpointDirKey(String id, String endpoint) {
        return jarDataPaths.endpointsDirKey(id).resolve(endpoint.replaceAll("[^a-zA-Z0-9._-]", "_"));
    }

    @GetMapping("/{id}/decompile")
    public Map<String, Object> decompileClass(@PathVariable String id,
                                               @RequestParam("class") String className,
                                               jakarta.servlet.http.HttpServletRequest request) {
        JarDataPaths paths = pathsFor(request);
        if (className == null || !VALID_CLASS_NAME.matcher(className).matches()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                    "Invalid class name — must be a valid Java fully-qualified name");
        }
        // decompilerService.decompile(ArtifactKey, ...) does its own materialize() internally —
        // and, unlike materializing here and handing it a bare Path, also knows the artifact's
        // real key. That key is what makes the per-class decompile cache possible: a temp
        // download path (the only kind materialize() returns in s3/mongodb mode) lives outside
        // jarDataPaths' base dir, so re-deriving a key from the Path (as this used to do via
        // decompile(Path, ...) -> keyForPath()) always failed remotely — decompilation silently
        // returned null for every non-local storage mode. Passing the key straight through avoids
        // that round-trip entirely.
        ArtifactKey jarKey = paths.storedJarKey(id);
        DecompilerService.DecompileResult decompiled = decompilerService.decompileWithMeta(jarKey, className);
        String source = decompiled == null ? null : decompiled.source();

        Map<String, Object> classData = extractClassData(id, className, paths);

        if (source == null && classData == null) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND,
                    "Class not found: " + className);
        }

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("className", className);
        result.put("source", source);
        result.put("decompiler", source != null ? "CFR" : null);
        // "origin" (not "source" — that name is already the decompiled text above): tells the
        // caller whether this response was served from the per-class decompile cache or freshly
        // decompiled from the jar's bytecode just now, per the user-facing "is this cached or
        // live" requirement. null when there is nothing to decompile from at all (classData-only).
        result.put("origin", decompiled == null ? null
                : decompiled.origin() == DecompilerService.DecompileSource.CACHE ? "cache" : "jar");
        if (classData != null) result.put("classData", classData);
        return result;
    }

    @GetMapping("/settings/claude-status")
    public Map<String, Object> getClaudeStatus() {
        Map<String, Object> status = new LinkedHashMap<>();
        status.put("configured", claudeAnalysisService.isConfigured());
        return status;
    }

    @GetMapping("/settings/claude-test")
    public Map<String, Object> testClaudePipe() {
        return claudeAnalysisService.runStdinPipeTest();
    }

    @GetMapping("/{id}/claude-progress")
    public Map<String, Object> getClaudeProgress(@PathVariable String id) {
        return claudeTracker.getStatus(id);
    }

    @GetMapping("/{id}/claude-fragments")
    public List<Map<String, Object>> listClaudeFragments(@PathVariable String id) {
        return claudeAnalysisService.listFragments(id);
    }

    @GetMapping("/{id}/claude-fragments/{filename}")
    public ResponseEntity<String> readClaudeFragment(@PathVariable String id,
                                                      @PathVariable String filename) throws IOException {
        // sanitize() already collapses separators; the explicit ".." rejection replaces the
        // startsWith(claudeDir) containment check, which no longer has a base path to compare to.
        String sanitizedFile = JarNameUtil.sanitize(filename);
        if (sanitizedFile.contains("..") || sanitizedFile.contains("/") || sanitizedFile.contains("\\")) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Fragment not found: " + filename);
        }
        ArtifactKey fragKey = jarDataPaths.claudeDirKey(id).resolve(sanitizedFile);
        if (!store.exists(fragKey)) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Fragment not found: " + filename);
        }
        String content = readString(fragKey);
        MediaType mt = filename.endsWith(".json") ? MediaType.APPLICATION_JSON : MediaType.TEXT_PLAIN;
        return ResponseEntity.ok().contentType(mt).body(content);
    }

    @GetMapping("/{id}/endpoints")
    public List<Map<String, Object>> listEndpointOutputs(@PathVariable String id) {
        return claudeAnalysisService.listEndpointOutputs(id);
    }

    @GetMapping("/{id}/endpoints/{endpoint}/nodes")
    public ResponseEntity<String> getEndpointNodes(@PathVariable String id,
                                                    @PathVariable String endpoint) throws IOException {
        ArtifactKey nodesKey = endpointDirKey(id, endpoint).resolve("nodes.json");
        if (!store.exists(nodesKey)) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Endpoint not found: " + endpoint);
        }
        return ResponseEntity.ok()
                .contentType(MediaType.APPLICATION_JSON)
                .body(readString(nodesKey));
    }

    @GetMapping("/{id}/endpoints/{endpoint}/nodes/{nodeId}")
    public ResponseEntity<String> getEndpointNode(@PathVariable String id,
                                                   @PathVariable String endpoint,
                                                   @PathVariable int nodeId) throws IOException {
        ArtifactKey nodeKey = endpointDirKey(id, endpoint)
                .resolve("nodes").resolve("node_" + nodeId + ".json");
        if (!store.exists(nodeKey)) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Node not found: " + nodeId);
        }
        return ResponseEntity.ok()
                .contentType(MediaType.APPLICATION_JSON)
                .body(readString(nodeKey));
    }

    @GetMapping("/{id}/endpoints/{endpoint}/call-tree")
    public ResponseEntity<String> getEndpointCallTree(@PathVariable String id,
                                                       @PathVariable String endpoint) throws IOException {
        ArtifactKey treeKey = endpointDirKey(id, endpoint).resolve("call-tree.json");
        if (!store.exists(treeKey)) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Endpoint not found: " + endpoint);
        }
        // The sidecar is stored as a node dictionary; expand it back to the nested tree this
        // endpoint has always returned. Legacy sidecars pass through untouched.
        var sidecar = objectMapper.readTree(readString(treeKey));
        var tree = com.jaranalyzer.service.CallTreeInflater.expandSidecar(sidecar, objectMapper);
        return ResponseEntity.ok()
                .contentType(MediaType.APPLICATION_JSON)
                .body(objectMapper.writeValueAsString(tree));
    }

    private Map<String, Object> extractClassData(String id, String className, JarDataPaths paths) {
        // Same corrected-then-static precedence PersistenceService.getFilePath applies, expressed
        // as keys so no local file has to exist.
        ArtifactKey key = paths.correctedKey(id);
        if (!store.exists(key)) key = paths.analysisKey(id);
        if (!store.exists(key)) return null;

        // Still streaming: analysis documents reach several GB, so the parser reads the store's
        // InputStream directly and stops at the first matching class.
        try (var parser = objectMapper.getFactory().createParser(store.read(key))) {
            // Captures the invocation pool on the way to `classes`. This is the whole-class
            // passthrough that feeds summary-codeview.js, which has 8 live reads of
            // `.invocations` — without inflation those would all come back empty.
            var invPool = com.jaranalyzer.service.InvocationPool.Inflater
                    .seekClasses(parser, objectMapper);
            if (parser.currentToken() == com.fasterxml.jackson.core.JsonToken.START_ARRAY) {
                while (parser.nextToken() != com.fasterxml.jackson.core.JsonToken.END_ARRAY) {
                    com.fasterxml.jackson.databind.JsonNode node = objectMapper.readTree(parser);
                    String fqn = node.has("fullyQualifiedName") ? node.get("fullyQualifiedName").asText() : "";
                    String simple = node.has("simpleName") ? node.get("simpleName").asText() : "";
                    if (className.equals(fqn) || className.equals(simple)) {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> result = objectMapper.convertValue(
                                invPool.inflateClass(node, objectMapper), Map.class);
                        return result;
                    }
                }
            }
        } catch (Exception e) {
            log.debug("Failed to extract class data for {}: {}", className, e.getMessage());
        }
        return null;
    }
}
