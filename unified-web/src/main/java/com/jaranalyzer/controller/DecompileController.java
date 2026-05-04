package com.jaranalyzer.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jaranalyzer.service.ClaudeAnalysisService;
import com.jaranalyzer.service.ClaudeEnrichmentTracker;
import com.jaranalyzer.service.DecompilerService;
import com.jaranalyzer.service.JarDataPaths;
import com.jaranalyzer.service.JarNameUtil;
import com.jaranalyzer.service.PersistenceService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

@RestController("jarDecompileController")
@RequestMapping("/api/jar/jars")
public class DecompileController {

    private static final Logger log = LoggerFactory.getLogger(DecompileController.class);
    private static final Pattern VALID_CLASS_NAME = Pattern.compile("^[a-zA-Z0-9._$]+$");

    private final PersistenceService persistenceService;
    private final ClaudeAnalysisService claudeAnalysisService;
    private final DecompilerService decompilerService;
    private final ObjectMapper objectMapper;
    private final ClaudeEnrichmentTracker claudeTracker;
    private final JarDataPaths jarDataPaths;

    public DecompileController(PersistenceService persistenceService,
                               ClaudeAnalysisService claudeAnalysisService,
                               DecompilerService decompilerService,
                               ObjectMapper objectMapper,
                               ClaudeEnrichmentTracker claudeTracker,
                               JarDataPaths jarDataPaths) {
        this.persistenceService = persistenceService;
        this.claudeAnalysisService = claudeAnalysisService;
        this.decompilerService = decompilerService;
        this.objectMapper = objectMapper;
        this.claudeTracker = claudeTracker;
        this.jarDataPaths = jarDataPaths;
    }

    @GetMapping("/{id}/decompile")
    public Map<String, Object> decompileClass(@PathVariable String id,
                                               @RequestParam("class") String className) {
        if (className == null || !VALID_CLASS_NAME.matcher(className).matches()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                    "Invalid class name — must be a valid Java fully-qualified name");
        }
        Path jarPath = persistenceService.getJarFilePath(id);
        String source = null;
        if (jarPath != null) {
            source = decompilerService.decompile(jarPath, className);
        }

        Map<String, Object> classData = extractClassData(id, className);

        if (source == null && classData == null) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND,
                    "Class not found: " + className);
        }

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("className", className);
        result.put("source", source);
        result.put("decompiler", source != null ? "CFR" : null);
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
        String sanitizedFile = JarNameUtil.sanitize(filename);
        Path fragPath = jarDataPaths.claudeDir(id).resolve(sanitizedFile);
        if (!Files.exists(fragPath)) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Fragment not found: " + filename);
        }
        String content = Files.readString(fragPath);
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
        Path nodesFile = jarDataPaths.endpointsDir(id)
                .resolve(endpoint.replaceAll("[^a-zA-Z0-9._-]", "_"))
                .resolve("nodes.json");
        if (!Files.exists(nodesFile)) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Endpoint not found: " + endpoint);
        }
        return ResponseEntity.ok()
                .contentType(MediaType.APPLICATION_JSON)
                .body(Files.readString(nodesFile));
    }

    @GetMapping("/{id}/endpoints/{endpoint}/nodes/{nodeId}")
    public ResponseEntity<String> getEndpointNode(@PathVariable String id,
                                                   @PathVariable String endpoint,
                                                   @PathVariable int nodeId) throws IOException {
        Path nodeFile = jarDataPaths.endpointsDir(id)
                .resolve(endpoint.replaceAll("[^a-zA-Z0-9._-]", "_"))
                .resolve("nodes").resolve("node_" + nodeId + ".json");
        if (!Files.exists(nodeFile)) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Node not found: " + nodeId);
        }
        return ResponseEntity.ok()
                .contentType(MediaType.APPLICATION_JSON)
                .body(Files.readString(nodeFile));
    }

    @GetMapping("/{id}/endpoints/{endpoint}/call-tree")
    public ResponseEntity<String> getEndpointCallTree(@PathVariable String id,
                                                       @PathVariable String endpoint) throws IOException {
        Path treeFile = jarDataPaths.endpointsDir(id)
                .resolve(endpoint.replaceAll("[^a-zA-Z0-9._-]", "_"))
                .resolve("call-tree.json");
        if (!Files.exists(treeFile)) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Endpoint not found: " + endpoint);
        }
        return ResponseEntity.ok()
                .contentType(MediaType.APPLICATION_JSON)
                .body(Files.readString(treeFile));
    }

    private Map<String, Object> extractClassData(String id, String className) {
        Path filePath = persistenceService.getFilePath(id);
        if (filePath == null || !Files.exists(filePath)) return null;

        try (var parser = objectMapper.getFactory().createParser(filePath.toFile())) {
            while (parser.nextToken() != null) {
                if (parser.currentToken() == com.fasterxml.jackson.core.JsonToken.FIELD_NAME
                        && "classes".equals(parser.currentName())) {
                    parser.nextToken();
                    while (parser.nextToken() != com.fasterxml.jackson.core.JsonToken.END_ARRAY) {
                        com.fasterxml.jackson.databind.JsonNode node = objectMapper.readTree(parser);
                        String fqn = node.has("fullyQualifiedName") ? node.get("fullyQualifiedName").asText() : "";
                        String simple = node.has("simpleName") ? node.get("simpleName").asText() : "";
                        if (className.equals(fqn) || className.equals(simple)) {
                            @SuppressWarnings("unchecked")
                            Map<String, Object> result = objectMapper.convertValue(node, Map.class);
                            return result;
                        }
                    }
                    break;
                }
            }
        } catch (Exception e) {
            log.debug("Failed to extract class data for {}: {}", className, e.getMessage());
        }
        return null;
    }
}
