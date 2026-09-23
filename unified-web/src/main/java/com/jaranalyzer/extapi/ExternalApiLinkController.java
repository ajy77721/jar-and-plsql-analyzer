package com.jaranalyzer.extapi;

import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;

/**
 * REST API for cross-JAR HTTP call linking (see ExternalApiLinkService).
 *
 *   GET  /api/jar/jars/{id}/external-links          → resolved + ambiguous links for this JAR
 *   GET  /api/jar/jars/{id}/external-links/pending   → unresolved calls with module hints
 *   POST /api/jar/jars/{id}/external-links/resolve   → (re)run resolution for this JAR
 */
@RestController
@RequestMapping("/api/jar/jars/{id}")
public class ExternalApiLinkController {

    private static final Logger log = LoggerFactory.getLogger(ExternalApiLinkController.class);

    private final ExternalApiLinkService service;

    public ExternalApiLinkController(ExternalApiLinkService service) {
        this.service = service;
    }

    @GetMapping("/external-links")
    public ResponseEntity<JsonNode> getLinks(@PathVariable String id) throws IOException {
        return ResponseEntity.ok(service.getLinks(id));
    }

    @GetMapping("/external-links/pending")
    public ResponseEntity<JsonNode> getPending(@PathVariable String id) throws IOException {
        return ResponseEntity.ok(service.getPending(id));
    }

    @PostMapping("/external-links/resolve")
    public ResponseEntity<?> resolve(@PathVariable String id) {
        try {
            var result = service.resolve(id);
            return ResponseEntity.ok(java.util.Map.of(
                    "resolved", result.resolved().size(),
                    "ambiguous", result.ambiguous().size(),
                    "pending", result.pending().size()
            ));
        } catch (Exception e) {
            log.warn("External link resolution failed for {}: {}", id, e.getMessage());
            return ResponseEntity.internalServerError().body(java.util.Map.of("error", e.getMessage()));
        }
    }
}
