package com.plsqlanalyzer.web.controller;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Stream;

@RestController
@RequestMapping("/api/docs")
public class DocsController {

    private final Path docsDir;

    public DocsController(@Value("${app.config-dir:./config}") String configDir) {
        this.docsDir = Path.of(configDir).getParent().resolve("docs");
    }

    @GetMapping
    public ResponseEntity<List<Map<String, String>>> listDocs() {
        if (!Files.isDirectory(docsDir)) {
            return ResponseEntity.ok(Collections.emptyList());
        }
        try (Stream<Path> files = Files.list(docsDir)) {
            List<Map<String, String>> docs = files
                    .filter(p -> p.toString().endsWith(".md"))
                    .sorted()
                    .map(p -> {
                        String name = p.getFileName().toString().replace(".md", "");
                        String title = name.replace("manual-", "").replace("-", " ");
                        title = title.substring(0, 1).toUpperCase() + title.substring(1);
                        return Map.of("id", name, "title", title);
                    })
                    .toList();
            return ResponseEntity.ok(docs);
        } catch (Exception e) {
            return ResponseEntity.ok(Collections.emptyList());
        }
    }

    @GetMapping("/{id}")
    public ResponseEntity<String> getDoc(@PathVariable String id) {
        if (id.contains("..") || id.contains("/") || id.contains("\\")) {
            return ResponseEntity.badRequest().body("Invalid document ID");
        }
        Path file = docsDir.resolve(id + ".md");
        if (!Files.exists(file)) {
            return ResponseEntity.notFound().build();
        }
        try {
            String content = Files.readString(file);
            return ResponseEntity.ok().contentType(MediaType.TEXT_PLAIN).body(content);
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body("Error reading document");
        }
    }
}
