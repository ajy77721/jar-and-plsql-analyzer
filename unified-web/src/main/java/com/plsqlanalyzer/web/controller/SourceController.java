package com.plsqlanalyzer.web.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

@RestController("plsqlSourceController")
@RequestMapping("/api/plsql/source")
public class SourceController {

    private static final Logger log = LoggerFactory.getLogger(SourceController.class);

    private final DatabaseController databaseController;

    public SourceController(DatabaseController databaseController) {
        this.databaseController = databaseController;
    }

    private static final Path ALLOWED_BASE = Path.of("data").toAbsolutePath().normalize();

    @GetMapping("/file")
    public ResponseEntity<Map<String, Object>> getSourceFile(
            @RequestParam String path,
            @RequestParam(required = false) Integer line) {

        log.info("GET /api/source/file?path={}&line={}", path, line);

        Path filePath = Path.of(path).toAbsolutePath().normalize();
        if (!filePath.startsWith(ALLOWED_BASE)) {
            log.warn("Path traversal blocked: {}", path);
            return databaseController.getCachedSource(path, line);
        }

        if (Files.exists(filePath)) {
            try {
                String content = Files.readString(filePath, StandardCharsets.UTF_8);
                Map<String, Object> response = new LinkedHashMap<>();
                response.put("path", path);
                response.put("fileName", filePath.getFileName().toString());
                response.put("content", content);
                response.put("lineCount", content.split("\n").length);
                if (line != null) {
                    response.put("highlightLine", line);
                }
                return ResponseEntity.ok(response);
            } catch (IOException e) {
                log.warn("Failed to read source file from disk: {}", path);
            }
        } else {
            log.warn("Source file not found on disk: {}, trying DB cache", path);
        }

        return databaseController.getCachedSource(path, line);
    }
}
