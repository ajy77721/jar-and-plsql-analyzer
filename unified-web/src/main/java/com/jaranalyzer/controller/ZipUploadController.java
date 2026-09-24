package com.jaranalyzer.controller;

import com.jaranalyzer.service.ZipUploadService;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.util.Map;

/**
 * {@code POST /api/jar/zip-upload} — ingest a ZIP of locally produced analysis results without
 * re-running any analysis. Lives under {@code /api/jar} (not {@code /api/jar/jars}, where the
 * ordinary JAR upload sits) because it accepts JAR, WAR and PL/SQL results alike.
 *
 * <p>The actual detection and the store writes are in {@link ZipUploadService}; this class is only
 * the HTTP shape. Because the writes go through the injected (per-user routed) {@code ArtifactStore}
 * beans, an unauthenticated request raises {@code NotAuthenticatedException}, which
 * {@code ApiExceptionAdvice} turns into a 401.
 *
 * @param dryRun detect and validate only, writing nothing — the UI calls this on file selection to
 *               render its "Detected: …" confirmation before the user commits.
 */
@RestController("zipUploadController")
@RequestMapping("/api/jar")
public class ZipUploadController {

    private final ZipUploadService service;

    public ZipUploadController(ZipUploadService service) {
        this.service = service;
    }

    @PostMapping("/zip-upload")
    public Map<String, Object> zipUpload(
            @RequestParam("file") MultipartFile file,
            @RequestParam(value = "project", required = false) String project,
            @RequestParam(value = "env", required = false) String env,
            @RequestParam(value = "dialect", required = false) String dialect,
            @RequestParam(value = "analysisName", required = false) String analysisName,
            @RequestParam(value = "mode", defaultValue = "static") String mode,
            @RequestParam(value = "dryRun", defaultValue = "false") boolean dryRun) throws IOException {
        return service.ingest(file, project, env, dialect, analysisName, mode, dryRun);
    }
}
