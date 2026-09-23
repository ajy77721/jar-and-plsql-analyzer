package com.plsqlanalyzer.web.controller;

import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * User-manual documents (Home/Architecture/Parser/JAR-analyzer/Properties) — served ENTIRELY from
 * classpath resources bundled into the jar at build time ({@code src/main/resources/docs/*.md}),
 * never from a filesystem {@code docs/} folder or the {@link com.analyzer.storage.ArtifactStore}.
 *
 * <p>Real bug this replaces: the previous version served these through {@code ArtifactStore}
 * under {@code {data}/docs/*.md} — which meant they had to be manually seeded into whatever
 * backing store the app was configured for (local disk / MongoDB / S3) as a SEPARATE step after
 * every fresh deployment, since {@code mvn package} never copied the project-root {@code docs/}
 * folder into the store. On a genuinely fresh mongodb-mode deployment (no prior manual seeding),
 * every manual 404'd — confirmed live. Docs are static, ship with the code, and never change per
 * environment/storage-mode, so they belong in the jar itself: the classpath is populated once, at
 * build time, by Maven copying {@code src/main/resources/**} into the jar — no runtime seeding
 * step, no dependency on storage mode, no dependency on any external {@code docs/} directory
 * existing on whatever machine runs the jar.
 */
@RestController
@RequestMapping("/api/docs")
public class DocsController {

    private static final String CLASSPATH_PREFIX = "classpath:docs/";

    @GetMapping
    public ResponseEntity<List<Map<String, String>>> listDocs() {
        try {
            ResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
            Resource[] resources = resolver.getResources(CLASSPATH_PREFIX + "*.md");
            List<Map<String, String>> docs = Arrays.stream(resources)
                    .map(Resource::getFilename)
                    .filter(Objects::nonNull)
                    .filter(n -> n.endsWith(".md"))
                    .sorted()
                    .map(fileName -> {
                        String name = fileName.substring(0, fileName.length() - ".md".length());
                        String title = name.replace("manual-", "").replace("-", " ");
                        title = title.substring(0, 1).toUpperCase() + title.substring(1);
                        return Map.of("id", name, "title", title);
                    })
                    .toList();
            return ResponseEntity.ok(docs);
        } catch (IOException e) {
            return ResponseEntity.ok(Collections.emptyList());
        }
    }

    @GetMapping("/{id}")
    public ResponseEntity<String> getDoc(@PathVariable String id) {
        // Traversal guard: id becomes a classpath resource path segment below.
        if (id.contains("..") || id.contains("/") || id.contains("\\")) {
            return ResponseEntity.badRequest().body("Invalid document ID");
        }
        Resource resource = new ClassPathResource("docs/" + id + ".md");
        if (!resource.exists()) {
            return ResponseEntity.notFound().build();
        }
        try (InputStream in = resource.getInputStream()) {
            String content = new String(in.readAllBytes(), StandardCharsets.UTF_8);
            return ResponseEntity.ok().contentType(MediaType.TEXT_PLAIN).body(content);
        } catch (IOException e) {
            return ResponseEntity.internalServerError().body("Error reading document");
        }
    }

    /**
     * Real gap, found live: {@code manual-feature-guide.md} embeds real screenshots via relative
     * markdown image links ({@code ![...](screenshots/foo.png)}), but nothing served them — this
     * controller only ever returned {@code .md} text, and even if it had, {@code UserManual.js}
     * renders markdown into an arbitrary DOM container with no {@code <base>} href, so a relative
     * path would resolve against the CURRENT PAGE's URL, not against {@code /api/docs/}, regardless.
     * Screenshots are rewritten (see {@code manual-feature-guide.md}) to reference this absolute
     * route directly instead.
     */
    @GetMapping("/screenshots/{filename}")
    public ResponseEntity<byte[]> getScreenshot(@PathVariable String filename) throws IOException {
        if (filename.contains("..") || filename.contains("/") || filename.contains("\\")) {
            return ResponseEntity.badRequest().build();
        }
        Resource resource = new ClassPathResource("docs/screenshots/" + filename);
        if (!resource.exists()) {
            return ResponseEntity.notFound().build();
        }
        try (InputStream in = resource.getInputStream()) {
            byte[] bytes = in.readAllBytes();
            return ResponseEntity.ok().contentType(MediaType.IMAGE_PNG).body(bytes);
        }
    }
}
