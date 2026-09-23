package com.plsqlanalyzer.web.promptconfig;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/**
 * REST endpoints for editing prompt configuration (templates, global extras,
 * per-analysis extras). Read/write only — does not affect prompt-building logic.
 */
@RestController
@RequestMapping("/api/prompt-config")
public class PromptConfigController {

    private final PromptConfigService service;

    public PromptConfigController(PromptConfigService service) {
        this.service = service;
    }

    // ==================== templates ====================

    @GetMapping("/templates")
    public List<Map<String, Object>> listTemplates() {
        return service.listTemplates();
    }

    @PostMapping("/templates/{name}")
    public ResponseEntity<?> saveTemplate(@PathVariable String name,
                                          @RequestBody(required = false) Map<String, Object> body) {
        if (!service.isKnownTemplate(name)) {
            return ResponseEntity.badRequest().body(Map.of("error", "Unknown template name: " + name));
        }
        String text = body == null ? "" : str(body.get("text"));
        service.saveTemplate(name, text);
        return ResponseEntity.ok(Map.of("ok", true, "name", name));
    }

    @PostMapping("/templates/{name}/reset")
    public ResponseEntity<?> resetTemplate(@PathVariable String name) {
        if (!service.isKnownTemplate(name)) {
            return ResponseEntity.badRequest().body(Map.of("error", "Unknown template name: " + name));
        }
        service.resetTemplate(name);
        return ResponseEntity.ok(Map.of("ok", true, "name", name));
    }

    // ==================== global extras ====================

    @GetMapping("/extras")
    public Map<String, String> getExtras() {
        return service.getExtras();
    }

    @PostMapping("/extras")
    public Map<String, String> saveExtras(@RequestBody(required = false) Map<String, Object> body) {
        String globalAppend = body == null ? "" : str(body.get("globalAppend"));
        String globalContext = body == null ? "" : str(body.get("globalContext"));
        return service.saveExtras(globalAppend, globalContext);
    }

    // ==================== per-analysis extras ====================

    @GetMapping("/analysis/{name}")
    public Map<String, Object> getAnalysis(@PathVariable String name) {
        return service.getAnalysis(name);
    }

    @PostMapping("/analysis/{name}")
    @SuppressWarnings("unchecked")
    public Map<String, Object> saveAnalysis(@PathVariable String name,
                                            @RequestBody(required = false) Map<String, Object> body) {
        String context = body == null ? "" : str(body.get("context"));
        String append = body == null ? "" : str(body.get("append"));
        Map<String, String> templates = new java.util.LinkedHashMap<>();
        if (body != null && body.get("templates") instanceof Map<?, ?> tm) {
            tm.forEach((k, v) -> templates.put(String.valueOf(k), v == null ? "" : v.toString()));
        }
        return service.saveAnalysis(name, context, append, templates);
    }

    private static String str(Object o) {
        return o == null ? "" : o.toString();
    }
}
