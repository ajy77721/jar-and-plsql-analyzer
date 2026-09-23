package com.plsqlanalyzer.web.controller.detail;

import com.analyzer.queue.AnalysisQueueService;
import com.analyzer.queue.QueueJob;
import com.plsqlanalyzer.config.DbUserConfig;
import com.plsqlanalyzer.config.PlsqlConfig;
import com.plsqlanalyzer.web.controller.detail.ObjectResolutionService.ResolvedObject;
import com.plsqlanalyzer.web.service.AnalysisJobService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Handles the /analyze-fast endpoint logic (extracted from AnalyzerController). */
@Service
public class FastAnalysisTriggerService {

    private static final Logger log = LoggerFactory.getLogger(FastAnalysisTriggerService.class);

    private final PlsqlConfig config;
    private final AnalysisJobService jobService;
    private final AnalysisQueueService queueService;
    private final ObjectResolutionService resolution;

    public FastAnalysisTriggerService(PlsqlConfig config, AnalysisJobService jobService,
                                      AnalysisQueueService queueService, ObjectResolutionService resolution) {
        this.config = config;
        this.jobService = jobService;
        this.queueService = queueService;
        this.resolution = resolution;
    }

    public ResponseEntity<Map<String, Object>> triggerFastAnalysis(Map<String, String> body) {
        String username = body.get("username");
        String objectName = body.get("objectName");
        String objectType = body.getOrDefault("objectType", "PACKAGE");
        String procedureName = body.get("procedureName");
        String projectName = body.get("project");
        String envName = body.get("env");

        Map<String, Object> guard = resolution.nonOracleAnalyzeGuard(projectName, envName);
        if (guard != null) return ResponseEntity.badRequest().body(guard);

        if (objectName == null || objectName.isBlank()) {
            return ResponseEntity.badRequest().body(Map.of("error", "objectName is required"));
        }

        // Smart parsing: split dots (same logic as /analyze)
        String[] parts = objectName.toUpperCase().trim().split("\\.");
        if (parts.length == 3) {
            if (username == null || username.isBlank()) username = parts[0];
            objectName = parts[1];
            if (procedureName == null || procedureName.isBlank()) procedureName = parts[2];
        } else if (parts.length == 2) {
            List<DbUserConfig> effectiveUsers = config.getDbUsers();
            boolean firstIsSchema = effectiveUsers.stream()
                    .anyMatch(u -> u.getUsername().equalsIgnoreCase(parts[0]));
            if (firstIsSchema) {
                if (username == null || username.isBlank()) username = parts[0];
                objectName = parts[1];
            } else {
                objectName = parts[0];
                if (procedureName == null || procedureName.isBlank()) procedureName = parts[1];
            }
        } else {
            objectName = parts[0];
        }

        if (username == null || username.isBlank()) {
            ResolvedObject resolved = resolution.findObjectInAllSchemas(objectName);
            if (resolved != null) {
                username = resolved.schema;
                if (objectType == null || objectType.isBlank()) objectType = resolution.normalizeObjectType(resolved.objectType);
            }
        }

        if (username == null || username.isBlank()) {
            return ResponseEntity.badRequest().body(Map.of("error", "username (schema) is required"));
        }

        AnalysisJobService.Job legacyJob = jobService.createJob(username, objectName, objectType, procedureName);

        Map<String, Object> metadata = new LinkedHashMap<>();
        metadata.put("username", username.toUpperCase());
        metadata.put("objectName", objectName.toUpperCase());
        metadata.put("objectType", objectType.toUpperCase());
        metadata.put("procedureName", procedureName != null ? procedureName.toUpperCase() : null);
        metadata.put("legacyJobId", legacyJob.id);
        metadata.put("project", projectName);
        metadata.put("env", envName);

        log.info("Submitting FAST analysis: schema={}, object={}, type={}, proc={}",
                username, objectName, objectType, procedureName);

        QueueJob queueJob = queueService.submit(
                QueueJob.Type.PLSQL_FAST_ANALYSIS,
                "Fast PL/SQL: " + objectName + (procedureName != null ? "." + procedureName : ""),
                metadata);
        legacyJob.queueJobId = queueJob.id;

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("status", "queued");
        response.put("mode", "fast");
        response.put("jobId", queueJob.id);
        response.put("legacyJobId", legacyJob.id);
        response.put("username", username);
        response.put("objectName", objectName);
        response.put("objectType", objectType);
        if (procedureName != null) response.put("procedureName", procedureName);
        return ResponseEntity.ok(response);
    }
}
