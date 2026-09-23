package com.plsqlanalyzer.web.controller.detail;

import com.analyzer.queue.AnalysisQueueService;
import com.analyzer.queue.QueueJob;
import com.plsqlanalyzer.analyzer.service.AnalysisService;
import com.plsqlanalyzer.config.DbUserConfig;
import com.plsqlanalyzer.config.EnvironmentConfig;
import com.plsqlanalyzer.config.PlsqlConfig;
import com.plsqlanalyzer.parser.service.OracleDictionaryService.ObjectRecord;
import com.plsqlanalyzer.web.controller.detail.ObjectResolutionService.ResolvedObject;
import com.plsqlanalyzer.web.service.AnalysisJobService;
import com.plsqlanalyzer.web.service.AnalysisJobService.Job;
import com.plsqlanalyzer.web.service.DbSourceFetcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Handles the /analyze endpoint logic (extracted from AnalyzerController). */
@Service
public class AnalysisTriggerService {

    private static final Logger log = LoggerFactory.getLogger(AnalysisTriggerService.class);

    private final AnalysisService analysisService;
    private final PlsqlConfig config;
    private final DbSourceFetcher dbFetcher;
    private final AnalysisJobService jobService;
    private final AnalysisQueueService queueService;
    private final ObjectResolutionService resolution;

    public AnalysisTriggerService(AnalysisService analysisService, PlsqlConfig config,
                                  DbSourceFetcher dbFetcher, AnalysisJobService jobService,
                                  AnalysisQueueService queueService, ObjectResolutionService resolution) {
        this.analysisService = analysisService;
        this.config = config;
        this.dbFetcher = dbFetcher;
        this.jobService = jobService;
        this.queueService = queueService;
        this.resolution = resolution;
    }

    public ResponseEntity<Map<String, Object>> triggerAnalysis(Map<String, String> body) {
        String username = body.get("username");
        String objectName = body.get("objectName");
        String objectType = body.get("objectType");
        String procedureName = body.get("procedureName");
        String projectName = body.get("project");
        String envName = body.get("env");

        Map<String, Object> guard = resolution.nonOracleAnalyzeGuard(projectName, envName);
        if (guard != null) return ResponseEntity.badRequest().body(guard);

        // Resolve environment-specific JDBC URL and users if project/env specified
        String resolvedJdbcUrl = null;
        List<DbUserConfig> resolvedUsers = null;
        if (projectName != null && !projectName.isBlank() && envName != null && !envName.isBlank()) {
            EnvironmentConfig envConfig = config.resolveEnvironment(projectName, envName);
            if (envConfig != null) {
                resolvedJdbcUrl = envConfig.getJdbcUrl();
                resolvedUsers = envConfig.getUsers();
                log.info("Using environment {}/{} -> {}", projectName, envName, resolvedJdbcUrl);
            }
        }

        if (objectName == null || objectName.isBlank()) {
            return ResponseEntity.badRequest().body(Map.of("error", "objectName is required"));
        }

        // ---- Smart parsing: split dots ----
        String[] parts = objectName.toUpperCase().trim().split("\\.");

        if (parts.length == 3) {
            if (username == null || username.isBlank()) username = parts[0];
            objectName = parts[1];
            if (procedureName == null || procedureName.isBlank()) procedureName = parts[2];
        } else if (parts.length == 2) {
            List<DbUserConfig> effectiveUsers = resolvedUsers != null ? resolvedUsers : config.getDbUsers();
            boolean firstIsSchema = effectiveUsers.stream()
                    .anyMatch(u -> u.getUsername().equalsIgnoreCase(parts[0]));
            if (firstIsSchema) {
                if (username == null || username.isBlank()) username = parts[0];
                objectName = parts[1];
            } else {
                ResolvedObject r0 = resolution.findObjectInAllSchemas(parts[0]);
                if (r0 != null && ("PACKAGE".equals(r0.objectType) || "PACKAGE BODY".equals(r0.objectType))) {
                    objectName = parts[0];
                    if (procedureName == null || procedureName.isBlank()) procedureName = parts[1];
                    if (username == null || username.isBlank()) username = r0.schema;
                    objectType = "PACKAGE";
                } else if (r0 != null) {
                    objectName = parts[0];
                    if (username == null || username.isBlank()) username = r0.schema;
                    objectType = r0.objectType;
                } else {
                    objectName = parts[0];
                    if (procedureName == null || procedureName.isBlank()) procedureName = parts[1];
                }
            }
        } else {
            objectName = parts[0];
        }

        // ---- Auto-detect schema and type if not yet known ----
        if (username == null || username.isBlank()) {
            ResolvedObject resolved = resolution.findObjectInAllSchemas(objectName);
            if (resolved == null) {
                return ResponseEntity.badRequest().body(Map.of(
                        "error", "Could not find object '" + objectName + "' in any configured schema"));
            }
            username = resolved.schema;
            if (objectType == null || objectType.isBlank()) {
                objectType = resolution.normalizeObjectType(resolved.objectType);
            }
            log.info("Auto-resolved: {} -> schema={}, type={}", objectName, username, objectType);
        }

        // If we have a schema but no type, detect the type
        if (objectType == null || objectType.isBlank()) {
            DbUserConfig tmpUser = resolution.findUser(username);
            if (tmpUser != null) {
                try (Connection conn = dbFetcher.getConnection(tmpUser)) {
                    ObjectRecord obj = analysisService.getDictionaryService()
                            .getObjectInfo(conn, username, objectName);
                    if (obj != null) {
                        objectType = resolution.normalizeObjectType(obj.objectType());
                    }
                } catch (Exception e) {
                    log.debug("Could not detect type for {}.{}: {}", username, objectName, e.getMessage());
                }
            }
            if (objectType == null || objectType.isBlank()) objectType = "PACKAGE";
        }

        List<DbUserConfig> activeUsers = resolvedUsers != null ? resolvedUsers : config.getDbUsers();
        DbUserConfig user = resolution.findUserIn(activeUsers, username);
        if (user == null) {
            return ResponseEntity.badRequest().body(Map.of("error", "User not found in config: " + username));
        }

        log.info("Submitting to queue: schema={}, object={}, type={}, proc={}",
                username, objectName, objectType, procedureName);

        String displayName = "PL/SQL: " + objectName
                + (procedureName != null ? "." + procedureName : "");

        Job legacyJob = jobService.createJob(username, objectName, objectType, procedureName);

        Map<String, Object> metadata = new LinkedHashMap<>();
        metadata.put("username", username);
        metadata.put("objectName", objectName);
        metadata.put("objectType", objectType);
        metadata.put("procedureName", procedureName);
        metadata.put("project", projectName);
        metadata.put("env", envName);
        metadata.put("legacyJobId", legacyJob.id);

        QueueJob queueJob = queueService.submit(QueueJob.Type.PLSQL_ANALYSIS, displayName, metadata);

        legacyJob.queueJobId = queueJob.id;

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("status", "queued");
        response.put("jobId", queueJob.id);
        response.put("legacyJobId", legacyJob.id);
        response.put("username", username);
        response.put("objectName", objectName);
        response.put("objectType", objectType);
        if (procedureName != null) response.put("procedureName", procedureName);
        return ResponseEntity.ok(response);
    }
}
