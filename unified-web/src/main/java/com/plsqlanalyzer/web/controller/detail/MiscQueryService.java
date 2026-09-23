package com.plsqlanalyzer.web.controller.detail;

import com.plsqlanalyzer.analyzer.model.AnalysisResult;
import com.plsqlanalyzer.analyzer.model.CallGraphNode;
import com.plsqlanalyzer.analyzer.model.TableOperationSummary;
import com.plsqlanalyzer.analyzer.service.AnalysisService;
import com.plsqlanalyzer.config.DbUserConfig;
import com.plsqlanalyzer.config.PlsqlConfig;
import com.plsqlanalyzer.parser.service.OracleDictionaryService.DependencyRecord;
import com.plsqlanalyzer.web.service.DbSourceFetcher;
import com.plsqlanalyzer.web.service.PersistenceService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.util.*;

/** Errors / source / references / search endpoints (extracted from AnalyzerController). */
@Service
public class MiscQueryService {

    private static final Logger log = LoggerFactory.getLogger(MiscQueryService.class);

    private final AnalysisService analysisService;
    private final PlsqlConfig config;
    private final DbSourceFetcher dbFetcher;
    private final GraphMapHelper graphMapHelper;
    private final ObjectResolutionService resolution;
    private final PersistenceService persistence;

    public MiscQueryService(AnalysisService analysisService, PlsqlConfig config, DbSourceFetcher dbFetcher,
                            GraphMapHelper graphMapHelper, ObjectResolutionService resolution,
                            @Qualifier("plsqlPersistenceService") PersistenceService persistence) {
        this.analysisService = analysisService;
        this.config = config;
        this.dbFetcher = dbFetcher;
        this.graphMapHelper = graphMapHelper;
        this.persistence = persistence;
        this.resolution = resolution;
    }

    /**
     * Parse errors for the loaded analysis.
     *
     * <p>Returns <b>404 when there is no analysis</b>, not an empty list. "No analysis is loaded"
     * and "this analysis parsed cleanly" are different facts, and the client could not tell them
     * apart: it read the empty array and showed a green <i>"No parse errors"</i> toast. An error
     * state that renders as a green confirmation about the user's own code is the worst thing this
     * application can do, because the whole point of it is to report what the code contains.
     */
    public ResponseEntity<List<Map<String, Object>>> getParseErrors() {
        AnalysisResult result = analysisService.getLatestResult();
        if (result == null || result.getUnits() == null) return ResponseEntity.notFound().build();

        List<Map<String, Object>> errors = new ArrayList<>();
        for (var unit : result.getUnits()) {
            if (unit.getParseErrors() != null && !unit.getParseErrors().isEmpty()) {
                Map<String, Object> entry = new LinkedHashMap<>();
                entry.put("unitName", unit.getName());
                entry.put("schemaName", unit.getSchemaName());
                entry.put("unitType", unit.getUnitType() != null ? unit.getUnitType().name() : null);
                entry.put("sourceFile", unit.getSourceFile());
                entry.put("errorCount", unit.getParseErrors().size());
                entry.put("errors", unit.getParseErrors());
                errors.add(entry);
            }
        }
        return ResponseEntity.ok(errors);
    }

    public ResponseEntity<Map<String, Object>> getSource(String owner, String objectName) {
        AnalysisResult result = analysisService.getLatestResult();
        if (result == null) return ResponseEntity.notFound().build();

        String key = owner.toUpperCase() + "." + objectName.toUpperCase();
        // MID-027 — the one real single-key consumer of this map (a user viewing one procedure's
        // source), so a request for one file no longer forces the whole analysis's every source
        // file resident just to answer it. See PersistenceService.getSourceLines' own doc comment.
        List<String> lines = persistence.getSourceLines(result, key);
        if (lines == null || lines.isEmpty()) return ResponseEntity.notFound().build();

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("owner", owner.toUpperCase());
        response.put("objectName", objectName.toUpperCase());
        response.put("content", String.join("\n", lines));
        response.put("lineCount", lines.size());
        return ResponseEntity.ok(response);
    }

    public ResponseEntity<List<Map<String, String>>> getReferences(String objectName, String owner) {
        if (owner == null && config.getDbUsers().isEmpty()) return ResponseEntity.ok(Collections.emptyList());
        String resolvedOwner = owner != null ? owner : config.getDbUsers().get(0).getUsername();
        DbUserConfig user = resolution.findUser(resolvedOwner);
        if (user == null) return ResponseEntity.ok(Collections.emptyList());

        try (Connection conn = dbFetcher.getConnection(user)) {
            List<DependencyRecord> refs = analysisService.getReferences(conn, resolvedOwner, objectName);
            List<Map<String, String>> mapped = new ArrayList<>();
            for (DependencyRecord r : refs) {
                Map<String, String> m = new LinkedHashMap<>();
                m.put("owner", r.referencedOwner());
                m.put("name", r.referencedName());
                m.put("type", r.referencedType());
                mapped.add(m);
            }
            return ResponseEntity.ok(mapped);
        } catch (Exception e) {
            // 500, not an empty list. This path queries the live database, so the empty answer meant
            // "this object is referenced by nothing" — which is what someone reads just before
            // deleting it. A database outage must not look like a finding about their schema.
            log.error("Failed to get references: {}", e.getMessage());
            return ResponseEntity.status(503).build();
        }
    }

    public ResponseEntity<List<Map<String, Object>>> search(String q, String type) {
        AnalysisResult result = analysisService.getLatestResult();
        // 404: "nothing matched" and "there is nothing to search" are different answers.
        if (result == null) return ResponseEntity.notFound().build();

        List<Map<String, Object>> results = new ArrayList<>();
        String query = q.toUpperCase();

        if ("procedure".equals(type) || "all".equals(type)) {
            for (CallGraphNode node : result.getCallGraph().searchNodes(query)) {
                Map<String, Object> r = graphMapHelper.nodeToMap(node);
                r.put("type", "procedure");
                results.add(r);
            }
        }

        if ("table".equals(type) || "all".equals(type)) {
            for (var entry : result.getTableOperations().entrySet()) {
                if (entry.getKey().contains(query)) {
                    TableOperationSummary s = entry.getValue();
                    Map<String, Object> r = new LinkedHashMap<>();
                    r.put("type", "table");
                    r.put("name", s.getTableName());
                    r.put("schemaName", s.getSchemaName());
                    r.put("operations", s.getOperations());
                    r.put("accessCount", s.getAccessDetails().size());
                    r.put("external", s.isExternal());
                    results.add(r);
                }
            }
        }

        return ResponseEntity.ok(results);
    }
}
