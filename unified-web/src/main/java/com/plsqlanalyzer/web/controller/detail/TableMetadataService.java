package com.plsqlanalyzer.web.controller.detail;

import com.plsqlanalyzer.analyzer.model.AnalysisResult;
import com.plsqlanalyzer.analyzer.model.TableOperationSummary;
import com.plsqlanalyzer.analyzer.service.AnalysisService;
import com.plsqlanalyzer.parser.service.OracleDictionaryService;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.util.*;

/** Table metadata + trigger endpoints (extracted from AnalyzerController). */
@Service
public class TableMetadataService {

    private final AnalysisService analysisService;

    public TableMetadataService(AnalysisService analysisService) {
        this.analysisService = analysisService;
    }

    public ResponseEntity<Map<String, Object>> getTableMetadata(String tableName) {
        AnalysisResult result = analysisService.getLatestResult();
        if (result == null) return ResponseEntity.notFound().build();

        OracleDictionaryService.TableMetadata meta = result.getTableMetadata() != null
                ? result.getTableMetadata().get(tableName.toUpperCase()) : null;
        if (meta == null) {
            // Try partial match
            if (result.getTableMetadata() != null) {
                for (var entry : result.getTableMetadata().entrySet()) {
                    if (entry.getKey().contains(tableName.toUpperCase())) {
                        meta = entry.getValue();
                        break;
                    }
                }
            }
        }
        if (meta == null) return ResponseEntity.notFound().build();

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("tableName", meta.tableName());
        response.put("schema", meta.owner());
        response.put("found", true);
        response.put("isView", meta.isView());
        response.put("columnCount", meta.columns().size());

        // Columns
        List<Map<String, Object>> columns = new ArrayList<>();
        for (var col : meta.columns()) {
            Map<String, Object> c = new LinkedHashMap<>();
            c.put("columnName", col.columnName());
            c.put("dataType", col.dataType());
            c.put("dataLength", col.dataLength());
            c.put("dataPrecision", col.dataPrecision());
            c.put("dataScale", col.dataScale());
            c.put("nullable", col.nullable());
            c.put("columnId", col.columnId());
            c.put("dataDefault", col.dataDefault());
            columns.add(c);
        }
        response.put("columns", columns);

        // Constraints
        List<Map<String, Object>> constraints = new ArrayList<>();
        for (var con : meta.constraints()) {
            Map<String, Object> c = new LinkedHashMap<>();
            c.put("constraintName", con.constraintName());
            c.put("constraintType", con.constraintType());
            c.put("columnName", con.columnName());
            c.put("position", con.position());
            c.put("refConstraint", con.refConstraint());
            constraints.add(c);
        }
        response.put("constraints", constraints);

        // Indexes
        List<Map<String, Object>> indexes = new ArrayList<>();
        for (var idx : meta.indexes()) {
            Map<String, Object> i = new LinkedHashMap<>();
            i.put("indexName", idx.indexName());
            i.put("uniqueness", idx.uniqueness());
            i.put("columnName", idx.columnName());
            i.put("position", idx.position());
            indexes.add(i);
        }
        response.put("indexes", indexes);

        // View definition (only present for views)
        if (meta.isView() && meta.viewDefinition() != null) {
            response.put("viewDefinition", meta.viewDefinition());
        }

        return ResponseEntity.ok(response);
    }

    public ResponseEntity<List<Map<String, Object>>> getTableTriggers(String tableName) {
        AnalysisResult result = analysisService.getLatestResult();
        if (result == null) return ResponseEntity.notFound().build();

        TableOperationSummary summary = result.getTableOperations().get(tableName.toUpperCase());
        if (summary == null) {
            for (var entry : result.getTableOperations().entrySet()) {
                if (entry.getKey().contains(tableName.toUpperCase())) {
                    summary = entry.getValue();
                    break;
                }
            }
        }
        if (summary == null || summary.getTriggers() == null || summary.getTriggers().isEmpty()) {
            return ResponseEntity.ok(Collections.emptyList());
        }

        List<Map<String, Object>> triggers = new ArrayList<>();
        for (var tr : summary.getTriggers()) {
            Map<String, Object> tm = new LinkedHashMap<>();
            tm.put("triggerName", tr.getTriggerName());
            tm.put("triggerOwner", tr.getTriggerOwner());
            tm.put("triggeringEvent", tr.getTriggeringEvent());
            tm.put("triggerType", tr.getTriggerType());
            tm.put("status", tr.getStatus());
            tm.put("calledProcedures", tr.getCalledProcedures());
            tm.put("triggerBody", tr.getTriggerBody());
            tm.put("tableOn", tableName.toUpperCase());

            // Extract referenced tables + DML operations from trigger body
            List<Map<String, Object>> referencedTableOps = new ArrayList<>();
            List<String> referencedTables = new ArrayList<>();
            if (tr.getTriggerBody() != null) {
                String body = tr.getTriggerBody().toUpperCase();
                for (var tableEntry : result.getTableOperations().entrySet()) {
                    String tName = tableEntry.getKey().toUpperCase();
                    if (body.contains(tName) && !tName.equals(tableName.toUpperCase())) {
                        referencedTables.add(tableEntry.getKey());
                        List<String> ops = new ArrayList<>();
                        if (body.contains("INSERT INTO " + tName) || body.contains("INSERT INTO\n" + tName)
                                || body.contains("INSERT  INTO " + tName))
                            ops.add("INSERT");
                        if (body.contains("UPDATE " + tName + " ") || body.contains("UPDATE " + tName + "\n")
                                || body.contains("UPDATE " + tName + "\r"))
                            ops.add("UPDATE");
                        if (body.contains("DELETE FROM " + tName) || body.contains("DELETE " + tName))
                            ops.add("DELETE");
                        if (body.contains("SELECT") && body.contains("FROM " + tName))
                            ops.add("SELECT");
                        if (ops.isEmpty() && body.contains(tName)) ops.add("REFERENCE");
                        Map<String, Object> refEntry = new LinkedHashMap<>();
                        refEntry.put("tableName", tableEntry.getKey());
                        refEntry.put("operations", ops);
                        referencedTableOps.add(refEntry);
                    }
                }
            }
            tm.put("referencedTables", referencedTables);
            tm.put("referencedTableOps", referencedTableOps);
            triggers.add(tm);
        }
        return ResponseEntity.ok(triggers);
    }
}
