package com.plsqlanalyzer.analyzer.service;

import com.plsqlanalyzer.analyzer.model.TableOperationSummary;
import com.plsqlanalyzer.analyzer.model.TableOperationSummary.TriggerDetail;
import com.plsqlanalyzer.parser.model.PlsqlProcedure;
import com.plsqlanalyzer.parser.model.PlsqlUnit;
import com.plsqlanalyzer.parser.model.ProcedureCall;
import com.plsqlanalyzer.parser.model.SqlOperationType;
import com.plsqlanalyzer.parser.service.OracleDictionaryService;
import com.plsqlanalyzer.parser.service.OracleDictionaryService.TriggerRecord;
import com.plsqlanalyzer.parser.service.PlSqlAnalyzerParserService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Resolves triggers for tables and extracts called procedures from trigger bodies.
 * For each table touched during analysis, queries ALL_TRIGGERS, fetches trigger source,
 * parses it, and extracts procedure calls.
 */
public class TriggerResolver {

    private static final Logger log = LoggerFactory.getLogger(TriggerResolver.class);

    private final OracleDictionaryService dictionaryService;
    private final PlSqlAnalyzerParserService parserService;

    public TriggerResolver(OracleDictionaryService dictionaryService,
                           PlSqlAnalyzerParserService parserService) {
        this.dictionaryService = dictionaryService;
        this.parserService = parserService;
    }

    /**
     * Resolve triggers from pre-fetched trigger records (bulk path).
     * Fetches and parses trigger source to extract called procedures.
     */
    public void resolveTriggersFromRecords(Connection conn, TableOperationSummary summary,
                                            List<TriggerRecord> triggers) {
        resolveTriggersFromRecords(conn, summary, triggers, null);
    }

    /**
     * Resolve triggers using pre-fetched source map (no DB calls needed).
     * sourceMap key = "OWNER.TRIGGER_NAME", value = source lines.
     */
    public void resolveTriggersFromRecords(Connection conn, TableOperationSummary summary,
                                            List<TriggerRecord> triggers,
                                            Map<String, List<String>> sourceMap) {
        String tableName = summary.getTableName();
        try {
            for (TriggerRecord trigger : triggers) {
                if (!"ENABLED".equalsIgnoreCase(trigger.status())) continue;
                if (!triggerEventMatchesOperations(trigger.triggeringEvent(), summary.getOperations())) {
                    log.debug("Skipping trigger {} — event '{}' does not match operations {}",
                            trigger.triggerName(), trigger.triggeringEvent(), summary.getOperations());
                    continue;
                }

                TriggerDetail detail = new TriggerDetail();
                detail.setTriggerName(trigger.triggerName());
                detail.setTriggerOwner(trigger.owner());
                detail.setTriggeringEvent(trigger.triggeringEvent());
                detail.setTriggerType(trigger.triggerType());
                detail.setStatus(trigger.status());

                List<String> calledProcs;
                if (sourceMap != null) {
                    calledProcs = extractCalledProceduresFromSource(trigger, detail, sourceMap);
                } else {
                    calledProcs = extractCalledProcedures(conn, trigger, detail);
                }
                detail.setCalledProcedures(calledProcs);

                summary.getTriggers().add(detail);
            }
        } catch (Exception e) {
            log.warn("Failed to resolve triggers for table {}: {}", tableName, e.getMessage());
        }
    }

    /**
     * Extract called procedures from pre-fetched source (no DB call).
     */
    private List<String> extractCalledProceduresFromSource(TriggerRecord trigger, TriggerDetail detail,
                                                            Map<String, List<String>> sourceMap) {
        List<String> calledProcs = new ArrayList<>();
        try {
            String key = trigger.owner().toUpperCase() + "." + trigger.triggerName().toUpperCase();
            List<String> source = sourceMap.getOrDefault(key, List.of());
            if (source.isEmpty()) return calledProcs;

            if (detail != null) {
                detail.setTriggerBody(String.join("\n", source));
            }

            PlsqlUnit unit = parserService.parseLines(source, trigger.owner(), trigger.triggerName());
            for (PlsqlProcedure proc : unit.getProcedures()) {
                for (ProcedureCall call : proc.getCalls()) {
                    calledProcs.add(call.getQualifiedName());
                }
            }
        } catch (Exception e) {
            log.debug("Could not parse trigger {} source: {}", trigger.triggerName(), e.getMessage());
        }
        return calledProcs;
    }

    /**
     * Resolve triggers for a table and populate the trigger details in the summary.
     */
    public void resolveTriggers(Connection conn, TableOperationSummary summary,
                                 List<String> allSchemas) {
        String tableName = summary.getTableName();
        String tableOwner = summary.getSchemaName();

        try {
            List<TriggerRecord> triggers;
            if (tableOwner != null) {
                triggers = dictionaryService.getTriggersForTable(conn, tableOwner, tableName);
            } else {
                triggers = dictionaryService.getTriggersForTableAnySchema(conn, tableName, allSchemas);
            }

            for (TriggerRecord trigger : triggers) {
                // Only include enabled triggers
                if (!"ENABLED".equalsIgnoreCase(trigger.status())) continue;

                // Only include triggers whose event matches the operations we actually perform
                // e.g., if we only SELECT from the table, don't attach INSERT/UPDATE/DELETE triggers
                if (!triggerEventMatchesOperations(trigger.triggeringEvent(), summary.getOperations())) {
                    log.debug("Skipping trigger {} — event '{}' does not match operations {}",
                            trigger.triggerName(), trigger.triggeringEvent(), summary.getOperations());
                    continue;
                }

                TriggerDetail detail = new TriggerDetail();
                detail.setTriggerName(trigger.triggerName());
                detail.setTriggerOwner(trigger.owner());
                detail.setTriggeringEvent(trigger.triggeringEvent());
                detail.setTriggerType(trigger.triggerType());
                detail.setStatus(trigger.status());

                // Fetch and parse trigger source to extract called procedures
                List<String> calledProcs = extractCalledProcedures(conn, trigger, detail);
                detail.setCalledProcedures(calledProcs);

                summary.getTriggers().add(detail);
            }
        } catch (Exception e) {
            log.warn("Failed to resolve triggers for table {}: {}", tableName, e.getMessage());
        }
    }

    private List<String> extractCalledProcedures(Connection conn, TriggerRecord trigger, TriggerDetail detail) {
        List<String> calledProcs = new ArrayList<>();
        try {
            List<String> source = dictionaryService.fetchSource(
                    conn, trigger.owner(), trigger.triggerName(), "TRIGGER");
            if (source.isEmpty()) return calledProcs;

            // Save the trigger source code
            if (detail != null) {
                detail.setTriggerBody(String.join("\n", source));
            }

            PlsqlUnit unit = parserService.parseLines(source, trigger.owner(), trigger.triggerName());
            for (PlsqlProcedure proc : unit.getProcedures()) {
                for (ProcedureCall call : proc.getCalls()) {
                    calledProcs.add(call.getQualifiedName());
                }
            }
        } catch (Exception e) {
            log.debug("Could not parse trigger {} source: {}", trigger.triggerName(), e.getMessage());
        }
        return calledProcs;
    }

    /**
     * Check if a trigger's event (e.g., "INSERT OR UPDATE" or "DELETE") matches
     * any of the DML operations we actually perform on the table.
     * SELECT-only tables should NOT have INSERT/UPDATE/DELETE triggers attached.
     */
    private boolean triggerEventMatchesOperations(String triggeringEvent, Set<SqlOperationType> operations) {
        if (triggeringEvent == null || operations == null || operations.isEmpty()) return false;
        String event = triggeringEvent.toUpperCase();
        for (SqlOperationType op : operations) {
            switch (op) {
                case INSERT:
                    if (event.contains("INSERT")) return true;
                    break;
                case UPDATE:
                    if (event.contains("UPDATE")) return true;
                    break;
                case DELETE:
                    if (event.contains("DELETE")) return true;
                    break;
                case MERGE:
                    // MERGE can trigger INSERT, UPDATE, or DELETE triggers
                    if (event.contains("INSERT") || event.contains("UPDATE") || event.contains("DELETE")) return true;
                    break;
                default:
                    // SELECT does not fire triggers
                    break;
            }
        }
        return false;
    }
}
