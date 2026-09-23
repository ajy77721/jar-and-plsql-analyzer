package com.plsqlanalyzer.web.parser.service;

import com.plsql.parser.flow.DbConnectionManager;
import com.plsql.parser.flow.TriggerDiscoverer;
import com.plsql.parser.model.SchemaTableInfo;
import com.plsqlanalyzer.web.parser.service.ClaudeVerificationModels.OperationVerification;
import com.plsqlanalyzer.web.parser.service.ClaudeVerificationModels.TableVerificationResult;
import com.plsqlanalyzer.web.parser.service.ClaudeVerificationModels.VerificationResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.*;

/**
 * Discovers triggers for NEW tables found during Claude verification.
 *
 * When Claude verification identifies table operations that static analysis missed,
 * those new tables may have Oracle triggers that should be discovered. This service
 * runs TriggerDiscoverer against those new tables and merges the results back into
 * the verification result.
 *
 * This is an optional enhancement -- if no DB connection is available, it logs
 * and returns silently without affecting the verification result.
 */
@Service("parserTriggerReAnalysisService")
public class TriggerReAnalysisService {

    private static final Logger log = LoggerFactory.getLogger(TriggerReAnalysisService.class);

    /** DML operations that can fire triggers. */
    private static final Set<String> DML_OPS = Set.of("INSERT", "UPDATE", "DELETE", "MERGE", "TRUNCATE");

    @Value("${app.parser-config-path:config/plsql-config.yaml}")
    private String configPath;

    /**
     * Discover triggers for NEW tables in the verification result.
     * Modifies the result in-place by populating the {@code triggers} field
     * on each TableVerificationResult that has new DML operations.
     *
     * @param result the verification result to enrich with trigger data
     */
    public void discoverTriggersForNewTables(VerificationResult result) {
        if (result == null || result.tables == null || result.tables.isEmpty()) {
            return;
        }

        Map<String, SchemaTableInfo> newDmlTables = collectNewDmlTables(result);
        if (newDmlTables.isEmpty()) {
            log.debug("No NEW DML tables found -- trigger re-analysis not needed");
            return;
        }

        log.info("[TriggerReAnalysis] Found {} NEW tables with DML ops, running trigger discovery",
                newDmlTables.size());

        List<Map<String, Object>> discoveredTriggers = runTriggerDiscovery(newDmlTables);
        if (discoveredTriggers.isEmpty()) {
            log.info("[TriggerReAnalysis] No triggers found for new tables");
            return;
        }

        mergeTriggers(result, discoveredTriggers);
        log.info("[TriggerReAnalysis] Merged {} triggers into verification result",
                discoveredTriggers.size());
    }

    /**
     * Collect tables from the verification result that have NEW status operations
     * with DML types (which could fire triggers).
     */
    private Map<String, SchemaTableInfo> collectNewDmlTables(VerificationResult result) {
        Map<String, SchemaTableInfo> newDmlTables = new LinkedHashMap<>();

        for (TableVerificationResult tvr : result.tables) {
            if (tvr.tableName == null || tvr.claudeVerifications == null) continue;

            // Check if this table has any NEW DML operations
            Set<String> newDmlOps = new LinkedHashSet<>();
            for (OperationVerification ov : tvr.claudeVerifications) {
                if ("NEW".equalsIgnoreCase(ov.status)
                        && ov.operation != null
                        && DML_OPS.contains(ov.operation.toUpperCase())) {
                    newDmlOps.add(ov.operation.toUpperCase());
                }
            }

            if (newDmlOps.isEmpty()) continue;

            // Skip tables that already have triggers from static analysis
            if (tvr.triggers != null && !tvr.triggers.isEmpty()) continue;

            // Build SchemaTableInfo for TriggerDiscoverer
            String tableName = tvr.tableName;
            String schema = null;
            if (tableName.contains(".")) {
                String[] parts = tableName.split("\\.", 2);
                schema = parts[0];
                tableName = parts[1];
            }

            SchemaTableInfo info = new SchemaTableInfo();
            info.setTableName(tableName);
            info.setSchema(schema);
            info.setOperations(newDmlOps);
            info.setObjectType("TABLE");

            newDmlTables.put(tvr.tableName.toUpperCase(), info);
        }

        return newDmlTables;
    }

    /**
     * Run TriggerDiscoverer against the given tables.
     * Returns empty list if DB connection is unavailable.
     */
    private List<Map<String, Object>> runTriggerDiscovery(Map<String, SchemaTableInfo> tables) {
        if (configPath == null || configPath.isBlank()) {
            log.debug("[TriggerReAnalysis] No config path -- skipping DB trigger discovery");
            return Collections.emptyList();
        }

        try (DbConnectionManager connManager = new DbConnectionManager(configPath)) {
            // SchemaResolver requires entry points which we don't have here --
            // pass null since TriggerDiscoverer only uses it for resolveObjectType
            // which is optional (just enriches the result with objectType info)
            TriggerDiscoverer discoverer = new TriggerDiscoverer(connManager, null);
            return discoverer.discover(tables);
        } catch (Exception e) {
            log.warn("[TriggerReAnalysis] Trigger discovery failed (DB unavailable): {}",
                    e.getMessage());
            return Collections.emptyList();
        }
    }

    /**
     * Merge discovered triggers into the verification result.
     * Each trigger is attached to its parent table's TableVerificationResult.
     */
    private void mergeTriggers(VerificationResult result, List<Map<String, Object>> discoveredTriggers) {
        // Index tables by name for fast lookup
        Map<String, TableVerificationResult> tableIndex = new LinkedHashMap<>();
        for (TableVerificationResult tvr : result.tables) {
            if (tvr.tableName != null) {
                tableIndex.put(tvr.tableName.toUpperCase(), tvr);
            }
        }

        for (Map<String, Object> trigger : discoveredTriggers) {
            String trigTableName = (String) trigger.get("tableName");
            if (trigTableName == null) continue;

            String key = trigTableName.toUpperCase();
            // Try exact match, then with schema prefix variants
            TableVerificationResult tvr = tableIndex.get(key);
            if (tvr == null) {
                // Try matching with schema-qualified names in the index
                for (Map.Entry<String, TableVerificationResult> entry : tableIndex.entrySet()) {
                    if (entry.getKey().endsWith("." + key) || key.endsWith("." + entry.getKey())) {
                        tvr = entry.getValue();
                        break;
                    }
                }
            }
            if (tvr == null) continue;

            // Initialize triggers list if needed
            if (tvr.triggers == null) {
                tvr.triggers = new ArrayList<>();
            }

            // Deduplicate: skip if trigger with same name already present
            String trigName = (String) trigger.get("name");
            boolean alreadyExists = tvr.triggers.stream().anyMatch(t ->
                    trigName != null && trigName.equalsIgnoreCase((String) t.get("name")));
            if (alreadyExists) continue;

            tvr.triggers.add(trigger);
        }
    }
}
