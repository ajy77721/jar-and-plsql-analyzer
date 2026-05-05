package com.flowpoc.analyzer;

import com.flowpoc.model.*;

import java.util.*;
import java.util.regex.Pattern;

/**
 * Classifies each query in the flow as STATIC (cacheable at startup) or
 * TRANSACTIONAL (must execute on every request).
 *
 * STATIC indicators:
 *   - No bind parameters (?0, :name, ${...}) in the filter or pipeline
 *   - Collection name suggests reference/config data
 *   - No writes to the same collection exist anywhere in the flow
 *
 * TRANSACTIONAL indicators:
 *   - Filter contains bind parameters
 *   - Filter field is userId / sessionId / tenantId / requestId
 *   - Writes to the same collection exist in the same flow
 *
 * The analyzer tags each ExtractedQuery's dataClass field AND emits
 * STATIC_CACHEABLE findings so the report can highlight caching opportunities.
 */
public class QueryClassificationAnalyzer implements OptimizationAnalyzer {

    private static final Pattern BIND_PARAM = Pattern.compile(
            "\\?\\d+|:\\w+|\\$\\{[^}]+\\}|#\\{[^}]+\\}");

    private static final Set<String> REF_COLLECTION_HINTS = Set.of(
            "config", "configuration", "setting", "settings",
            "type", "types", "code", "codes",
            "lookup", "lookups", "reference", "enum", "enums",
            "status", "statuses", "category", "categories",
            "country", "countries", "currency", "currencies",
            "role", "roles", "permission", "permissions");

    @Override
    public List<OptimizationFinding> analyze(FlowResult flowResult) {
        List<OptimizationFinding> findings = new ArrayList<>();

        // Collect collections that have at least one write in this flow
        Set<String> writtenCollections = new HashSet<>();
        for (FlowStep step : flowResult.allSteps()) {
            for (ExtractedQuery eq : step.getQueries()) {
                if (eq.getType() == ExtractedQuery.QueryType.INSERT
                        || eq.getType() == ExtractedQuery.QueryType.UPDATE
                        || eq.getType() == ExtractedQuery.QueryType.DELETE) {
                    if (eq.getTableName() != null) writtenCollections.add(eq.getTableName());
                }
            }
        }

        for (FlowStep step : flowResult.allSteps()) {
            for (ExtractedQuery eq : step.getQueries()) {
                if (eq.getType() != ExtractedQuery.QueryType.SELECT) continue;

                ExtractedQuery.DataClass dc = classify(eq, writtenCollections);
                eq.setDataClass(dc);

                if (dc == ExtractedQuery.DataClass.STATIC) {
                    String location = step.getClassName() + "." + step.getMethodName();
                    String table    = eq.getTableName() != null ? eq.getTableName() : "unknown";
                    findings.add(new OptimizationFinding(
                            OptimizationFinding.Category.STATIC_CACHEABLE,
                            OptimizationFinding.Severity.MEDIUM,
                            table, null,
                            "Query on '" + table + "' has no bind parameters and targets "
                                + "a reference-like collection — result can be cached at "
                                + "startup with @Cacheable or preloaded into a static Map.",
                            location,
                            eq.getRawSql(),
                            "@Cacheable(\"" + table + "\") on " + step.getMethodName() + "()"
                                    + " or preload into static Map<K,V> at ApplicationReadyEvent"));
                }
            }
        }
        return findings;
    }

    private ExtractedQuery.DataClass classify(ExtractedQuery eq,
                                              Set<String> writtenCollections) {
        String table = eq.getTableName() != null ? eq.getTableName().toLowerCase() : "";

        // If this collection is written anywhere in the same flow it's transactional
        if (writtenCollections.contains(eq.getTableName())) {
            return ExtractedQuery.DataClass.TRANSACTIONAL;
        }

        // Check filter + pipeline text for bind params
        boolean hasBindParam = false;
        for (Predicate p : eq.getPredicates()) {
            if (p.getRawValue() != null && BIND_PARAM.matcher(p.getRawValue()).find()) {
                hasBindParam = true;
                break;
            }
            // Field names like userId, sessionId, tenantId always imply transactional
            String col = p.getColumn();
            if (col != null && isSessionField(col)) {
                return ExtractedQuery.DataClass.TRANSACTIONAL;
            }
        }
        if (!hasBindParam && eq.getAggregationPipeline() != null) {
            hasBindParam = BIND_PARAM.matcher(eq.getAggregationPipeline()).find();
        }
        if (!hasBindParam && eq.getRawSql() != null) {
            hasBindParam = BIND_PARAM.matcher(eq.getRawSql()).find();
        }

        if (hasBindParam) return ExtractedQuery.DataClass.TRANSACTIONAL;

        // Collection name hints at reference data AND no bind params → static
        if (looksLikeReferenceCollection(table)) return ExtractedQuery.DataClass.STATIC;

        // No predicates at all on a small ref collection is also static
        if (eq.getPredicates().isEmpty() && looksLikeReferenceCollection(table)) {
            return ExtractedQuery.DataClass.STATIC;
        }

        return ExtractedQuery.DataClass.TRANSACTIONAL;
    }

    private boolean looksLikeReferenceCollection(String tableLower) {
        for (String hint : REF_COLLECTION_HINTS) {
            if (tableLower.contains(hint)) return true;
        }
        return false;
    }

    private boolean isSessionField(String col) {
        String c = col.toLowerCase();
        return c.contains("userid") || c.contains("user_id")
                || c.contains("sessionid") || c.contains("session_id")
                || c.contains("tenantid") || c.contains("tenant_id")
                || c.contains("requestid") || c.contains("request_id")
                || c.contains("customerid") || c.contains("customer_id");
    }
}
