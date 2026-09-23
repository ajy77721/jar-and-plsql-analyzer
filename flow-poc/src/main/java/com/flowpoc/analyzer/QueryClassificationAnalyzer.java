/*
 * Decompiled with CFR 0.152.
 */
package com.flowpoc.analyzer;

import com.flowpoc.analyzer.OptimizationAnalyzer;
import com.flowpoc.model.ExtractedQuery;
import com.flowpoc.model.FlowResult;
import com.flowpoc.model.FlowStep;
import com.flowpoc.model.OptimizationFinding;
import com.flowpoc.model.Predicate;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

public class QueryClassificationAnalyzer
implements OptimizationAnalyzer {
    private static final Pattern BIND_PARAM = Pattern.compile("\\?\\d+|:\\w+|\\$\\{[^}]+\\}|#\\{[^}]+\\}");
    private static final Set<String> REF_COLLECTION_HINTS = Set.of("config", "configuration", "setting", "settings", "type", "types", "code", "codes", "lookup", "lookups", "reference", "enum", "enums", "status", "statuses", "category", "categories", "country", "countries", "currency", "currencies", "role", "roles", "permission", "permissions");

    @Override
    public List<OptimizationFinding> analyze(FlowResult flowResult) {
        ArrayList<OptimizationFinding> findings = new ArrayList<OptimizationFinding>();
        HashSet<String> writtenCollections = new HashSet<String>();
        for (FlowStep step : flowResult.allSteps()) {
            for (ExtractedQuery eq : step.getQueries()) {
                if (eq.getType() != ExtractedQuery.QueryType.INSERT && eq.getType() != ExtractedQuery.QueryType.UPDATE && eq.getType() != ExtractedQuery.QueryType.DELETE || eq.getTableName() == null) continue;
                writtenCollections.add(eq.getTableName());
            }
        }
        for (FlowStep step : flowResult.allSteps()) {
            for (ExtractedQuery eq : step.getQueries()) {
                if (eq.getType() != ExtractedQuery.QueryType.SELECT) continue;
                ExtractedQuery.DataClass dc = this.classify(eq, writtenCollections);
                eq.setDataClass(dc);
                if (dc != ExtractedQuery.DataClass.STATIC) continue;
                String location = step.getClassName() + "." + step.getMethodName();
                String table = eq.getTableName() != null ? eq.getTableName() : "unknown";
                findings.add(new OptimizationFinding(OptimizationFinding.Category.STATIC_CACHEABLE, OptimizationFinding.Severity.MEDIUM, table, null, "Query on '" + table + "' has no bind parameters and targets a reference-like collection \u2014 result can be cached at startup with @Cacheable or preloaded into a static Map.", location, eq.getRawSql(), "@Cacheable(\"" + table + "\") on " + step.getMethodName() + "() or preload into static Map<K,V> at ApplicationReadyEvent"));
            }
        }
        return findings;
    }

    private ExtractedQuery.DataClass classify(ExtractedQuery eq, Set<String> writtenCollections) {
        String table;
        String string = table = eq.getTableName() != null ? eq.getTableName().toLowerCase() : "";
        if (writtenCollections.contains(eq.getTableName())) {
            return ExtractedQuery.DataClass.TRANSACTIONAL;
        }
        boolean hasBindParam = false;
        for (Predicate p : eq.getPredicates()) {
            if (p.getRawValue() != null && BIND_PARAM.matcher(p.getRawValue()).find()) {
                hasBindParam = true;
                break;
            }
            String col = p.getColumn();
            if (col == null || !this.isSessionField(col)) continue;
            return ExtractedQuery.DataClass.TRANSACTIONAL;
        }
        if (!hasBindParam && eq.getAggregationPipeline() != null) {
            hasBindParam = BIND_PARAM.matcher(eq.getAggregationPipeline()).find();
        }
        if (!hasBindParam && eq.getRawSql() != null) {
            hasBindParam = BIND_PARAM.matcher(eq.getRawSql()).find();
        }
        if (hasBindParam) {
            return ExtractedQuery.DataClass.TRANSACTIONAL;
        }
        if (this.looksLikeReferenceCollection(table)) {
            return ExtractedQuery.DataClass.STATIC;
        }
        if (eq.getPredicates().isEmpty() && this.looksLikeReferenceCollection(table)) {
            return ExtractedQuery.DataClass.STATIC;
        }
        return ExtractedQuery.DataClass.TRANSACTIONAL;
    }

    private boolean looksLikeReferenceCollection(String tableLower) {
        for (String hint : REF_COLLECTION_HINTS) {
            if (!tableLower.contains(hint)) continue;
            return true;
        }
        return false;
    }

    private boolean isSessionField(String col) {
        String c = col.toLowerCase();
        return c.contains("userid") || c.contains("user_id") || c.contains("sessionid") || c.contains("session_id") || c.contains("tenantid") || c.contains("tenant_id") || c.contains("requestid") || c.contains("request_id") || c.contains("customerid") || c.contains("customer_id");
    }
}

