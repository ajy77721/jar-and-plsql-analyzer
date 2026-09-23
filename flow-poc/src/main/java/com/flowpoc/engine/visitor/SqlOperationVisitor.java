/*
 * Decompiled with CFR 0.152.
 * 
 * Could not load the following classes:
 *  com.fasterxml.jackson.databind.JsonNode
 */
package com.flowpoc.engine.visitor;

import com.fasterxml.jackson.databind.JsonNode;
import com.flowpoc.engine.SqlPredicateExtractor;
import com.flowpoc.engine.visitor.CallTreeVisitor;
import com.flowpoc.model.FlowStep;

public class SqlOperationVisitor
implements CallTreeVisitor {
    private final SqlPredicateExtractor extractor;

    public SqlOperationVisitor(SqlPredicateExtractor extractor) {
        this.extractor = extractor;
    }

    @Override
    public void visit(JsonNode node, FlowStep step) {
        JsonNode lits;
        String cls = node.path("className").asText("");
        String method = node.path("methodName").asText("");
        JsonNode sqls = node.path("sqlStatements");
        if (sqls.isArray()) {
            for (JsonNode sql : sqls) {
                String raw = sql.isTextual() ? sql.asText() : sql.path("sql").asText("");
                if (raw.isBlank()) continue;
                step.addQuery(this.extractor.parse(raw, cls, method));
            }
        }
        if ((lits = node.path("stringLiterals")).isArray()) {
            for (JsonNode lit : lits) {
                String s = lit.asText("").trim();
                if (!this.looksLikeSql(s)) continue;
                step.addQuery(this.extractor.parse(s, cls, method));
            }
        }
    }

    private boolean looksLikeSql(String s) {
        if (s == null || s.length() < 10) {
            return false;
        }
        String u = s.toUpperCase();
        return u.startsWith("SELECT") || u.startsWith("INSERT") || u.startsWith("UPDATE") || u.startsWith("DELETE") || u.startsWith("MERGE") || u.contains(" FROM ") || u.contains(" WHERE ");
    }
}

