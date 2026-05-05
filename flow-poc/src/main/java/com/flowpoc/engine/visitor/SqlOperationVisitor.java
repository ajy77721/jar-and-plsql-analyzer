package com.flowpoc.engine.visitor;

import com.fasterxml.jackson.databind.JsonNode;
import com.flowpoc.engine.SqlPredicateExtractor;
import com.flowpoc.model.FlowStep;

/**
 * Visitor that extracts SQL statements from a call-tree node.
 * Mines sqlStatements field + SQL-looking stringLiterals.
 */
public class SqlOperationVisitor implements CallTreeVisitor {

    private final SqlPredicateExtractor extractor;

    public SqlOperationVisitor(SqlPredicateExtractor extractor) {
        this.extractor = extractor;
    }

    @Override
    public void visit(JsonNode node, FlowStep step) {
        String cls    = node.path("className").asText("");
        String method = node.path("methodName").asText("");

        JsonNode sqls = node.path("sqlStatements");
        if (sqls.isArray()) {
            for (JsonNode sql : sqls) {
                String raw = sql.isTextual() ? sql.asText() : sql.path("sql").asText("");
                if (!raw.isBlank()) step.addQuery(extractor.parse(raw, cls, method));
            }
        }

        JsonNode lits = node.path("stringLiterals");
        if (lits.isArray()) {
            for (JsonNode lit : lits) {
                String s = lit.asText("").trim();
                if (looksLikeSql(s)) step.addQuery(extractor.parse(s, cls, method));
            }
        }
    }

    private boolean looksLikeSql(String s) {
        if (s == null || s.length() < 10) return false;
        String u = s.toUpperCase();
        return u.startsWith("SELECT") || u.startsWith("INSERT") || u.startsWith("UPDATE")
                || u.startsWith("DELETE") || u.startsWith("MERGE")
                || u.contains(" FROM ") || u.contains(" WHERE ");
    }
}
