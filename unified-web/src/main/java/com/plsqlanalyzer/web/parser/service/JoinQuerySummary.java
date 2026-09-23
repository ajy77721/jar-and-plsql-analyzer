package com.plsqlanalyzer.web.parser.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * UI-013 §60/§62/§64 — server-side port of {@code parser/js/joinSummary.js:_buildQueryItems}, the
 * per-query grouping view of the Join Summary screen: joins from the SAME originating query
 * (or, for legacy data without a {@code queryId}, the same coarse 50-line bucket) are folded into
 * one row per {@code procedure::query}, instead of one row per join pair.
 *
 * <p>Unlike {@code statements.js}/{@code refs.js}/{@code complexity.js} (§62 — blocked, because the
 * array they aggregate is already resident client-side for ~20 unrelated consumers), this
 * aggregation's input, {@code PA.joins.data}, is <b>not</b> guaranteed resident: the Join Summary
 * screen is reachable as its own lazy tab ({@code loadQueryOnly}) without the plain Joins tab ever
 * having been opened, in which case building this view today means a full, unpaged {@code /joins}
 * fetch (measured elsewhere at 438,556 B for the one real corpus available) paid solely to compute
 * this grouping. Moving the fold here means that fetch, when it happens at all, happens once,
 * server-side, and the response is the small per-query summary instead of every join pair's full
 * {@code onPredicate} text.
 */
public final class JoinQuerySummary {

    private JoinQuerySummary() {
    }

    private static final Pattern AND_SPLIT = Pattern.compile("\\bAND\\b", Pattern.CASE_INSENSITIVE);

    /** {@code join-complexity.*} — the same {@link com.plsqlanalyzer.web.parser.config.JoinComplexityConfig}
     *  fields {@code _calcComplexity}/{@code _complexityLabel} read from {@code /config/join-complexity}. */
    public record Config(double baseScore, double outerJoinPenalty, double crossFullJoinPenalty,
                          double multiPredBonus, double highPredBonus, double noPredPenalty,
                          int multiPredThreshold, int highPredThreshold,
                          double thresholdMedium, double thresholdHigh) {

        /** The JS's own hard-coded fallback values, used when no config is supplied. */
        public static Config defaults() {
            return new Config(1, 0.5, 1, 0.5, 1, 0.5, 2, 3, 2, 3);
        }
    }

    private static String text(JsonNode n) {
        return n == null || n.isNull() ? "" : n.asText("");
    }

    /** {@code _calcComplexity}, transcribed. */
    static double calcComplexity(String joinType, int predCount, Config cfg) {
        double score = cfg.baseScore();
        String jt = (joinType == null ? "" : joinType).toUpperCase(Locale.ROOT);
        if (jt.contains("LEFT") || jt.contains("RIGHT")) score += cfg.outerJoinPenalty();
        if (jt.contains("FULL") || jt.contains("CROSS")) score += cfg.crossFullJoinPenalty();
        if (predCount >= cfg.highPredThreshold()) score += cfg.highPredBonus();
        else if (predCount >= cfg.multiPredThreshold()) score += cfg.multiPredBonus();
        if (predCount == 0 && !"CROSS".equals(jt)) score += cfg.noPredPenalty();
        return score;
    }

    /** {@code _complexityLabel}, transcribed. */
    static String complexityLabel(double score, Config cfg) {
        if (score >= cfg.thresholdHigh()) return "HIGH";
        if (score >= cfg.thresholdMedium()) return "MEDIUM";
        return "LOW";
    }

    private static int predCount(String onPredicate) {
        return (onPredicate == null || onPredicate.isEmpty()) ? 0 : AND_SPLIT.split(onPredicate, -1).length;
    }

    private static final class Group {
        String procedureId = "";
        String procedureName = "";
        String sourceFile = "";
        int minLine;
        int maxLine;
        int joinCount;
        double totalPreds;
        double maxComplexity;
        final Set<String> tables = new LinkedHashSet<>();
        final Map<String, Integer> joinTypeCounts = new LinkedHashMap<>();
        final ArrayNode joins;

        Group(ObjectMapper mapper) {
            this.joins = mapper.createArrayNode();
        }
    }

    /**
     * Builds the query-grouped rows, transcribed field-for-field from {@code _buildQueryItems}:
     * grouping key is {@code procedureId::q<queryId>} when a {@code queryId} is present, or a
     * coarse {@code procedureId::b<line/50>} bucket for legacy data without one — matching the JS's
     * own fallback so old and new analyses group identically either way.
     *
     * @param raw the same array {@code /joins} serves: one entry per left/right table pair, each
     *            carrying an {@code accessDetails} array of individual join occurrences
     */
    public static ArrayNode build(ObjectMapper mapper, JsonNode raw, Config cfg) {
        Map<String, Group> byKey = new LinkedHashMap<>();

        if (raw != null && raw.isArray()) {
            for (JsonNode pair : raw) {
                JsonNode details = pair.get("accessDetails");
                if (details == null || !details.isArray()) continue;
                String leftTable = text(pair.get("leftTable")).toUpperCase(Locale.ROOT);
                String rightTable = text(pair.get("rightTable")).toUpperCase(Locale.ROOT);

                for (JsonNode d : details) {
                    String procId = text(d.get("procedureId"));
                    if (procId.isEmpty()) procId = text(d.get("procedureName"));
                    int line = d.path("lineNumber").asInt(0);

                    String key;
                    JsonNode queryIdNode = d.get("queryId");
                    boolean hasQueryId = queryIdNode != null && !queryIdNode.isNull()
                            && !text(queryIdNode).isEmpty();
                    if (hasQueryId) {
                        key = procId + "::q" + text(queryIdNode);
                    } else {
                        key = procId + "::b" + Math.floorDiv(line, 50);
                    }

                    Group g = byKey.get(key);
                    if (g == null) {
                        g = new Group(mapper);
                        g.procedureId = procId;
                        g.procedureName = text(d.get("procedureName"));
                        g.sourceFile = text(d.get("sourceFile"));
                        g.minLine = line;
                        g.maxLine = line;
                        byKey.put(key, g);
                    }
                    if (line < g.minLine) g.minLine = line;
                    if (line > g.maxLine) g.maxLine = line;

                    String onPredicate = text(d.get("onPredicate"));
                    int pc = predCount(onPredicate);
                    g.totalPreds += pc;

                    if (!leftTable.isEmpty()) g.tables.add(leftTable);
                    if (!rightTable.isEmpty()) g.tables.add(rightTable);

                    String joinType = text(d.get("joinType"));
                    if (joinType.isEmpty()) joinType = "UNKNOWN";
                    g.joinTypeCounts.merge(joinType, 1, Integer::sum);

                    double complexity = calcComplexity(joinType, pc, cfg);
                    if (complexity > g.maxComplexity) g.maxComplexity = complexity;

                    ObjectNode j = mapper.createObjectNode();
                    j.put("leftTable", text(pair.get("leftTable")));
                    j.put("rightTable", text(pair.get("rightTable")));
                    j.put("joinType", joinType);
                    j.put("predicate", onPredicate);
                    j.put("predCount", pc);
                    j.put("lineNumber", line);
                    g.joins.add(j);
                    g.joinCount++;
                }
            }
        }

        ArrayNode items = mapper.createArrayNode();
        for (Group g : byKey.values()) {
            int lineSpan = (g.maxLine > g.minLine) ? (g.maxLine - g.minLine + 1) : 1;
            ObjectNode item = mapper.createObjectNode();
            item.put("procedureId", g.procedureId);
            item.put("procedureName", g.procedureName);
            item.put("sourceFile", g.sourceFile);
            item.put("lineNumber", g.minLine);
            item.put("lineEnd", g.maxLine);
            item.put("lineSpan", lineSpan);
            item.put("joinCount", g.joinCount);
            item.put("tableCount", g.tables.size());
            ArrayNode tablesOut = item.putArray("tables");
            for (String t : g.tables) tablesOut.add(t);
            ArrayNode joinTypesOut = item.putArray("joinTypes");
            for (String jt : g.joinTypeCounts.keySet()) joinTypesOut.add(jt);
            ObjectNode joinTypeMap = item.putObject("joinTypeMap");
            g.joinTypeCounts.forEach(joinTypeMap::put);
            item.put("totalPreds", g.totalPreds);
            item.set("joins", g.joins);
            item.put("complexity", g.maxComplexity);
            item.put("complexityLabel", complexityLabel(g.maxComplexity, cfg));
            items.add(item);
        }
        return items;
    }
}
