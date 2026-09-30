package com.jaranalyzer.service;

import com.jaranalyzer.model.DbCallRef;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * The precomputed aggregate behind {@code GET /api/jar/jars/{id}/db-calls/summary}.
 *
 * <p>Built in the same pass that writes {@code db-calls/index.json} and stored as the
 * {@code views/db-calls.json} sidecar, so the summary endpoint is one small read rather than a
 * second pass over the call list. The same builder is used by the read side as a fallback when the
 * sidecar is absent (an analysis written before the sidecar existed, or a correction that cleared
 * the {@code views/} directory), so the two can never disagree.
 */
public final class DbCallSummary {

    private DbCallSummary() {}

    /**
     * @return a JSON-serialisable summary, or {@code null} when {@code refs} is null
     */
    public static Map<String, Object> build(List<DbCallRef> refs) {
        if (refs == null) return null;

        Map<String, Integer> byKind = new TreeMap<>();
        Map<String, Integer> byPackage = new TreeMap<>();
        Map<String, Integer> byProcedure = new TreeMap<>();

        for (DbCallRef r : refs) {
            if (r == null) continue;
            byKind.merge(nonNull(r.getKind(), "unknown"), 1, Integer::sum);
            String pkg = r.getSchema() == null || r.getSchema().isBlank() ? "(none)" : r.getSchema();
            byPackage.merge(pkg, 1, Integer::sum);
            byProcedure.merge(nonNull(r.getDbObject(), "?"), 1, Integer::sum);
        }

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("available", true);
        out.put("total", refs.size());
        out.put("byKind", byKind);
        out.put("byPackage", ranked(byPackage, "package"));
        out.put("byProcedure", ranked(byProcedure, "procedure"));
        return out;
    }

    /**
     * The unavailable shape returned for a JAR analysed before this facet existed. Detection runs
     * at parse time, so there is nothing to lazily backfill: the caller must re-analyse the JAR.
     */
    public static Map<String, Object> unavailable() {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("available", false);
        out.put("total", 0);
        out.put("byKind", Map.of());
        out.put("byPackage", List.of());
        out.put("byProcedure", List.of());
        out.put("reason", "DB call detection runs at analysis time; re-analyze this JAR to populate it.");
        return out;
    }

    /** Most-frequent first, then by name, so the ordering is total and stable. */
    private static List<Map<String, Object>> ranked(Map<String, Integer> counts, String nameField) {
        List<Map.Entry<String, Integer>> entries = new ArrayList<>(counts.entrySet());
        entries.sort((a, b) -> {
            int c = Integer.compare(b.getValue(), a.getValue());
            return c != 0 ? c : a.getKey().compareTo(b.getKey());
        });
        List<Map<String, Object>> out = new ArrayList<>(entries.size());
        for (Map.Entry<String, Integer> e : entries) {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put(nameField, e.getKey());
            row.put("count", e.getValue());
            out.add(row);
        }
        return out;
    }

    private static String nonNull(String s, String fallback) {
        return s == null || s.isBlank() ? fallback : s;
    }
}
