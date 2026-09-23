package com.plsql.sqlserver;

import com.plsql.sqlserver.model.SqlServerCallInfo;
import com.plsql.sqlserver.model.SqlServerRoutineInfo;
import com.plsql.sqlserver.model.SqlServerTableOpInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Diagnostic coverage cross-check: compares what the extractor produced against
 * a keyword scan of the raw source. When the source clearly contains a construct
 * (JOIN, MERGE, cursor, EXEC, NEXT VALUE FOR) but the extractor found fewer than
 * expected, that gap is a candidate silent-failure to root-cause. Also flags call
 * targets that don't resolve to any known object in the flow set.
 *
 * Not extraction logic — read-only, test-scope only. Never affects analysis output.
 */
final class CoverageCheck {

    private CoverageCheck() {}

    /** A single discrepancy worth investigating. */
    static final class Gap {
        final String kind; final int source; final int extracted;
        Gap(String kind, int source, int extracted) {
            this.kind = kind; this.source = source; this.extracted = extracted;
        }
        @Override public String toString() {
            return String.format("%-14s source=%d extracted=%d", kind, source, extracted);
        }
    }

    private static final Pattern LINE_COMMENT  = Pattern.compile("--[^\n]*");
    private static final Pattern BLOCK_COMMENT = Pattern.compile("/\\*.*?\\*/", Pattern.DOTALL);
    private static final Pattern STRING_LIT    = Pattern.compile("'(?:''|[^'])*'", Pattern.DOTALL);

    /** T-SQL builtins that legitimately look like calls but are not routines we crawl. */
    private static final Set<String> BUILTINS = Set.of(
            "ISNULL", "COALESCE", "CAST", "CONVERT", "GETDATE", "SYSDATETIME", "NEWID",
            "ROW_NUMBER", "RANK", "DENSE_RANK", "COUNT", "SUM", "AVG", "MIN", "MAX",
            "SUBSTRING", "CHARINDEX", "LEN", "UPPER", "LOWER", "LTRIM", "RTRIM", "REPLACE",
            "FORMAT", "TRY_CAST", "TRY_CONVERT", "IIF", "NULLIF", "DATEADD", "DATEDIFF",
            "DATEPART", "YEAR", "MONTH", "DAY", "ABS", "ROUND", "FLOOR", "CEILING",
            "CONCAT", "STRING_AGG", "OBJECT_ID", "SCOPE_IDENTITY", "ERROR_MESSAGE",
            "ERROR_NUMBER", "ERROR_SEVERITY", "ERROR_STATE", "ERROR_LINE", "RAISERROR");

    /**
     * Compare extracted entities against a source keyword scan.
     * Returns the list of gaps where the source clearly has more than we extracted.
     */
    static List<Gap> gaps(String rawSource, SqlServerRoutineInfo info) {
        String s = strip(rawSource);
        List<Gap> out = new ArrayList<>();

        // Count only joins to a QUALIFIED base table (schema.table) — that's what the
        // extractor records. Joins to a derived subquery "JOIN (SELECT…)" or to a CTE/alias
        // (unqualified name) are intentionally excluded, so the count reflects real gaps only.
        int srcJoin = count(s, "\\bJOIN\\s+\\[?[\\w#$]+\\]?\\.\\[?[\\w#$]+\\]?")
                + count(s, "\\bAPPLY\\s+\\[?[\\w#$]+\\]?\\.\\[?[\\w#$]+\\]?");
        int extJoin = 0;
        if (info.getTableOps() != null) {
            for (SqlServerTableOpInfo t : info.getTableOps()) {
                if (t.getJoins() != null) extJoin += t.getJoins().size();
            }
        }
        if (srcJoin > extJoin) out.add(new Gap("JOIN", srcJoin, extJoin));

        int srcMerge = count(s, "\\bMERGE\\b");
        int extMerge = countOps(info, "MERGE");
        if (srcMerge > extMerge) out.add(new Gap("MERGE", srcMerge, extMerge));

        int srcCursor = count(s, "\\bDECLARE\\s+[\\w@#]+\\s+(?:INSENSITIVE\\s+|SCROLL\\s+)*CURSOR\\b");
        int extCursor = info.getCursors() != null ? info.getCursors().size() : 0;
        if (srcCursor > extCursor) out.add(new Gap("CURSOR", srcCursor, extCursor));

        int srcSeq = count(s, "\\bNEXT\\s+VALUE\\s+FOR\\b");
        int extSeq = info.getSequences() != null ? info.getSequences().size() : 0;
        if (srcSeq > extSeq) out.add(new Gap("SEQUENCE", srcSeq, extSeq));

        int srcExec = count(s, "\\b(?:EXEC|EXECUTE)\\s+(?!\\s*\\()[\\[\\w]");
        int extCalls = info.getCalls() != null ? info.getCalls().size() : 0;
        if (srcExec > extCalls) out.add(new Gap("EXEC/CALL", srcExec, extCalls));

        return out;
    }

    /**
     * Call targets that resolve to no known object and are not builtins — candidates
     * for "call not followed" (wrong resolution) or genuinely external references.
     */
    static List<String> unresolvedCalls(SqlServerRoutineInfo info, Set<String> knownNamesUpper) {
        List<String> out = new ArrayList<>();
        if (info.getCalls() == null) return out;
        for (SqlServerCallInfo c : info.getCalls()) {
            String n = c.getName();
            if (n == null || n.isBlank()) continue;
            String bare = n.contains(".") ? n.substring(n.lastIndexOf('.') + 1) : n;
            String up = bare.toUpperCase().replace("[", "").replace("]", "");
            if (BUILTINS.contains(up)) continue;
            if (!knownNamesUpper.contains(up)) out.add(n + " (line " + c.getLine() + ")");
        }
        return out;
    }

    private static int countOps(SqlServerRoutineInfo info, String dml) {
        if (info.getTableOps() == null) return 0;
        int n = 0;
        for (SqlServerTableOpInfo t : info.getTableOps()) {
            if (t.getDmlType() != null && dml.equalsIgnoreCase(t.getDmlType().name())) n++;
        }
        return n;
    }

    /** Remove comments and string literals so keyword counts don't count text inside them. */
    private static String strip(String src) {
        if (src == null) return "";
        String s = BLOCK_COMMENT.matcher(src).replaceAll(" ");
        s = LINE_COMMENT.matcher(s).replaceAll(" ");
        s = STRING_LIT.matcher(s).replaceAll("''");
        return s;
    }

    private static int count(String s, String regex) {
        Matcher m = Pattern.compile(regex, Pattern.CASE_INSENSITIVE).matcher(s);
        int n = 0;
        while (m.find()) n++;
        return n;
    }
}
