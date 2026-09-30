package com.jaranalyzer.service;

import com.jaranalyzer.model.AnnotationInfo;
import com.jaranalyzer.model.ClassInfo;
import com.jaranalyzer.model.DbCallRef;
import com.jaranalyzer.model.MethodCallInfo;
import com.jaranalyzer.model.MethodInfo;
import com.jaranalyzer.util.SqlStatementParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Finds every place the analysed Java code calls a database procedure, function or package, and
 * reports the DB object plus the class/method it was called from.
 *
 * <p><b>Why a separate detector.</b> {@code JpaMethodDetector} already recognises several of these
 * shapes, but it is table-oriented: it folds a detected proc into the collections list as a table
 * name with an operation type. That loses the distinction between a table and a package, and there
 * is no facet that answers "which procedures does this JAR call". This detector is deliberately
 * independent — it reads {@link ClassInfo} (the parse output), produces its own model
 * ({@link com.jaranalyzer.model.DbCallRef}) and its own artifacts, and touches no existing output.
 *
 * <p><b>False positives are reported, not hidden.</b> A {@code {call ...}} constant in a comment or
 * an unreachable branch is reported the same as a live one; the row carries the class, method, line
 * and raw text so a human can judge it, and a {@code kind} so the UI can filter by shape. No
 * attempt is made to prove the call executes — that is dynamic taint analysis, which this feature
 * does not do.
 *
 * <p><b>Byte-identical guarantee.</b> Nothing here mutates {@link ClassInfo}, {@code analysis.json}
 * or any existing sidecar. It is a read-only pass over the same objects the parser already built.
 */
@Component
public class DbCallDetector {

    private static final Logger log = LoggerFactory.getLogger(DbCallDetector.class);

    /** Raw call text is truncated to this many characters before it is stored. */
    private static final int MAX_RAW = 500;

    /* ── owner-class simple names ─────────────────────────────────────────────────────────── */

    private static final List<String> CONNECTION_OWNERS =
            List.of("Connection", "OracleConnection");
    private static final List<String> CALLABLE_STMT_OWNERS =
            List.of("CallableStatement", "OracleCallableStatement");
    private static final List<String> SIMPLE_JDBC_OWNERS =
            List.of("SimpleJdbcCall", "JdbcCall", "SimpleJdbcInsert");
    private static final List<String> EM_OWNERS =
            List.of("EntityManager", "Session", "StatelessSession");

    /** Methods on a JDBC connection that can carry a callable statement string. */
    private static final Set<String> CONNECTION_SQL_METHODS = Set.of(
            "prepareCall", "prepareStatement", "nativeSQL", "execute", "executeQuery", "executeUpdate");

    /** Terminal methods on a CallableStatement that carry its call string in the rolling window. */
    private static final Set<String> CALLABLE_TERMINALS = Set.of(
            "execute", "executeQuery", "executeUpdate", "executeLargeUpdate");

    private static final Set<String> SIMPLE_CALL_EXECUTE =
            Set.of("execute", "executeFunction", "executeScalar", "call");

    /* ── patterns ─────────────────────────────────────────────────────────────────────────── */

    /** A callable SQL string: {call ...}, {? = call ...}, CALL ..., EXEC ... or begin ... end; */
    private static final Pattern CALLABLE_SQL = Pattern.compile(
            "(?is)^\\s*(?:\\{\\s*(?:\\?\\s*=\\s*)?call\\b|call\\b|exec(?:ute)?\\b|begin\\b)");

    /** The SELECT column list of a plain SQL statement, up to the first FROM. */
    private static final Pattern SELECT_LIST =
            Pattern.compile("(?is)\\bselect\\b(.*?)\\bfrom\\b");

    /** An identifier (optionally schema-qualified) immediately followed by '('. */
    private static final Pattern FUNCTION_CALL = Pattern.compile(
            "([A-Za-z_][A-Za-z0-9_$]*)(?:\\s*\\.\\s*([A-Za-z_][A-Za-z0-9_$]*))?\\s*\\(");

    /**
     * SQL/aggregate built-ins that are not database procedures or functions of interest. Matched
     * case-insensitively against the bare function name; anything here is skipped in the
     * SELECT-column scan so a report does not fill with COUNT/SUM/NVL noise.
     */
    private static final Set<String> SQL_BUILTINS = Set.of(
            "count", "sum", "avg", "min", "max", "nvl", "nvl2", "decode", "coalesce", "nullif",
            "to_char", "to_date", "to_number", "to_timestamp", "cast", "convert", "upper", "lower",
            "substr", "substring", "trim", "ltrim", "rtrim", "length", "instr", "replace", "round",
            "trunc", "floor", "ceil", "ceiling", "abs", "mod", "power", "sqrt", "exp", "ln", "log",
            "sysdate", "systimestamp", "current_date", "current_timestamp", "row_number", "rank",
            "dense_rank", "lead", "lag", "first_value", "last_value", "ntile", "case", "ifnull",
            "concat", "regexp_replace", "regexp_substr", "regexp_like", "extract", "greatest", "least");

    /* ── public API ───────────────────────────────────────────────────────────────────────── */

    /**
     * Detect DB calls in one parsed class. Never throws for a malformed class; returns an empty
     * list rather than propagating.
     */
    public List<DbCallRef> detectClass(ClassInfo cls) {
        List<DbCallRef> out = new ArrayList<>();
        if (cls == null) return out;
        try {
            String clsFqn = cls.getFullyQualifiedName();

            // Class-level @NamedStoredProcedureQuery / @Procedure.
            for (AnnotationInfo ann : safeAnnotations(cls.getAnnotations())) {
                scanProcAnnotation(ann, clsFqn, null, out);
            }

            // Build the field -> procedure/package map for SimpleJdbcCall wiring that is split
            // across a constructor (which sets the field) and an execute method elsewhere in the
            // class. Only invocations that name a receiver field participate.
            Map<String, String> fieldProc = new LinkedHashMap<>();
            Map<String, String> fieldPrefix = new LinkedHashMap<>();
            for (MethodInfo m : safeMethods(cls)) {
                for (MethodCallInfo inv : safeInvocations(m)) {
                    String field = inv.getReceiverFieldName();
                    if (field == null || !isSimpleJdbcOwner(inv.getOwnerClass())) continue;
                    String mn = inv.getMethodName();
                    if ("withCatalogName".equals(mn)) {
                        String v = firstString(inv);
                        if (v != null) fieldPrefix.put(field, v);
                    } else if ("withSchemaName".equals(mn)) {
                        fieldPrefix.remove(field);
                    } else if ("withProcedureName".equals(mn) || "withFunctionName".equals(mn)) {
                        String name = firstString(inv);
                        if (name != null && !name.isBlank()) {
                            String pfx = fieldPrefix.get(field);
                            fieldProc.put(field, (pfx != null && !pfx.isBlank()) ? pfx + "." + name.trim() : name.trim());
                        }
                    }
                }
            }

            for (MethodInfo m : safeMethods(cls)) {
                scanMethod(clsFqn, m, fieldProc, out);
            }
        } catch (RuntimeException e) {
            log.debug("DB-call detection failed for {}: {}", cls.getFullyQualifiedName(), e.toString());
        }
        return dedupe(out);
    }

    /**
     * Detect DB calls in the JAR's resource files — specifically MyBatis mapper XMLs, whose
     * {@code statementType="CALLABLE"} statements and {@code {call ...}} bodies are a call site the
     * bytecode does not carry.
     *
     * @param resources filename -> content, as produced by
     *                  {@code JarParserService.extractResourceFiles}
     */
    public List<DbCallRef> detectResources(Map<String, String> resources) {
        List<DbCallRef> out = new ArrayList<>();
        if (resources == null || resources.isEmpty()) return out;
        for (Map.Entry<String, String> e : resources.entrySet()) {
            String name = e.getKey();
            String body = e.getValue();
            if (name == null || body == null) continue;
            String lower = name.toLowerCase(Locale.ROOT);
            if (!lower.endsWith(".xml")) continue;
            if (!body.contains("{call") && !body.contains("{CALL") && !body.contains("CALLABLE")
                    && !body.contains("callable")) {
                continue;
            }
            try {
                scanMapperXml(name, body, out);
            } catch (RuntimeException ex) {
                log.debug("Mapper XML scan failed for {}: {}", name, ex.toString());
            }
        }
        return dedupe(out);
    }

    /* ── per-method scan ──────────────────────────────────────────────────────────────────── */

    private void scanMethod(String clsFqn, MethodInfo m, Map<String, String> fieldProc,
                            List<DbCallRef> out) {
        String methodName = m.getName();
        // Method-local SimpleJdbcCall chain prefix (the common "new SimpleJdbcCall(ds)
        // .withCatalogName(...).withProcedureName(...)" in one method).
        String pendingPrefix = null;

        for (AnnotationInfo ann : safeAnnotations(m.getAnnotations())) {
            scanProcAnnotation(ann, clsFqn, methodName, out);
        }

        for (MethodCallInfo inv : safeInvocations(m)) {
            String owner = inv.getOwnerClass();
            String mn = inv.getMethodName();
            int line = inv.getLineNumber();

            // Spring SimpleJdbcCall / JdbcCall fluent chain.
            if (isSimpleJdbcOwner(owner)) {
                if ("withCatalogName".equals(mn)) {
                    pendingPrefix = firstString(inv);
                    continue;
                }
                if ("withSchemaName".equals(mn)) {
                    pendingPrefix = null;
                    continue;
                }
                if ("withProcedureName".equals(mn) || "withFunctionName".equals(mn)) {
                    String name = firstString(inv);
                    if (name != null && !name.isBlank()) {
                        String obj = (pendingPrefix != null && !pendingPrefix.isBlank())
                                ? pendingPrefix.trim() + "." + name.trim() : name.trim();
                        add(out, "spring-jdbc", obj, rawOf(inv), clsFqn, methodName, line, -1, "bytecode");
                    }
                    pendingPrefix = null;
                    continue;
                }
                if (SIMPLE_CALL_EXECUTE.contains(mn)) {
                    String field = inv.getReceiverFieldName();
                    String proc = field == null ? null : fieldProc.get(field);
                    if (proc != null) {
                        add(out, "spring-jdbc", proc, rawOf(inv), clsFqn, methodName, line, -1, "bytecode");
                    }
                    continue;
                }
                continue;
            }

            // JPA / Hibernate stored-procedure creation.
            if (ownerMatches(owner, EM_OWNERS)) {
                if ("createStoredProcedureQuery".equals(mn) || "createNamedStoredProcedureQuery".equals(mn)) {
                    for (String s : safeStrings(inv)) {
                        if (s == null || s.isBlank() || s.startsWith("__class:")) continue;
                        add(out, "jpa", normalizeObject(s), rawOf(inv), clsFqn, methodName, line, -1, "bytecode");
                    }
                    continue;
                }
            }

            // JDBC: Connection.prepareCall / prepareStatement / execute* carrying SQL text.
            if (ownerMatches(owner, CONNECTION_OWNERS) && CONNECTION_SQL_METHODS.contains(mn)) {
                scanSqlStrings(inv, clsFqn, methodName, line, out);
                continue;
            }

            // JDBC: CallableStatement terminal methods (the call string sits in the window).
            if (ownerMatches(owner, CALLABLE_STMT_OWNERS) && CALLABLE_TERMINALS.contains(mn)) {
                scanSqlStrings(inv, clsFqn, methodName, line, out);
                continue;
            }
        }
    }

    /** Scan a method call's string window for callable SQL and SELECT-column function calls. */
    private void scanSqlStrings(MethodCallInfo inv, String clsFqn, String methodName, int line,
                                List<DbCallRef> out) {
        for (String s : safeStrings(inv)) {
            if (s == null || s.isBlank() || s.startsWith("__class:")) continue;
            if (CALLABLE_SQL.matcher(s).find()) {
                addCallable(s, clsFqn, methodName, line, out);
            } else {
                addSelectFunctions(s, clsFqn, methodName, line, out);
            }
        }
    }

    private void addCallable(String sql, String clsFqn, String methodName, int line, List<DbCallRef> out) {
        Set<String> procs = SqlStatementParser.extractProcedureNames(sql);
        int params = countQuestionMarks(sql);
        if (procs.isEmpty()) {
            // Recognised as callable but no name could be extracted — still report the site.
            add(out, "callable", "?", raw(sql), clsFqn, methodName, line, params, "bytecode");
            return;
        }
        for (String proc : procs) {
            add(out, "callable", normalizeObject(proc), raw(sql), clsFqn, methodName, line, params, "bytecode");
        }
    }

    private void addSelectFunctions(String sql, String clsFqn, String methodName, int line,
                                    List<DbCallRef> out) {
        if (sql == null || sql.length() < 8) return;
        Matcher sel = SELECT_LIST.matcher(sql);
        while (sel.find()) {
            String columnList = sel.group(1);
            Matcher fn = FUNCTION_CALL.matcher(columnList);
            while (fn.find()) {
                String first = fn.group(1);
                String second = fn.group(2);
                String bare = (second != null ? second : first).toLowerCase(Locale.ROOT);
                if (SQL_BUILTINS.contains(bare)) continue;
                if (bare.length() < 2) continue;
                String object = second != null ? first + "." + second : first;
                add(out, "stmt-function", normalizeObject(object), raw(sql), clsFqn, methodName, line,
                        -1, "bytecode");
            }
        }
    }

    /* ── annotation scan ──────────────────────────────────────────────────────────────────── */

    private void scanProcAnnotation(AnnotationInfo ann, String clsFqn, String methodName,
                                    List<DbCallRef> out) {
        if (ann == null) return;
        String name = ann.getName();
        Map<String, Object> attrs = ann.getAttributes();
        if (attrs == null) attrs = Map.of();

        if ("NamedStoredProcedureQuery".equals(name) || "NamedStoredProcedureQueries".equals(name)) {
            Object proc = firstNonNull(attrs.get("procedureName"), attrs.get("value"));
            if (proc instanceof String s && !s.isBlank()) {
                add(out, "jpa", normalizeObject(s), "@NamedStoredProcedureQuery(procedureName=\"" + s + "\")",
                        clsFqn, methodName, 0, -1, "bytecode");
            }
            return;
        }
        if ("Procedure".equals(name)) {
            Object proc = firstNonNull(attrs.get("name"), attrs.get("procedureName"), attrs.get("value"));
            if (proc instanceof String s && !s.isBlank()) {
                add(out, "jpa", normalizeObject(s), "@Procedure(\"" + s + "\")",
                        clsFqn, methodName, 0, -1, "bytecode");
            }
        }
    }

    /* ── MyBatis mapper XML ───────────────────────────────────────────────────────────────── */

    private static final Pattern XML_STATEMENT = Pattern.compile(
            "(?is)<(select|insert|update|delete)\\b([^>]*)>(.*?)</\\1\\s*>");
    private static final Pattern ID_ATTR = Pattern.compile("(?is)\\bid\\s*=\\s*\"([^\"]+)\"");
    private static final Pattern NAMESPACE_ATTR =
            Pattern.compile("(?is)<mapper\\b[^>]*\\bnamespace\\s*=\\s*\"([^\"]+)\"");
    private static final Pattern HASH_PARAM = Pattern.compile("#\\{[^}]*}");

    private void scanMapperXml(String fileName, String xml, List<DbCallRef> out) {
        String namespace = null;
        Matcher ns = NAMESPACE_ATTR.matcher(xml);
        if (ns.find()) namespace = ns.group(1);

        Matcher st = XML_STATEMENT.matcher(xml);
        while (st.find()) {
            String attrs = st.group(2);
            String body = st.group(3);
            boolean callable = attrs.toLowerCase(Locale.ROOT).contains("statementtype")
                    && attrs.toLowerCase(Locale.ROOT).replaceAll("\\s", "").contains("statementtype=\"callable\"");
            boolean hasCall = body.toLowerCase(Locale.ROOT).contains("{call");
            if (!callable && !hasCall) continue;

            String id = null;
            Matcher idm = ID_ATTR.matcher(attrs);
            if (idm.find()) id = idm.group(1);
            String owner = namespace != null ? namespace : fileName;
            String where = id != null ? id : fileName;

            Set<String> procs = SqlStatementParser.extractProcedureNames(body);
            int params = countMatches(HASH_PARAM, body);
            if (procs.isEmpty()) {
                // CALLABLE but no extractable name (a fully dynamic statement) — still report it.
                add(out, "mybatis", "?", raw(body), owner, where, 0, params, fileName);
            } else {
                for (String proc : procs) {
                    add(out, "mybatis", normalizeObject(proc), raw(body), owner, where, 0, params, fileName);
                }
            }
        }
    }

    /* ── helpers ──────────────────────────────────────────────────────────────────────────── */

    private void add(List<DbCallRef> out, String kind, String dbObject, String rawCall,
                     String className, String methodName, int lineNumber, int paramCount,
                     String source) {
        String[] parts = splitObject(dbObject);
        out.add(new DbCallRef(kind, parts[0], parts.length > 1 ? parts[1] : null, parts[2],
                rawCall, className, methodName, lineNumber, paramCount, source));
    }

    /** Strip quotes/backticks/brackets and collapse whitespace around the dot separators. */
    static String normalizeObject(String raw) {
        if (raw == null) return "?";
        String s = raw.trim();
        if (s.isEmpty()) return "?";
        StringBuilder sb = new StringBuilder(s.length());
        for (String part : s.split("\\.")) {
            String p = part.trim();
            while (p.length() >= 2
                    && ((p.startsWith("\"") && p.endsWith("\""))
                        || (p.startsWith("`") && p.endsWith("`"))
                        || (p.startsWith("[") && p.endsWith("]")))) {
                p = p.substring(1, p.length() - 1).trim();
            }
            if (p.isEmpty()) continue;
            if (sb.length() > 0) sb.append('.');
            sb.append(p);
        }
        return sb.length() == 0 ? "?" : sb.toString();
    }

    /** @return {@code [dbObject, schemaOrNull, objectName]}. */
    private static String[] splitObject(String normalized) {
        if (normalized == null || normalized.isBlank() || "?".equals(normalized)) {
            return new String[]{"?", null, "?"};
        }
        int dot = normalized.lastIndexOf('.');
        if (dot <= 0) return new String[]{normalized, null, normalized};
        return new String[]{normalized, normalized.substring(0, dot), normalized.substring(dot + 1)};
    }

    private static int countQuestionMarks(String s) {
        if (s == null) return -1;
        int n = 0;
        boolean inQuote = false;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '\'') inQuote = !inQuote;
            else if (c == '?' && !inQuote) n++;
        }
        return n;
    }

    private static int countMatches(Pattern p, String s) {
        if (s == null) return -1;
        Matcher m = p.matcher(s);
        int n = 0;
        while (m.find()) n++;
        return n;
    }

    private String rawOf(MethodCallInfo inv) {
        for (String s : safeStrings(inv)) {
            if (s != null && !s.isBlank() && !s.startsWith("__class:")) return raw(s);
        }
        return inv.getMethodName();
    }

    private static String raw(String s) {
        if (s == null) return null;
        String t = s.trim();
        return t.length() > MAX_RAW ? t.substring(0, MAX_RAW) + "..." : t;
    }

    private static String firstString(MethodCallInfo inv) {
        for (String s : safeStrings(inv)) {
            if (s != null && !s.isBlank() && !s.startsWith("__class:")) return s;
        }
        return null;
    }

    private static List<String> safeStrings(MethodCallInfo inv) {
        List<String> s = inv.getRecentStringArgs();
        return s == null ? List.of() : s;
    }

    private static List<AnnotationInfo> safeAnnotations(List<AnnotationInfo> a) {
        return a == null ? List.of() : a;
    }

    private static List<MethodInfo> safeMethods(ClassInfo c) {
        return c.getMethods() == null ? List.of() : c.getMethods();
    }

    private static List<MethodCallInfo> safeInvocations(MethodInfo m) {
        return m.getInvocations() == null ? List.of() : m.getInvocations();
    }

    private static boolean isSimpleJdbcOwner(String owner) {
        return ownerMatches(owner, SIMPLE_JDBC_OWNERS);
    }

    private static boolean ownerMatches(String owner, List<String> patterns) {
        if (owner == null) return false;
        for (String p : patterns) {
            if (owner.equals(p) || owner.endsWith("." + p)) return true;
        }
        return false;
    }

    private static Object firstNonNull(Object... values) {
        for (Object v : values) if (v != null) return v;
        return null;
    }

    private static List<DbCallRef> dedupe(List<DbCallRef> refs) {
        Map<String, DbCallRef> byKey = new LinkedHashMap<>();
        for (DbCallRef r : refs) byKey.putIfAbsent(r.naturalKey(), r);
        return new ArrayList<>(byKey.values());
    }
}
