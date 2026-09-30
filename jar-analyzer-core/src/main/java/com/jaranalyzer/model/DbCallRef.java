package com.jaranalyzer.model;

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * One place a JAR's Java code calls a database procedure, function or package.
 *
 * <p>This is an <b>additive facet</b>: it is produced by a dedicated detection pass at analysis
 * time and written to its own artifacts ({@code db-calls/index.json} and
 * {@code views/db-calls.json}). Nothing here is folded into {@code analysis.json}, so existing
 * documents and their byte-for-byte content are unchanged.
 *
 * <p><b>What a row is not.</b> A row is a constant found in the bytecode (or in a mapper XML
 * shipped inside the JAR), reported with the class and method it sits in. It is not proof the call
 * executes: a {@code {call ...}} string inside a comment or a dead branch is reported exactly like
 * a live one. That is deliberate — the alternative is dynamic taint analysis, which this feature
 * does not attempt. {@link #getKind()} lets the UI filter by the shape that was matched.
 *
 * <p>Fields are spelled for the wire: they are read directly by the UI, so they are plain getters
 * with no Jackson naming strategy applied.
 */
public class DbCallRef {

    /**
     * Which call shape matched. One of:
     * <ul>
     *   <li>{@code callable} — JDBC {@code CallableStatement} / {@code Connection.prepareCall},
     *       including {@code {call ...}}, {@code {? = call ...}} and {@code begin ... end;}
     *       PL/SQL blocks</li>
     *   <li>{@code stmt-function} — a function invoked in the SELECT column list of a plain SQL
     *       statement (e.g. {@code SELECT PKG.FUNC(x) FROM DUAL}); best-effort</li>
     *   <li>{@code jpa} — JPA/Hibernate {@code @NamedStoredProcedureQuery}, {@code @Procedure} or
     *       {@code EntityManager.createStoredProcedureQuery}</li>
     *   <li>{@code mybatis} — a mapper-XML statement with {@code statementType="CALLABLE"} or a
     *       {@code {call ...}} body</li>
     *   <li>{@code spring-jdbc} — Spring {@code SimpleJdbcCall}/{@code JdbcCall} fluent wiring</li>
     * </ul>
     */
    private String kind;

    /** Normalized object reference, quotes stripped, e.g. {@code PKG.PROC} or {@code PROC}. */
    private String dbObject;

    /** Schema/package prefix when the reference carried one, else null. */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String schema;

    /** The last segment of {@link #dbObject} — the procedure/function name on its own. */
    private String objectName;

    /** The raw text the reference was extracted from (truncated), for auditability. */
    private String rawCall;

    /** Fully-qualified name of the class containing the call site (mapper namespace for MyBatis). */
    private String className;

    /** The method containing the call site (mapper statement id for MyBatis). */
    private String methodName;

    /** Source line of the invocation when the bytecode carried one, else 0. */
    private int lineNumber;

    /** Number of bind parameters when cheaply visible, else -1. */
    private int paramCount;

    /** Where it was found: {@code bytecode}, or the resource file name for a mapper XML. */
    private String source;

    public DbCallRef() {}

    public DbCallRef(String kind, String dbObject, String schema, String objectName, String rawCall,
                     String className, String methodName, int lineNumber, int paramCount, String source) {
        this.kind = kind;
        this.dbObject = dbObject;
        this.schema = schema;
        this.objectName = objectName;
        this.rawCall = rawCall;
        this.className = className;
        this.methodName = methodName;
        this.lineNumber = lineNumber;
        this.paramCount = paramCount;
        this.source = source;
    }

    public String getKind() { return kind; }
    public void setKind(String kind) { this.kind = kind; }

    public String getDbObject() { return dbObject; }
    public void setDbObject(String dbObject) { this.dbObject = dbObject; }

    public String getSchema() { return schema; }
    public void setSchema(String schema) { this.schema = schema; }

    public String getObjectName() { return objectName; }
    public void setObjectName(String objectName) { this.objectName = objectName; }

    public String getRawCall() { return rawCall; }
    public void setRawCall(String rawCall) { this.rawCall = rawCall; }

    public String getClassName() { return className; }
    public void setClassName(String className) { this.className = className; }

    public String getMethodName() { return methodName; }
    public void setMethodName(String methodName) { this.methodName = methodName; }

    public int getLineNumber() { return lineNumber; }
    public void setLineNumber(int lineNumber) { this.lineNumber = lineNumber; }

    public int getParamCount() { return paramCount; }
    public void setParamCount(int paramCount) { this.paramCount = paramCount; }

    public String getSource() { return source; }
    public void setSource(String source) { this.source = source; }

    /** Stable identity for paging cursors and de-duplication — never an array ordinal. */
    public String naturalKey() {
        return kind + "|" + dbObject + "|" + className + "|" + methodName + "|" + rawCall;
    }
}
