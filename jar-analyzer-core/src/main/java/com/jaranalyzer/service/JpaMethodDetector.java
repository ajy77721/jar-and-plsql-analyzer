package com.jaranalyzer.service;

import com.jaranalyzer.model.MethodCallInfo;
import com.jaranalyzer.service.CallGraphIndex.*;
import com.jaranalyzer.util.SqlStatementParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.*;

/**
 * Detects JPA/JDBC table access from method invocations in bytecode.
 * Parallel to MongoMethodDetector — handles relational database patterns:
 *
 *   A. JdbcTemplate / NamedParameterJdbcTemplate: query, update, execute with SQL string args
 *      A1. Spring 6.1 JdbcClient fluent API: JdbcClient.sql(sql).query() / .update() (GAP-JDBC-CLIENT)
 *   B. EntityManager / Session: createQuery, createNativeQuery, createSQLQuery, find, persist, merge, remove
 *      B1: createStoredProcedureQuery / createNamedStoredProcedureQuery → proc name extraction
 *   C. JPA @Query, @NamedQuery, @NamedNativeQuery annotation SQL extraction
 *      C2: @Procedure annotation (name/procedureName/value) → proc name
 *      C3: Hibernate @SQLInsert, @SQLUpdate, @SQLDelete annotations
 *   D. Spring Data JPA derived query method name parsing (findByXxx, deleteByXxx)
 *   E. JpaRepository/CrudRepository built-in methods (save, findById, delete, etc.)
 *   F. SimpleJdbcCall.withProcedureName / withFunctionName → proc name extraction
 *   G. Connection.prepareCall("{call PKG.PROC(?)}") → callable SQL proc extraction
 *      G1. Connection.prepareStatement(sql) → SQL string arg extraction (GAP-CONN-PREPARE)
 *   H. CallableStatement / OracleCallableStatement: execute, executeQuery, executeUpdate
 */
@Component
class JpaMethodDetector {

    private static final Logger log = LoggerFactory.getLogger(JpaMethodDetector.class);

    /**
     * Task #30(d): the JPQL/HQL regex-last-resort counter.
     *
     * <p><b>Decision (documented, not a grammar).</b> There is no JPQL/HQL grammar in this
     * codebase. Adding one would mean a new ANTLR grammar module for two dialects whose query
     * strings arrive as opaque bytecode literals, to recover only the entity/table names and the
     * operation verb — which the regexes in {@link SqlStatementParser} already do reliably for the
     * JPQL/HQL subset that appears in real repositories (SELECT / UPDATE / DELETE, FROM-shorthand,
     * joins). A grammar is therefore disproportionate here; this counter makes the residual regex
     * usage observable (one increment per JPQL/HQL string handled by the regex path) so the
     * decision can be revisited if the counter ever climbs in a corpus that needs more than
     * name/verb extraction. The matching note is on {@link SqlStatementParser}.
     */
    public static final java.util.concurrent.atomic.AtomicLong JPQL_HQL_REGEX_FALLBACKS =
            new java.util.concurrent.atomic.AtomicLong();

    // JdbcTemplate methods that accept SQL string as first argument.
    // "sql" is the JdbcClient (Spring 6.1) entry-point method that accepts the SQL string
    // (GAP-JDBC-CLIENT).
    private static final Set<String> JDBC_TEMPLATE_METHODS = Set.of(
            "query", "queryForObject", "queryForList", "queryForMap", "queryForRowSet",
            "queryForStream", "queryForInt", "queryForLong",
            "update", "batchUpdate", "execute",
            "call",
            // Spring 6.1 JdbcClient entry-point method (GAP-JDBC-CLIENT)
            "sql"
    );

    private static final List<String> JDBC_TEMPLATE_OWNERS = List.of(
            "JdbcTemplate", "NamedParameterJdbcTemplate", "JdbcOperations",
            "NamedParameterJdbcOperations", "SimpleJdbcCall", "SimpleJdbcInsert",
            // Spring 6.1 fluent JDBC API (GAP-JDBC-CLIENT)
            "JdbcClient", "StatementSpec", "MappedQuerySpec"
    );

    // Section A (SQL-string extraction) must NOT include SimpleJdbcCall/SimpleJdbcInsert:
    // those classes use execute()/call() to invoke stored procs (no SQL string arg),
    // and they must fall through to sections F0/F/F2 for proc-name field lookup.
    // Without this split, section A fires first on SimpleJdbcCall.execute() → continue
    // skips F2 entirely, making stored-proc detection dead code.
    // Spring 6.1 JdbcClient fluent API (GAP-JDBC-CLIENT): JdbcClient.sql("SELECT ...").query()
    // passes the SQL string via sql() which is the entry-point method; StatementSpec.query/update
    // and MappedQuerySpec have the terminal calls. All three are SQL-owners so that the rolling-
    // window string arg is extracted.
    private static final List<String> JDBC_SQL_OWNERS = List.of(
            "JdbcTemplate", "NamedParameterJdbcTemplate", "JdbcOperations",
            "NamedParameterJdbcOperations",
            // Spring 6.1 JdbcClient (GAP-JDBC-CLIENT)
            "JdbcClient", "StatementSpec", "MappedQuerySpec"
    );

    // EntityManager / Session methods
    private static final Set<String> EM_QUERY_METHODS = Set.of(
            "createQuery", "createNativeQuery", "createNamedQuery",
            "createStoredProcedureQuery", "createNamedStoredProcedureQuery",
            "createSQLQuery"
    );

    private static final Set<String> EM_ENTITY_METHODS = Set.of(
            "find", "getReference", "persist", "merge", "remove", "refresh",
            "detach", "lock",
            // em.contains(entity) → READ: checks if entity is managed by this persistence context.
            // NOTE: "contains" was formerly in SKIP_METHODS in some versions. It is correctly
            // handled here because the EM_OWNERS owner guard prevents false positives from
            // java.util.Collection.contains() (whose owner class is never in EM_OWNERS).
            "contains"
    );

    private static final List<String> EM_OWNERS = List.of(
            "EntityManager", "Session", "StatelessSession",
            "EntityManagerFactory", "SessionFactory"
    );

    // CallableStatement / OracleCallableStatement methods
    private static final Set<String> CALLABLE_STMT_METHODS = Set.of(
            "executeQuery", "executeUpdate", "execute", "registerOutParameter"
    );
    private static final List<String> CALLABLE_STMT_OWNERS = List.of(
            "CallableStatement", "OracleCallableStatement"
    );

    // SimpleJdbcCall methods that set procedure/function names directly
    private static final Set<String> SIMPLE_CALL_PROC_METHODS = Set.of(
            "withProcedureName", "withFunctionName"
    );

    // Connection methods that create callable statements
    private static final Set<String> CONNECTION_CALL_METHODS = Set.of(
            "prepareCall"
    );
    // Connection methods that accept a SQL string and prepare a statement (not a call).
    // GAP-CONN-PREPARE: Connection.prepareStatement(sql) must be detected like JdbcTemplate.query(sql).
    private static final Set<String> CONNECTION_PREPARE_METHODS = Set.of(
            "prepareStatement"
    );
    private static final List<String> CONNECTION_OWNERS = List.of(
            "Connection", "OracleConnection", "java.sql.Connection"
    );

    // Spring Data JPA built-in repository methods
    private static final Map<String, String> REPO_METHOD_OPS = Map.ofEntries(
            Map.entry("save", "WRITE"),
            Map.entry("saveAll", "WRITE"),
            Map.entry("saveAndFlush", "WRITE"),
            Map.entry("saveAllAndFlush", "WRITE"),
            Map.entry("findById", "READ"),
            Map.entry("findAll", "READ"),
            Map.entry("findAllById", "READ"),
            Map.entry("existsById", "READ"),
            Map.entry("count", "COUNT"),
            Map.entry("deleteById", "DELETE"),
            Map.entry("delete", "DELETE"),
            Map.entry("deleteAll", "DELETE"),
            Map.entry("deleteAllById", "DELETE"),
            Map.entry("deleteAllInBatch", "DELETE"),
            Map.entry("deleteInBatch", "DELETE"),
            Map.entry("flush", "WRITE"),
            Map.entry("getById", "READ"),
            Map.entry("getReferenceById", "READ"),
            Map.entry("getOne", "READ")
    );

    record DetectedTable(String tableName, String source, String operationType, String sqlText) {
        DetectedTable(String tableName, String source, String operationType) {
            this(tableName, source, operationType, null);
        }
    }

    /**
     * CALL-009: mirrors MongoMethodDetector.DetectionResult — resolved literal table detections
     * plus dynamic (variable-sourced) references that CollectionResolver's JDBC-parallel hop/
     * concat/class-derivation logic can attempt to resolve later. Deliberately reuses
     * CallGraphIndex.DetectedDynamicCollection (already domain-agnostic — see its own javadoc)
     * rather than inventing a JDBC-specific record.
     */
    record DetectionResult(List<DetectedTable> resolved, List<CallGraphIndex.DetectedDynamicCollection> dynamic) {}

    /**
     * CALL-009: single-hop call-site argument resolution for JDBC/SQL — mirrors
     * MongoMethodDetector.resolveGenericCallSiteArg/CallSiteCollection, but the traced value here
     * is a raw SQL STRING (which must still be parsed for its table name once resolved), not a
     * table name directly — that structural difference (an extra SQL-parse step after resolution)
     * is exactly why this is a parallel mechanism rather than a shared/generalized one. {@code
     * literalSql} is the first same-call-site string arg that looks like SQL; else the first
     * captured dynamic (variable/param/field/concat) argument is carried for CollectionResolver's
     * bounded ancestor walk to attempt resolving.
     */
    record CallSiteSql(String literalSql, MethodCallInfo.DynamicArgSource dynamicArg) {}

    CallSiteSql resolveGenericSqlCallSiteArg(InvocationRef call) {
        if (call.stringArgs() != null) {
            for (String arg : call.stringArgs()) {
                if (arg != null && !arg.startsWith("__class:") && looksLikeSql(arg)) {
                    return new CallSiteSql(arg, null);
                }
            }
        }
        List<MethodCallInfo.DynamicArgSource> dyns = call.dynamicArgSources();
        if (dyns != null && !dyns.isEmpty()) return new CallSiteSql(null, dyns.get(0));
        return null;
    }

    /**
     * CALL-009: single-hop call-site Class&lt;?&gt; argument resolution, mirroring
     * MongoMethodDetector's PARAM_CLASS handling shape — used for JPA entity-class propagation
     * (e.g. an EntityManager.find(Xxx.class, id) style call where the Class literal itself is
     * threaded down through 1+ wrapper methods before reaching the JPA call).
     */
    record CallSiteClass(String classFqn, MethodCallInfo.DynamicArgSource dynamicArg) {}

    CallSiteClass resolveGenericClassCallSiteArg(InvocationRef call) {
        if (call.stringArgs() != null) {
            for (String arg : call.stringArgs()) {
                if (arg != null && arg.startsWith("__class:")) return new CallSiteClass(arg.substring(8), null);
            }
        }
        List<MethodCallInfo.DynamicArgSource> dyns = call.dynamicArgSources();
        if (dyns != null) {
            for (MethodCallInfo.DynamicArgSource dyn : dyns) {
                if ("PARAM_CLASS".equals(dyn.kind()) || "LOCAL_VAR_CLASS".equals(dyn.kind())) {
                    return new CallSiteClass(null, dyn);
                }
            }
        }
        return null;
    }

    private final DomainConfigLoader configLoader;

    JpaMethodDetector(DomainConfigLoader configLoader) {
        this.configLoader = configLoader;
    }

    // JPA Criteria API methods that carry an entity class reference
    private static final Set<String> CRITERIA_ENTITY_METHODS = Set.of("createQuery", "from");
    private static final List<String> CRITERIA_OWNERS = List.of(
            "CriteriaBuilder", "CriteriaQuery", "CriteriaUpdate", "CriteriaDelete",
            "AbstractQuery", "CommonAbstractCriteria"
    );

    /**
     * Scan all invocations in a method for JPA/JDBC table access patterns.
     *
     * @param namedQueryMap  named query name → JPQL/SQL text (from @NamedQuery on entities);
     *                       may be empty but must not be null
     */
    DetectionResult detect(IndexedMethod method, IndexedClass cls,
                               Map<String, String> entityTableMap,
                               Map<String, String> namedQueryMap) {
        List<DetectedTable> results = new ArrayList<>();
        // CALL-009: dynamic (variable-sourced) SQL/entity-class references — mirrors
        // MongoMethodDetector's dynamicResults list. Never folded into `results` here;
        // CollectionResolver's JDBC-parallel hop/concat/class-derivation logic attempts to
        // resolve these at the CALL SITE, same as it already does for Mongo.
        List<CallGraphIndex.DetectedDynamicCollection> dynamicResults = new ArrayList<>();
        // Track inline Oracle package prefix from withCatalogName() for the next withProcedureName/withFunctionName
        String pendingCatalogPrefix = null;

        // A1/A5. StoredProcedure subclass: class extends Spring's StoredProcedure and the proc name
        // was captured at index time (indexClass scanned the INVOKESPECIAL to the super constructor).
        // Detect execute-wrapper methods (named execute/run/call, or containing a super.execute() call).
        if (cls.storedProcedureName != null) {
            boolean isExecuteWrapper = method.name.startsWith("execute")
                    || "run".equals(method.name) || "call".equals(method.name);
            if (!isExecuteWrapper) {
                for (InvocationRef inv : method.invocations) {
                    if (inv.ownerClass() != null
                            && (inv.ownerClass().endsWith(".StoredProcedure")
                                || "StoredProcedure".equals(inv.ownerClass()))
                            && ("execute".equals(inv.methodName()) || "run".equals(inv.methodName()))) {
                        isExecuteWrapper = true;
                        break;
                    }
                }
            }
            if (isExecuteWrapper) {
                results.add(new DetectedTable(cls.storedProcedureName, "STORED_PROCEDURE", "CALL"));
            }
        }

        for (InvocationRef inv : method.invocations) {
            String owner = inv.ownerClass();
            String methodName = inv.methodName();
            List<String> stringArgs = inv.stringArgs();

            // A. JdbcTemplate / NamedParameterJdbcTemplate / JdbcClient (Spring 6.1)
            // Section A uses JDBC_SQL_OWNERS (no SimpleJdbcCall/SimpleJdbcInsert) so that
            // SimpleJdbcCall.execute()/call() fall through to sections F0/F/F2 below.
            // JdbcClient.sql("SELECT ...") carries the SQL in its first arg and is matched here.
            if (JDBC_TEMPLATE_METHODS.contains(methodName) && ownerMatchesAny(owner, JDBC_SQL_OWNERS)) {
                int before = results.size();
                extractFromSqlArgs(stringArgs, "JDBC_TEMPLATE", results);
                // CALL-009: no literal SQL string found at this call site — the SQL argument may
                // be a variable (param/field/local/concat) resolvable at a CALLER's call site.
                if (results.size() == before) {
                    captureDynamicSql(inv, "JDBC_TEMPLATE", dynamicResults);
                }
                continue;
            }

            // B. EntityManager query methods (createQuery, createNativeQuery, etc.)
            if (EM_QUERY_METHODS.contains(methodName) && ownerMatchesAny(owner, EM_OWNERS)) {
                if ("createStoredProcedureQuery".equals(methodName)) {
                    extractProcedureFromArgs(stringArgs, "STORED_PROCEDURE", results);
                } else if ("createNamedStoredProcedureQuery".equals(methodName)) {
                    // A2. Look up the logical name in namedQueryMap (populated from @NamedStoredProcedureQuery).
                    // Entries are stored as "PROC:<actualProcedureName>"; fall back to arg as proc name directly.
                    if (stringArgs != null) {
                        for (String arg : stringArgs) {
                            if (arg == null || arg.startsWith("__class:")) continue;
                            String entry = namedQueryMap.get(arg.trim());
                            if (entry != null && entry.startsWith("PROC:")) {
                                results.add(new DetectedTable(entry.substring(5), "STORED_PROCEDURE", "CALL"));
                            } else {
                                extractProcedureFromArgs(List.of(arg), "STORED_PROCEDURE", results);
                            }
                        }
                    }
                } else if ("createNamedQuery".equals(methodName)) {
                    // B-named. Resolve named query → JPQL/SQL text via @NamedQuery registry
                    if (stringArgs != null) {
                        for (String arg : stringArgs) {
                            if (arg.startsWith("__class:")) continue;
                            String queryText = namedQueryMap.get(arg.trim());
                            // Skip "PROC:" entries (those are @NamedStoredProcedureQuery, not queries)
                            if (queryText != null && !queryText.isBlank() && !queryText.startsWith("PROC:")) {
                                // Always try JPQL entity resolution first; fall back to native SQL table extraction
                                extractJpqlEntities(queryText, entityTableMap, results);
                                // If JPQL resolution found nothing, try native SQL path
                                if (results.isEmpty() && looksLikeSql(queryText)) {
                                    extractFromSql(queryText, "NAMED_QUERY", results);
                                }
                            }
                        }
                    }
                } else if ("createQuery".equals(methodName)) {
                    // JPQL path: entity names in FROM clause need to be resolved to table names.
                    // Also handle native-style JPQL (rare but possible).
                    if (stringArgs != null) {
                        for (String arg : stringArgs) {
                            if (arg.startsWith("__class:")) continue;
                            if (looksLikeSql(arg)) {
                                extractJpqlEntities(arg, entityTableMap, results);
                            }
                        }
                    }
                } else {
                    // createNativeQuery, createSQLQuery → native SQL, extract table names directly
                    int before = results.size();
                    extractFromSqlArgs(stringArgs, "ENTITY_MANAGER", results);
                    if (results.size() == before) {
                        captureDynamicSql(inv, "ENTITY_MANAGER", dynamicResults);
                    }
                }
                continue;
            }

            // I. JPA Criteria API — cb.createQuery(Entity.class) / query.from(Entity.class)
            // The entity class literal goes into the rolling window as __class:EntityFqn.
            if (CRITERIA_ENTITY_METHODS.contains(methodName) && ownerMatchesAny(owner, CRITERIA_OWNERS)) {
                boolean resolvedHere = false;
                if (stringArgs != null && !entityTableMap.isEmpty()) {
                    for (String arg : stringArgs) {
                        if (!arg.startsWith("__class:")) continue;
                        String table = resolveEntityTable(arg.substring(8), entityTableMap);
                        if (table != null) {
                            results.add(new DetectedTable(table, "CRITERIA_API", "READ"));
                            resolvedHere = true;
                            break;
                        }
                    }
                }
                if (!resolvedHere) {
                    captureDynamicClass(inv, "CRITERIA_API", "READ", dynamicResults);
                }
                continue;
            }

            // B2. EntityManager entity methods (find, persist, merge, remove, contains, lock, etc.)
            // "contains" is included: em.contains(entity) → READ, checks persistence-context membership.
            // Owner guard (EM_OWNERS) ensures java.util.Collection.contains() is never matched here.
            if (EM_ENTITY_METHODS.contains(methodName) && ownerMatchesAny(owner, EM_OWNERS)) {
                String opType = switch (methodName) {
                    case "persist" -> "WRITE";
                    case "merge" -> "UPDATE";
                    case "remove" -> "DELETE";
                    default -> "READ";
                };
                // Try to resolve entity class from __class: args
                boolean resolvedHere = false;
                if (stringArgs != null && !entityTableMap.isEmpty()) {
                    for (String arg : stringArgs) {
                        if (arg.startsWith("__class:")) {
                            String entityFqn = arg.substring(8);
                            String table = resolveEntityTable(entityFqn, entityTableMap);
                            if (table != null) {
                                // GAP-JAR-035: set sqlText to descriptive string so audit trail is non-null
                                String entityClassName = shortName(entityFqn);
                                String sqlText = "EntityManager." + methodName + ":" + entityClassName;
                                results.add(new DetectedTable(table, "ENTITY_MANAGER", opType, sqlText));
                                resolvedHere = true;
                                break;
                            }
                        }
                    }
                }
                if (!resolvedHere) {
                    captureDynamicClass(inv, "ENTITY_MANAGER", opType, dynamicResults);
                }
                continue;
            }

            // F0. withCatalogName() — Oracle package name, used as prefix for the next withProcedureName/withFunctionName
            if ("withCatalogName".equals(methodName) && ownerMatchesAny(owner, JDBC_TEMPLATE_OWNERS)) {
                if (stringArgs != null && !stringArgs.isEmpty() && stringArgs.get(0) != null
                        && !stringArgs.get(0).isBlank()) {
                    pendingCatalogPrefix = stringArgs.get(0).trim();
                }
                continue;
            }
            // withSchemaName resets the package context
            if ("withSchemaName".equals(methodName) && ownerMatchesAny(owner, JDBC_TEMPLATE_OWNERS)) {
                pendingCatalogPrefix = null;
                continue;
            }

            // F. SimpleJdbcCall.withProcedureName / withFunctionName (in-place, rare — usually in constructor).
            // Applies any pending Oracle package prefix captured from a preceding withCatalogName() call.
            if (SIMPLE_CALL_PROC_METHODS.contains(methodName) && ownerMatchesAny(owner, JDBC_TEMPLATE_OWNERS)) {
                String source = "withFunctionName".equals(methodName) ? "STORED_FUNCTION" : "STORED_PROCEDURE";
                if (pendingCatalogPrefix != null && stringArgs != null && !stringArgs.isEmpty()) {
                    String procName = stringArgs.get(0);
                    if (procName != null && !procName.isBlank()) {
                        results.add(new DetectedTable(pendingCatalogPrefix + "." + procName.trim(), source, "CALL"));
                    }
                    pendingCatalogPrefix = null;
                } else {
                    extractProcedureFromArgs(stringArgs, source, results);
                }
                continue;
            }

            // F2. SimpleJdbcCall.execute() / executeFunction() / executeScalar() —
            // look up procedure/function name via the field that holds the call object.
            // withProcedureName/withFunctionName was called in the constructor and stored to a field;
            // we resolve the procedure name from cls.simpleJdbcCallProcedures (built during class indexing).
            if (("execute".equals(methodName) || "executeFunction".equals(methodName)
                    || "executeScalar".equals(methodName) || "call".equals(methodName))
                    && ownerMatchesAny(owner, JDBC_TEMPLATE_OWNERS)) {
                String fieldName = inv.receiverFieldName();
                if (fieldName != null && !cls.simpleJdbcCallProcedures.isEmpty()) {
                    String entry = cls.simpleJdbcCallProcedures.get(fieldName);
                    if (entry != null) {
                        int colon = entry.indexOf(':');
                        if (colon > 0) {
                            String kind = entry.substring(0, colon);
                            String procName = entry.substring(colon + 1);
                            String source = "FUNC".equals(kind) ? "STORED_FUNCTION" : "STORED_PROCEDURE";
                            results.add(new DetectedTable(procName, source, "CALL"));
                        }
                    }
                }
                continue;
            }

            // G. Connection.prepareCall("{call PKG.PROC(?)}")
            if (CONNECTION_CALL_METHODS.contains(methodName) && ownerMatchesAny(owner, CONNECTION_OWNERS)) {
                extractProceduresFromSqlArgs(stringArgs, "CALLABLE_STATEMENT", results);
                continue;
            }

            // G1. Connection.prepareStatement(sql) — SQL string arg is the query text (not a callable).
            // GAP-CONN-PREPARE: e.g. conn.prepareStatement("SELECT * FROM users WHERE id=?") → READ/users.
            // Also marks hasJpaInvocations=true so hasDbInteraction is set correctly.
            if (CONNECTION_PREPARE_METHODS.contains(methodName) && ownerMatchesAny(owner, CONNECTION_OWNERS)) {
                int before = results.size();
                extractFromSqlArgs(stringArgs, "PREPARED_STATEMENT", results);
                if (results.size() == before) {
                    captureDynamicSql(inv, "PREPARED_STATEMENT", dynamicResults);
                }
                continue;
            }

            // H. CallableStatement / OracleCallableStatement
            if (CALLABLE_STMT_METHODS.contains(methodName) && ownerMatchesAny(owner, CALLABLE_STMT_OWNERS)) {
                extractProceduresFromSqlArgs(stringArgs, "CALLABLE_STATEMENT", results);
                extractFromSqlArgs(stringArgs, "CALLABLE_STATEMENT", results);
                continue;
            }
        }

        // G2 fallback: if any JDBC/EM method was detected but rolling window missed the SQL
        // (e.g. MapSqlParameterSource built between LDC and query() call consumed the window),
        // scan the method's full stringLiterals for SQL text.
        // G2b: also scan string literals from lambda bodies invoked from this method —
        // handles jdbcTemplate.update(con -> { ps = con.prepareStatement(sql); ... }) patterns.
        boolean hasJdbcInvocation = method.invocations.stream().anyMatch(inv ->
                (JDBC_TEMPLATE_METHODS.contains(inv.methodName()) && ownerMatchesAny(inv.ownerClass(), JDBC_TEMPLATE_OWNERS))
                || (EM_QUERY_METHODS.contains(inv.methodName()) && ownerMatchesAny(inv.ownerClass(), EM_OWNERS)));
        if (hasJdbcInvocation && results.isEmpty()) {
            // Collect candidate string literals: this method + any in-class lambda targets
            List<String> candidates = new ArrayList<>();
            if (method.stringLiterals != null) candidates.addAll(method.stringLiterals);
            for (InvocationRef inv : method.invocations) {
                if (inv.isLambdaInvocation() && inv.lambdaTargetClass() != null
                        && inv.lambdaTargetClass().equals(cls.fqn)
                        && inv.lambdaTargetMethod() != null) {
                    // Find the lambda's IndexedMethod by scanning the class methods map
                    for (IndexedMethod lambdaMethod : cls.methods.values()) {
                        if (lambdaMethod.name.equals(inv.lambdaTargetMethod())
                                && lambdaMethod.stringLiterals != null) {
                            candidates.addAll(lambdaMethod.stringLiterals);
                            break;
                        }
                    }
                }
            }
            for (String lit : candidates) {
                if (looksLikeSql(lit)) {
                    extractFromSql(lit, "JDBC_TEMPLATE", results);
                }
            }
        }

        // C. @Query annotation on the method itself
        for (AnnotationData ad : method.annotationDetailList) {
            if ("Query".equals(ad.name())) {
                Object val = ad.attributes().get("value");
                if (val instanceof String sql && !sql.isBlank()) {
                    boolean isNative = Boolean.TRUE.equals(ad.attributes().get("nativeQuery"));
                    if (isNative) {
                        extractFromSql(sql, "JPA_NATIVE_QUERY", results);
                    } else {
                        // JPQL: entity names, not table names — resolve via entityTableMap
                        extractJpqlEntities(sql, entityTableMap, results);
                    }
                }
            }
            if ("NamedQuery".equals(ad.name()) || "NamedNativeQuery".equals(ad.name())) {
                Object val = ad.attributes().get("query");
                if (val instanceof String sql && !sql.isBlank()) {
                    extractFromSql(sql, "NAMED_QUERY", results);
                }
            }

            // C2. @Procedure annotation (Spring Data JPA)
            if ("Procedure".equals(ad.name())) {
                Object procName = ad.attributes().get("name");
                if (procName == null) procName = ad.attributes().get("procedureName");
                if (procName == null) procName = ad.attributes().get("value");
                if (procName instanceof String s && !s.isBlank()) {
                    results.add(new DetectedTable(s, "JPA_PROCEDURE_ANNOTATION", "CALL"));
                }
            }

            // C3. Hibernate @SQLInsert, @SQLUpdate, @SQLDelete annotations
            if ("SQLInsert".equals(ad.name()) || "SQLUpdate".equals(ad.name()) || "SQLDelete".equals(ad.name())) {
                Object val = ad.attributes().get("sql");
                if (val instanceof String sql && !sql.isBlank()) {
                    extractFromSql(sql, "HIBERNATE_SQL_ANNOTATION", results);
                }
            }
        }

        // D. Spring Data derived query methods (findByXxx → READ, deleteByXxx → DELETE)
        if ("REPOSITORY".equals(cls.stereotype)) {
            String derivedOp = inferDerivedQueryOp(method.name);
            if (derivedOp != null && !entityTableMap.isEmpty()) {
                String table = resolveRepoEntityTable(cls, entityTableMap);
                if (table != null) {
                    results.add(new DetectedTable(table, "DERIVED_QUERY", derivedOp));
                }
            }
        }

        // E. Built-in JpaRepository/CrudRepository methods
        if ("REPOSITORY".equals(cls.stereotype)) {
            String repoOp = REPO_METHOD_OPS.get(method.name);
            if (repoOp != null && !entityTableMap.isEmpty()) {
                String table = resolveRepoEntityTable(cls, entityTableMap);
                if (table != null) {
                    results.add(new DetectedTable(table, "JPA_REPOSITORY", repoOp));
                }
            }
        }

        // Deduplicate
        Map<String, DetectedTable> deduped = new LinkedHashMap<>();
        for (DetectedTable dt : results) {
            deduped.putIfAbsent(dt.tableName(), dt);
        }
        return new DetectionResult(new ArrayList<>(deduped.values()), dynamicResults);
    }

    /**
     * CALL-009: stage a dynamic (unresolved-at-this-call-site) SQL-string reference for
     * CollectionResolver's bounded ancestor walk. varKind is deliberately "SQL_PARAM"/
     * "SQL_LOCAL"/etc. (prefixed, NOT the bare "PARAM"/"LOCAL_VAR"/"CONCAT_*" kind BytecodeClassParser
     * captured) so CollectionResolver's Mongo-specific tagGeneralParamCallSite (which matches bare
     * "PARAM"/"PARAM_CLASS") never mistakes a JDBC SQL-string reference for a Mongo collection-name
     * one — the two mechanisms must resolve completely different things at the final step (a table
     * name parsed out of resolved SQL text vs. a collection name used as-is).
     */
    private void captureDynamicSql(InvocationRef inv, String source,
                                    List<CallGraphIndex.DetectedDynamicCollection> dynamicResults) {
        List<MethodCallInfo.DynamicArgSource> dyns = inv.dynamicArgSources();
        if (dyns == null || dyns.isEmpty()) return;
        MethodCallInfo.DynamicArgSource dyn = dyns.get(0);
        dynamicResults.add(new CallGraphIndex.DetectedDynamicCollection(
                dyn.name(), "SQL_" + dyn.kind(), dyn.ownerClass(), source + "_SQL_DYNAMIC", source, null));
    }

    /** CALL-009: mirrors captureDynamicSql, for entity-Class&lt;?&gt;-typed dynamic references. */
    private void captureDynamicClass(InvocationRef inv, String source, String opType,
                                      List<CallGraphIndex.DetectedDynamicCollection> dynamicResults) {
        List<MethodCallInfo.DynamicArgSource> dyns = inv.dynamicArgSources();
        if (dyns == null) return;
        for (MethodCallInfo.DynamicArgSource dyn : dyns) {
            if ("PARAM_CLASS".equals(dyn.kind()) || "LOCAL_VAR_CLASS".equals(dyn.kind())) {
                dynamicResults.add(new CallGraphIndex.DetectedDynamicCollection(
                        dyn.name(), "SQL_" + dyn.kind(), dyn.ownerClass(), source + "_CLASS_DYNAMIC", source, opType));
                return;
            }
        }
    }

    /** Check if this method invokes any JPA/JDBC methods (for hasDbInteraction). */
    boolean hasJpaInvocations(IndexedMethod method) {
        for (InvocationRef inv : method.invocations) {
            String owner = inv.ownerClass();
            String mn = inv.methodName();
            if (JDBC_TEMPLATE_METHODS.contains(mn) && ownerMatchesAny(owner, JDBC_TEMPLATE_OWNERS)) return true;
            if (EM_QUERY_METHODS.contains(mn) && ownerMatchesAny(owner, EM_OWNERS)) return true;
            if (EM_ENTITY_METHODS.contains(mn) && ownerMatchesAny(owner, EM_OWNERS)) return true;
            if (SIMPLE_CALL_PROC_METHODS.contains(mn) && ownerMatchesAny(owner, JDBC_TEMPLATE_OWNERS)) return true;
            if (CONNECTION_CALL_METHODS.contains(mn) && ownerMatchesAny(owner, CONNECTION_OWNERS)) return true;
            // GAP-CONN-PREPARE: Connection.prepareStatement(sql) must mark hasDbInteraction=true.
            if (CONNECTION_PREPARE_METHODS.contains(mn) && ownerMatchesAny(owner, CONNECTION_OWNERS)) return true;
            if (CALLABLE_STMT_METHODS.contains(mn) && ownerMatchesAny(owner, CALLABLE_STMT_OWNERS)) return true;
            if (CRITERIA_ENTITY_METHODS.contains(mn) && ownerMatchesAny(owner, CRITERIA_OWNERS)) return true;
        }
        return false;
    }

    private void extractFromSqlArgs(List<String> stringArgs, String source, List<DetectedTable> results) {
        if (stringArgs == null) return;
        for (String arg : stringArgs) {
            if (arg.startsWith("__class:")) continue;
            if (looksLikeSql(arg)) {
                extractFromSql(arg, source, results);
            }
        }
    }

    private void extractProcedureFromArgs(List<String> stringArgs, String source, List<DetectedTable> results) {
        if (stringArgs == null) return;
        for (String arg : stringArgs) {
            if (arg.startsWith("__class:")) continue;
            String trimmed = arg.trim();
            if (!trimmed.isEmpty() && trimmed.matches("[A-Za-z_][A-Za-z0-9_.]*")) {
                results.add(new DetectedTable(trimmed, source, "CALL"));
            }
        }
    }

    private void extractProceduresFromSqlArgs(List<String> stringArgs, String source, List<DetectedTable> results) {
        if (stringArgs == null) return;
        for (String arg : stringArgs) {
            if (arg.startsWith("__class:")) continue;
            Set<String> procs = SqlStatementParser.extractProcedureNames(arg);
            if (!procs.isEmpty()) {
                for (String proc : procs) {
                    results.add(new DetectedTable(proc, source, "CALL"));
                }
            } else if (looksLikeSql(arg)) {
                extractFromSql(arg, source, results);
            }
        }
    }

    private void extractFromSql(String sql, String source, List<DetectedTable> results) {
        Set<String> tables = SqlStatementParser.extractTableNames(sql);
        String opType = SqlStatementParser.inferOperationType(sql);
        String trimmedSql = sql.length() > 2000 ? sql.substring(0, 2000) + "..." : sql;
        // GAP-JAR-036: filter out "?" placeholder table names that pollute the collections list.
        // SqlStatementParser may return "?" when it cannot resolve a bind-variable table reference.
        Set<String> cleanTables = new java.util.LinkedHashSet<>();
        for (String t : tables) {
            if (t != null && !t.isBlank() && !"?".equals(t)) {
                cleanTables.add(t);
            }
        }
        if (cleanTables.isEmpty() && looksLikeSql(sql)) {
            // SQL was detected but no table names extracted — skip rather than record "?" placeholder.
            // Recording "?" pollutes the detected-collections list (GAP-JAR-036).
        } else {
            for (String table : cleanTables) {
                results.add(new DetectedTable(table, source, opType != null ? opType : "READ", trimmedSql));
            }
        }
    }

    private void extractJpqlEntities(String jpql, Map<String, String> entityTableMap, List<DetectedTable> results) {
        // Task #30(d): every JPQL/HQL string goes through the documented regex-last-resort path —
        // there is no grammar — so count it here. See JPQL_HQL_REGEX_FALLBACKS.
        JPQL_HQL_REGEX_FALLBACKS.incrementAndGet();
        Set<String> names = SqlStatementParser.extractTableNames(jpql);
        String opType = SqlStatementParser.inferOperationType(jpql);
        String trimmedJpql = jpql.length() > 2000 ? jpql.substring(0, 2000) + "..." : jpql;
        for (String name : names) {
            String table = resolveEntityTable(name, entityTableMap);
            if (table != null) {
                results.add(new DetectedTable(table, "JPQL_QUERY", opType != null ? opType : "READ", trimmedJpql));
            } else {
                results.add(new DetectedTable(name, "JPQL_QUERY", opType != null ? opType : "READ", trimmedJpql));
            }
        }
    }

    private String resolveEntityTable(String entityRef, Map<String, String> entityTableMap) {
        if (entityRef == null) return null;
        String table = entityTableMap.get(entityRef);
        if (table != null) return table;
        int dot = entityRef.lastIndexOf('.');
        if (dot > 0) table = entityTableMap.get(entityRef.substring(dot + 1));
        return table;
    }

    private String resolveRepoEntityTable(IndexedClass cls, Map<String, String> entityTableMap) {
        if (cls.repositoryEntityType != null) {
            String table = resolveEntityTable(cls.repositoryEntityType, entityTableMap);
            if (table != null) return table;
        }
        return null;
    }

    private String inferDerivedQueryOp(String methodName) {
        if (methodName.startsWith("find") || methodName.startsWith("get")
                || methodName.startsWith("read") || methodName.startsWith("query")
                || methodName.startsWith("search") || methodName.startsWith("stream")
                || methodName.startsWith("exists") || methodName.startsWith("count")) {
            return "READ";
        }
        if (methodName.startsWith("delete") || methodName.startsWith("remove")) {
            return "DELETE";
        }
        if (methodName.startsWith("update") || methodName.startsWith("modify") || methodName.startsWith("set")) {
            return "UPDATE";
        }
        if (methodName.startsWith("save") || methodName.startsWith("insert") || methodName.startsWith("persist")) {
            return "WRITE";
        }
        return null;
    }

    private boolean looksLikeSql(String s) {
        if (s == null || s.length() < 6) return false;
        String upper = s.trim().toUpperCase();
        return upper.startsWith("SELECT ") || upper.startsWith("INSERT ")
                || upper.startsWith("UPDATE ") || upper.startsWith("DELETE ")
                || upper.startsWith("MERGE ") || upper.startsWith("WITH ")
                || upper.startsWith("CALL ") || upper.startsWith("EXEC ")
                || upper.startsWith("TRUNCATE ") || upper.startsWith("{CALL ")
                || upper.startsWith("{?") || upper.startsWith("BEGIN ")
                // JPQL allows FROM-shorthand queries (no leading SELECT): "FROM Entity e WHERE ..."
                || upper.startsWith("FROM ");
    }

    private boolean ownerMatchesAny(String owner, List<String> patterns) {
        if (owner == null) return false;
        for (String pattern : patterns) {
            if (owner.endsWith(pattern) || owner.endsWith("." + pattern)) return true;
        }
        return false;
    }

    /**
     * Returns the simple (unqualified) class name from a fully-qualified class name.
     * E.g. "com.example.Order" → "Order", "Order" → "Order".
     * GAP-JAR-035: used to build descriptive sqlText for em.persist/merge/remove/find.
     */
    private String shortName(String fqn) {
        if (fqn == null || fqn.isBlank()) return fqn;
        int dot = fqn.lastIndexOf('.');
        return dot >= 0 ? fqn.substring(dot + 1) : fqn;
    }
}
