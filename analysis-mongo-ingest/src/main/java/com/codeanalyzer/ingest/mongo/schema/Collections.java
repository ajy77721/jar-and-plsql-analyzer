package com.codeanalyzer.ingest.mongo.schema;

/**
 * Collection names. One place; no string literals anywhere else.
 *
 * <h2>Why every name is prefixed {@code am_}</h2>
 *
 * <p>{@code MongoAnalysisRepository.java:123} already owns a collection literally named
 * {@code analysis_run}, with a different shape and a different lifecycle. The data-model plan had
 * independently assigned that same name to this module's run spine. Two writers with incompatible
 * schemas on one collection is a data-corruption path, not a naming preference, and it would have
 * surfaced as unexplained documents rather than as an error.
 *
 * <p>The prefix also makes ownership legible in {@code show collections}: everything this module
 * writes is grouped, and nothing it writes can be confused with the existing store's.
 */
public final class Collections {

    public static final String RUN                = "am_run";
    // ── JAR side ─────────────────────────────────────────────────────────────
    public static final String DAG_NODE           = "am_dag_node";
    public static final String DAG_EDGE           = "am_dag_edge";
    public static final String ENDPOINT           = "am_endpoint";
    public static final String ENDPOINT_POOL      = "am_endpoint_pool";
    /**
     * The endpoint-to-pool junction (ING-112).
     *
     * <p>Endpoints reference the three shared pools by <b>position</b> —
     * {@code externalCallsRefs: [0,3,7]}. Splitter stored each position as a bare integer with no
     * reference to the pool document, so "which endpoints use this bean" required the caller to
     * rebuild {@code am_endpoint_pool~<jar>~beans~7} by string concatenation, and
     * {@code OrphanRefChecker} had no {@code *Ref} field to check. This collection stores the join
     * with both sides resolved.
     */
    public static final String ENDPOINT_POOL_LINK = "am_endpoint_pool_link";
    /**
     * A JAR-level fact from {@code analysis.json}'s {@code advice} array (ING-117).
     *
     * <p>Two rows in the corpus measured, one per JAR run — a servlet filter and a handler
     * interceptor found registered (or not) outside the endpoint call graph. Previously read by
     * neither JAR mapper: {@code DagAndEndpointMapper}'s field switch fell through to
     * {@code default -> p.skipChildren()}, and {@code AnalysisJsonMapper} never named the field
     * either, so {@code advice} was silently dropped on every ingest.
     */
    public static final String ANALYSIS_ADVICE     = "am_analysis_advice";

    /**
     * A JAR the analysed application bundles — ING-116. See {@code BundledJarMapper} for why this
     * was never ingested until now, and for the measured split.
     */
    public static final String BUNDLED_JAR          = "am_bundled_jar";
    /** One resource file from a {@link #BUNDLED_JAR}'s {@code resources} map, split by position. */
    public static final String BUNDLED_JAR_RESOURCE = "am_bundled_jar_resource";
    /**
     * One (jar, class) edge from {@code jar-dep-map.json} — ING-117. Not embedded in
     * {@link #BUNDLED_JAR} as {@code RELATIONAL-MODEL-PLAN.md} recommends: see
     * {@code JarDependencyMapper} for why the invariant wins over that recommendation here, as it
     * already did for {@link #BUNDLED_JAR_RESOURCE}.
     */
    public static final String JAR_DEPENDENCY       = "am_jar_dependency";
    public static final String JAVA_CLASS         = "am_java_class";
    public static final String JAVA_METHOD        = "am_java_method";
    public static final String JAVA_FIELD         = "am_java_field";
    public static final String INVOCATION         = "am_invocation";
    public static final String METHOD_INVOCATION  = "am_method_invocation";
    public static final String CALL_EDGE          = "am_call_edge";
    public static final String CALL_EDGE_UNIQUE   = "am_call_edge_unique";
    public static final String STATEMENT          = "am_statement";
    public static final String SQL_TEXT           = "am_sql_text";
    public static final String PROC_NODE          = "am_proc_node";
    /**
     * The cursor-to-object junction (ING-014).
     *
     * <p>A cursor's declared tables were stored by Splitter as {@code {value: "ERP_CUSTOMER"}} —
     * a name with nothing to resolve it, so "which cursors read this table" had no reference to
     * match. These rows carry {@code objectRef}, nullable so an unresolvable bare name is
     * preserved rather than attached to a guessed schema.
     */
    public static final String CURSOR_OBJECT     = "am_cursor_object";
    public static final String CURSOR_DECL        = "am_cursor_decl";
    public static final String DYNAMIC_SQL        = "am_dynamic_sql";
    public static final String EXCEPTION_HANDLER  = "am_exception_handler";
    public static final String TXN_STATEMENT      = "am_txn_statement";

    /**
     * Every statement the parser saw, from {@code chunks/chunk_*.json}.
     *
     * <p>Distinct from {@link #STATEMENT}, which is {@code api/statements/index.json} — the
     * table-touching subset. Measured on ORACLE: 3,832 chunk statements against 2,436 api rows, and
     * eight fields the api index does not carry ({@code type}, {@code nestingDepth},
     * {@code lineEnd}, {@code dmlType}, {@code boundsExpression}, {@code indexVariable},
     * {@code savepointName}, {@code tables}). Control flow — {@code IF}, {@code FOR_LOOP},
     * {@code CASE}, {@code ASSIGNMENT} — appears only here.
     */
    public static final String CHUNK_STATEMENT   = "am_chunk_statement";

    /**
     * Precomputed facet counts: one row per (run, collection, field, value).
     *
     * <p>The facet panel asks "how many statements per operation, per schema, per table" every time
     * it opens. Answering with {@code $group} means touching every statement row — fine at 2,436,
     * not fine at the 1,000,000 this model is sized for. The counts are derived once at ingest,
     * when the rows are already being walked, and read back through one index.
     */
    public static final String FACET_COUNT       = "am_facet_count";

    /**
     * One row per (endpoint, collection accessed) — ING-113.
     *
     * <p>Replaces three parallel structures on the endpoint document: {@code aggregatedCollections}
     * (an array of names) and {@code aggregatedCollectionDomains} / {@code aggregatedOperations}
     * (maps keyed by those same names). They describe one relation and are joined here.
     */
    public static final String ENDPOINT_COLLECTION = "am_endpoint_collection";

    /**
     * Which call-tree nodes an endpoint can reach — ING-114.
     *
     * <p>The transitive closure, stored once, because computing it on demand is not an option:
     * expanded naively the largest endpoint is 20,300,167,401 nodes and the whole set is
     * 466,903,865,409. Collapsed by a visited set it is 57,700 rows.
     */
    public static final String ENDPOINT_REACHABLE = "am_endpoint_reachable";

    /**
     * One row per (method, declared local variable) — the ING-104 half that was missed.
     *
     * <p>{@code localVariables} arrives as an object keyed by variable NAME, so {@code Splitter},
     * which splits arrays, walked straight past it and the map was stored inline. Measured: 2,857
     * methods, 11,996 variable keys, the widest single method declaring 99.
     */
    public static final String JAVA_METHOD_LOCAL_VAR = "am_java_method_local_var";

    /**
     * One row per (call-tree node, collection it accesses).
     *
     * <p>Joins three parallel maps on a DAG node — {@code collectionDomains},
     * {@code collectionSources} and {@code collectionVerification} — all keyed by collection name.
     * The same shape as the endpoint case in ING-113, on 19 nodes, and 4 of the names contain a
     * dot, so as field names they could not be queried at all.
     */
    public static final String DAG_NODE_COLLECTION = "am_dag_node_collection";
    /**
     * The sequence as an entity (ING-017).
     *
     * <p>{@code am_sequence_use} carried {@code schemaName} and {@code sequenceName} as bare
     * strings: nothing to point at, and so nothing for {@code OrphanRefChecker} to check. "Which
     * sequences does this system have" was a {@code distinct} over use rows, and a sequence had
     * nowhere to hang anything that is true of the sequence rather than of one use of it.
     */
    public static final String SEQUENCE_OBJ       = "am_sequence_obj";
    public static final String SEQUENCE_USE       = "am_sequence_use";
    public static final String VARIABLE_TYPE_REF  = "am_variable_type_ref";
    public static final String NODE_TABLE_ACCESS  = "am_node_table_access";
    public static final String SUBTREE_TABLE      = "am_subtree_table";
    public static final String DB_OBJECT          = "am_db_object";
    public static final String TABLE_COLUMN_USAGE = "am_table_column_usage";
    /**
     * One row per cross-package global-variable reference (PKG.VAR), with the
     * referencing node, the source line, and READ/WRITE/UNKNOWN access type.
     * Populated from {@code externalPackageVarRefDetails} on {@code api/nodes/*.json}.
     */
    public static final String PACKAGE_VAR_REF    = "am_package_var_ref";
    /**
     * One row per (node, statement type) — ING-118.
     *
     * <p>{@code statementSummary} on {@code api/nodes/*.json} arrives as an object keyed by
     * statement TYPE, so {@code Splitter} — which splits arrays — walked past it and stored the map
     * inline. The key set is not a closed enum: PLAN-11 §3 records 11–28 dialect-specific tokens,
     * {@code FlowNodeRow} measured 43 on the stored corpus, and the
     * DB2 entry point {@code ERP_USP_RUN_CYCLE} reached <b>41 on one node</b> — past
     * {@link com.codeanalyzer.ingest.mongo.map.Splitter#MAX_SCHEMA_KEYS}, so the data-keyed-map
     * gate refused the document and took the whole entity-ingest of that run with it (13 nodes
     * written instead of 63).
     *
     * <p>Rows rather than an exemption, because the gate is right about the shape even though it
     * was wrong about the harm here: as field names the types cannot be indexed, so "which nodes
     * contain a MERGE" is a collection scan; as values it is one seek. The same remedy the gate's
     * own message names, and the same one {@code FlowNodeRow.StatementCount} already models in
     * the storage layer.
     */
    public static final String NODE_STATEMENT_SUMMARY = "am_node_statement_summary";
    /**
     * One row per declared FOREIGN KEY relationship, from {@code foreignKeys} on
     * {@code api/nodes/*.json}: the constraint name, the child table and its columns, the
     * parent table and its columns, and the source line.
     *
     * <p>Its own collection rather than another {@code am_node_table_access} row because a
     * foreign key is a different kind of fact. Every other table relationship here is
     * <em>inferred</em> from how DML is written; this one is the schema stating the relationship
     * outright, and it carries the column pairing that makes it checkable. Flattened into two
     * anonymous table-ops, both the certainty and the columns are lost.
     *
     * <p>{@code objectRef} / {@code referencedObjectRef} resolve each side to {@code am_db_object}
     * where the catalogue knows it, so "what references this table" is one indexed lookup in
     * either direction. Both are nullable by the same rule the cursor rows follow: a name with no
     * known schema keeps its name rather than being attached to a guessed one.
     */
    public static final String FOREIGN_KEY        = "am_foreign_key";

    private Collections() {}
}
