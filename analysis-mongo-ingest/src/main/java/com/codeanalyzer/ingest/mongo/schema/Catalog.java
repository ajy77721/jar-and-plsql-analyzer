package com.codeanalyzer.ingest.mongo.schema;

import org.bson.Document;

import java.util.List;

/**
 * The collections this module owns, with their validators and indexes.
 *
 * <p>Indexes exist for two distinct reasons and both are represented: to enforce a constraint the
 * database cannot otherwise express (unique, partial-unique), and to make a declared relationship
 * traversable in one seek in <em>both</em> directions. Every {@code Ref} field has an index —
 * without it, "the children of X" degrades to a collection scan and the relation chain is only
 * nominally usable.
 */
public final class Catalog {

    private Catalog() {}

    public static List<CollectionSpec> all() {
        List<CollectionSpec> out = new java.util.ArrayList<>(
                List.of(run(), dbObject(), tableColumnUsage(), procNode(),
                        statement(), sqlText(), callEdge(), callEdgeUnique(),
                        javaClass(), javaMethod(), javaField(), invocation(), methodInvocation(),
                        dagNode(), dagEdge(), endpoint(), endpointPool(), endpointPoolLink(), analysisAdvice(), bundledJar(), bundledJarResource(), jarDependency(), runMember()));
        // Node-scoped children all share one shape: a runId, a nodeRef, and their own payload.
        // Declaring them from a list rather than by hand keeps their indexes identical — a child
        // whose nodeRef is unindexed turns "the cursors of this node" into a collection scan, and
        // that is the kind of omission a hand-written list acquires one entry at a time.
        for (String c : List.of(Collections.CURSOR_DECL, Collections.DYNAMIC_SQL,
                Collections.EXCEPTION_HANDLER, Collections.TXN_STATEMENT,
                Collections.VARIABLE_TYPE_REF, Collections.SEQUENCE_USE,
                Collections.NODE_TABLE_ACCESS, Collections.SUBTREE_TABLE,
                Collections.CHUNK_STATEMENT,
                // PACKAGE_VAR_REF was written by NodeDetailMapper but never declared here, so it
                // was created implicitly on first insert: no validator, and no index on nodeRef —
                // exactly the "children of this node" collection scan this list exists to prevent.
                Collections.PACKAGE_VAR_REF)) {
            out.add(nodeChild(c));
        }
        out.add(foreignKey());
        out.add(sequenceObj());
        out.add(cursorObject());
        out.add(facetCount());
        out.add(endpointCollection());
        out.add(endpointReachable());
        out.add(javaMethodLocalVar());
        out.add(dagNodeCollection());
        out.add(nodeStatementSummary());
        return List.copyOf(out);
    }

    /**
     * Facet counts. Indexed on the three fields a read filters by, in that order, so the panel's
     * query is a single index scan rather than a collection scan plus a sort.
     */
    /** Both directions: an endpoint's collections, and a collection's endpoints. */
    private static CollectionSpec endpointCollection() {
        return new CollectionSpec(Collections.ENDPOINT_COLLECTION, null, List.of(
                IndexSpec.of("ix_epc_endpoint", new Document("endpointRef", 1).append("ordinal", 1)),
                IndexSpec.of("ix_epc_collection", new Document("collectionName", 1)),
                IndexSpec.of("ix_epc_run", new Document("runId", 1))));
    }

    /** Both directions again: what an endpoint reaches, and what reaches a node. */
    private static CollectionSpec endpointReachable() {
        return new CollectionSpec(Collections.ENDPOINT_REACHABLE, null, List.of(
                IndexSpec.of("ix_reach_endpoint", new Document("endpointRef", 1).append("depth", 1)),
                IndexSpec.of("ix_reach_node", new Document("nodeRef", 1)),
                IndexSpec.of("ix_reach_run", new Document("runId", 1))));
    }

    /** Both directions: a method's variables, and which methods declare a given name. */
    private static CollectionSpec javaMethodLocalVar() {
        return new CollectionSpec(Collections.JAVA_METHOD_LOCAL_VAR, null, List.of(
                IndexSpec.of("ix_jmlv_method", new Document("methodRef", 1).append("ordinal", 1)),
                IndexSpec.of("ix_jmlv_name", new Document("variableName", 1)),
                IndexSpec.of("ix_jmlv_run", new Document("runId", 1))));
    }

    private static CollectionSpec dagNodeCollection() {
        return new CollectionSpec(Collections.DAG_NODE_COLLECTION, null, List.of(
                IndexSpec.of("ix_dnc_node", new Document("nodeRef", 1).append("ordinal", 1)),
                IndexSpec.of("ix_dnc_collection", new Document("collectionName", 1)),
                IndexSpec.of("ix_dnc_run", new Document("runId", 1))));
    }

    /**
     * Both directions, which is the reason the map became rows: "the statement mix of this node",
     * and "which nodes contain a MERGE" — the second is unanswerable while the type is a field
     * name.
     */
    private static CollectionSpec nodeStatementSummary() {
        return new CollectionSpec(Collections.NODE_STATEMENT_SUMMARY, Validators.nodeChild(),
                List.of(
                        IndexSpec.of("ix_nss_node",
                                new Document("nodeRef", 1).append("ordinal", 1)),
                        IndexSpec.of("ix_nss_type", new Document("statementType", 1)),
                        IndexSpec.of("ix_nss_run", new Document("runId", 1))));
    }

    private static CollectionSpec facetCount() {
        return new CollectionSpec(Collections.FACET_COUNT, null, List.of(
                IndexSpec.of("ix_facet_lookup", new Document("runId", 1)
                        .append("entityCollection", 1).append("field", 1).append("count", -1))));
    }

    private static CollectionSpec procNode() {
        return new CollectionSpec(Collections.PROC_NODE, Validators.procNode(), List.of(
                IndexSpec.of("ix_node_run", new Document("runId", 1)),
                IndexSpec.unique("uk_node_natural",
                        new Document("runId", 1).append("nodeId", 1)),
                IndexSpec.of("ix_node_name", new Document("runId", 1).append("name", 1))));
    }

    private static CollectionSpec statement() {
        return new CollectionSpec(Collections.STATEMENT, Validators.statement(), List.of(
                IndexSpec.of("ix_stmt_run", new Document("runId", 1)),
                IndexSpec.of("ix_stmt_node", new Document("nodeRef", 1)),
                // The hot query the UI actually makes: a table's accesses, filtered by operation.
                // Compound and in this order because objectRef is the equality the screen always
                // supplies, operation is the optional pill filter, and line is the sort — the
                // order that lets one index serve the filter and the sort together.
                IndexSpec.of("ix_stmt_hot",
                        new Document("objectRef", 1).append("operation", 1).append("line", 1)),
                IndexSpec.unique("uk_stmt_natural", new Document("entityKey", 1))));
    }

    private static CollectionSpec sqlText() {
        return new CollectionSpec(Collections.SQL_TEXT, Validators.sqlText(), List.of(
                IndexSpec.unique("uk_sqltext", new Document("entityKey", 1))));
    }

    private static CollectionSpec callEdge() {
        return new CollectionSpec(Collections.CALL_EDGE, Validators.callEdge(), List.of(
                IndexSpec.of("ix_edge_run", new Document("runId", 1)),
                // Both directions are indexed because both are asked: "what does this call" and
                // "who calls this". An edge collection with only one is half a graph.
                IndexSpec.of("ix_edge_from", new Document("fromRef", 1)),
                IndexSpec.of("ix_edge_to", new Document("toRef", 1)),
                IndexSpec.unique("uk_edge_natural", new Document("entityKey", 1))));
    }

    private static CollectionSpec callEdgeUnique() {
        return new CollectionSpec(Collections.CALL_EDGE_UNIQUE, Validators.callEdge(), List.of(
                IndexSpec.of("ix_uedge_run", new Document("runId", 1)),
                IndexSpec.of("ix_uedge_from", new Document("fromRef", 1)),
                IndexSpec.of("ix_uedge_to", new Document("toRef", 1)),
                IndexSpec.unique("uk_uedge_natural", new Document("entityKey", 1))));
    }

    private static CollectionSpec javaClass() {
        return new CollectionSpec(Collections.JAVA_CLASS, Validators.javaEntity(), List.of(
                IndexSpec.of("ix_class_run", new Document("runId", 1)),
                IndexSpec.of("ix_class_pkg", new Document("packageName", 1)),
                IndexSpec.unique("uk_class_natural", new Document("entityKey", 1))));
    }

    private static CollectionSpec javaMethod() {
        return new CollectionSpec(Collections.JAVA_METHOD, Validators.javaEntity(), List.of(
                // The parent link. Without it "the methods of this class" is a scan of 34 million
                // rows at the target scale — the single most important index on the JAR side.
                IndexSpec.of("ix_method_class", new Document("classRef", 1)),
                IndexSpec.of("ix_method_name", new Document("name", 1)),
                IndexSpec.unique("uk_method_natural", new Document("entityKey", 1))));
    }

    private static CollectionSpec javaField() {
        return new CollectionSpec(Collections.JAVA_FIELD, Validators.javaEntity(), List.of(
                IndexSpec.of("ix_field_class", new Document("classRef", 1)),
                IndexSpec.unique("uk_field_natural", new Document("entityKey", 1))));
    }

    private static CollectionSpec invocation() {
        return new CollectionSpec(Collections.INVOCATION, Validators.javaEntity(), List.of(
                IndexSpec.of("ix_inv_run", new Document("runId", 1)),
                IndexSpec.unique("uk_inv_natural", new Document("entityKey", 1))));
    }

    private static CollectionSpec methodInvocation() {
        return new CollectionSpec(Collections.METHOD_INVOCATION, Validators.javaEntity(), List.of(
                // Both directions: "what does this method call" and "who calls this target".
                IndexSpec.of("ix_mi_method", new Document("methodRef", 1)),
                IndexSpec.of("ix_mi_target", new Document("invocationRef", 1)),
                IndexSpec.unique("uk_mi_natural", new Document("entityKey", 1))));
    }

    private static CollectionSpec dagNode() {
        return new CollectionSpec(Collections.DAG_NODE, Validators.javaEntity(), List.of(
                IndexSpec.of("ix_dag_run", new Document("runId", 1)),
                IndexSpec.of("ix_dag_method", new Document("methodRef", 1)),
                IndexSpec.unique("uk_dag_natural", new Document("entityKey", 1))));
    }

    private static CollectionSpec dagEdge() {
        return new CollectionSpec(Collections.DAG_EDGE, Validators.javaEntity(), List.of(
                // Traversal down (children of a node) and up (parents of a node). A tree walked
                // only downwards cannot answer "how is this reached", which is the question the
                // call-tree screen exists for.
                IndexSpec.of("ix_dagedge_parent", new Document("parentRef", 1).append("ordinal", 1)),
                IndexSpec.of("ix_dagedge_child", new Document("childRef", 1)),
                IndexSpec.unique("uk_dagedge_natural", new Document("entityKey", 1))));
    }

    private static CollectionSpec endpoint() {
        return new CollectionSpec(Collections.ENDPOINT, Validators.javaEntity(), List.of(
                IndexSpec.of("ix_ep_run", new Document("runId", 1)),
                IndexSpec.of("ix_ep_root", new Document("rootRef", 1)),
                IndexSpec.unique("uk_ep_natural", new Document("entityKey", 1))));
    }

    private static CollectionSpec endpointPool() {
        return new CollectionSpec(Collections.ENDPOINT_POOL, Validators.javaEntity(), List.of(
                IndexSpec.of("ix_pool_run", new Document("runId", 1).append("pool", 1)),
                IndexSpec.unique("uk_pool_natural", new Document("entityKey", 1))));
    }

    private static CollectionSpec cursorObject() {
        return new CollectionSpec(Collections.CURSOR_OBJECT, Validators.nodeChild(), List.of(
                // Both directions: "the tables this cursor reads" and "the cursors that read this
                // table". The second is the one the bare names could not answer.
                IndexSpec.of("ix_curobj_cursor", new Document("cursorRef", 1)),
                IndexSpec.of("ix_curobj_object", new Document("objectRef", 1)),
                IndexSpec.of("ix_curobj_run", new Document("runId", 1)),
                IndexSpec.unique("uk_curobj_natural", new Document("key", 1))));
    }

    private static CollectionSpec sequenceObj() {
        return new CollectionSpec(Collections.SEQUENCE_OBJ, Validators.nodeChild(), List.of(
                IndexSpec.of("ix_seqobj_run", new Document("runId", 1)),
                IndexSpec.unique("uk_seqobj_natural", new Document("key", 1))));
    }

    private static CollectionSpec bundledJar() {
        return new CollectionSpec(Collections.BUNDLED_JAR, Validators.javaEntity(), List.of(
                IndexSpec.of("ix_bjar_run", new Document("runId", 1)),
                IndexSpec.unique("uk_bjar_natural", new Document("entityKey", 1))));
    }

    private static CollectionSpec bundledJarResource() {
        return new CollectionSpec(Collections.BUNDLED_JAR_RESOURCE, Validators.nodeChild(), List.of(
                IndexSpec.of("ix_bjres_parent", new Document("parentRef", 1)),
                IndexSpec.of("ix_bjres_run", new Document("runId", 1)),
                IndexSpec.unique("uk_bjres_natural", new Document("key", 1))));
    }

    private static CollectionSpec jarDependency() {
        return new CollectionSpec(Collections.JAR_DEPENDENCY, Validators.nodeChild(), List.of(
                IndexSpec.of("ix_jdep_parent", new Document("parentRef", 1)),
                IndexSpec.of("ix_jdep_run", new Document("runId", 1)),
                IndexSpec.unique("uk_jdep_natural", new Document("key", 1))));
    }

    /**
     * VER-007's own finding: this collection had no spec at all before it, so it was created
     * implicitly on first insert with no indexes — "which entities does run X touch" and "which
     * runs has entity Y appeared in", the two questions {@code am_run_member} exists to answer,
     * were both collection scans over 14,810+ rows.
     */
    private static CollectionSpec runMember() {
        return new CollectionSpec(com.codeanalyzer.ingest.mongo.model.RunMember.COLLECTION,
                Validators.runMember(), List.of(
                IndexSpec.of("ix_rm_run", new Document("runRef", 1)),
                IndexSpec.of("ix_rm_entity", new Document("entityRef", 1))));
        // _id is already runRef~entityRef (RunMember.of), which is the pair's natural uniqueness —
        // no separate unique index is needed for that.
    }

    private static CollectionSpec analysisAdvice() {
        return new CollectionSpec(Collections.ANALYSIS_ADVICE, Validators.javaEntity(), List.of(
                IndexSpec.of("ix_advice_run", new Document("runId", 1)),
                IndexSpec.unique("uk_advice_natural", new Document("entityKey", 1))));
    }

    private static CollectionSpec endpointPoolLink() {
        return new CollectionSpec(Collections.ENDPOINT_POOL_LINK, Validators.poolLink(), List.of(
                // Both directions indexed, because both are asked. "What does this endpoint use"
                // is the screen; "which endpoints use this bean" is the question that made the
                // positional integers unusable, and an index on one side answers half of them.
                IndexSpec.of("ix_epl_endpoint", new Document("endpointRef", 1)),
                IndexSpec.of("ix_epl_pool", new Document("poolRef", 1)),
                IndexSpec.of("ix_epl_run", new Document("runId", 1).append("pool", 1)),
                IndexSpec.unique("uk_epl_natural", new Document("key", 1))));
    }

    /**
     * Declared foreign keys. A node child like the rest, but with two extra indexes because the
     * question this collection exists to answer is directional: "what does this table depend on"
     * and "what depends on this table" must both be one seek, or the second one — the one that
     * matters before dropping or migrating a table — degrades to a scan.
     */
    private static CollectionSpec foreignKey() {
        return new CollectionSpec(Collections.FOREIGN_KEY, Validators.nodeChild(), List.of(
                IndexSpec.of("ix_fk_node", new Document("nodeRef", 1)),
                IndexSpec.of("ix_fk_run", new Document("runId", 1)),
                IndexSpec.of("ix_fk_child", new Document("schemaName", 1).append("tableName", 1)),
                IndexSpec.of("ix_fk_parent",
                        new Document("referencedSchema", 1).append("referencedTable", 1))));
    }

    private static CollectionSpec nodeChild(String name) {
        return new CollectionSpec(name, Validators.nodeChild(), List.of(
                IndexSpec.of("ix_" + name + "_node", new Document("nodeRef", 1)),
                IndexSpec.of("ix_" + name + "_run", new Document("runId", 1))));
    }

    private static CollectionSpec run() {
        return new CollectionSpec(Collections.RUN, Validators.run(), List.of(
                // "At most one CURRENT run per entry point" — the filtered unique index that
                // replaces a relational partial unique constraint. Historical runs coexist.
                IndexSpec.uniquePartial("uk_run_current",
                        new Document("dialect", 1).append("project", 1)
                                .append("environment", 1).append("entryPoint", 1),
                        new Document("status", "COMPLETE").append("isCurrent", true)),
                // "At most one IN_PROGRESS run per entry point" — the structural half of MID-025.
                //
                // Without it, two concurrent ingests of the same analysis both run to completion
                // and the second demotes the first at publish time. Nothing errors: one machine's
                // work is silently discarded, and the only evidence is a SUPERSEDED run that
                // finished seconds before the one that replaced it. The index makes the second
                // ingest fail at its first write instead of at its last.
                IndexSpec.uniquePartial("uk_run_in_progress",
                        new Document("dialect", 1).append("project", 1)
                                .append("environment", 1).append("entryPoint", 1),
                        new Document("status", "IN_PROGRESS")),
                IndexSpec.of("ix_run_status", new Document("status", 1)),
                IndexSpec.of("ix_run_entry", new Document("entryPoint", 1).append("version", -1))));
    }

    private static CollectionSpec dbObject() {
        return new CollectionSpec(Collections.DB_OBJECT, Validators.dbObject(), List.of(
                // Reverse traversal: every object of a run, in one seek.
                IndexSpec.of("ix_obj_run", new Document("runId", 1)),
                // The natural key. Unique per run, which is what makes the _id recipe safe.
                IndexSpec.unique("uk_obj_natural",
                        new Document("runId", 1).append("schemaName", 1).append("objectName", 1)),
                IndexSpec.of("ix_obj_name", new Document("runId", 1).append("objectName", 1))));
    }

    private static CollectionSpec tableColumnUsage() {
        return new CollectionSpec(Collections.TABLE_COLUMN_USAGE, Validators.tableColumnUsage(), List.of(
                // Parent → children: the columns of one table.
                IndexSpec.of("ix_tcu_object", new Document("objectRef", 1)),
                IndexSpec.of("ix_tcu_run", new Document("runId", 1)),
                IndexSpec.unique("uk_tcu_natural",
                        new Document("objectRef", 1).append("columnName", 1))));
    }
}
