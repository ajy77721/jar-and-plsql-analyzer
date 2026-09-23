package com.codeanalyzer.ingest.mongo;

import com.codeanalyzer.ingest.mongo.map.IngestContext;
import com.codeanalyzer.ingest.mongo.map.Splitter;
import com.codeanalyzer.ingest.mongo.map.plsql.NodeDetailMapper;
import com.codeanalyzer.ingest.mongo.schema.Collections;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A node field with a hand-written emitter must never also reach {@code Splitter.flatten}.
 *
 * <h2>The failure this locks down</h2>
 *
 * <p>{@code Splitter.flatten} emits every array it walks into a collection named after the field.
 * That is deliberate — it is what stops a newly added array from being silently embedded. But when
 * the same field also has a hand-written emitter in {@link NodeDetailMapper}, the rows are written
 * <b>twice</b>: once in the resolved shape the readers use, and once into
 * {@code am_proc_node__<field>}, which carries no {@code objectRef}, gets no index, and is read by
 * nothing.
 *
 * <p>{@code foreignKeys} was found this way and fixed on its own. It was not the only one. Measured
 * against three real stored nodes (SQLSERVER {@code USP_ALLOCATEDEMAND}, POSTGRES
 * {@code ERP_USP_COLLECT_DEMAND}, DB2 {@code ERP_USP_ALLOCATE_INVENTORY}), eight more fields were
 * duplicating — {@code subtreeTables} worst at 2,405/781/4,162 shadow rows against 78/34/58 real
 * ones, and {@code tables} written three times over because {@code directTables} is a byte-identical
 * copy that got its own full shadow tree.
 *
 * <h2>Why the assertion is written as a negative over all collections</h2>
 *
 * <p>Asserting "am_foreign_key has 2 rows" cannot catch this: the correct rows are present in the
 * buggy version too. The only thing that distinguishes the two is a collection that should not
 * exist, so the test has to look at the <em>set</em> of collections written, not at any one of
 * them. Listing the permitted names also means a field that gains an emitter later fails here until
 * its strip is added, which is the property that was missing when {@code foreignKeys} was fixed
 * alone.
 */
class NoShadowCollectionTest {

    /**
     * A node exercising every field that has a hand-written emitter, plus the two that correctly
     * have none. Shapes follow the real stored artifacts.
     */
    private static final String NODE_JSON = """
            {
              "nodeId": "ERP$USP_ALLOCATE",
              "schema": "ERP",
              "name": "USP_ALLOCATE",
              "objectType": "PROCEDURE",
              "calls": [{"target": "ERP.FN_TAX", "line": 4}],
              "parameters": [{"name": "p_id", "dataType": "INT"}],
              "cursors": [{"name": "c_ord", "line": 7, "tables": ["ERP_SALES_ORDER"]}],
              "dynamicSql": [{"type": "EXECUTE", "sqlExpression": "v_sql", "line": 9,
                              "usingVariables": ["v_id"]}],
              "exceptionHandlers": [{"exceptionName": "NO_DATA_FOUND", "line": 11}],
              "externalPackageVarRefDetails": [{"packageName": "PKG_CFG", "variableName": "G_MODE",
                                                "accessType": "READ", "line": 13}],
              "sequences": [{"schema": "ERP", "name": "SEQ_ALLOC",
                             "operations": {"NEXTVAL": [15]}}],
              "foreignKeys": [{"constraintName": "fk_c", "schema": "ERP", "tableName": "orders",
                               "columns": ["customer_id"], "referencedSchema": "ERP",
                               "referencedTable": "customers", "referencedColumns": ["id"],
                               "line": 17}],
              "transactionScope": {"commitCount": 1,
                                   "statements": [{"type": "COMMIT", "line": 19}]},
              "variables": {"total": 2, "plainCount": 1,
                            "plain": [{"name": "v_qty", "dataType": "INT", "line": 21}],
                            "typeRefs": [{"name": "v_r", "table": "ERP_PRODUCT", "line": 22}],
                            "rowtypeRefs": [], "collections": []},
              "tables": [
                {"schema": "ERP", "name": "ERP_SALES_ORDER", "operation": "SELECT",
                 "select": [25],
                 "joins": [{"joinType": "LEFT", "joinedTable": "ERP_CUSTOMER",
                            "joinedTableAlias": "c", "leftTable": "ERP_SALES_ORDER",
                            "leftTableAlias": "so", "condition": "c.id = so.customer_id",
                            "line": 25, "queryId": 24}],
                 "merge": [26]}
              ],
              "directTables": [
                {"schema": "ERP", "name": "ERP_SALES_ORDER", "operation": "SELECT",
                 "select": [25],
                 "joins": [{"joinType": "LEFT", "joinedTable": "ERP_CUSTOMER",
                            "joinedTableAlias": "c", "leftTable": "ERP_SALES_ORDER",
                            "leftTableAlias": "so", "condition": "c.id = so.customer_id",
                            "line": 25, "queryId": 24}],
                 "merge": [26]}
              ],
              "subtreeTables": [
                {"schema": "ERP", "name": "ERP_INVENTORY", "totalUsageCount": 3,
                 "select": [30], "usedByNodes": ["ERP$USP_ALLOCATE"]}
              ]
            }
            """;

    private static Map<String, Integer> collectionsWritten(String nodeJson) throws Exception {
        IngestContext ctx = new IngestContext("SQLSERVER~DEMO~LOCAL~TESTRUN~v1", "SQLSERVER");
        Map<String, Integer> counts = new TreeMap<>();
        new NodeDetailMapper().map(ctx,
                new ByteArrayInputStream(nodeJson.getBytes(StandardCharsets.UTF_8)),
                (coll, doc) -> counts.merge(coll, 1, Integer::sum));
        return counts;
    }

    /**
     * Every field {@link NodeDetailMapper} emits by hand, with the shadow collection name the
     * generic splitter would derive for it if the strip were ever dropped.
     */
    private static final String[] HAND_MAPPED_FIELDS = {
            "foreignKeys", "cursors", "dynamicSql", "exceptionHandlers",
            "externalPackageVarRefDetails", "sequences", "subtreeTables", "tables",
            "directTables", "statements", "plain", "typeRefs", "rowtypeRefs", "collections",
    };

    @Test
    void noHandMappedFieldAlsoProducesASplitterShadowCollection() throws Exception {
        Map<String, Integer> written = collectionsWritten(NODE_JSON);

        List<String> shadows = new ArrayList<>();
        for (String field : HAND_MAPPED_FIELDS) {
            String shadow = Splitter.childCollection(Collections.PROC_NODE, field);
            if (written.containsKey(shadow)) {
                shadows.add(shadow + " (" + written.get(shadow) + " rows, duplicating the "
                        + "hand-built collection for '" + field + "')");
            }
        }
        assertTrue(shadows.isEmpty(),
                "these fields reached Splitter.flatten as well as their own emitter: " + shadows
                        + "\nall collections written: " + written);
    }

    /**
     * The strip must not cost the rows it was protecting. Every hand-built collection is still
     * written, with the counts the source implies — this is what distinguishes "stripped before
     * flatten" from "stripped, and the emitter now sees nothing".
     */
    @Test
    void everyHandBuiltCollectionStillReceivesItsRows() throws Exception {
        Map<String, Integer> w = collectionsWritten(NODE_JSON);

        assertEquals(1, w.get(Collections.PROC_NODE), "the node itself");
        assertEquals(1, w.get(Collections.FOREIGN_KEY));
        assertEquals(1, w.get(Collections.CURSOR_DECL));
        assertEquals(1, w.get(Collections.CURSOR_OBJECT), "cursor table resolved to an object");
        assertEquals(1, w.get(Collections.DYNAMIC_SQL));
        assertEquals(1, w.get(Collections.EXCEPTION_HANDLER));
        assertEquals(1, w.get(Collections.PACKAGE_VAR_REF));
        assertEquals(1, w.get(Collections.TXN_STATEMENT), "transactionScope.statements");
        assertEquals(2, w.get(Collections.VARIABLE_TYPE_REF), "plain + typeRefs, one collection");
        assertEquals(1, w.get(Collections.SEQUENCE_OBJ));
        assertEquals(1, w.get(Collections.SEQUENCE_USE));
        assertEquals(1, w.get(Collections.NODE_TABLE_ACCESS));
        assertEquals(1, w.get(Collections.SUBTREE_TABLE));
    }

    /**
     * Joins are the case that must NOT be "fixed". They have no hand-written emitter: the generic
     * splitter is their only writer, and {@code EntityJoinsReader} queries
     * {@code am_node_table_access__joins} directly. Stripping {@code tables} at the top level had
     * to leave that intact, because the row comes from {@code emitObjectRows}' own flatten — a
     * distinction that is invisible unless it is asserted.
     */
    @Test
    void joinsStillReachTheCollectionTheUiReads() throws Exception {
        Map<String, Integer> w = collectionsWritten(NODE_JSON);

        String joinsCollection =
                Splitter.childCollection(Collections.NODE_TABLE_ACCESS, "joins");
        assertEquals("am_node_table_access__joins", joinsCollection);
        assertEquals(1, w.get(joinsCollection),
                "EntityJoinsReader reads this collection; it must survive the strip. Written: " + w);
    }

    /** MERGE and the per-operation line arrays hang off the table row, and must survive too. */
    @Test
    void perOperationChildRowsStillReachTheCollectionEntityTablesReaderReads() throws Exception {
        Map<String, Integer> w = collectionsWritten(NODE_JSON);

        assertEquals(1, w.get("am_node_table_access__merge"), "MERGE usage line; written: " + w);
        assertEquals(1, w.get("am_node_table_access__select"));
        assertEquals(1, w.get("am_subtree_table__select"));
        assertEquals(1, w.get("am_subtree_table__used_by_nodes"));
    }

    /**
     * {@code calls} and {@code parameters} have no hand-written emitter, so the splitter is their
     * only home and stripping them would lose them outright. Asserted so a future "strip
     * everything" does not quietly delete two collections.
     */
    @Test
    void fieldsWithNoHandWrittenEmitterAreStillSplitOut() throws Exception {
        Map<String, Integer> w = collectionsWritten(NODE_JSON);

        assertEquals(1, w.get("am_proc_node__calls"),
                "no emitter writes calls; the splitter must still do it. Written: " + w);
        assertEquals(1, w.get("am_proc_node__parameters"));
    }

    /**
     * The summary scalars on the two wrapper objects stay on the node document. Only the arrays
     * inside them are taken, so a screen reading {@code transactionScope.commitCount} or
     * {@code variables.total} is unaffected.
     */
    @Test
    void wrapperObjectSummaryScalarsRemainOnTheNodeDocument() throws Exception {
        IngestContext ctx = new IngestContext("SQLSERVER~DEMO~LOCAL~TESTRUN~v1", "SQLSERVER");
        List<org.bson.Document> nodes = new ArrayList<>();
        new NodeDetailMapper().map(ctx,
                new ByteArrayInputStream(NODE_JSON.getBytes(StandardCharsets.UTF_8)),
                (coll, doc) -> { if (Collections.PROC_NODE.equals(coll)) nodes.add(doc); });

        org.bson.Document node = nodes.get(0);
        org.bson.Document txn = (org.bson.Document) node.get("transactionScope");
        assertNotNull(txn, "transactionScope must survive with its summary: " + node);
        assertEquals(1, txn.getInteger("commitCount"));
        assertNull(txn.get("statements"), "the rows moved to am_txn_statement");

        org.bson.Document vars = (org.bson.Document) node.get("variables");
        assertNotNull(vars, "variables must survive with its counts: " + node);
        assertEquals(2, vars.getInteger("total"));
        assertNull(vars.get("plain"), "the rows moved to am_variable_type_ref");
    }
}
