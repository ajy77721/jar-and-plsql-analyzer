package com.codeanalyzer.ingest.mongo;

import com.codeanalyzer.ingest.mongo.map.IngestContext;
import com.codeanalyzer.ingest.mongo.map.plsql.NodeDetailMapper;
import com.codeanalyzer.ingest.mongo.schema.Collections;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Step 4: {@code externalPackageVarRefDetails} on {@code api/nodes/*.json} (the structured
 * package-variable-reference data added alongside the older bare {@code externalPackageVarRefs}
 * string list) must reach a real {@code am_*} collection — {@link Collections#PACKAGE_VAR_REF} —
 * one row per reference, each pointing back at the node it was declared in via {@code nodeRef}.
 *
 * <p>Mirrors {@code CallGraphMapperDuplicateEdgeMergeTest}'s pattern: {@link NodeDetailMapper} is
 * exercised directly against a synthetic node document with an in-memory {@code DocSink}, so no
 * MongoDB instance is required.
 */
class PackageVarRefMapperTest {

    private static final String NODE_JSON = """
            {
              "nodeId": "ERP_USP_FINALIZE_GL_BATCH",
              "schema": "ANALYZER",
              "objectName": "ERP_USP_FINALIZE_GL_BATCH",
              "objectType": "PROCEDURE",
              "externalPackageVarRefs": ["ERP_PKG_PLANNER.G_LAST_CONTEXT"],
              "externalPackageVarRefDetails": [
                {
                  "packageName": "ERP_PKG_PLANNER",
                  "variableName": "G_LAST_CONTEXT",
                  "line": 42,
                  "accessType": "READ"
                }
              ]
            }
            """;

    @Test
    void externalPackageVarRefDetailsReachTheirOwnCollection() throws Exception {
        IngestContext ctx = new IngestContext("ORACLE~ORACLEDEMO~LOCAL~TESTRUN~v1", "ORACLE");

        List<Document> rows = new ArrayList<>();
        new NodeDetailMapper().map(ctx,
                new ByteArrayInputStream(NODE_JSON.getBytes(StandardCharsets.UTF_8)),
                (coll, doc) -> {
                    if (Collections.PACKAGE_VAR_REF.equals(coll)) rows.add(doc);
                });

        assertEquals(1, rows.size(), "expected exactly one am_package_var_ref row, got: " + rows);
        Document row = rows.get(0);
        assertEquals("ERP_PKG_PLANNER", row.getString("packageName"));
        assertEquals("G_LAST_CONTEXT", row.getString("variableName"));
        assertEquals(42, row.getInteger("line"));
        assertEquals("READ", row.getString("accessType"));
        assertNotNull(row.getString("nodeRef"), "row must point back at the node it was declared in");
        assertEquals(ctx.runId(), row.getString("runId"));
    }

    @Test
    void noRowsEmittedWhenNodeHasNoExternalPackageVarRefs() throws Exception {
        String plain = """
                {"nodeId": "NO_REFS", "schema": "ANALYZER", "objectName": "NO_REFS"}
                """;
        IngestContext ctx = new IngestContext("ORACLE~ORACLEDEMO~LOCAL~TESTRUN~v1", "ORACLE");
        List<Document> rows = new ArrayList<>();
        new NodeDetailMapper().map(ctx,
                new ByteArrayInputStream(plain.getBytes(StandardCharsets.UTF_8)),
                (coll, doc) -> {
                    if (Collections.PACKAGE_VAR_REF.equals(coll)) rows.add(doc);
                });
        assertTrue(rows.isEmpty(), "a node with no package-var refs must emit none, got: " + rows);
    }
}
