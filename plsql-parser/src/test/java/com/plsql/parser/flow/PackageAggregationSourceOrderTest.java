package com.plsql.parser.flow;

import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.InMemoryArtifactStore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.plsql.parser.PlSqlParserEngine;
import com.plsql.parser.model.*;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * {@code DependencyCrawler}'s package-as-entry-point aggregation concatenates the package object's
 * OWN members first and then subprogram by subprogram. That is not source order: a package body's
 * initialization block sits at the END of the file, but its statements are ParsedObject-level (see
 * {@code PlSqlAnalysisVisitor.addStatement}'s else-branch), so they land at the FRONT of the
 * aggregated list — proven against a real parse in {@link #initBlockStatementsAreObjectLevelAndComeLast()}.
 *
 * <p>Every writer output that walks a node's statement list in list order and presents the result
 * as a sequence was therefore out of execution order. Pinned here for the two that do:
 * {@code transactionScope.statements} and each cursor's {@code stmtOperations} lifecycle list.
 */
class PackageAggregationSourceOrderTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    /** The real package the crawler aggregation mis-orders: init block COMMIT is the LAST line. */
    private static final String PKG_ORDER = """
            CREATE OR REPLACE PACKAGE BODY PKG_ORDER AS
              CURSOR c_all IS SELECT id FROM ERP_ORDER;
              PROCEDURE p_one IS
                v NUMBER;
              BEGIN
                SELECT count(*) INTO v FROM ERP_STOCK;
                COMMIT;
                OPEN c_all;
                FETCH c_all INTO v;
                CLOSE c_all;
              END p_one;
              PROCEDURE p_two IS
              BEGIN
                INSERT INTO ERP_STOCK(id) VALUES (1);
                COMMIT;
              END p_two;
            BEGIN
              UPDATE ERP_STOCK SET id = 0;
              COMMIT;
            END PKG_ORDER;
            """;

    @Test
    void initBlockStatementsAreObjectLevelAndComeLast() {
        ParseResult r = new PlSqlParserEngine().parseContent(PKG_ORDER, "pkg_order.sql");
        ParsedObject pkg = r.getObjects().stream()
                .filter(o -> o.getSubprograms() != null && !o.getSubprograms().isEmpty())
                .findFirst().orElseThrow();

        // Object-level statements are the init block's, at the highest lines in the file.
        List<String> objStmts = new ArrayList<>();
        for (StatementInfo s : pkg.getStatements()) objStmts.add(s.getType() + "@" + s.getLine());
        assertEquals(List.of("UPDATE@18", "COMMIT@19"), objStmts);

        // ...while the subprograms they are concatenated in FRONT of sit at lower lines.
        SubprogramInfo pOne = pkg.getSubprograms().get(0);
        assertEquals(6, pOne.getStatements().get(0).getLine());

        // So the crawler's raw concatenation order is NOT ascending by line — the exact condition
        // the final stable sort in DependencyCrawler's package aggregation now corrects.
        List<Integer> concatenated = new ArrayList<>();
        for (StatementInfo s : pkg.getStatements()) concatenated.add(s.getLine());
        for (SubprogramInfo sub : pkg.getSubprograms()) {
            for (StatementInfo s : sub.getStatements()) concatenated.add(s.getLine());
        }
        List<Integer> sorted = new ArrayList<>(concatenated);
        sorted.sort(Comparator.naturalOrder());
        assertNotEquals(sorted, concatenated,
                "the raw package aggregation order must be the out-of-order case this pins");
    }

    /**
     * The aggregated node the crawler hands the writer for {@code PKG_ORDER} above, in the raw
     * concatenation order (init block first). Both sequence-shaped outputs must come out by line.
     */
    @Test
    void transactionScopeAndCursorLifecycleComeOutInTrueSourceOrder() throws Exception {
        FlowNode node = new FlowNode();
        node.setOrder(1);
        node.setDepth(0);
        node.setSchema("ERP");
        node.setObjectName("PKG_ORDER");
        node.setObjectType("PACKAGE");
        node.setLineStart(1);
        node.setLineEnd(20);
        node.setStatements(List.of(
                // init block (ParsedObject-level, end of file) — concatenated FIRST
                stmt("UPDATE", 18, null),
                stmt("COMMIT", 19, null),
                stmt("OPEN_CURSOR", 20, "C_ALL"),   // init block also drives the package cursor
                // p_one
                stmt("SELECT", 6, null),
                stmt("COMMIT", 7, null),
                stmt("OPEN_CURSOR", 8, "C_ALL"),
                stmt("FETCH", 9, "C_ALL"),
                stmt("CLOSE_CURSOR", 10, "C_ALL"),
                // p_two
                stmt("INSERT", 14, null),
                stmt("COMMIT", 15, null)));
        CursorInfo cur = new CursorInfo();
        cur.setName("C_ALL");
        cur.setLine(2);
        node.setCursors(List.of(cur));
        node.generateNodeId();

        JsonNode detail = writeAndReadNodeDetail(node, "b-pkg-order");

        List<Integer> commitLines = new ArrayList<>();
        for (JsonNode s : detail.get("transactionScope").get("statements")) {
            commitLines.add(s.get("line").asInt());
        }
        assertEquals(List.of(7, 15, 19), commitLines,
                "transactionScope.statements must be in execution order, not aggregation order");

        List<String> lifecycle = new ArrayList<>();
        for (JsonNode o : detail.get("cursors").get(0).get("stmtOperations")) {
            lifecycle.add(o.get("operation").asText() + "@" + o.get("line").asInt());
        }
        assertEquals(List.of("OPEN@8", "FETCH@9", "CLOSE@10", "OPEN@20"), lifecycle,
                "a cursor's OPEN/FETCH/CLOSE lifecycle must be in execution order");
    }

    private static StatementInfo stmt(String type, int line, String sqlText) {
        StatementInfo s = new StatementInfo();
        s.setType(type);
        s.setLine(line);
        if (sqlText != null) s.setSqlText(sqlText);
        return s;
    }

    private JsonNode writeAndReadNodeDetail(FlowNode node, String buildId) throws Exception {
        InMemoryArtifactStore store = new InMemoryArtifactStore();
        ChunkedFlowWriter writer = new ChunkedFlowWriter(store, ArtifactKey.of(buildId));
        writer.writeChunk(node);
        FlowResult result = writer.mergeChunks("PKG_ORDER", "ERP", 100, 0, new ArrayList<>());
        writer.writeFinalResult(result, Map.of());

        ArtifactKey nodesDir = ArtifactKey.of(buildId, "api", "nodes");
        JsonNode index;
        try (var in = store.read(ArtifactKey.of(buildId, "api", "index.json"))) {
            index = JSON.readTree(in);
        }
        String nodeId = index.get("nodes").get(0).get("nodeId").asText();
        try (var in = store.read(nodesDir.resolve(nodeId.replace('/', '_') + ".json"))) {
            return JSON.readTree(in);
        }
    }
}
