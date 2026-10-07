package com.plsql.parser.flow;

import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.InMemoryArtifactStore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.plsql.parser.model.FlowNode;
import com.plsql.parser.model.FlowResult;
import com.plsql.parser.model.StatementInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Writer-side proof for the transaction-scope enrichment. Exercises the node-scope
 * {@code transactionScope} and the flow-scope {@code transactionSummary}/{@code transactionStatements}
 * through a real chunk round-trip, asserting:
 *
 * <ul>
 *   <li>the additive row keys ({@code isPartial}, {@code sqlText}, {@code guard}, {@code handler})
 *       on both emitters;</li>
 *   <li>the new count keys, with the OLD keys still first and unchanged;</li>
 *   <li>the extended type filter (SET_TRANSACTION / LOCK_TABLE).</li>
 * </ul>
 */
class TransactionScopeWriterEnrichmentTest {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String BUILD = "b-txn-enrich";

    private static StatementInfo stmt(String type, int line) {
        StatementInfo s = new StatementInfo();
        s.setType(type);
        s.setLine(line);
        return s;
    }

    private static StatementInfo commit(int line) {
        StatementInfo s = stmt("COMMIT", line);
        s.setSqlText("COMMIT");
        return s;
    }

    private static StatementInfo rollbackToSavepoint(int line, String name, String handler) {
        StatementInfo s = stmt("ROLLBACK", line);
        s.setSavepointName(name);
        s.setSqlText("ROLLBACK TO SAVEPOINT " + name);
        if (handler != null) {
            s.setHandlerName(handler);
            s.setHandlerLine(14);
        }
        return s;
    }

    private FlowNode buildNode() {
        FlowNode node = new FlowNode();
        node.setOrder(1);
        node.setDepth(0);
        node.setSchema("ERP");
        node.setPackageName("TXN_PKG");
        node.setObjectName("DO_TXN");
        node.setObjectType("PROCEDURE");
        node.setLineStart(1);
        node.setLineEnd(20);

        StatementInfo topCommit = commit(4);

        StatementInfo ifCommit = commit(6);
        ifCommit.setGuardKind("IF");
        ifCommit.setGuardLine(5);
        ifCommit.setGuardCondition("v_flag = 1");

        StatementInfo sp = stmt("SAVEPOINT", 9);
        sp.setSavepointName("LOOP_SP");
        sp.setSqlText("SAVEPOINT loop_sp");
        sp.setGuardKind("LOOP");
        sp.setGuardLine(8);
        sp.setGuardCondition("i IN 1..10");

        StatementInfo lock = stmt("LOCK_TABLE", 10);
        lock.setSqlText("LOCK TABLE accounts IN EXCLUSIVE MODE");
        lock.setGuardKind("LOOP");
        lock.setGuardLine(8);
        lock.setGuardCondition("i IN 1..10");

        StatementInfo setTxn = stmt("SET_TRANSACTION", 12);
        setTxn.setSqlText("SET TRANSACTION READ ONLY");

        StatementInfo rb = rollbackToSavepoint(15, "BEFORE_UPDATE", "OTHERS");

        StatementInfo handlerCommit = commit(16);
        handlerCommit.setHandlerName("OTHERS");
        handlerCommit.setHandlerLine(14);

        node.setStatements(new ArrayList<>(List.of(
                topCommit, ifCommit, sp, lock, setTxn, rb, handlerCommit)));
        node.generateNodeId();
        return node;
    }

    private JsonNode writeAndRead(FlowNode node, String buildId, boolean wantIndex) throws Exception {
        InMemoryArtifactStore store = new InMemoryArtifactStore();
        ChunkedFlowWriter writer = new ChunkedFlowWriter(store, ArtifactKey.of(buildId));
        writer.writeChunk(node);
        FlowResult result = writer.mergeChunks("TXN_PKG", "ERP", 100, 0, new ArrayList<>());
        writer.writeFinalResult(result, Map.of());

        if (wantIndex) {
            try (var in = store.read(ArtifactKey.of(buildId, "api", "index.json"))) {
                return JSON.readTree(in);
            }
        }
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

    @Test
    @DisplayName("node transactionScope: old keys first, additive keys + new counts")
    void nodeScopeShape() throws Exception {
        JsonNode detail = writeAndRead(buildNode(), BUILD, false);
        JsonNode ts = detail.get("transactionScope");
        assertNotNull(ts, "transactionScope must be present");

        // Old keys, unchanged semantics.
        assertEquals(3, ts.get("commits").asInt());
        assertEquals(1, ts.get("rollbacks").asInt());
        assertEquals(1, ts.get("savepoints").asInt());
        assertFalse(ts.get("autonomous").asBoolean());
        assertTrue(ts.get("statements").isArray());

        // New count keys.
        assertEquals(1, ts.get("partialRollbacks").asInt(), "ROLLBACK TO SAVEPOINT counts as partial");
        assertEquals(1, ts.get("conditionalCommits").asInt(), "the IF-guarded COMMIT is conditional");
        assertEquals(0, ts.get("beginTrans").asInt(), "Oracle has no BEGIN_TRAN");
        assertEquals(1, ts.get("setTransactions").asInt());
        assertEquals(0, ts.get("releaseSavepoints").asInt());

        // Old keys come FIRST; the new count keys are appended after them.
        List<String> keys = fieldNames(ts);
        assertTrue(keys.indexOf("commits") < keys.indexOf("partialRollbacks"));
        assertTrue(keys.indexOf("statements") < keys.indexOf("partialRollbacks"));
        assertTrue(keys.indexOf("setTransactions") < keys.indexOf("releaseSavepoints"));
    }

    @Test
    @DisplayName("node transactionScope rows: guard / handler / isPartial / sqlText additive")
    void nodeScopeRows() throws Exception {
        JsonNode detail = writeAndRead(buildNode(), BUILD, false);
        JsonNode stmts = detail.get("transactionScope").get("statements");

        List<Integer> lines = new ArrayList<>();
        for (JsonNode s : stmts) lines.add(s.get("line").asInt());
        assertEquals(List.of(4, 6, 9, 10, 12, 15, 16), lines, "rows stay in source order");

        // Unconditional COMMIT: old keys only (+ sqlText), no guard/handler.
        JsonNode c4 = byLine(stmts, 4);
        assertEquals("COMMIT", c4.get("type").asText());
        assertEquals("COMMIT", c4.get("sqlText").asText());
        assertFalse(c4.has("guard"));
        assertFalse(c4.has("handler"));
        assertFalse(c4.has("isPartial"));

        // IF-guarded COMMIT.
        JsonNode c6 = byLine(stmts, 6);
        JsonNode g6 = c6.get("guard");
        assertNotNull(g6);
        assertEquals("IF", g6.get("kind").asText());
        assertEquals(5, g6.get("line").asInt());
        assertEquals("v_flag = 1", g6.get("condition").asText());

        // LOOP-guarded SAVEPOINT: name kept, guard added.
        JsonNode sp9 = byLine(stmts, 9);
        assertEquals("SAVEPOINT", sp9.get("type").asText());
        assertEquals("LOOP_SP", sp9.get("name").asText());
        assertEquals("LOOP", sp9.get("guard").get("kind").asText());
        assertEquals(8, sp9.get("guard").get("line").asInt());

        // LOCK_TABLE is now a transaction row.
        JsonNode lock10 = byLine(stmts, 10);
        assertEquals("LOCK_TABLE", lock10.get("type").asText());
        assertTrue(lock10.get("sqlText").asText().startsWith("LOCK TABLE"));

        // SET_TRANSACTION is now a transaction row.
        assertEquals("SET_TRANSACTION", byLine(stmts, 12).get("type").asText());

        // ROLLBACK TO SAVEPOINT: name + isPartial + handler.
        JsonNode rb15 = byLine(stmts, 15);
        assertEquals("ROLLBACK", rb15.get("type").asText());
        assertEquals("BEFORE_UPDATE", rb15.get("name").asText());
        assertTrue(rb15.get("isPartial").asBoolean());
        JsonNode h15 = rb15.get("handler");
        assertNotNull(h15);
        assertEquals("OTHERS", h15.get("name").asText());
        assertEquals(14, h15.get("line").asInt());

        // Handler-body COMMIT carries the handler but no guard.
        JsonNode c16 = byLine(stmts, 16);
        assertEquals("OTHERS", c16.get("handler").get("name").asText());
        assertFalse(c16.has("guard"));
    }

    @Test
    @DisplayName("flow transactionSummary + transactionStatements gain the same additive keys")
    void flowScopeShape() throws Exception {
        JsonNode index = writeAndRead(buildNode(), BUILD, true);

        JsonNode summary = index.get("transactionSummary");
        assertNotNull(summary);
        assertEquals(3, summary.get("commits").asInt());
        assertEquals(1, summary.get("rollbacks").asInt());
        assertEquals(1, summary.get("savepoints").asInt());
        assertEquals(0, summary.get("autonomousProcs").asInt());
        assertEquals(1, summary.get("partialRollbacks").asInt());
        assertEquals(1, summary.get("conditionalCommits").asInt());
        assertEquals(0, summary.get("beginTrans").asInt());
        assertEquals(1, summary.get("setTransactions").asInt());
        assertEquals(0, summary.get("releaseSavepoints").asInt());

        List<String> keys = fieldNames(summary);
        assertTrue(keys.indexOf("autonomousProcs") < keys.indexOf("partialRollbacks"),
                "old keys first, new counts after");

        JsonNode txnStmts = index.get("transactionStatements");
        assertEquals(7, txnStmts.size(), "all seven transaction rows reach the flow aggregate");

        // Flow row keeps its leading keys and gains the additive ones.
        JsonNode rbRow = null;
        for (JsonNode s : txnStmts) {
            if ("ROLLBACK".equals(s.get("type").asText())) rbRow = s;
        }
        assertNotNull(rbRow);
        assertEquals("DO_TXN", rbRow.get("proc").asText());
        assertEquals("ERP", rbRow.get("schema").asText());
        assertTrue(rbRow.has("nodeId"));
        assertTrue(rbRow.get("isPartial").asBoolean());
        assertEquals("BEFORE_UPDATE", rbRow.get("name").asText());
        assertEquals("OTHERS", rbRow.get("handler").get("name").asText());
        assertEquals("ROLLBACK TO SAVEPOINT BEFORE_UPDATE", rbRow.get("sqlText").asText());

        JsonNode commitRow = null;
        for (JsonNode s : txnStmts) {
            if ("COMMIT".equals(s.get("type").asText()) && s.has("guard")) commitRow = s;
        }
        assertNotNull(commitRow);
        assertEquals("IF", commitRow.get("guard").get("kind").asText());
    }

    private static JsonNode byLine(JsonNode rows, int line) {
        for (JsonNode r : rows) {
            if (r.get("line").asInt() == line) return r;
        }
        return null;
    }

    private static List<String> fieldNames(JsonNode obj) {
        List<String> names = new ArrayList<>();
        for (Iterator<String> it = obj.fieldNames(); it.hasNext(); ) names.add(it.next());
        return names;
    }
}
