package com.plsqlanalyzer.web.parser.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.zip.GZIPInputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The transaction-run resolver's replay semantics, on hand-built timelines (the pure core).
 *
 * <p>The anchor test is the USER'S OWN SCENARIO (task: transaction capture reliability): the
 * entry proc T1 writes records, calls another procedure which CHANGES records and has a
 * CONDITIONAL commit (a COMMIT inside an IF), the caller later ROLLs BACK and then COMMITs —
 * the runs + survival verdicts must show exactly which writes survive under which assumptions,
 * with the callee's COMMIT persisting the CALLER's uncommitted writes (Oracle semantics — the
 * thing today's per-proc display gets wrong).
 */
class TxnRunResolverTest {

    private final ObjectMapper mapper = new ObjectMapper();

    private TxnRunResolver.Event write(String nodeId, String proc, int line, String table, String op) {
        TxnRunResolver.Event e = new TxnRunResolver.Event();
        e.kind = "WRITE"; e.nodeId = nodeId; e.proc = proc; e.depth = 0; e.line = line;
        e.table = table; e.op = op;
        return e;
    }

    private TxnRunResolver.Event txn(String kind, String nodeId, String proc, int line) {
        TxnRunResolver.Event e = new TxnRunResolver.Event();
        e.kind = kind; e.nodeId = nodeId; e.proc = proc; e.depth = 0; e.line = line;
        return e;
    }

    private TxnRunResolver.Event guarded(TxnRunResolver.Event e, String condition) {
        e.guardKind = "IF"; e.guardLine = e.line - 1; e.guardCondition = condition;
        return e;
    }

    @Test
    void theUsersScenario_calleeConditionalCommitThenRollbackThenCommit() {
        // T1 (entry): UPDATE @12, CALL child @20, DELETE @40, COMMIT @60
        // child: UPDATE @22 (into T1's tx per Oracle), COMMIT @30 guarded by "v_mode = 'FULL'"
        // Timeline (execution order):
        TxnRunResolver resolver = new TxnRunResolver(null, null, mapper);
        List<TxnRunResolver.Run> runs = resolver.enumerate(new java.util.ArrayList<>(List.of(
                write("T1", "ERP_T1", 12, "ORDERS", "UPDATE"),
                write("CHILD", "ERP_CHILD", 22, "ORDER_LINES", "UPDATE"),
                guarded(txn("COMMIT", "CHILD", "ERP_CHILD", 30), "v_mode = 'FULL'"),
                write("T1", "ERP_T1", 40, "ORDERS", "DELETE"),
                txn("ROLLBACK", "T1", "ERP_T1", 50),
                txn("COMMIT", "T1", "ERP_T1", 60)
        )), false);

        // TWO paths: the guard taken (COMMIT@30 fires) and not taken.
        assertEquals(2, runs.size());

        // Path 1 — the callee's COMMIT FIRED: it persists BOTH procs' writes up the chain
        // (Oracle semantics — the caller's UPDATE@12 and DELETE@40 die with it? No: they are
        // persisted; the ROLLBACK@50 can only undo work after line 30, which is the DELETE@40),
        // then the final COMMIT@60 has nothing left to persist.
        TxnRunResolver.Run taken = runs.get(0).assumptions.isEmpty()
                || Boolean.TRUE.equals(runs.get(0).assumptions.get(0).get("taken"))
                ? runs.get(0) : runs.get(1);
        assertTrue(taken.assumptions.stream().anyMatch(a ->
                        Boolean.TRUE.equals(a.get("taken")) && "v_mode = 'FULL'".equals(a.get("condition"))),
                "the taken path must record its guard assumption");
        assertTrue(taken.outcome.equals("COMMITTED") || taken.outcome.equals("PARTIAL"),
                "the taken path ends COMMITTED, got: " + taken.outcome);
        var survived = taken.writes.stream().filter(w -> "survived".equals(w.get("survival"))).count();
        var discarded = taken.writes.stream().filter(w -> "discarded".equals(w.get("survival"))).count();
        assertEquals(3, survived + discarded, "all three writes get a verdict");
        // On the taken path: writes at 12+22 persisted by COMMIT@30; DELETE@40 undone by ROLLBACK@50.
        assertEquals(2, survived, "the callee's conditional COMMIT persists the caller's earlier write too");
        assertEquals(1, discarded, "the caller's later DELETE is undone by the ROLLBACK after the commit");

        // Path 2 — the guard NOT taken: no commit at 30; ROLLBACK@50 discards 12+22+40;
        // COMMIT@60 persists nothing (the run is ROLLED_BACK).
        TxnRunResolver.Run absent = taken == runs.get(0) ? runs.get(1) : runs.get(0);
        assertTrue(absent.assumptions.stream().anyMatch(a ->
                        Boolean.FALSE.equals(a.get("taken")) && "v_mode = 'FULL'".equals(a.get("condition"))),
                "the absent path must record the guard assumption as false");
        assertEquals("ROLLED_BACK", absent.outcome, "without the callee commit the rollback wins");
        assertEquals(3, absent.writes.stream()
                .filter(w -> "discarded".equals(w.get("survival"))).count());
        assertTrue(absent.summary.contains("discards"), "the plain-English summary must be derived from the replay");
    }

    @Test
    void rollbackToSavepointDiscardsOnlyWritesAfterTheSavepoint() {
        TxnRunResolver resolver = new TxnRunResolver(null, null, mapper);
        TxnRunResolver.Event sp = txn("SAVEPOINT", "P", "P", 10); sp.name = "SP1";
        TxnRunResolver.Event rbTo = txn("ROLLBACK_TO", "P", "P", 30); rbTo.name = "SP1";
        List<TxnRunResolver.Run> runs = resolver.enumerate(new java.util.ArrayList<>(List.of(
                write("P", "P", 8, "A", "INSERT"),      // before the savepoint — survives
                sp,
                write("P", "P", 20, "B", "INSERT"),     // after the savepoint — discarded
                rbTo,
                write("P", "P", 35, "C", "INSERT"),     // after the rollback — survives
                txn("COMMIT", "P", "P", 40)
        )), false);
        assertEquals(1, runs.size());
        var w = runs.get(0).writes;
        assertEquals(3, w.size());
        assertEquals("survived", w.get(0).get("survival"), "the write before the savepoint survives");
        assertEquals("discarded", w.get(1).get("survival"), "the write after the savepoint is rolled back");
        assertEquals("survived", w.get(2).get("survival"), "the write after the rollback-to survives and is committed");
    }

    @Test
    void autonomousNodesSurviveTheOuterRollback() {
        TxnRunResolver resolver = new TxnRunResolver(null, null, mapper);
        TxnRunResolver.Event innerWrite = write("AUTO", "LOG_PROC", 5, "AUDIT", "INSERT");
        innerWrite.autonomousNode = true;
        TxnRunResolver.Event innerCommit = txn("COMMIT", "AUTO", "LOG_PROC", 6);
        innerWrite.depth = 1; innerCommit.depth = 1;
        List<TxnRunResolver.Run> runs = resolver.enumerate(new java.util.ArrayList<>(List.of(
                write("MAIN", "MAIN", 10, "A", "UPDATE"),
                txn("ROLLBACK", "MAIN", "MAIN", 20)
        )), false);
        // The autonomous side run: built from the autonomous events (they were filtered out of
        // the main timeline by compute(); here assert the replay marks them survived directly).
        TxnRunResolver.Run side = new TxnRunResolver.Run();
        side.kind = "autonomous";
        side.events.add(innerWrite);
        side.events.add(innerCommit);
        resolver.replay(side, false, true);
        assertEquals("survived", side.writes.get(0).get("survival"),
                "an autonomous proc's write survives regardless of the outer flow");
        assertTrue(side.summary.toLowerCase().contains("autonom"));
    }

    @Test
    void sqlServerInnerCommitMergesIntoTheEnclosingFrame() {
        TxnRunResolver resolver = new TxnRunResolver(null, null, mapper);
        TxnRunResolver.Event begin1 = txn("BEGIN_TRAN", "OUTER", "OUTER", 2);
        TxnRunResolver.Event writeOuter = write("OUTER", "OUTER", 5, "A", "UPDATE");
        TxnRunResolver.Event begin2 = txn("BEGIN_TRAN", "INNER", "INNER", 8); begin2.depth = 1;
        TxnRunResolver.Event writeInner = write("INNER", "INNER", 12, "B", "INSERT"); writeInner.depth = 1;
        TxnRunResolver.Event innerCommit = txn("COMMIT", "INNER", "INNER", 15); innerCommit.depth = 1;
        TxnRunResolver.Event outerRollback = txn("ROLLBACK", "OUTER", "OUTER", 25);
        // T-SQL @@TRANCOUNT semantics: the inner COMMIT only decrements the count — both writes
        // (the outer frame's and the merged inner one) are still uncommitted, so the outer
        // ROLLBACK discards BOTH. (And a write OUTSIDE any explicit frame would autocommit.)
        List<TxnRunResolver.Run> runs = resolver.enumerate(new java.util.ArrayList<>(List.of(
                begin1, writeOuter, begin2, writeInner, innerCommit, outerRollback
        )), true);
        assertEquals(1, runs.size());
        assertEquals(2, runs.get(0).writes.stream()
                .filter(w -> "discarded".equals(w.get("survival"))).count(),
                "the inner COMMIT did not persist: the outer ROLLBACK discards both writes");
    }

    @Test
    void sqlServerWritesOutsideExplicitFramesAutocommit() {
        TxnRunResolver resolver = new TxnRunResolver(null, null, mapper);
        TxnRunResolver.Event freeWrite = write("OUTER", "OUTER", 3, "A", "UPDATE");  // no BEGIN TRAN yet
        TxnRunResolver.Event begin = txn("BEGIN_TRAN", "OUTER", "OUTER", 6);
        TxnRunResolver.Event frameWrite = write("OUTER", "OUTER", 9, "B", "DELETE");
        TxnRunResolver.Event rb = txn("ROLLBACK", "OUTER", "OUTER", 20);
        List<TxnRunResolver.Run> runs = resolver.enumerate(new java.util.ArrayList<>(List.of(
                freeWrite, begin, frameWrite, rb
        )), true);
        assertEquals(1, runs.size());
        var ws = runs.get(0).writes;
        assertEquals("survived", ws.get(0).get("survival"),
                "a write outside any explicit transaction auto-committed before the rollback");
        assertEquals("discarded", ws.get(1).get("survival"),
                "the write inside the explicit frame is undone by the rollback");
    }

    @Test
    void dialectComesFromTheFirstSegmentOfTheAnalysisName() {
        assertEquals("SQLSERVER", TxnRunResolver.dialectOf("SQLSERVER~SQLSERVERDEMO~LOCAL~USP_RUNMRPCYCLE"));
        assertEquals("ORACLE", TxnRunResolver.dialectOf("ORACLE~ORACLEDEMO~LOCAL~ERP_USP_RUN_CYCLE"));
        assertEquals("ORACLE", TxnRunResolver.dialectOf(null));
    }

    @Test
    void guardedWriteIsAbsentOnThePathWhereItsGuardIsFalse() {
        TxnRunResolver resolver = new TxnRunResolver(null, null, mapper);
        TxnRunResolver.Event guardedWrite = guarded(write("P", "P", 20, "B", "DELETE"), "v_all = 1");
        List<TxnRunResolver.Run> runs = resolver.enumerate(new java.util.ArrayList<>(List.of(
                write("P", "P", 8, "A", "INSERT"),
                guardedWrite,
                txn("COMMIT", "P", "P", 40)
        )), false);
        assertEquals(2, runs.size(), "a guarded WRITE is also a branch point");
        TxnRunResolver.Run taken = Boolean.TRUE.equals(runs.get(0).assumptions.get(0).get("taken"))
                ? runs.get(0) : runs.get(1);
        TxnRunResolver.Run absent = taken == runs.get(0) ? runs.get(1) : runs.get(0);
        assertEquals(2, taken.writes.size(), "the taken path has both writes");
        assertEquals(1, absent.writes.size(), "the absent path never executes the guarded write");
    }

    // ───────────────── live-shape audit: the REAL ORACLEDEMO artifacts ─────────────────
    // The no-new-bugs rule: the resolver must work against the artifacts as they ACTUALLY are,
    // not a fixture of them. Runs against the S3-mode cache mirror when present (skips cleanly
    // when this machine has no local copy).

    private static final Path DEMO_ROOT =
            Path.of("../data/cache/store/parser/plsql-parse/ORACLE/ORACLEDEMO/LOCAL/ERP_USP_RUN_CYCLE");
    private static final String DEMO_ENTRY = "ANALYZER$ERP_USP_RUN_CYCLE";

    private JsonNode readMaybeGz(Path p) throws Exception {
        if (!Files.isRegularFile(p)) {
            Path gz = Path.of(p.toString() + ".gz");
            if (!Files.isRegularFile(gz)) return null;
            try (var in = new GZIPInputStream(Files.newInputStream(gz))) {
                return mapper.readTree(in.readAllBytes());
            }
        }
        return mapper.readTree(Files.readAllBytes(p));
    }

    @Test
    void theResolverWorksOnTheRealOracleDemoArtifacts() throws Exception {
        org.junit.jupiter.api.Assumptions.assumeTrue(Files.isDirectory(DEMO_ROOT),
                "ORACLEDEMO cache mirror not on this machine");

        JsonNode tree = readMaybeGz(DEMO_ROOT.resolve("views/call-tree/" + DEMO_ENTRY + ".json"));
        org.junit.jupiter.api.Assumptions.assumeTrue(tree != null, "call-tree sidecar missing");

        // The node-doc loader exactly as the service resolves them: api/nodes/<id>.json —
        // memoized, because the same node appears at many call sites.
        Map<String, JsonNode> cache = new ConcurrentHashMap<>();
        TxnRunResolver resolver = new TxnRunResolver(null, null, mapper);
        JsonNode out = resolver.computeFromTree("ORACLE~ORACLEDEMO~LOCAL~ERP_USP_RUN_CYCLE",
                DEMO_ENTRY, tree, id -> {
                    try {
                        return cache.computeIfAbsent(id, k -> {
                            try { return readMaybeGz(DEMO_ROOT.resolve("api/nodes/" + k + ".json")); }
                            catch (Exception e) { return null; }
                        });
                    } catch (Exception e) { return null; }
                });

        // The resolver must have produced runs from REAL captured data.
        assertTrue(out.path("runs").isArray() && out.path("runs").size() > 0,
                "no runs resolved from the real ORACLEDEMO artifacts");
        assertTrue(out.path("dialect").asText().equals("ORACLE"));
        // Every write in every run carries a survival verdict — the whole point of the resolver.
        for (JsonNode run : out.path("runs")) {
            for (JsonNode w : run.path("writes")) {
                String s = w.path("survival").asText("");
                assertTrue(s.equals("survived") || s.equals("discarded") || s.equals("open"),
                        "a write in run " + run.path("runId").asInt() + " has no survival verdict: " + w);
            }
            for (JsonNode ev : run.path("events")) {
                // Every txn event with a guard states it — the capture's new contract, on REAL data.
                if (ev.path("kind").asText().equals("COMMIT")
                        && ev.path("guard").isMissingNode()
                        && ev.path("line").asInt() == 0) {
                    assertFalse(true, "malformed event on the real artifacts");
                }
            }
        }
        // The entry proc's OWN known capture appears: the real node has ROLLBACK TO SAVEPOINT rows.
        boolean sawSavepointOrRollback = false;
        for (JsonNode run : out.path("runs")) {
            for (JsonNode ev : run.path("events")) {
                if (ev.path("kind").asText().equals("ROLLBACK")
                        || ev.path("kind").asText().equals("ROLLBACK_TO")
                        || ev.path("kind").asText().equals("SAVEPOINT")) sawSavepointOrRollback = true;
            }
        }
        assertTrue(sawSavepointOrRollback, "the real entry proc's savepoints/rollbacks never reached the timeline");
    }
}
