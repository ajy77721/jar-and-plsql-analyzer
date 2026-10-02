package com.plsql.parser.flow;

import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.InMemoryArtifactStore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.plsql.parser.model.CallInfo;
import com.plsql.parser.model.FlowNode;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A discovered scheduled job becomes a first-class node with a SCHEDULES edge to its target
 * routine — asserted end-to-end against a real {@link ChunkedFlowWriter} over an in-memory store.
 *
 * <p>This is the crawl-side acceptance test for the scheduled-entry-point feature: per dialect the
 * discovered job map (name/schedule/enabled/target/type) is fed through
 * {@link JobSchedulerEmitter#emit} and the emitted JOB node, the SCHEDULES edge and the facet row
 * are all verified. A target routine node is written first, exactly as the BFS writes it.
 */
class JobSchedulerEmitterTest {

    private static final ObjectMapper M = new ObjectMapper();

    private record Fixture(InMemoryArtifactStore store, ChunkedFlowWriter writer, String buildDir)
            implements AutoCloseable {
        @Override public void close() {}
    }

    private Fixture newFixture() throws Exception {
        InMemoryArtifactStore store = new InMemoryArtifactStore();
        ChunkedFlowWriter writer = new ChunkedFlowWriter(store, ArtifactKey.of("ANALYSIS"));
        return new Fixture(store, writer, "ANALYSIS");
    }

    private void writeTargetProc(ChunkedFlowWriter writer, String schema, String name)
            throws Exception {
        FlowNode proc = new FlowNode();
        proc.setSchema(schema);
        proc.setObjectName(name);
        proc.setObjectType("PROCEDURE");
        proc.generateNodeId();
        writer.writeChunk(proc);
    }

    private Map<String, Object> job(String name, String schema, String schedule, Boolean enabled,
                                    String target, String type) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("name", name);
        m.put("schema", schema);
        m.put("schedule", schedule);
        m.put("enabled", enabled);
        m.put("target", target);
        m.put("type", type);
        List<CallInfo> calls = new ArrayList<>();
        CallInfo c = new CallInfo();
        c.setName(target.contains(".") ? target.substring(target.lastIndexOf('.') + 1) : target);
        calls.add(c);
        m.put("calls", calls);
        return m;
    }

    private List<JsonNode> readJsonFiles(InMemoryArtifactStore store, String pathFragment)
            throws Exception {
        List<JsonNode> out = new ArrayList<>();
        for (String key : store.keys()) {
            if (!key.contains(pathFragment)) continue;
            try (InputStream in = store.read(ArtifactKey.of(key.split("/")))) {
                out.add(M.readTree(in));
            }
        }
        return out;
    }

    @Test
    void emitsJobNodeSchedulesEdgeAndFacetRow() throws Exception {
        Fixture f = newFixture();
        writeTargetProc(f.writer, "ERP", "LOAD_ORDERS");
        Map<String, Object> j = job("NIGHTLY_LOAD", "ERP", "FREQ=DAILY;BYHOUR=2",
                true, "LOAD_ORDERS", "JOB");

        List<Map<String, Object>> rows = JobSchedulerEmitter.emit(
                List.of(j), f.writer, Set.of("LOAD_ORDERS"), new ArrayList<>(), 0);

        // ── facet row: job, schedule, enabled, target ──
        assertEquals(1, rows.size());
        Map<String, Object> row = rows.get(0);
        assertEquals("NIGHTLY_LOAD", row.get("name"));
        assertEquals("FREQ=DAILY;BYHOUR=2", row.get("schedule"));
        assertEquals(Boolean.TRUE, row.get("enabled"));
        assertEquals("LOAD_ORDERS", row.get("target"));
        assertNotNull(row.get("nodeId"));
        assertEquals("ERP$LOAD_ORDERS", row.get("targetNodeId"));

        // ── the JOB node was written ──
        boolean sawJobNode = false;
        for (JsonNode n : readJsonFiles(f.store, "/chunks/")) {
            if ("JOB".equals(n.path("objectType").asText())
                    && "NIGHTLY_LOAD".equals(n.path("objectName").asText())) {
                sawJobNode = true;
                assertEquals("FREQ=DAILY;BYHOUR=2", n.path("schedule").asText());
                assertTrue(n.path("jobEnabled").asBoolean());
                assertEquals("LOAD_ORDERS", n.path("jobTarget").asText());
            }
        }
        assertTrue(sawJobNode, "expected a JOB node chunk for NIGHTLY_LOAD");

        // ── the SCHEDULES edge was written job -> target ──
        boolean sawSchedules = false;
        for (JsonNode edges : readJsonFiles(f.store, "/edges/")) {
            for (JsonNode e : edges) {
                if ("SCHEDULES".equals(e.path("edgeType").asText())) {
                    sawSchedules = true;
                    assertEquals("ERP$JOB$NIGHTLY_LOAD", e.path("fromNodeId").asText());
                    assertEquals("ERP$LOAD_ORDERS", e.path("toNodeId").asText());
                }
            }
        }
        assertTrue(sawSchedules, "expected a SCHEDULES edge from the job to its target routine");
    }

    @Test
    void mysqlEventBecomesEventNodeWithSchedulesEdge() throws Exception {
        Fixture f = newFixture();
        writeTargetProc(f.writer, "erp", "erp_usp_refresh_mv");
        Map<String, Object> j = job("ev_refresh_customer_sales", "erp", "EVERY 1 DAY",
                Boolean.TRUE, "erp_usp_refresh_mv", "EVENT");

        List<Map<String, Object>> rows = JobSchedulerEmitter.emit(
                List.of(j), f.writer, Set.of("erp_usp_refresh_mv"), new ArrayList<>(), 0);

        assertEquals(1, rows.size());
        assertEquals("EVENT", rows.get(0).get("type"));

        boolean sawEventNode = false;
        for (JsonNode n : readJsonFiles(f.store, "/chunks/")) {
            if ("EVENT".equals(n.path("objectType").asText())) {
                sawEventNode = true;
                assertEquals("erp$EVENT$ev_refresh_customer_sales".toUpperCase(),
                        n.path("nodeId").asText());
            }
        }
        assertTrue(sawEventNode, "expected an EVENT node chunk");

        boolean sawSchedules = false;
        for (JsonNode edges : readJsonFiles(f.store, "/edges/")) {
            for (JsonNode e : edges) {
                if ("SCHEDULES".equals(e.path("edgeType").asText())) sawSchedules = true;
            }
        }
        assertTrue(sawSchedules, "expected the MySQL event to schedule its target procedure");
    }

    @Test
    void everyDialectJobYieldsNodeAndSchedulesEdge() throws Exception {
        // The per-dialect discovery is tested in JobFetchStrategyMockTest; this asserts the shared
        // emitter turns each dialect's discovered job into the node + SCHEDULES edge the AC requires.
        record Dialect(String label, String schema, String jobName, String target, String type) {}
        Dialect[] dialects = {
                new Dialect("oracle-dbms-scheduler", "ERP", "NIGHTLY_LOAD", "LOAD_ORDERS", "JOB"),
                new Dialect("sqlserver-agent", "dbo", "Nightly ETL", "usp_load_orders", "JOB"),
                new Dialect("postgres-pg-cron", "public", "pg_cron_job_7", "refresh_reporting", "JOB"),
                new Dialect("mysql-event", "erp", "ev_refresh", "erp_usp_refresh_mv", "EVENT"),
        };
        for (Dialect d : dialects) {
            Fixture f = newFixture();
            writeTargetProc(f.writer, d.schema, d.target);
            Map<String, Object> j = job(d.jobName, d.schema, "EVERY 1 DAY", true, d.target, d.type);

            List<Map<String, Object>> rows = JobSchedulerEmitter.emit(
                    List.of(j), f.writer, Set.of(d.target), new ArrayList<>(), 0);

            assertEquals(1, rows.size(), d.label + ": one facet row");
            assertNotNull(rows.get(0).get("nodeId"), d.label + ": job node id");
            assertEquals(buildNodeId(d.schema, null, d.target),
                    rows.get(0).get("targetNodeId"), d.label + ": target node id");

            boolean node = false;
            for (JsonNode n : readJsonFiles(f.store, "/chunks/")) {
                if (d.type.equals(n.path("objectType").asText())) node = true;
            }
            assertTrue(node, d.label + ": expected a " + d.type + " node");

            boolean edge = false;
            for (JsonNode edges : readJsonFiles(f.store, "/edges/")) {
                for (JsonNode e : edges) {
                    if ("SCHEDULES".equals(e.path("edgeType").asText())) edge = true;
                }
            }
            assertTrue(edge, d.label + ": expected a SCHEDULES edge to " + d.target);
        }
    }

    private static String buildNodeId(String schema, String pkg, String object) {
        StringBuilder sb = new StringBuilder();
        if (schema != null && !schema.isEmpty()) sb.append(schema);
        if (pkg != null && !pkg.isEmpty()) sb.append("$").append(pkg);
        if (object != null && !object.isEmpty()) sb.append("$").append(object);
        return sb.toString().toUpperCase().replaceAll("[^A-Z0-9_$]", "_");
    }

    @Test
    void jobWithUnknownTarget_emitsNodeButNoDanglingEdge() throws Exception {
        Fixture f = newFixture();
        Map<String, Object> j = job("ORPHAN_JOB", "ERP", "EVERY 1 DAY", true, "MISSING_PROC", "JOB");

        List<Map<String, Object>> rows = JobSchedulerEmitter.emit(
                List.of(j), f.writer, Set.of("SOME_OTHER_PROC"), new ArrayList<>(), 0);

        assertEquals(1, rows.size());
        assertNull(rows.get(0).get("targetNodeId"));
        for (JsonNode edges : readJsonFiles(f.store, "/edges/")) {
            for (JsonNode e : edges) {
                assertNotEquals("SCHEDULES", e.path("edgeType").asText(),
                        "no SCHEDULES edge should point at a routine absent from the crawl");
            }
        }
    }
}
