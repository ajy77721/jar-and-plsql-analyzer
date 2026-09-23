package com.analyzer.storage.entity;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * PLAN-09 Increment E's mapper, against <b>the real corpus where there is one</b> and against
 * hand-written documents for the shapes the corpus does not contain.
 *
 * <p>The corpus half is not decoration. PLAN-11 exists because "it cannot be answered by reading
 * code; only the data knows" — six field paths change shape in real data and a mapper validated only
 * against fixtures its author wrote is validated against its author's assumptions. So the sweep over
 * {@code data/plsql-parse} runs whenever that tree is present and is skipped, loudly, when it is not,
 * rather than being replaced by a fixture that would pass either way.
 */
class PlsqlEntityMapperTest {

    private static final ObjectMapper M = new ObjectMapper();
    private final PlsqlEntityMapper mapper = new PlsqlEntityMapper(M);

    /** Collects every row so an assertion can be made about the whole mapping of one document. */
    private static final class Rows implements PlsqlEntityMapper.RowSink {
        final List<FlowNodeRow> nodes = new ArrayList<>();
        final List<FlowEdgeRow> edges = new ArrayList<>();
        final List<TableUsageRow> usages = new ArrayList<>();

        @Override public void node(FlowNodeRow r) { nodes.add(r); }

        @Override public void edge(FlowEdgeRow r) { edges.add(r); }

        @Override public void tableUsage(TableUsageRow r) { usages.add(r); }
    }

    private Rows map(String json) throws IOException {
        Rows rows = new Rows();
        try (InputStream in = new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8))) {
            mapper.map("A", in, rows);
        }
        return rows;
    }

    /* ─────────────────────────── the field rules ─────────────────────────── */

    @Test
    @DisplayName("one node document becomes one flow_node row and its children")
    void mapsTheSpine() throws Exception {
        Rows r = map("""
                {"nodeId":"S$P","name":"p","schema":"S","objectType":"PROCEDURE","depth":3,
                 "lineStart":1,"lineEnd":9,"linesOfCode":9,"sourceFile":"S.p.sql",
                 "parameters":[{"name":"a","dataType":"INT","direction":"IN","line":2}],
                 "statementSummary":{"IF":2,"SELECT":5},
                 "calls":[{"target":"S$Q","schema":"S","objectName":"q","type":"PROCEDURE",
                           "argCount":1,"count":2,"lines":[4,5]}],
                 "tables":[{"schema":"S","name":"t","objectType":"TABLE",
                            "operations":{"SELECT":[4],"UPDATE":[6]}}]}
                """);
        assertEquals(1, r.nodes.size());
        FlowNodeRow n = r.nodes.get(0);
        assertEquals("S$P", n.nodeId());
        assertEquals(3, n.depth());
        assertEquals(9, n.linesOfCode());
        assertEquals(List.of(new FlowNodeRow.Parameter(0, "a", "INT", "IN", 2)), n.parameters());
        // PLAN-11 §3: statementSummary is a child table of (type, count), never an enum.
        assertEquals(Set.of("IF", "SELECT"),
                n.statements().stream().map(FlowNodeRow.StatementCount::statementType)
                        .collect(java.util.stream.Collectors.toSet()));

        assertEquals(1, r.edges.size());
        assertEquals("PROCEDURE", r.edges.get(0).callType());
        assertEquals(List.of(4, 5), r.edges.get(0).lines());

        // One row per (table, operation): "which nodes WRITE table t" is a different question from
        // "which nodes touch it", and a single row carrying an operations object answers only the
        // second.
        assertEquals(2, r.usages.size());
        assertEquals(Set.of("SELECT", "UPDATE"),
                r.usages.stream().map(TableUsageRow::operation)
                        .collect(java.util.stream.Collectors.toSet()));
        assertEquals("S.T", r.usages.get(0).tableKey());
    }

    @Test
    @DisplayName("tableKey is case-normalised — the corpus writes the same table both ways")
    void tableKeyIsCaseNormalised() {
        // tables.json says ANALYZERDB.ERP_AUDIT_LOG; the same table inside a node document is
        // schema=ANALYZERDB, name=erp_audit_log. An index on the raw name answers one spelling.
        assertEquals("ANALYZERDB.ERP_AUDIT_LOG",
                TableUsageRow.keyOf("ANALYZERDB", "erp_audit_log"));
        assertEquals("ANALYZERDB.ERP_AUDIT_LOG",
                TableUsageRow.keyOf("analyzerdb", "ERP_AUDIT_LOG"));
        assertEquals("ERP_AUDIT_LOG", TableUsageRow.keyOf(null, "erp_audit_log"));
    }

    @Test
    @DisplayName("a null table name emits no row — PLAN-11 rule 4, 68 of 40,748 entries")
    void nullTableNameEmitsNothing() throws Exception {
        Rows r = map("""
                {"nodeId":"S$P","tables":[{"schema":"S","name":null,"operations":{"SELECT":[1]}},
                                          {"schema":null,"name":"t","operations":{"SELECT":[2]}}]}
                """);
        assertEquals(1, r.usages.size(), "the null-named entry must not become a table called null");
        assertEquals("T", r.usages.get(0).tableKey(), "an absent schema is not the string 'null'");
        assertNull(r.usages.get(0).tableSchema());
    }

    @Test
    @DisplayName("a null scalar emits nothing rather than a zero")
    void nullScalarsAreNotZero() throws Exception {
        Rows r = map("{\"nodeId\":\"S$P\",\"depth\":null,\"linesOfCode\":null,\"name\":null}");
        assertNull(r.nodes.get(0).depth(), "null depth must not become 0 — counts would drift");
        assertNull(r.nodes.get(0).linesOfCode());
        assertNull(r.nodes.get(0).name());
    }

    @Test
    @DisplayName("tables[] and directTables[] hold the same content and collapse to one row set")
    void directTablesAreDeduplicated() throws Exception {
        Rows r = map("""
                {"nodeId":"S$P",
                 "tables":[{"schema":"S","name":"t","operations":{"SELECT":[1]}}],
                 "directTables":[{"schema":"S","name":"t","operations":{"SELECT":[1]}}],
                 "subtreeTables":[{"schema":"S","name":"t","operations":{"SELECT":[1]},
                                   "totalUsageCount":3,"directUsageCount":1}]}
                """);
        // Two rows, not three: one DIRECT (collapsed from the two identical lists) and one SUBTREE.
        // Doubling DIRECT would make "how many nodes write this table" answer twice the truth.
        assertEquals(2, r.usages.size());
        assertEquals(1, r.usages.stream().filter(u -> TableUsageRow.DIRECT.equals(u.usage())).count());
        assertEquals(1, r.usages.stream().filter(u -> TableUsageRow.SUBTREE.equals(u.usage())).count());
    }

    @Test
    @DisplayName("an unobserved shape fails loudly rather than being coerced — rule 6")
    void unobservedShapeThrows() {
        // A string where every one of 712 node documents has an integer. Coercing it would produce
        // a row with a wrong or missing value; the failure of coercion is a missing row, which is
        // silent and indistinguishable from "this node touches nothing".
        EntityShapeException e = assertThrows(EntityShapeException.class,
                () -> map("{\"nodeId\":\"S$P\",\"depth\":\"three\"}"));
        assertTrue(e.getMessage().contains("depth"), e.getMessage());
        assertEquals(PlsqlEntityMapper.ARTIFACT, e.artifact());

        assertThrows(EntityShapeException.class,
                () -> map("{\"nodeId\":\"S$P\",\"parameters\":{\"a\":1}}"),
                "an object where the corpus only ever had an array is new information");
        assertThrows(EntityShapeException.class,
                () -> map("{\"nodeId\":\"S$P\",\"tables\":[{\"schema\":\"S\",\"name\":\"t\","
                          + "\"operations\":[\"SELECT\"]}]}"),
                "operations as an array is a shape the scan never saw");
        assertThrows(EntityShapeException.class, () -> map("{\"name\":\"p\"}"),
                "no nodeId means it is not a node, and everything downstream keys on it");
    }

    @Test
    @DisplayName("fields the slice does not model are skipped, not parsed")
    void unmappedFieldsAreSkipped() throws Exception {
        // cursors/dynamicSql/variables are real fields of a real node document and are deliberately
        // out of this slice (PLAN-08 Q61 owns them). They must not break the pass, and they must not
        // be materialised: skipChildren() is what keeps this streaming.
        Rows r = map("""
                {"nodeId":"S$P","variables":{"total":9,"plain":[{"name":"v"}]},
                 "cursors":[{"name":"c","query":"select 1"}],"dynamicSql":[],
                 "transactionScope":{"commits":1},"unknownFutureField":{"a":[1,2,{"b":3}]},
                 "tables":[{"schema":"S","name":"t","operations":{"SELECT":[1]}}]}
                """);
        assertEquals(1, r.nodes.size());
        assertEquals(1, r.usages.size());
    }

    /* ─────────────────────────── against the real corpus ─────────────────────────── */

    /**
     * Maps every {@code api/nodes/*.json} on this machine and asserts the invariants the whole
     * design rests on.
     *
     * <p>This is the test that would have caught each of the six hazards PLAN-11 found, because it
     * is the only one that sees data nobody wrote for it.
     */
    @Test
    @DisplayName("every real node document in data/plsql-parse maps without a shape failure")
    void mapsTheWholeCorpus() throws Exception {
        Path root = corpusRoot();
        assumeTrue(root != null, "no data/plsql-parse on this machine — skipping the corpus sweep. "
                                 + "A fixture in its place would pass either way.");

        List<Path> files = new ArrayList<>();
        try (Stream<Path> s = Files.walk(root)) {
            s.filter(Files::isRegularFile)
             .filter(p -> p.getFileName().toString().endsWith(".json"))
             .filter(p -> p.getParent().getFileName().toString().equals("nodes"))
             .forEach(files::add);
        }
        assumeTrue(!files.isEmpty(), "data/plsql-parse has no node documents");

        long nodes = 0, edges = 0, usages = 0;
        Set<String> statementTokens = new HashSet<>();
        Set<String> operations = new HashSet<>();
        for (Path f : files) {
            Rows r = new Rows();
            try (InputStream in = Files.newInputStream(f)) {
                mapper.map(f.getParent().getParent().getParent().getFileName().toString(), in, r);
            }
            nodes += r.nodes.size();
            edges += r.edges.size();
            usages += r.usages.size();
            for (FlowNodeRow n : r.nodes) {
                assertNotNull(n.nodeId(), f + " produced a node with no id");
                n.statements().forEach(s -> statementTokens.add(s.statementType()));
            }
            for (TableUsageRow u : r.usages) {
                assertNotNull(u.tableKey(), f + " produced a usage row with no table key");
                assertFalse(u.tableKey().contains("null"),
                        f + " produced a table key containing 'null': " + u.tableKey());
                assertEquals(u.tableKey(), u.tableKey().toUpperCase(java.util.Locale.ROOT),
                        "table keys must be normalised for case or the index answers one spelling");
                operations.add(u.operation());
            }
            for (FlowEdgeRow e : r.edges) {
                assertNotNull(e.fromNodeId());
                assertNotNull(e.toNodeId(), f + " produced an edge with no callee");
            }
        }

        assertEquals(files.size(), nodes, "one flow_node row per node document, no more and no less");
        assertTrue(usages > 0 && edges > 0, "the corpus must produce edges and table usages");
        // PLAN-11 §3 says the statement token set is 11-28 dialect-specific tokens and must never be
        // an enum. If this corpus already exceeds that range, the point is made rather than broken.
        assertTrue(statementTokens.size() > 28,
                "expected the open token set to exceed PLAN-11's 11-28 estimate; got "
                + statementTokens.size() + " — an enum would have dropped the rest silently");
        System.out.printf("[corpus] %d node documents -> %d flow_node, %d flow_edge, %d table_usage; "
                          + "%d statement tokens, operations=%s%n",
                files.size(), nodes, edges, usages, statementTokens.size(), operations);
    }

    /**
     * @return the PL/SQL parse tree, or null when this machine has none. Walks up from the working
     *         directory because the test runs from the module and the data sits at the repo root.
     */
    static Path corpusRoot() {
        Path p = Path.of("").toAbsolutePath();
        for (int i = 0; i < 4 && p != null; i++, p = p.getParent()) {
            Path candidate = p.resolve("data").resolve("plsql-parse");
            if (Files.isDirectory(candidate)) return candidate;
        }
        return null;
    }
}
