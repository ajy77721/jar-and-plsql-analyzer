package com.plsql.parser.flow;

import com.plsql.parser.model.JoinInfo;
import com.plsql.parser.model.TableOperationInfo;
import com.plsqlanalyzer.config.DbType;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Regression tests for the estate audit gaps fixed in the parser lane:
 *
 * <ul>
 *   <li>a catalog view definition that carries leading {@code --} / block comments before its
 *       {@code CREATE VIEW} header must still have its base tables and joins extracted
 *       ({@link ViewDefinitionAnalyzer});</li>
 *   <li>a trigger body's outbound calls are extracted by the dialect parser
 *       ({@link TriggerDiscoverer#extractCallsFromBody});</li>
 *   <li>SQL Server's INSERTED / DELETED transition pseudo-tables are not recorded as touched
 *       tables ({@link TriggerDiscoverer#extractDmlFromBody}).</li>
 * </ul>
 */
class AuditGapsParserTest {

    private final TriggerDiscoverer discoverer = new TriggerDiscoverer(null, null);

    /* ───────────────────────── views ───────────────────────── */

    @Test
    void commentedSchemaQualifiedJoinedViewExtractsBaseTablesAndJoins() {
        // The shape sys.sql_modules.definition returns for erp.vw_OrderSummary: a leading comment
        // block, a schema-qualified FROM, and two joins. Before the fix the leading comment made
        // the wrapper treat the whole statement as a bare SELECT body, double-wrapping it into an
        // unparseable DDL, so tableOperations/joins came back empty.
        String def = """
                -- ---------------------------------------------------------------- views (regular)
                CREATE VIEW erp.vw_OrderSummary AS
                    SELECT o.OrderId, o.CustomerId, c.Name AS CustomerName, c.Tier,
                           o.Total, o.Status, r.Name AS Region
                    FROM erp.SalesOrder o
                    INNER JOIN erp.Customer c ON c.CustomerId = o.CustomerId
                    LEFT JOIN erp.Region r ON r.RegionId = c.RegionId
                    WHERE o.IsDeleted = 0
                """;
        ViewDefinitionAnalyzer.ViewDefinitionResult r =
                ViewDefinitionAnalyzer.analyze(DbType.SQLSERVER, "vw_OrderSummary", def, false);

        List<String> tables = r.tableOperations.stream()
                .map(op -> (op.getSchema() == null ? "" : op.getSchema() + ".") + op.getTableName())
                .map(String::toUpperCase)
                .toList();
        assertTrue(tables.contains("ERP.SALESORDER"), "base table erp.SalesOrder: " + tables);
        assertTrue(tables.contains("ERP.CUSTOMER"), "base table erp.Customer: " + tables);
        assertTrue(tables.contains("ERP.REGION"), "base table erp.Region: " + tables);
        assertEquals(2, r.joins.size(), "INNER + LEFT join edges must be captured: " + r.joins);
    }

    @Test
    void commentedSingleTableViewExtractsItsBaseTable() {
        String def = """
                -- view over the reference table
                CREATE VIEW erp_ref.vw_Region AS
                    SELECT r.region_id, r.region_name, r.active
                    FROM erp_ref.RefRegion r
                    WHERE r.active = 1;
                """;
        ViewDefinitionAnalyzer.ViewDefinitionResult r =
                ViewDefinitionAnalyzer.analyze(DbType.SQLSERVER, "vw_Region", def, false);
        assertEquals(1, r.tableOperations.size());
        assertEquals("RefRegion", r.tableOperations.get(0).getTableName());
        assertEquals("erp_ref", r.tableOperations.get(0).getSchema());
    }

    @Test
    void blockCommentBeforeCreateViewIsAlsoHandled() {
        String def = "/* block comment */ CREATE VIEW erp.v AS SELECT 1 FROM erp.SalesOrder o";
        ViewDefinitionAnalyzer.ViewDefinitionResult r =
                ViewDefinitionAnalyzer.analyze(DbType.SQLSERVER, "v", def, false);
        assertEquals(1, r.tableOperations.size());
        assertEquals("SalesOrder", r.tableOperations.get(0).getTableName());
    }

    /* ───────────────────── trigger body calls ───────────────────── */

    @Test
    void sqlServerTriggerBodyCallsAreExtracted() {
        String body = """
                CREATE TRIGGER erp.trg_orders ON erp.Orders AFTER UPDATE AS
                BEGIN
                    SET NOCOUNT ON;
                    INSERT INTO erp.AuditTrail(Area) SELECT N'x' FROM inserted;
                    EXEC dbo.usp_ArchiveOrder @id;
                END;
                """;
        List<Map<String, Object>> calls = discoverer.extractCallsFromBody(body, DbType.SQLSERVER);
        assertEquals(1, calls.size(), "one EXEC call expected: " + calls);
        assertEquals("usp_ArchiveOrder", calls.get(0).get("objectName"));
        assertEquals("dbo", calls.get(0).get("schema"));
        assertEquals("EXTERNAL", calls.get(0).get("type"));
        assertEquals(1, calls.get(0).get("count"));
    }

    @Test
    void triggerBodyWithoutCallsYieldsEmptyList() {
        String body = "CREATE TRIGGER erp.trg ON erp.Orders AFTER INSERT AS BEGIN INSERT INTO "
                + "erp.AuditTrail(Area) SELECT N'x' FROM inserted; END;";
        assertTrue(discoverer.extractCallsFromBody(body, DbType.SQLSERVER).isEmpty());
    }

    /* ─────────────────── pseudo-table filtering ─────────────────── */

    @Test
    void sqlServerTransitionPseudoTablesAreNotTouchedTables() {
        String body = "CREATE TRIGGER erp.trg ON erp.JournalLine AFTER DELETE AS\nBEGIN\n"
                + "    INSERT INTO erp.AuditTrail(Area) SELECT N'x' FROM deleted d;\nEND;";
        List<Map<String, Object>> ops = discoverer.extractDmlFromBody(body, Collections.emptyMap());
        List<String> tables = ops.stream().map(o -> String.valueOf(o.get("tableName"))).toList();
        assertTrue(tables.contains("ERP.AUDITTRAIL"), "real target still captured: " + tables);
        assertFalse(tables.contains("DELETED"), "DELETED pseudo-table must be dropped: " + tables);
    }

    @Test
    void sqlServerInsertedPseudoTableIsNotATouchedTable() {
        String body = "CREATE TRIGGER erp.trg ON erp.PurchaseOrder AFTER INSERT AS\nBEGIN\n"
                + "    INSERT INTO erp.AuditTrail(Area) SELECT N'x' FROM inserted i;\nEND;";
        List<Map<String, Object>> ops = discoverer.extractDmlFromBody(body, Collections.emptyMap());
        List<String> tables = ops.stream().map(o -> String.valueOf(o.get("tableName"))).toList();
        assertFalse(tables.contains("INSERTED"), "INSERTED pseudo-table must be dropped: " + tables);
    }
}
