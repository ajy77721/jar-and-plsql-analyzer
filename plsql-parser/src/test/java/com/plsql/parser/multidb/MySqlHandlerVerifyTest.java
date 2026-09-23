package com.plsql.parser.multidb;

import com.plsql.parser.model.ParseResult;
import com.plsql.parser.model.ParsedObject;
import com.plsqlanalyzer.config.DbType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Manual verification test for audit #9 (MySQL exception/condition handlers).
 * Deleted after use — see task report for the captured output.
 */
public class MySqlHandlerVerifyTest {
    @Test
    void handlerIsCapturedAndBodyIsTagged() {
        var p = StoredProcedureParserFactory.get(DbType.MYSQL);
        ParseResult r = p.parse(
                "CREATE PROCEDURE do_audit()\n" +
                "BEGIN\n" +
                "  DECLARE EXIT HANDLER FOR SQLEXCEPTION\n" +
                "  BEGIN\n" +
                "    ROLLBACK;\n" +
                "    CALL erp_usp_audit(1);\n" +
                "  END;\n" +
                "  INSERT INTO orders(id) VALUES (1);\n" +
                "END", "do_audit", "app");

        ParsedObject obj = r.getObjects().get(0);
        System.out.println("exceptionHandlers=" + obj.getExceptionHandlers().size());
        obj.getExceptionHandlers().forEach(h -> System.out.println(
                "  handler: name=" + h.getExceptionName() + " line=" + h.getLine()
                + " statementsCount=" + h.getStatementsCount()
                + " calls=" + h.getCalls().size()
                + " tableOps=" + h.getTableOperations().size()));

        System.out.println("top-level statements=" + obj.getStatements().size());
        obj.getStatements().forEach(s -> System.out.println("  " + s.getType() + " line=" + s.getLine()));
        System.out.println("top-level calls=" + obj.getCalls().size());
        obj.getCalls().forEach(c -> System.out.println("  " + c.getName() + " line=" + c.getLine()));
        System.out.println("top-level tableOps=" + obj.getTableOperations().size());
        obj.getTableOperations().forEach(t -> System.out.println("  " + t.getOperation() + " " + t.getTableName()));

        assertEquals(1, obj.getExceptionHandlers().size());
        var h = obj.getExceptionHandlers().get(0);
        assertTrue(h.getExceptionName().contains("EXIT"));
        assertTrue(h.getExceptionName().contains("SQLEXCEPTION"));
        assertEquals(1, h.getCalls().size(), "handler's CALL must be attributed to the handler, not top-level");
        assertEquals("erp_usp_audit", h.getCalls().get(0).getName());

        // Top-level calls still include the handler's CALL too — this mirrors Oracle's own
        // established addCall()/addTableOperation() pattern (PlSqlAnalysisVisitor.java:207,340),
        // which adds to both the enclosing scope's list AND currentExceptionHandler's own list.
        // The handler's list is what actually resolves the ambiguity (audit #9's point): a reader
        // can now tell this CALL came from a handler by finding it in exceptionHandlers[0].calls.
        assertTrue(obj.getCalls().stream().anyMatch(c -> "erp_usp_audit".equalsIgnoreCase(c.getName())));

        // the routine's own INSERT is still recorded as a normal top-level table op
        assertTrue(obj.getTableOperations().stream().anyMatch(t -> "orders".equalsIgnoreCase(t.getTableName())));
    }
}
