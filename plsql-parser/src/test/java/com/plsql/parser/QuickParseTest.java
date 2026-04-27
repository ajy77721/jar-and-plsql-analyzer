package com.plsql.parser;

import com.plsql.parser.model.*;
import java.nio.file.*;

public class QuickParseTest {
    public static void main(String[] args) throws Exception {
        PlSqlParserEngine engine = new PlSqlParserEngine();
        String sql = Files.readString(Path.of(args[0]));
        ParseResult result = engine.parseContent(sql, "test.sql");

        System.out.println("Objects: " + result.getObjects().size());
        System.out.println("Errors: " + (result.getErrors() != null ? result.getErrors().size() : 0));
        if (result.getErrors() != null) {
            for (String err : result.getErrors()) {
                System.out.println("  ERR: " + err);
            }
        }
        for (ParsedObject obj : result.getObjects()) {
            System.out.println("Object: " + obj.getName() + " type=" + obj.getType());
            if (obj.getTableOperations() != null) {
                System.out.println("  Object-level table ops: " + obj.getTableOperations().size());
                for (TableOperationInfo t : obj.getTableOperations()) {
                    System.out.println("    TABLE: " + t.getTableName() + " op=" + t.getOperation()
                        + " line=" + t.getLine() + " joins=" + (t.getJoins() != null ? t.getJoins().size() : 0));
                    if (t.getJoins() != null) {
                        for (JoinInfo j : t.getJoins()) {
                            System.out.println("      JOIN: " + j.getJoinType() + " -> " + j.getJoinedTable()
                                + " ON " + j.getCondition() + " line=" + j.getLine());
                        }
                    }
                }
            }
            if (obj.getStatements() != null) {
                System.out.println("  Object-level statements: " + obj.getStatements().size());
                for (StatementInfo s : obj.getStatements()) {
                    System.out.println("    STMT: " + s.getType() + " line=" + s.getLine() + " sql=" + (s.getSqlText() != null ? s.getSqlText().substring(0, Math.min(80, s.getSqlText().length())) : ""));
                }
            }
            if (obj.getCursors() != null) {
                System.out.println("  Object-level cursors: " + obj.getCursors().size());
                for (CursorInfo c : obj.getCursors()) {
                    System.out.println("    CURSOR: " + c.getName() + " forLoop=" + c.isForLoop() + " line=" + c.getLine());
                }
            }
            System.out.println("  Subprograms: " + obj.getSubprograms().size());
            for (SubprogramInfo sub : obj.getSubprograms()) {
                System.out.println("    " + sub.getType() + " " + sub.getName()
                    + " lines=" + sub.getLineStart() + "-" + sub.getLineEnd()
                    + " tables=" + (sub.getTableOperations() != null ? sub.getTableOperations().size() : 0)
                    + " calls=" + (sub.getCalls() != null ? sub.getCalls().size() : 0));
                if (sub.getCursors() != null && !sub.getCursors().isEmpty()) {
                    for (CursorInfo c : sub.getCursors()) {
                        System.out.println("      CURSOR: " + c.getName() + " forLoop=" + c.isForLoop()
                            + " refCursor=" + c.isRefCursor() + " line=" + c.getLine()
                            + " query=" + (c.getQuery() != null ? c.getQuery().substring(0, Math.min(80, c.getQuery().length())) : "null"));
                    }
                }
                if (sub.getStatements() != null) {
                    for (StatementInfo s : sub.getStatements()) {
                        String t = s.getType();
                        if ("OPEN_CURSOR".equals(t) || "FETCH".equals(t) || "BULK_COLLECT".equals(t)
                            || "CLOSE_CURSOR".equals(t) || "CURSOR_FOR_LOOP".equals(t)) {
                            System.out.println("      STMT: " + t + " line=" + s.getLine()
                                + " sql=" + (s.getSqlText() != null ? s.getSqlText().substring(0, Math.min(80, s.getSqlText().length())) : ""));
                        }
                    }
                }
                if (sub.getTableOperations() != null) {
                    for (TableOperationInfo t : sub.getTableOperations()) {
                        System.out.println("      TABLE: " + t.getTableName() + " op=" + t.getOperation()
                            + " joins=" + (t.getJoins() != null ? t.getJoins().size() : 0));
                        if (t.getJoins() != null) {
                            for (JoinInfo j : t.getJoins()) {
                                System.out.println("        JOIN: " + j.getJoinType() + " -> " + j.getJoinedTable()
                                    + " ON " + j.getCondition() + " line=" + j.getLine());
                            }
                        }
                    }
                }
            }
        }
    }
}
