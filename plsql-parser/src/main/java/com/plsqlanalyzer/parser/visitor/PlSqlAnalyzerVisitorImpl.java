package com.plsqlanalyzer.parser.visitor;

import com.plsqlanalyzer.parser.grammar.PlSqlAnalyzerBaseVisitor;
import com.plsqlanalyzer.parser.grammar.PlSqlAnalyzerParser;
import com.plsqlanalyzer.parser.model.*;
import com.plsqlanalyzer.parser.service.SqlAnalyzer;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Visits the custom PlSqlAnalyzer parse tree and extracts:
 * 1. Package/procedure/function declarations with line ranges
 * 2. Call statements with qualified names and line numbers
 * 3. SQL DML with operation type, table name, line number, full SQL text
 * 4. EXECUTE IMMEDIATE occurrences (dynamic SQL)
 */
public class PlSqlAnalyzerVisitorImpl extends PlSqlAnalyzerBaseVisitor<Void> {

    private static final Logger log = LoggerFactory.getLogger(PlSqlAnalyzerVisitorImpl.class);

    private final PlsqlUnit unit = new PlsqlUnit();
    private final Deque<PlsqlProcedure> procStack = new ArrayDeque<>();
    private final SqlAnalyzer sqlAnalyzer = new SqlAnalyzer();

    public PlsqlUnit getUnit() {
        return unit;
    }

    // ---- Top-level constructs ----

    @Override
    public Void visitCreatePackageBody(PlSqlAnalyzerParser.CreatePackageBodyContext ctx) {
        String name = getQualifiedText(ctx.qualifiedName());
        unit.setName(extractSimpleName(name));
        unit.setSchemaName(extractSchemaName(name));
        unit.setUnitType(PlsqlUnitType.PACKAGE_BODY);
        unit.setStartLine(ctx.getStart().getLine());
        unit.setEndLine(ctx.getStop().getLine());
        return super.visitCreatePackageBody(ctx);
    }

    @Override
    public Void visitCreatePackageSpec(PlSqlAnalyzerParser.CreatePackageSpecContext ctx) {
        String name = getQualifiedText(ctx.qualifiedName());
        unit.setName(extractSimpleName(name));
        unit.setSchemaName(extractSchemaName(name));
        unit.setUnitType(PlsqlUnitType.PACKAGE_SPEC);
        unit.setStartLine(ctx.getStart().getLine());
        unit.setEndLine(ctx.getStop().getLine());
        return super.visitCreatePackageSpec(ctx);
    }

    @Override
    public Void visitCreateProcedure(PlSqlAnalyzerParser.CreateProcedureContext ctx) {
        String name = getQualifiedText(ctx.qualifiedName());
        unit.setName(extractSimpleName(name));
        unit.setSchemaName(extractSchemaName(name));
        unit.setUnitType(PlsqlUnitType.PROCEDURE);
        unit.setStartLine(ctx.getStart().getLine());
        unit.setEndLine(ctx.getStop().getLine());

        PlsqlProcedure proc = new PlsqlProcedure(
                extractSimpleName(name), PlsqlUnitType.PROCEDURE,
                ctx.getStart().getLine(), ctx.getStop().getLine());
        if (ctx.parameterList() != null) extractParameters(ctx.parameterList(), proc);
        unit.getProcedures().add(proc);
        procStack.push(proc);
        super.visitCreateProcedure(ctx);
        procStack.poll();
        return null;
    }

    @Override
    public Void visitCreateFunction(PlSqlAnalyzerParser.CreateFunctionContext ctx) {
        String name = getQualifiedText(ctx.qualifiedName());
        unit.setName(extractSimpleName(name));
        unit.setSchemaName(extractSchemaName(name));
        unit.setUnitType(PlsqlUnitType.FUNCTION);
        unit.setStartLine(ctx.getStart().getLine());
        unit.setEndLine(ctx.getStop().getLine());

        PlsqlProcedure proc = new PlsqlProcedure(
                extractSimpleName(name), PlsqlUnitType.FUNCTION,
                ctx.getStart().getLine(), ctx.getStop().getLine());
        if (ctx.parameterList() != null) extractParameters(ctx.parameterList(), proc);
        unit.getProcedures().add(proc);
        procStack.push(proc);
        super.visitCreateFunction(ctx);
        procStack.poll();
        return null;
    }

    @Override
    public Void visitCreateTrigger(PlSqlAnalyzerParser.CreateTriggerContext ctx) {
        // First qualifiedName is trigger name, second is table name
        var names = ctx.qualifiedName();
        String triggerName = getQualifiedText(names.get(0));
        unit.setName(extractSimpleName(triggerName));
        unit.setSchemaName(extractSchemaName(triggerName));
        unit.setUnitType(PlsqlUnitType.TRIGGER);
        unit.setStartLine(ctx.getStart().getLine());
        unit.setEndLine(ctx.getStop().getLine());

        PlsqlProcedure proc = new PlsqlProcedure(
                extractSimpleName(triggerName), PlsqlUnitType.TRIGGER,
                ctx.getStart().getLine(), ctx.getStop().getLine());
        unit.getProcedures().add(proc);

        // Capture the table the trigger is on as a table reference
        if (names.size() > 1) {
            String tableName = getQualifiedText(names.get(1));
            TableReference tableRef = new TableReference();
            tableRef.setTableName(extractSimpleName(tableName).toUpperCase());
            tableRef.setSchemaName(extractSchemaName(tableName));
            // Determine operation from trigger events
            tableRef.setOperation(determineTriggerOperation(ctx.triggerTimingClause()));
            tableRef.setLineNumber(ctx.getStart().getLine());
            proc.getTableReferences().add(tableRef);
        }

        procStack.push(proc);
        super.visitCreateTrigger(ctx);
        procStack.poll();
        return null;
    }

    // ---- Package body procedure/function ----

    @Override
    public Void visitProcedureBody(PlSqlAnalyzerParser.ProcedureBodyContext ctx) {
        String name = ctx.identifier(0).getText().toUpperCase();
        PlsqlProcedure proc = new PlsqlProcedure(
                name, PlsqlUnitType.PROCEDURE,
                ctx.getStart().getLine(), ctx.getStop().getLine());
        if (ctx.parameterList() != null) extractParameters(ctx.parameterList(), proc);
        unit.getProcedures().add(proc);
        procStack.push(proc);
        super.visitProcedureBody(ctx);
        procStack.poll();
        return null;
    }

    @Override
    public Void visitFunctionBody(PlSqlAnalyzerParser.FunctionBodyContext ctx) {
        String name = ctx.identifier(0).getText().toUpperCase();
        PlsqlProcedure proc = new PlsqlProcedure(
                name, PlsqlUnitType.FUNCTION,
                ctx.getStart().getLine(), ctx.getStop().getLine());
        if (ctx.parameterList() != null) extractParameters(ctx.parameterList(), proc);
        unit.getProcedures().add(proc);
        procStack.push(proc);
        super.visitFunctionBody(ctx);
        procStack.poll();
        return null;
    }

    // ---- Call or Assignment (critical for call tracing) ----

    @Override
    public Void visitCallOrAssignmentStatement(PlSqlAnalyzerParser.CallOrAssignmentStatementContext ctx) {
        PlsqlProcedure currentProc = procStack.peek();
        if (currentProc == null) return super.visitCallOrAssignmentStatement(ctx);

        String qualifiedName = getQualifiedText(ctx.qualifiedName());
        int line = ctx.getStart().getLine();

        boolean hasAssign = ctx.ASSIGN() != null;

        if (hasAssign) {
            // var := expression — scan the RHS expression for function calls
            // e.g. v_result := inf_gam_api.gam_pack_error(args);
            //      v_flag := pkg.func(a) + other_pkg.func2(b);
            if (ctx.expression() != null) {
                extractCallsFromExpression(ctx.expression(), currentProc);
                // Also check for sequence references like v_id := my_seq.NEXTVAL
                extractSequenceFromExpression(currentProc, ctx.expression());
            }
        } else {
            // This is a procedure call: qualifiedName(args)?;
            ProcedureCall call = buildProcedureCall(qualifiedName, line, false);
            currentProc.getCalls().add(call);
        }

        return super.visitCallOrAssignmentStatement(ctx);
    }

    // ---- Expression-bearing statements: extract function calls from conditions/values ----

    @Override
    public Void visitIfStatement(PlSqlAnalyzerParser.IfStatementContext ctx) {
        PlsqlProcedure currentProc = procStack.peek();
        if (currentProc != null) {
            // IF expression THEN ... (ELSIF expression THEN ...)*
            for (var expr : ctx.expression()) {
                extractCallsFromExpression(expr, currentProc);
            }
        }
        return super.visitIfStatement(ctx);
    }

    @Override
    public Void visitWhileLoopStatement(PlSqlAnalyzerParser.WhileLoopStatementContext ctx) {
        PlsqlProcedure currentProc = procStack.peek();
        if (currentProc != null && ctx.expression() != null) {
            extractCallsFromExpression(ctx.expression(), currentProc);
        }
        return super.visitWhileLoopStatement(ctx);
    }

    @Override
    public Void visitCaseStatement(PlSqlAnalyzerParser.CaseStatementContext ctx) {
        PlsqlProcedure currentProc = procStack.peek();
        if (currentProc != null) {
            for (var expr : ctx.expression()) {
                extractCallsFromExpression(expr, currentProc);
            }
        }
        return super.visitCaseStatement(ctx);
    }

    @Override
    public Void visitReturnStatement(PlSqlAnalyzerParser.ReturnStatementContext ctx) {
        PlsqlProcedure currentProc = procStack.peek();
        if (currentProc != null && ctx.expression() != null) {
            extractCallsFromExpression(ctx.expression(), currentProc);
        }
        return super.visitReturnStatement(ctx);
    }

    @Override
    public Void visitExitStatement(PlSqlAnalyzerParser.ExitStatementContext ctx) {
        PlsqlProcedure currentProc = procStack.peek();
        if (currentProc != null && ctx.expression() != null) {
            extractCallsFromExpression(ctx.expression(), currentProc);
        }
        return super.visitExitStatement(ctx);
    }

    @Override
    public Void visitContinueStatement(PlSqlAnalyzerParser.ContinueStatementContext ctx) {
        PlsqlProcedure currentProc = procStack.peek();
        if (currentProc != null && ctx.expression() != null) {
            extractCallsFromExpression(ctx.expression(), currentProc);
        }
        return super.visitContinueStatement(ctx);
    }

    @Override
    public Void visitPipeRowStatement(PlSqlAnalyzerParser.PipeRowStatementContext ctx) {
        PlsqlProcedure currentProc = procStack.peek();
        if (currentProc != null && ctx.expression() != null) {
            extractCallsFromExpression(ctx.expression(), currentProc);
        }
        return super.visitPipeRowStatement(ctx);
    }

    // ---- SQL DML Statements ----

    @Override
    public Void visitSelectIntoStatement(PlSqlAnalyzerParser.SelectIntoStatementContext ctx) {
        PlsqlProcedure currentProc = procStack.peek();
        if (currentProc == null) return super.visitSelectIntoStatement(ctx);

        int line = ctx.getStart().getLine();
        String rawSql = getFullText(ctx);

        // Extract table references from the FROM clause
        if (ctx.tableReferenceList() != null) {
            for (var tableRefCtx : ctx.tableReferenceList().tableReference()) {
                addTableRef(currentProc, tableRefCtx, SqlOperationType.SELECT, line);
            }
        }

        // Feed to JSqlParser for WHERE clause extraction + join info + sequences
        analyzeWithJSqlParser(currentProc, rawSql, SqlOperationType.SELECT, line);

        // Also extract sequences from the raw SQL text
        extractSequenceReferences(currentProc, rawSql, line);

        return null; // Don't visit children again
    }

    @Override
    public Void visitInsertStatement(PlSqlAnalyzerParser.InsertStatementContext ctx) {
        PlsqlProcedure currentProc = procStack.peek();
        if (currentProc == null) return super.visitInsertStatement(ctx);

        int line = ctx.getStart().getLine();
        if (ctx.tableReference() != null) {
            addTableRef(currentProc, ctx.tableReference(), SqlOperationType.INSERT, line);
        }
        String rawSql = getFullText(ctx);
        analyzeWithJSqlParser(currentProc, rawSql, SqlOperationType.INSERT, line);
        extractSequenceReferences(currentProc, rawSql, line);
        return null;
    }

    @Override
    public Void visitUpdateStatement(PlSqlAnalyzerParser.UpdateStatementContext ctx) {
        PlsqlProcedure currentProc = procStack.peek();
        if (currentProc == null) return super.visitUpdateStatement(ctx);

        int line = ctx.getStart().getLine();
        if (ctx.tableReference() != null) {
            addTableRef(currentProc, ctx.tableReference(), SqlOperationType.UPDATE, line);
        }
        String rawSqlU = getFullText(ctx);
        analyzeWithJSqlParser(currentProc, rawSqlU, SqlOperationType.UPDATE, line);
        extractSequenceReferences(currentProc, rawSqlU, line);
        return null;
    }

    @Override
    public Void visitDeleteStatement(PlSqlAnalyzerParser.DeleteStatementContext ctx) {
        PlsqlProcedure currentProc = procStack.peek();
        if (currentProc == null) return super.visitDeleteStatement(ctx);

        int line = ctx.getStart().getLine();
        if (ctx.tableReference() != null) {
            addTableRef(currentProc, ctx.tableReference(), SqlOperationType.DELETE, line);
        }
        analyzeWithJSqlParser(currentProc, getFullText(ctx), SqlOperationType.DELETE, line);
        return null;
    }

    @Override
    public Void visitMergeStatement(PlSqlAnalyzerParser.MergeStatementContext ctx) {
        PlsqlProcedure currentProc = procStack.peek();
        if (currentProc == null) return super.visitMergeStatement(ctx);

        int line = ctx.getStart().getLine();
        if (ctx.tableReference() != null) {
            addTableRef(currentProc, ctx.tableReference(), SqlOperationType.MERGE, line);
        }
        String rawSqlM = getFullText(ctx);
        analyzeWithJSqlParser(currentProc, rawSqlM, SqlOperationType.MERGE, line);
        extractSequenceReferences(currentProc, rawSqlM, line);
        return null;
    }

    // ---- EXECUTE IMMEDIATE (dynamic SQL) ----

    @Override
    public Void visitExecuteImmediateStatement(PlSqlAnalyzerParser.ExecuteImmediateStatementContext ctx) {
        PlsqlProcedure currentProc = procStack.peek();
        if (currentProc == null) return super.visitExecuteImmediateStatement(ctx);

        int line = ctx.getStart().getLine();

        // Mark as dynamic SQL call
        ProcedureCall dynCall = new ProcedureCall(null, null, "EXECUTE_IMMEDIATE", line, true);
        currentProc.getCalls().add(dynCall);

        // Try to extract the SQL string if it's a literal
        if (ctx.expression() != null && !ctx.expression().isEmpty()) {
            String exprText = ctx.expression(0).getText();
            // If it's a string literal, try to parse the SQL inside
            if (exprText.startsWith("'") && exprText.endsWith("'")) {
                String innerSql = exprText.substring(1, exprText.length() - 1)
                        .replace("''", "'");
                try {
                    SqlAnalysisResult result = sqlAnalyzer.analyze(innerSql, line);
                    if (result.getOperationType() != null) {
                        currentProc.getSqlStatements().add(result);
                        currentProc.getTableReferences().addAll(result.getTableReferences());
                    }
                } catch (Exception e) {
                    log.debug("Could not parse dynamic SQL at line {}: {}", line, e.getMessage());
                }
            }
        }

        return null;
    }

    // ---- Cursor declarations and operations ----

    @Override
    public Void visitCursorDeclaration(PlSqlAnalyzerParser.CursorDeclarationContext ctx) {
        PlsqlProcedure currentProc = procStack.peek();
        if (currentProc != null) {
            String cursorName = ctx.identifier().getText().toUpperCase();
            int line = ctx.getStart().getLine();
            String queryText = null;
            if (ctx.selectStatement() != null) {
                queryText = getFullText(ctx.selectStatement()).trim();
            }
            currentProc.getCursorInfos().add(
                    new CursorInfo(cursorName, "EXPLICIT", "DECLARE", queryText, line));
            log.debug("Cursor declaration at line {}: {} {}", line, cursorName,
                    queryText != null ? "(has query)" : "(no query)");
        }
        return super.visitCursorDeclaration(ctx);
    }

    @Override
    public Void visitOpenStatement(PlSqlAnalyzerParser.OpenStatementContext ctx) {
        PlsqlProcedure currentProc = procStack.peek();
        if (currentProc != null) {
            String cursorName = ctx.identifier().getText().toUpperCase();
            int line = ctx.getStart().getLine();
            String queryText = null;
            String cursorType = "EXPLICIT";
            if (ctx.FOR() != null) {
                cursorType = "OPEN_FOR";
                if (ctx.selectStatement() != null) {
                    queryText = getFullText(ctx.selectStatement()).trim();
                } else if (ctx.expression() != null) {
                    queryText = getFullText(ctx.expression()).trim();
                }
            }
            currentProc.getCursorInfos().add(
                    new CursorInfo(cursorName, cursorType, "OPEN", queryText, line));
        }
        return super.visitOpenStatement(ctx);
    }

    @Override
    public Void visitFetchStatement(PlSqlAnalyzerParser.FetchStatementContext ctx) {
        PlsqlProcedure currentProc = procStack.peek();
        if (currentProc != null) {
            String cursorName = ctx.identifier().getText().toUpperCase();
            int line = ctx.getStart().getLine();
            boolean bulkCollect = ctx.BULK() != null;
            currentProc.getCursorInfos().add(
                    new CursorInfo(cursorName, "EXPLICIT", bulkCollect ? "FETCH_BULK" : "FETCH", null, line));
        }
        return super.visitFetchStatement(ctx);
    }

    @Override
    public Void visitCloseStatement(PlSqlAnalyzerParser.CloseStatementContext ctx) {
        PlsqlProcedure currentProc = procStack.peek();
        if (currentProc != null) {
            String cursorName = ctx.identifier().getText().toUpperCase();
            int line = ctx.getStart().getLine();
            currentProc.getCursorInfos().add(
                    new CursorInfo(cursorName, "EXPLICIT", "CLOSE", null, line));
        }
        return super.visitCloseStatement(ctx);
    }

    @Override
    public Void visitCursorForLoopStatement(PlSqlAnalyzerParser.CursorForLoopStatementContext ctx) {
        PlsqlProcedure currentProc = procStack.peek();
        if (currentProc != null) {
            int line = ctx.getStart().getLine();
            String cursorName = null;
            String queryText = null;
            String cursorType = "FOR_LOOP";
            if (ctx.qualifiedName() != null) {
                cursorName = getFullText(ctx.qualifiedName()).trim().toUpperCase();
            }
            if (ctx.inlineSelect() != null) {
                queryText = getFullText(ctx.inlineSelect()).trim();
                if (cursorName == null) cursorName = "(INLINE)";
            }
            if (cursorName != null) {
                currentProc.getCursorInfos().add(
                        new CursorInfo(cursorName, cursorType, "FOR_LOOP", queryText, line));
            }
        }
        return super.visitCursorForLoopStatement(ctx);
    }

    // ---- Expression call extraction ----

    /**
     * Scan an expression parse tree for function/procedure calls.
     * Looks for patterns like: identifier DOT identifier (DOT identifier)? LPAREN
     * This catches calls in assignments, IF conditions, WHILE conditions, RETURN values, etc.
     *
     * We collect all terminal tokens in order, then scan for the pattern:
     *   IDENTIFIER/keyword DOT IDENTIFIER/keyword (DOT IDENTIFIER/keyword)* LPAREN
     * to detect qualified function calls like pkg.func(args) or schema.pkg.func(args).
     */
    private void extractCallsFromExpression(ParserRuleContext exprCtx, PlsqlProcedure currentProc) {
        if (exprCtx == null) return;

        // Collect all terminal tokens from the expression in order
        List<Token> tokens = new ArrayList<>();
        collectTokens(exprCtx, tokens);
        if (tokens.isEmpty()) return;

        // Known SQL/PL-SQL built-in functions to SKIP (not real cross-package calls)
        Set<String> builtins = BUILTIN_FUNCTIONS;

        // Scan for pattern: name DOT name (DOT name)? LPAREN
        // This catches pkg.func(...), schema.pkg.func(...)
        // Single-name calls (func(...)) are already handled by callOrAssignmentStatement visitor
        for (int i = 0; i < tokens.size(); i++) {
            Token t = tokens.get(i);
            if (!isIdentifierToken(t)) continue;

            // Look ahead for DOT IDENTIFIER (DOT IDENTIFIER)? LPAREN
            List<String> parts = new ArrayList<>();
            parts.add(t.getText());
            int j = i + 1;

            while (j + 1 < tokens.size()
                    && tokens.get(j).getType() == PlSqlAnalyzerParser.DOT
                    && isIdentifierToken(tokens.get(j + 1))) {
                parts.add(tokens.get(j + 1).getText());
                j += 2;
            }

            // Must be followed by LPAREN to be a call
            if (j < tokens.size() && tokens.get(j).getType() == PlSqlAnalyzerParser.LPAREN) {
                String qualifiedName = String.join(".", parts);
                String funcName = parts.get(parts.size() - 1).toUpperCase();
                String firstName = parts.get(0).toUpperCase();

                // Skip SQL built-in functions and common noise
                if (builtins.contains(funcName) || builtins.contains(firstName)) {
                    i = j;
                    continue;
                }

                // Skip single-part names that look like PL/SQL constructs, not function calls
                // (e.g., SUBSTR, NVL are caught by builtins; but also skip bare type casts etc.)
                if (parts.size() == 1 && SKIP_SINGLE_NAMES.contains(firstName)) {
                    i = j;
                    continue;
                }

                // Avoid duplicating calls already captured as standalone statements
                int callLine = t.getLine();
                boolean alreadyCaptured = currentProc.getCalls().stream()
                        .anyMatch(c -> c.getLineNumber() == callLine
                                && c.getQualifiedName().equalsIgnoreCase(qualifiedName));
                if (!alreadyCaptured) {
                    ProcedureCall call = buildProcedureCall(qualifiedName, callLine, false);
                    currentProc.getCalls().add(call);
                    log.debug("Extracted call from expression at line {}: {}", callLine, qualifiedName);
                }
                i = j; // advance past the matched tokens
            }
        }
    }

    /** Recursively collect all terminal tokens from a parse tree node */
    private void collectTokens(ParseTree node, List<Token> tokens) {
        if (node instanceof TerminalNode) {
            Token t = ((TerminalNode) node).getSymbol();
            // Skip hidden channel tokens (whitespace, comments)
            if (t.getChannel() == Token.DEFAULT_CHANNEL) {
                tokens.add(t);
            }
        } else {
            for (int i = 0; i < node.getChildCount(); i++) {
                collectTokens(node.getChild(i), tokens);
            }
        }
    }

    /** Check if a token is an identifier or a keyword that could be used as a name */
    private boolean isIdentifierToken(Token t) {
        int type = t.getType();
        return type == PlSqlAnalyzerParser.IDENTIFIER
                || type == PlSqlAnalyzerParser.QUOTED_IDENTIFIER
                // Keywords that can be used as identifiers (unreservedKeyword rule)
                || type == PlSqlAnalyzerParser.NAME
                || type == PlSqlAnalyzerParser.TYPE
                || type == PlSqlAnalyzerParser.BODY
                || type == PlSqlAnalyzerParser.REPLACE
                || type == PlSqlAnalyzerParser.WORK
                || type == PlSqlAnalyzerParser.LIMIT
                || type == PlSqlAnalyzerParser.SAVE
                || type == PlSqlAnalyzerParser.EXCEPTIONS
                || type == PlSqlAnalyzerParser.REVERSE
                || type == PlSqlAnalyzerParser.CONSTANT
                || type == PlSqlAnalyzerParser.EACH
                || type == PlSqlAnalyzerParser.INSTEAD
                || type == PlSqlAnalyzerParser.SAVEPOINT
                || type == PlSqlAnalyzerParser.RESULT
                || type == PlSqlAnalyzerParser.ROW
                || type == PlSqlAnalyzerParser.NOCOPY
                || type == PlSqlAnalyzerParser.OF
                || type == PlSqlAnalyzerParser.CONTINUE
                || type == PlSqlAnalyzerParser.EDITIONABLE
                || type == PlSqlAnalyzerParser.NONEDITIONABLE
                // Common PL/SQL keywords used as package/procedure names
                || type == PlSqlAnalyzerParser.OPEN
                || type == PlSqlAnalyzerParser.CLOSE
                || type == PlSqlAnalyzerParser.DELETE
                || type == PlSqlAnalyzerParser.UPDATE
                || type == PlSqlAnalyzerParser.INSERT;
    }

    /** SQL/PL-SQL built-in functions that should NOT be treated as cross-package calls */
    private static final Set<String> BUILTIN_FUNCTIONS = Set.of(
            "NVL", "NVL2", "DECODE", "COALESCE", "NULLIF",
            "TO_CHAR", "TO_DATE", "TO_NUMBER", "TO_TIMESTAMP", "TO_CLOB", "TO_BLOB",
            "SUBSTR", "INSTR", "LENGTH", "TRIM", "LTRIM", "RTRIM", "LPAD", "RPAD",
            "UPPER", "LOWER", "INITCAP", "REPLACE", "TRANSLATE",
            "ROUND", "TRUNC", "MOD", "ABS", "SIGN", "CEIL", "FLOOR", "POWER", "SQRT",
            "SYSDATE", "SYSTIMESTAMP", "CURRENT_DATE", "CURRENT_TIMESTAMP",
            "ADD_MONTHS", "MONTHS_BETWEEN", "LAST_DAY", "NEXT_DAY", "EXTRACT",
            "COUNT", "SUM", "AVG", "MIN", "MAX", "LISTAGG",
            "CAST", "CONVERT", "VALIDATE_CONVERSION", "GREATEST", "LEAST",
            "ROW_NUMBER", "RANK", "DENSE_RANK", "LEAD", "LAG",
            "REGEXP_LIKE", "REGEXP_SUBSTR", "REGEXP_REPLACE", "REGEXP_INSTR",
            "DBMS_OUTPUT", "RAISE_APPLICATION_ERROR", "UTL_FILE", "UTL_RAW",
            "SYS_CONTEXT", "USERENV", "USER", "UID",
            "SQLCODE", "SQLERRM", "SQL"
    );

    /**
     * Single-part names to skip when scanning expressions — these are PL/SQL constructs
     * or common variables that happen to be followed by parentheses but aren't function calls.
     * Qualified names (pkg.func) are NOT skipped by this list.
     */
    private static final Set<String> SKIP_SINGLE_NAMES = Set.of(
            // Type constructors / pseudo-functions
            "EXCEPTION", "TABLE", "RECORD", "VARRAY", "ASSOCIATIVE",
            // Common single-word expressions before parens that aren't calls
            "CASE", "WHEN", "THEN", "ELSE", "IF", "ELSIF", "LOOP", "WHILE", "FOR",
            "AND", "OR", "NOT", "IN", "EXISTS", "BETWEEN", "LIKE", "IS",
            "SELECT", "FROM", "WHERE", "SET", "INTO", "VALUES", "RETURNING",
            "CURSOR", "TYPE", "PRAGMA"
    );

    // ---- Variable declarations ----

    @Override
    public Void visitVariableDeclaration(PlSqlAnalyzerParser.VariableDeclarationContext ctx) {
        PlsqlProcedure currentProc = procStack.peek();
        if (currentProc != null) {
            String varName = ctx.identifier().getText().toUpperCase();
            boolean isConst = ctx.CONSTANT() != null;
            String dataType = ctx.dataTypeRef() != null ? getFullText(ctx.dataTypeRef()).trim().toUpperCase() : "UNKNOWN";
            int line = ctx.getStart().getLine();
            currentProc.getVariables().add(new VariableInfo(varName, dataType, isConst, line));

            // Detect REF CURSOR / SYS_REFCURSOR variable declarations as cursor info
            if (dataType.contains("SYS_REFCURSOR") || dataType.contains("REF CURSOR")
                    || dataType.contains("REFCURSOR") || dataType.contains("REF_CURSOR")) {
                currentProc.getCursorInfos().add(
                        new CursorInfo(varName, "REF_CURSOR", "DECLARE", null, line));
            }
        }
        return super.visitVariableDeclaration(ctx);
    }

    // ---- Statement tracking ----

    @Override
    public Void visitStatement(PlSqlAnalyzerParser.StatementContext ctx) {
        PlsqlProcedure currentProc = procStack.peek();
        if (currentProc != null && ctx.statementBody() != null) {
            var body = ctx.statementBody();
            int line = ctx.getStart().getLine();
            int endLine = ctx.getStop() != null ? ctx.getStop().getLine() : line;
            String type = classifyStatement(body);
            String target = extractStatementTarget(body);
            currentProc.getStatements().add(new StatementInfo(type, line, endLine, target));
        }
        return super.visitStatement(ctx);
    }

    private String classifyStatement(PlSqlAnalyzerParser.StatementBodyContext body) {
        if (body.selectIntoStatement() != null) return "SELECT";
        if (body.insertStatement() != null) return "INSERT";
        if (body.updateStatement() != null) return "UPDATE";
        if (body.deleteStatement() != null) return "DELETE";
        if (body.mergeStatement() != null) return "MERGE";
        if (body.executeImmediateStatement() != null) return "EXECUTE_IMMEDIATE";
        if (body.ifStatement() != null) return "IF";
        if (body.caseStatement() != null) return "CASE";
        if (body.basicLoopStatement() != null) return "LOOP";
        if (body.whileLoopStatement() != null) return "WHILE";
        if (body.forLoopStatement() != null) return "FOR";
        if (body.cursorForLoopStatement() != null) return "FOR_CURSOR";
        if (body.returnStatement() != null) return "RETURN";
        if (body.raiseStatement() != null) return "RAISE";
        if (body.commitStatement() != null) return "COMMIT";
        if (body.rollbackStatement() != null) return "ROLLBACK";
        if (body.blockStatement() != null) return "BLOCK";
        if (body.exitStatement() != null) return "EXIT";
        if (body.continueStatement() != null) return "CONTINUE";
        if (body.gotoStatement() != null) return "GOTO";
        if (body.nullStatement() != null) return "NULL";
        if (body.openStatement() != null) return "OPEN";
        if (body.fetchStatement() != null) return "FETCH";
        if (body.closeStatement() != null) return "CLOSE";
        if (body.forallStatement() != null) return "FORALL";
        if (body.pipeRowStatement() != null) return "PIPE_ROW";
        if (body.savepointStatement() != null) return "SAVEPOINT";
        if (body.callOrAssignmentStatement() != null) {
            var cas = body.callOrAssignmentStatement();
            return cas.ASSIGN() != null ? "ASSIGN" : "CALL";
        }
        return "OTHER";
    }

    private String extractStatementTarget(PlSqlAnalyzerParser.StatementBodyContext body) {
        if (body.insertStatement() != null && body.insertStatement().tableReference() != null)
            return getTableRefName(body.insertStatement().tableReference());
        if (body.updateStatement() != null && body.updateStatement().tableReference() != null)
            return getTableRefName(body.updateStatement().tableReference());
        if (body.deleteStatement() != null && body.deleteStatement().tableReference() != null)
            return getTableRefName(body.deleteStatement().tableReference());
        if (body.mergeStatement() != null && body.mergeStatement().tableReference() != null)
            return getTableRefName(body.mergeStatement().tableReference());
        if (body.callOrAssignmentStatement() != null) {
            String text = getQualifiedText(body.callOrAssignmentStatement().qualifiedName());
            return text != null ? text.toUpperCase() : null;
        }
        return null;
    }

    private String getTableRefName(PlSqlAnalyzerParser.TableReferenceContext ref) {
        return ref.qualifiedName() != null ? getQualifiedText(ref.qualifiedName()).toUpperCase() : null;
    }

    // ---- Parameter extraction ----

    private void extractParameters(PlSqlAnalyzerParser.ParameterListContext paramList, PlsqlProcedure proc) {
        if (paramList == null || paramList.parameterDecl() == null) return;
        for (var pd : paramList.parameterDecl()) {
            String name = pd.identifier().getText().toUpperCase();
            String mode = "IN"; // default
            if (pd.OUT() != null && pd.IN() != null) mode = "IN OUT";
            else if (pd.OUT() != null) mode = "OUT";
            boolean noCopy = pd.NOCOPY() != null;
            String dataType = pd.dataTypeRef() != null ? getFullText(pd.dataTypeRef()).trim().toUpperCase() : "UNKNOWN";
            int line = pd.getStart().getLine();
            proc.getParameters().add(new ProcedureParameter(name, mode, dataType, noCopy, line));
        }
    }

    // ---- Helper methods ----

    private ProcedureCall buildProcedureCall(String qualifiedName, int line, boolean isDynamic) {
        String[] parts = qualifiedName.split("\\.");
        String schema = null;
        String pkg = null;
        String proc;

        if (parts.length == 3) {
            schema = parts[0].toUpperCase();
            pkg = parts[1].toUpperCase();
            proc = parts[2].toUpperCase();
        } else if (parts.length == 2) {
            pkg = parts[0].toUpperCase();
            proc = parts[1].toUpperCase();
        } else {
            proc = parts[0].toUpperCase();
        }

        return new ProcedureCall(schema, pkg, proc, line, isDynamic);
    }

    private void addTableRef(PlsqlProcedure proc,
                             PlSqlAnalyzerParser.TableReferenceContext tableRefCtx,
                             SqlOperationType operation, int line) {
        // Skip subquery table references — they have no qualifiedName, only parenExpression
        if (tableRefCtx.qualifiedName() == null) return;

        String qualifiedName = getQualifiedText(tableRefCtx.qualifiedName());
        if (qualifiedName.isEmpty()) return;

        String[] parts = qualifiedName.split("\\.");
        String schema = parts.length > 1 ? parts[0].toUpperCase() : null;
        String tableName = parts[parts.length - 1].toUpperCase();

        String alias = null;
        // The grammar has optional identifier for alias after the table name
        if (tableRefCtx.identifier() != null && !tableRefCtx.identifier().isEmpty()) {
            // Last identifier could be alias or dblink identifier
            if (tableRefCtx.AT_SIGN() == null) {
                alias = tableRefCtx.identifier(0).getText();
            } else if (tableRefCtx.identifier().size() > 1) {
                alias = tableRefCtx.identifier(1).getText();
            }
        }

        TableReference ref = new TableReference(tableName, schema, alias, operation, line);
        proc.getTableReferences().add(ref);
    }

    private void analyzeWithJSqlParser(PlsqlProcedure proc, String rawSql, SqlOperationType op, int line) {
        try {
            // Clean PL/SQL-specific syntax before feeding to JSqlParser
            String cleanSql = cleanForJSqlParser(rawSql);
            SqlAnalysisResult result = sqlAnalyzer.analyze(cleanSql, line);
            if (result.getOperationType() != null) {
                proc.getSqlStatements().add(result);
                // Propagate joins and sequences from JSqlParser result to procedure
                if (result.getJoinInfos() != null) proc.getJoinInfos().addAll(result.getJoinInfos());
                if (result.getSequenceReferences() != null) proc.getSequenceReferences().addAll(result.getSequenceReferences());
            }
        } catch (Exception e) {
            log.debug("JSqlParser failed for SQL at line {}: {}", line, e.getMessage());
            // Still create a basic result even if JSqlParser fails
            SqlAnalysisResult result = new SqlAnalysisResult();
            result.setRawSql(rawSql);
            result.setOperationType(op);
            result.setLineNumber(line);
            proc.getSqlStatements().add(result);
        }
    }

    private String cleanForJSqlParser(String sql) {
        // Remove PL/SQL-specific INTO clause from SELECT INTO
        String cleaned = sql.replaceAll("(?i)\\bINTO\\s+[\\w.,\\s]+\\bFROM\\b", "FROM");
        // Remove trailing semicolons
        cleaned = cleaned.replaceAll(";\\s*$", "");
        // Remove BULK COLLECT
        cleaned = cleaned.replaceAll("(?i)\\bBULK\\s+COLLECT\\s+", "");
        return cleaned.trim();
    }

    private String getQualifiedText(PlSqlAnalyzerParser.QualifiedNameContext ctx) {
        if (ctx == null) return "";
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < ctx.identifier().size(); i++) {
            if (i > 0) sb.append(".");
            String text = ctx.identifier(i).getText();
            if (text.startsWith("\"") && text.endsWith("\"") && text.length() > 2) {
                text = text.substring(1, text.length() - 1);
            }
            sb.append(text);
        }
        return sb.toString();
    }

    private String getFullText(ParserRuleContext ctx) {
        if (ctx == null || ctx.getStart() == null || ctx.getStop() == null) return "";
        return ctx.getStart().getInputStream().getText(
                new org.antlr.v4.runtime.misc.Interval(
                        ctx.getStart().getStartIndex(),
                        ctx.getStop().getStopIndex()));
    }

    private String extractSimpleName(String qualifiedName) {
        if (qualifiedName == null || qualifiedName.isEmpty()) return "";
        String[] parts = qualifiedName.split("\\.");
        return parts[parts.length - 1].toUpperCase();
    }

    private String extractSchemaName(String qualifiedName) {
        if (qualifiedName == null) return null;
        String[] parts = qualifiedName.split("\\.");
        return parts.length > 1 ? parts[0].toUpperCase() : null;
    }

    // ---- Sequence extraction ----

    /** Regex for SEQUENCE_NAME.NEXTVAL or SCHEMA.SEQUENCE_NAME.NEXTVAL / CURRVAL */
    private static final Pattern SEQ_PATTERN = Pattern.compile(
            "(?i)(?:([A-Z_][A-Z0-9_$#]*)\\.)?([A-Z_][A-Z0-9_$#]*)\\.(NEXTVAL|CURRVAL)");

    private void extractSequenceReferences(PlsqlProcedure proc, String rawSql, int line) {
        if (rawSql == null || rawSql.isEmpty()) return;
        Matcher m = SEQ_PATTERN.matcher(rawSql);
        while (m.find()) {
            String schema = m.group(1) != null ? m.group(1).toUpperCase() : null;
            String seqName = m.group(2).toUpperCase();
            String op = m.group(3).toUpperCase();
            proc.getSequenceReferences().add(new SequenceReference(seqName, schema, op, line));
        }
    }

    private void extractSequenceFromExpression(PlsqlProcedure proc, ParserRuleContext exprCtx) {
        if (exprCtx == null) return;
        String text = getFullText(exprCtx);
        if (text != null && !text.isEmpty()) {
            extractSequenceReferences(proc, text, exprCtx.getStart().getLine());
        }
    }

    private SqlOperationType determineTriggerOperation(PlSqlAnalyzerParser.TriggerTimingClauseContext ctx) {
        if (ctx == null || ctx.triggerEvent() == null) return null;
        // Return the first event type found
        for (var event : ctx.triggerEvent()) {
            if (event.INSERT() != null) return SqlOperationType.INSERT;
            if (event.UPDATE() != null) return SqlOperationType.UPDATE;
            if (event.DELETE() != null) return SqlOperationType.DELETE;
        }
        return null;
    }
}
