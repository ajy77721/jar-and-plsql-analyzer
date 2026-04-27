package com.plsql.parser.visitor;

import com.plsql.parser.PlSqlParser;
import com.plsql.parser.PlSqlParserBaseVisitor;
import com.plsql.parser.model.*;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Single-pass visitor that walks the ANTLR4 parse tree and collects all
 * structural information about PL/SQL source code into model objects.
 */
public class PlSqlAnalysisVisitor extends PlSqlParserBaseVisitor<Void> {

    // ---------------------------------------------------------------
    // Builtin package names (upper-cased) used for call classification
    // ---------------------------------------------------------------
    private static final Set<String> BUILTIN_PACKAGES = new HashSet<>(Arrays.asList(
        "DBMS_OUTPUT", "DBMS_SQL", "DBMS_LOB", "DBMS_UTILITY", "DBMS_LOCK",
        "DBMS_RANDOM", "DBMS_CRYPTO", "DBMS_JOB", "DBMS_SCHEDULER", "DBMS_PIPE",
        "DBMS_AQ", "DBMS_AQADM", "DBMS_APPLICATION_INFO", "DBMS_SESSION",
        "DBMS_METADATA", "DBMS_STATS", "DBMS_XMLDOM", "DBMS_XMLPARSER",
        "DBMS_XMLGEN", "DBMS_XMLQUERY", "DBMS_XMLSTORE", "DBMS_ALERT",
        "DBMS_PROFILER", "DBMS_TRACE", "DBMS_ASSERT", "DBMS_FLASHBACK",
        "DBMS_REDEFINITION", "DBMS_ROWID", "DBMS_SPACE", "DBMS_TRANSACTION",
        "UTL_FILE", "UTL_HTTP", "UTL_SMTP", "UTL_TCP", "UTL_URL", "UTL_RAW",
        "UTL_ENCODE", "UTL_COMPRESS", "UTL_I18N", "UTL_MAIL", "UTL_COLL",
        "UTL_REF", "UTL_MATCH",
        "HTF", "HTP", "OWA_UTIL", "OWA_COOKIE", "OWA_TEXT",
        "APEX_UTIL", "APEX_APPLICATION", "APEX_ITEM", "APEX_COLLECTION",
        "APEX_JSON", "APEX_STRING", "APEX_PAGE", "APEX_REGION",
        "STANDARD", "SYS"
    ));

    // ---------------------------------------------------------------
    // Context stack for tracking where we are in the parse tree
    // ---------------------------------------------------------------
    private static class AnalysisContext {
        ParsedObject parsedObject;      // non-null for top-level objects
        SubprogramInfo subprogramInfo;  // non-null for subprograms inside packages
        String currentPackageName;
        String dmlOperation;            // current DML context for table tracking

        AnalysisContext(ParsedObject po) {
            this.parsedObject = po;
        }

        AnalysisContext(SubprogramInfo si, String pkgName) {
            this.subprogramInfo = si;
            this.currentPackageName = pkgName;
        }
    }

    private final Deque<AnalysisContext> contextStack = new ArrayDeque<>();
    private final List<ParsedObject> results = new ArrayList<>();

    // Track known subprogram names in the current package for INTERNAL call detection
    private final Set<String> currentPackageSubprograms = new HashSet<>();
    private String currentPackageName = null;

    // Track the current DML operation for table-operation pairing
    private String currentDmlOperation = null;

    // Track tables collected for the current DML statement
    private List<String> currentDmlTables = null;

    // Track CTE (WITH clause) names and inline view aliases to avoid false-positive table refs
    private final Set<String> queryAliases = new HashSet<>();

    // Track loop nesting depth for complexity analysis
    private int loopNestingDepth = 0;

    // Track current exception handler for body analysis
    private ExceptionHandlerInfo currentExceptionHandler = null;

    // Track subquery context classification (EXISTS, IN, SCALAR, FROM)
    private String currentSubqueryContext = null;

    // Track the target table name of the current MERGE statement (for branch attribution)
    private String currentMergeTargetTable = null;
    private String currentMergeTargetSchema = null;

    // ---------------------------------------------------------------
    // Public API
    // ---------------------------------------------------------------
    public List<ParsedObject> getResults() {
        return results;
    }

    // ---------------------------------------------------------------
    // Helper methods
    // ---------------------------------------------------------------

    private void pushObject(ParsedObject po) {
        AnalysisContext ctx = new AnalysisContext(po);
        ctx.currentPackageName = currentPackageName;
        contextStack.push(ctx);
    }

    private void pushSubprogram(SubprogramInfo si) {
        AnalysisContext ctx = new AnalysisContext(si, currentPackageName);
        contextStack.push(ctx);
    }

    private void popContext() {
        if (!contextStack.isEmpty()) {
            contextStack.pop();
        }
    }

    /**
     * Return the current ParsedObject (top-level). Walks up the stack.
     */
    private ParsedObject currentObject() {
        for (AnalysisContext ctx : contextStack) {
            if (ctx.parsedObject != null) return ctx.parsedObject;
        }
        return null;
    }

    /**
     * Return the current SubprogramInfo if we are inside one.
     */
    private SubprogramInfo currentSubprogram() {
        if (!contextStack.isEmpty()) {
            AnalysisContext top = contextStack.peek();
            if (top.subprogramInfo != null) return top.subprogramInfo;
        }
        return null;
    }

    private boolean insideSubprogram() {
        return currentSubprogram() != null;
    }

    private void addStatement(StatementInfo si) {
        SubprogramInfo sub = currentSubprogram();
        if (sub != null) {
            sub.getStatements().add(si);
        } else {
            ParsedObject po = currentObject();
            if (po != null) {
                po.getStatements().add(si);
            }
        }
    }

    private void addCall(CallInfo ci) {
        SubprogramInfo sub = currentSubprogram();
        if (sub != null) {
            sub.getCalls().add(ci);
        } else {
            ParsedObject po = currentObject();
            if (po != null) {
                po.getCalls().add(ci);
            }
        }
        if (currentExceptionHandler != null) {
            currentExceptionHandler.getCalls().add(ci);
        }
    }

    private void addTableOperation(TableOperationInfo toi) {
        if (currentSubqueryContext != null) {
            toi.setSubqueryContext(currentSubqueryContext);
        }
        SubprogramInfo sub = currentSubprogram();
        if (sub != null) {
            sub.getTableOperations().add(toi);
        } else {
            ParsedObject po = currentObject();
            if (po != null) {
                po.getTableOperations().add(toi);
            }
        }
        if (currentExceptionHandler != null) {
            currentExceptionHandler.getTableOperations().add(toi);
        }
        // Also add to dependency summary
        ParsedObject po = currentObject();
        if (po != null && toi.getTableName() != null) {
            po.getDependencies().getTables().add(toi.getTableName().toUpperCase());
        }
    }

    private void addCursor(CursorInfo ci) {
        SubprogramInfo sub = currentSubprogram();
        if (sub != null) {
            sub.getCursors().add(ci);
        } else {
            ParsedObject po = currentObject();
            if (po != null) {
                po.getCursors().add(ci);
            }
        }
    }

    private void addDynamicSql(DynamicSqlInfo dsi) {
        SubprogramInfo sub = currentSubprogram();
        if (sub != null) {
            sub.getDynamicSql().add(dsi);
        } else {
            ParsedObject po = currentObject();
            if (po != null) {
                po.getDynamicSql().add(dsi);
            }
        }
    }

    private void addExceptionHandler(ExceptionHandlerInfo ehi) {
        SubprogramInfo sub = currentSubprogram();
        if (sub != null) {
            sub.getExceptionHandlers().add(ehi);
        } else {
            ParsedObject po = currentObject();
            if (po != null) {
                po.getExceptionHandlers().add(ehi);
            }
        }
    }

    private void addVariable(VariableInfo vi) {
        SubprogramInfo sub = currentSubprogram();
        if (sub != null) {
            vi.setScope("LOCAL");
            sub.getLocalVariables().add(vi);
        } else {
            ParsedObject po = currentObject();
            if (po != null) {
                String type = po.getType();
                if ("PACKAGE_BODY".equals(type) || "PACKAGE_SPEC".equals(type)) {
                    vi.setScope("PACKAGE");
                    po.getPackageVariables().add(vi);
                } else {
                    vi.setScope("LOCAL");
                    po.getGlobalVariables().add(vi);
                }
            }
        }
    }

    private void addExternalPackageVarRef(String ref) {
        SubprogramInfo sub = currentSubprogram();
        if (sub != null) {
            if (!sub.getExternalPackageVarRefs().contains(ref)) {
                sub.getExternalPackageVarRefs().add(ref);
            }
        } else {
            ParsedObject po = currentObject();
            if (po != null) {
                if (!po.getExternalPackageVarRefs().contains(ref)) {
                    po.getExternalPackageVarRefs().add(ref);
                }
            }
        }
    }

    /**
     * Record a sequence reference (NEXTVAL/CURRVAL) detected from a general_element.
     * Supports patterns: seq.NEXTVAL, schema.seq.NEXTVAL, seq.CURRVAL, schema.seq.CURRVAL.
     * Creates a CallInfo with the sequence name as packageName so that downstream
     * extractSequences() in ChunkedFlowWriter can pick it up.
     */
    private void addSequenceReference(List<String> partNames, String operation, int line) {
        String seqName;
        String schemaName = null;
        if (partNames.size() == 2) {
            // seq.NEXTVAL
            seqName = partNames.get(0);
        } else {
            // schema.seq.NEXTVAL (3+ parts)
            schemaName = partNames.get(0);
            seqName = partNames.get(partNames.size() - 2);
        }

        // Create a CallInfo so that ChunkedFlowWriter.extractSequences() works unchanged
        CallInfo ci = new CallInfo();
        ci.setLine(line);
        ci.setName(operation);
        ci.setPackageName(seqName);
        ci.setSchema(schemaName);
        ci.setType("SEQUENCE");
        ci.setArguments(0);
        addCall(ci);

        // Also add to the dependency summary
        ParsedObject po = currentObject();
        if (po != null) {
            po.getDependencies().getSequences().add(seqName);
        }
    }

    /**
     * Get original text for a context, truncated to maxLen.
     */
    private String getOriginalText(ParserRuleContext ctx, int maxLen) {
        if (ctx == null || ctx.start == null || ctx.stop == null) return null;
        try {
            String text = ctx.start.getInputStream().getText(
                new org.antlr.v4.runtime.misc.Interval(
                    ctx.start.getStartIndex(), ctx.stop.getStopIndex()));
            if (text != null && text.length() > maxLen) {
                text = text.substring(0, maxLen) + "...";
            }
            return text;
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Extract a clean identifier string from an identifier context, removing quotes.
     */
    private String cleanId(String text) {
        if (text == null) return null;
        text = text.trim();
        if (text.startsWith("\"") && text.endsWith("\"")) {
            text = text.substring(1, text.length() - 1);
        }
        return text.toUpperCase();
    }

    private String getIdentifierText(ParserRuleContext ctx) {
        if (ctx == null) return null;
        return cleanId(ctx.getText());
    }

    private int getStartLine(ParserRuleContext ctx) {
        return ctx != null && ctx.start != null ? ctx.start.getLine() : 0;
    }

    private int getStopLine(ParserRuleContext ctx) {
        return ctx != null && ctx.stop != null ? ctx.stop.getLine() : 0;
    }

    /**
     * Extract the type_spec text.
     */
    private String getTypeSpecText(PlSqlParser.Type_specContext ctx) {
        if (ctx == null) return null;
        return ctx.getText().toUpperCase();
    }

    /**
     * Extract table/object references from %TYPE and %ROWTYPE declarations.
     * For example, SAPM_SYS_CONSTANTS.NUM_VALUE%TYPE references the SAPM_SYS_CONSTANTS table.
     * For table.column%TYPE, the table is recorded as a dependency.
     * For table%ROWTYPE, the table is recorded as a dependency.
     */
    private void extractTypeSpecTableRef(PlSqlParser.Type_specContext ctx, int line) {
        if (ctx == null) return;
        if (ctx.type_name() == null) return;

        boolean hasPercentType = ctx.PERCENT_TYPE() != null;
        boolean hasPercentRowType = ctx.PERCENT_ROWTYPE() != null;
        if (!hasPercentType && !hasPercentRowType) return;

        List<PlSqlParser.Id_expressionContext> idParts = ctx.type_name().id_expression();
        if (idParts == null || idParts.isEmpty()) return;

        // For %TYPE:  table.column%TYPE  → 2+ parts, first is the table
        // For %ROWTYPE: table%ROWTYPE    → 1+ parts, first is the table
        // Skip if only 1 part for %TYPE (it's a local variable%TYPE, not a table ref)
        String tableName = null;
        String schema = null;

        if (hasPercentRowType) {
            // schema.table%ROWTYPE or table%ROWTYPE
            if (idParts.size() >= 2) {
                schema = cleanId(idParts.get(0).getText());
                tableName = cleanId(idParts.get(1).getText());
            } else {
                tableName = cleanId(idParts.get(0).getText());
            }
        } else if (hasPercentType && idParts.size() >= 2) {
            // schema.table.column%TYPE or table.column%TYPE
            if (idParts.size() >= 3) {
                schema = cleanId(idParts.get(0).getText());
                tableName = cleanId(idParts.get(1).getText());
            } else {
                tableName = cleanId(idParts.get(0).getText());
            }
        }

        if (tableName != null && !isDualTable(tableName)) {
            // Add to dependency summary
            ParsedObject po = currentObject();
            if (po != null) {
                po.getDependencies().getTables().add(
                        (schema != null ? schema + "." : "") + tableName);
            }
        }
    }

    // ---------------------------------------------------------------
    // PACKAGE SPEC
    // ---------------------------------------------------------------
    @Override
    public Void visitCreate_package(PlSqlParser.Create_packageContext ctx) {
        ParsedObject po = new ParsedObject();
        po.setType("PACKAGE_SPEC");
        po.setLineStart(getStartLine(ctx));
        po.setLineEnd(getStopLine(ctx));

        // Package name
        if (ctx.package_name() != null && !ctx.package_name().isEmpty()) {
            po.setName(getIdentifierText(ctx.package_name(0)));
        }
        // Schema
        if (ctx.schema_object_name() != null) {
            po.setSchema(getIdentifierText(ctx.schema_object_name()));
        }

        currentPackageName = po.getName();
        currentPackageSubprograms.clear();

        // Pre-scan for subprogram names in spec
        for (PlSqlParser.Package_obj_specContext objSpec : ctx.package_obj_spec()) {
            if (objSpec.procedure_spec() != null) {
                PlSqlParser.Procedure_specContext ps = objSpec.procedure_spec();
                if (ps.identifier() != null) {
                    currentPackageSubprograms.add(cleanId(ps.identifier().getText()));
                }
            }
            if (objSpec.function_spec() != null) {
                PlSqlParser.Function_specContext fs = objSpec.function_spec();
                if (fs.identifier() != null) {
                    currentPackageSubprograms.add(cleanId(fs.identifier().getText()));
                }
            }
        }

        results.add(po);
        pushObject(po);
        Void result = visitChildren(ctx);
        popContext();

        currentPackageName = null;
        return result;
    }

    // ---------------------------------------------------------------
    // PACKAGE BODY
    // ---------------------------------------------------------------
    @Override
    public Void visitCreate_package_body(PlSqlParser.Create_package_bodyContext ctx) {
        ParsedObject po = new ParsedObject();
        po.setType("PACKAGE_BODY");
        po.setLineStart(getStartLine(ctx));
        po.setLineEnd(getStopLine(ctx));

        if (ctx.package_name() != null && !ctx.package_name().isEmpty()) {
            po.setName(getIdentifierText(ctx.package_name(0)));
        }
        if (ctx.schema_object_name() != null) {
            po.setSchema(getIdentifierText(ctx.schema_object_name()));
        }

        currentPackageName = po.getName();
        currentPackageSubprograms.clear();

        // Pre-scan all package_obj_body for subprogram names
        for (PlSqlParser.Package_obj_bodyContext objBody : ctx.package_obj_body()) {
            if (objBody.procedure_body() != null) {
                PlSqlParser.Procedure_bodyContext pb = objBody.procedure_body();
                if (pb.identifier() != null) {
                    currentPackageSubprograms.add(cleanId(pb.identifier().getText()));
                }
            }
            if (objBody.function_body() != null) {
                PlSqlParser.Function_bodyContext fb = objBody.function_body();
                if (fb.identifier() != null) {
                    currentPackageSubprograms.add(cleanId(fb.identifier().getText()));
                }
            }
            if (objBody.procedure_spec() != null) {
                PlSqlParser.Procedure_specContext ps = objBody.procedure_spec();
                if (ps.identifier() != null) {
                    currentPackageSubprograms.add(cleanId(ps.identifier().getText()));
                }
            }
            if (objBody.function_spec() != null) {
                PlSqlParser.Function_specContext fs = objBody.function_spec();
                if (fs.identifier() != null) {
                    currentPackageSubprograms.add(cleanId(fs.identifier().getText()));
                }
            }
        }

        results.add(po);
        pushObject(po);
        Void result = visitChildren(ctx);
        popContext();

        currentPackageName = null;
        return result;
    }

    // ---------------------------------------------------------------
    // PROCEDURE BODY (inside package)
    // ---------------------------------------------------------------
    @Override
    public Void visitProcedure_body(PlSqlParser.Procedure_bodyContext ctx) {
        SubprogramInfo si = new SubprogramInfo();
        si.setType("PROCEDURE");
        si.setLineStart(getStartLine(ctx));
        si.setLineEnd(getStopLine(ctx));

        if (ctx.identifier() != null) {
            si.setName(getIdentifierText(ctx.identifier()));
        }

        // Parameters
        if (ctx.parameter() != null) {
            for (PlSqlParser.ParameterContext p : ctx.parameter()) {
                si.getParameters().add(extractParameter(p));
            }
        }

        ParsedObject po = currentObject();
        if (po != null) {
            po.getSubprograms().add(si);
        }

        pushSubprogram(si);
        Void result = visitChildren(ctx);
        // Fallback: scan procedure body text for SQL-embedded pkg.fn(args) calls
        extractFunctionCallsFromSqlText(getOriginalText(ctx, 10000), getStartLine(ctx));
        popContext();
        return result;
    }

    // ---------------------------------------------------------------
    // FUNCTION BODY (inside package)
    // ---------------------------------------------------------------
    @Override
    public Void visitFunction_body(PlSqlParser.Function_bodyContext ctx) {
        SubprogramInfo si = new SubprogramInfo();
        si.setType("FUNCTION");
        si.setLineStart(getStartLine(ctx));
        si.setLineEnd(getStopLine(ctx));

        if (ctx.identifier() != null) {
            si.setName(getIdentifierText(ctx.identifier()));
        }
        if (ctx.type_spec() != null) {
            si.setReturnType(getTypeSpecText(ctx.type_spec()));
        }

        if (ctx.parameter() != null) {
            for (PlSqlParser.ParameterContext p : ctx.parameter()) {
                si.getParameters().add(extractParameter(p));
            }
        }

        ParsedObject po = currentObject();
        if (po != null) {
            po.getSubprograms().add(si);
        }

        pushSubprogram(si);
        Void result = visitChildren(ctx);
        // Fallback: scan function body text for SQL-embedded pkg.fn(args) calls
        extractFunctionCallsFromSqlText(getOriginalText(ctx, 10000), getStartLine(ctx));
        popContext();
        return result;
    }

    // ---------------------------------------------------------------
    // CREATE PROCEDURE (standalone)
    // ---------------------------------------------------------------
    @Override
    public Void visitCreate_procedure_body(PlSqlParser.Create_procedure_bodyContext ctx) {
        ParsedObject po = new ParsedObject();
        po.setType("PROCEDURE");
        po.setLineStart(getStartLine(ctx));
        po.setLineEnd(getStopLine(ctx));

        if (ctx.procedure_name() != null) {
            String fullName = ctx.procedure_name().getText();
            parseQualifiedName(po, fullName);
        }

        if (ctx.parameter() != null) {
            for (PlSqlParser.ParameterContext p : ctx.parameter()) {
                po.getParameters().add(extractParameter(p));
            }
        }

        currentPackageName = null;
        currentPackageSubprograms.clear();

        results.add(po);
        pushObject(po);
        Void result = visitChildren(ctx);
        // Fallback: scan standalone procedure text for SQL-embedded pkg.fn(args) calls
        extractFunctionCallsFromSqlText(getOriginalText(ctx, 10000), getStartLine(ctx));
        popContext();
        return result;
    }

    // ---------------------------------------------------------------
    // CREATE FUNCTION (standalone)
    // ---------------------------------------------------------------
    @Override
    public Void visitCreate_function_body(PlSqlParser.Create_function_bodyContext ctx) {
        ParsedObject po = new ParsedObject();
        po.setType("FUNCTION");
        po.setLineStart(getStartLine(ctx));
        po.setLineEnd(getStopLine(ctx));

        if (ctx.function_name() != null) {
            String fullName = ctx.function_name().getText();
            parseQualifiedName(po, fullName);
        }

        if (ctx.parameter() != null) {
            for (PlSqlParser.ParameterContext p : ctx.parameter()) {
                po.getParameters().add(extractParameter(p));
            }
        }

        currentPackageName = null;
        currentPackageSubprograms.clear();

        results.add(po);
        pushObject(po);
        Void result = visitChildren(ctx);
        // Fallback: scan standalone function text for SQL-embedded pkg.fn(args) calls
        extractFunctionCallsFromSqlText(getOriginalText(ctx, 10000), getStartLine(ctx));
        popContext();
        return result;
    }

    // ---------------------------------------------------------------
    // CREATE TRIGGER
    // ---------------------------------------------------------------
    @Override
    public Void visitCreate_trigger(PlSqlParser.Create_triggerContext ctx) {
        ParsedObject po = new ParsedObject();
        po.setType("TRIGGER");
        po.setLineStart(getStartLine(ctx));
        po.setLineEnd(getStopLine(ctx));

        if (ctx.trigger_name() != null) {
            String fullName = ctx.trigger_name().getText();
            parseQualifiedName(po, fullName);
        }

        // Extract trigger details from simple_dml_trigger
        if (ctx.simple_dml_trigger() != null) {
            PlSqlParser.Simple_dml_triggerContext sdml = ctx.simple_dml_trigger();

            // Timing: BEFORE / AFTER / INSTEAD OF
            if (sdml.BEFORE() != null) po.setTriggerTiming("BEFORE");
            else if (sdml.AFTER() != null) po.setTriggerTiming("AFTER");
            else if (sdml.INSTEAD() != null) po.setTriggerTiming("INSTEAD OF");

            // DML event clause
            if (sdml.dml_event_clause() != null) {
                PlSqlParser.Dml_event_clauseContext dec = sdml.dml_event_clause();

                // Events
                List<String> events = new ArrayList<>();
                for (PlSqlParser.Dml_event_elementContext el : dec.dml_event_element()) {
                    if (el.DELETE() != null) events.add("DELETE");
                    if (el.INSERT() != null) events.add("INSERT");
                    if (el.UPDATE() != null) events.add("UPDATE");
                }
                po.setTriggerEvent(String.join(" OR ", events));

                // Table
                if (dec.tableview_name() != null) {
                    po.setTriggerTable(cleanId(dec.tableview_name().getText()));
                }
            }
        }

        // Compound DML trigger
        if (ctx.compound_dml_trigger() != null) {
            PlSqlParser.Compound_dml_triggerContext cdml = ctx.compound_dml_trigger();
            if (cdml.dml_event_clause() != null) {
                PlSqlParser.Dml_event_clauseContext dec = cdml.dml_event_clause();
                List<String> events = new ArrayList<>();
                for (PlSqlParser.Dml_event_elementContext el : dec.dml_event_element()) {
                    if (el.DELETE() != null) events.add("DELETE");
                    if (el.INSERT() != null) events.add("INSERT");
                    if (el.UPDATE() != null) events.add("UPDATE");
                }
                po.setTriggerEvent(String.join(" OR ", events));
                if (dec.tableview_name() != null) {
                    po.setTriggerTable(cleanId(dec.tableview_name().getText()));
                }
            }
        }

        currentPackageName = null;
        currentPackageSubprograms.clear();

        results.add(po);
        pushObject(po);
        Void result = visitChildren(ctx);
        popContext();
        return result;
    }

    // ---------------------------------------------------------------
    // VARIABLE DECLARATION
    // ---------------------------------------------------------------
    @Override
    public Void visitVariable_declaration(PlSqlParser.Variable_declarationContext ctx) {
        VariableInfo vi = new VariableInfo();
        vi.setLine(getStartLine(ctx));

        if (ctx.identifier() != null) {
            vi.setName(getIdentifierText(ctx.identifier()));
        }

        if (ctx.type_spec() != null) {
            vi.setDataType(getTypeSpecText(ctx.type_spec()));
            // Extract table references from %TYPE / %ROWTYPE declarations
            extractTypeSpecTableRef(ctx.type_spec(), getStartLine(ctx));
        }

        vi.setConstant(ctx.CONSTANT() != null);

        if (ctx.default_value_part() != null) {
            vi.setDefaultValue(getOriginalText(ctx.default_value_part(), 200));
        }

        addVariable(vi);
        return visitChildren(ctx);
    }

    // ---------------------------------------------------------------
    // PARAMETER (for procedures/functions)
    // ---------------------------------------------------------------
    private ParameterInfo extractParameter(PlSqlParser.ParameterContext ctx) {
        ParameterInfo pi = new ParameterInfo();
        pi.setLine(getStartLine(ctx));

        if (ctx.parameter_name() != null) {
            pi.setName(getIdentifierText(ctx.parameter_name()));
        }

        // Direction
        boolean hasIn = ctx.IN() != null && !ctx.IN().isEmpty();
        boolean hasOut = ctx.OUT() != null && !ctx.OUT().isEmpty();
        boolean hasInOut = ctx.INOUT() != null && !ctx.INOUT().isEmpty();

        if (hasInOut || (hasIn && hasOut)) {
            pi.setDirection("IN OUT");
        } else if (hasOut) {
            pi.setDirection("OUT");
        } else {
            pi.setDirection("IN");
        }

        if (ctx.type_spec() != null) {
            pi.setDataType(getTypeSpecText(ctx.type_spec()));
            // Extract table references from %TYPE / %ROWTYPE parameter types
            extractTypeSpecTableRef(ctx.type_spec(), getStartLine(ctx));
        }

        if (ctx.default_value_part() != null) {
            pi.setDefaultValue(getOriginalText(ctx.default_value_part(), 200));
        }

        return pi;
    }

    // ---------------------------------------------------------------
    // CURSOR DECLARATION
    // ---------------------------------------------------------------
    @Override
    public Void visitCursor_declaration(PlSqlParser.Cursor_declarationContext ctx) {
        CursorInfo ci = new CursorInfo();
        ci.setLine(getStartLine(ctx));
        ci.setLineEnd(getStopLine(ctx));

        if (ctx.identifier() != null) {
            ci.setName(getIdentifierText(ctx.identifier()));
        }

        // Cursor parameters
        if (ctx.parameter_spec() != null) {
            for (PlSqlParser.Parameter_specContext ps : ctx.parameter_spec()) {
                ParameterInfo pi = new ParameterInfo();
                pi.setLine(getStartLine(ps));
                if (ps.parameter_name() != null) {
                    pi.setName(getIdentifierText(ps.parameter_name()));
                }
                if (ps.type_spec() != null) {
                    pi.setDataType(getTypeSpecText(ps.type_spec()));
                }
                pi.setDirection("IN");
                if (ps.default_value_part() != null) {
                    pi.setDefaultValue(getOriginalText(ps.default_value_part(), 200));
                }
                ci.getParameters().add(pi);
            }
        }

        // Query
        if (ctx.select_statement() != null) {
            String query = getOriginalText(ctx.select_statement(), 10000);
            ci.setQuery(query);

            // Extract table names from cursor query
            List<String> tables = extractTableNamesFromContext(ctx.select_statement());
            ci.setTables(tables);

            // Promote cursor tables to directTables as SELECT operations
            for (String tbl : tables) {
                TableOperationInfo toi = new TableOperationInfo();
                String schema = null;
                String name = tbl;
                if (tbl.contains(".")) {
                    String[] parts = tbl.split("\\.", 2);
                    schema = parts[0];
                    name = parts[1];
                }
                toi.setTableName(name);
                toi.setSchema(schema);
                toi.setOperation("SELECT");
                toi.setLine(getStartLine(ctx));
                addTableOperation(toi);
            }
        }

        // Check if it is a ref cursor (no query = just declaration)
        if (ctx.select_statement() == null && ctx.RETURN() == null) {
            ci.setRefCursor(false); // simple forward declaration
        }

        addCursor(ci);
        Void result = visitChildren(ctx);

        // Fallback: scan cursor query text for pkg.fn(args) patterns
        // that the tree walk may have missed due to parse error recovery
        if (ctx.select_statement() != null) {
            String queryText = getOriginalText(ctx.select_statement(), 5000);
            extractFunctionCallsFromSqlText(queryText, getStartLine(ctx));
        }

        return result;
    }

    // ---------------------------------------------------------------
    // PRAGMA DECLARATION
    // ---------------------------------------------------------------
    @Override
    public Void visitPragma_declaration(PlSqlParser.Pragma_declarationContext ctx) {
        if (ctx.AUTONOMOUS_TRANSACTION() != null) {
            SubprogramInfo sub = currentSubprogram();
            if (sub != null) {
                sub.setPragmaAutonomousTransaction(true);
            }
        }
        return visitChildren(ctx);
    }

    // ---------------------------------------------------------------
    // SELECT STATEMENT
    // ---------------------------------------------------------------
    @Override
    public Void visitSelect_statement(PlSqlParser.Select_statementContext ctx) {
        // Only record as a statement if we are at the DML statement level
        // (not inside a cursor declaration or subquery)
        if (isDirectDmlStatement(ctx)) {
            StatementInfo si = new StatementInfo();
            si.setType("SELECT");
            si.setLine(getStartLine(ctx));
            si.setLineEnd(getStopLine(ctx));
            si.setSqlText(getOriginalText(ctx, 200));

            String prevDml = currentDmlOperation;
            List<String> prevTables = currentDmlTables;
            currentDmlOperation = "SELECT";
            currentDmlTables = new ArrayList<>();

            Void result = visitChildren(ctx);

            si.setTables(new ArrayList<>(currentDmlTables));
            addStatement(si);

            // Fallback: scan SELECT text for pkg.fn(args) patterns
            // that the tree walk may have missed due to parse error recovery
            String selectText = getOriginalText(ctx, 5000);
            extractFunctionCallsFromSqlText(selectText, getStartLine(ctx));

            currentDmlOperation = prevDml;
            currentDmlTables = prevTables;
            return result;
        }

        return visitChildren(ctx);
    }

    // ---------------------------------------------------------------
    // SUBQUERY — override DML op for nested subqueries
    // ---------------------------------------------------------------
    @Override
    public Void visitSubquery(PlSqlParser.SubqueryContext ctx) {
        String prevSubqCtx = currentSubqueryContext;

        if (currentSubqueryContext == null && isEmbeddedSubquery(ctx)) {
            currentSubqueryContext = classifySubqueryContext(ctx);
        }

        if (!"SELECT".equals(currentDmlOperation)) {
            String prevDml = currentDmlOperation;
            currentDmlOperation = "SELECT";
            Void result = visitChildren(ctx);
            currentDmlOperation = prevDml;
            currentSubqueryContext = prevSubqCtx;
            return result;
        }
        Void result = visitChildren(ctx);
        currentSubqueryContext = prevSubqCtx;
        return result;
    }

    private boolean isEmbeddedSubquery(PlSqlParser.SubqueryContext ctx) {
        ParserRuleContext parent = ctx.getParent();
        if (parent == null) return false;
        // Direct child of select_statement/select_only_statement → top-level, not embedded
        if (parent instanceof PlSqlParser.Select_only_statementContext) {
            ParserRuleContext grandparent = parent.getParent();
            // If select_only_statement is child of select_statement → check if that's a DML statement
            if (grandparent instanceof PlSqlParser.Select_statementContext) {
                return false;
            }
            // select_only_statement inside quantified_expression → embedded (EXISTS, ALL, etc.)
            return true;
        }
        // Subquery inside parenthesized expression or IN clause → embedded
        return true;
    }

    private String classifySubqueryContext(PlSqlParser.SubqueryContext ctx) {
        ParserRuleContext parent = ctx.getParent();
        while (parent != null) {
            if (parent instanceof PlSqlParser.Table_ref_aux_internal_twoContext
                    || parent instanceof PlSqlParser.Dml_table_expression_clauseContext) {
                PlSqlParser.Dml_table_expression_clauseContext dtec =
                        (parent instanceof PlSqlParser.Dml_table_expression_clauseContext)
                                ? (PlSqlParser.Dml_table_expression_clauseContext) parent : null;
                if (dtec != null && dtec.select_statement() != null) return "FROM";
                if (parent instanceof PlSqlParser.Table_ref_aux_internal_twoContext) return "FROM";
            }
            if (parent instanceof PlSqlParser.Quantified_expressionContext) {
                PlSqlParser.Quantified_expressionContext qe =
                        (PlSqlParser.Quantified_expressionContext) parent;
                if (qe.EXISTS() != null) return "EXISTS";
                if (qe.ALL() != null) return "ALL";
                if (qe.ANY() != null || qe.SOME() != null) return "ANY";
            }
            if (parent instanceof PlSqlParser.In_elementsContext) {
                ParserRuleContext inParent = parent.getParent();
                if (inParent != null) {
                    String text = inParent.getText().toUpperCase();
                    if (text.contains("NOTIN") || text.contains("NOT IN")) return "NOT_IN";
                }
                return "IN";
            }
            if (parent instanceof PlSqlParser.StatementContext
                    || parent instanceof PlSqlParser.Data_manipulation_language_statementsContext) {
                break;
            }
            parent = parent.getParent();
        }
        return "SCALAR";
    }

    // ---------------------------------------------------------------
    // CTE (WITH clause) — track alias names so they aren't mistaken for tables
    // ---------------------------------------------------------------
    @Override
    public Void visitSubquery_factoring_clause(PlSqlParser.Subquery_factoring_clauseContext ctx) {
        if (ctx.query_name() != null && ctx.query_name().identifier() != null) {
            queryAliases.add(cleanId(ctx.query_name().identifier().getText()));
        }
        return visitChildren(ctx);
    }

    // ---------------------------------------------------------------
    // Query block — pre-scan FROM clause for inline view aliases before visiting
    // ---------------------------------------------------------------
    @Override
    public Void visitQuery_block(PlSqlParser.Query_blockContext ctx) {
        if (ctx.from_clause() != null) {
            preCollectInlineViewAliases(ctx.from_clause());
        }
        return visitChildren(ctx);
    }

    private void preCollectInlineViewAliases(ParseTree node) {
        if (node instanceof PlSqlParser.Table_ref_auxContext) {
            PlSqlParser.Table_ref_auxContext tra = (PlSqlParser.Table_ref_auxContext) node;
            if (tra.table_alias() != null && tra.table_ref_aux_internal() != null) {
                if (isInlineViewInternal(tra.table_ref_aux_internal())) {
                    String alias = cleanId(tra.table_alias().getText());
                    if (alias != null) {
                        queryAliases.add(alias);
                    }
                }
            }
        }
        for (int i = 0; i < node.getChildCount(); i++) {
            preCollectInlineViewAliases(node.getChild(i));
        }
    }

    private boolean isInlineViewInternal(PlSqlParser.Table_ref_aux_internalContext ctx) {
        if (ctx instanceof PlSqlParser.Table_ref_aux_internal_twoContext) {
            return true;
        }
        if (ctx instanceof PlSqlParser.Table_ref_aux_internal_oneContext) {
            PlSqlParser.Table_ref_aux_internal_oneContext one =
                    (PlSqlParser.Table_ref_aux_internal_oneContext) ctx;
            if (one.dml_table_expression_clause() != null
                    && one.dml_table_expression_clause().select_statement() != null) {
                return true;
            }
        }
        return false;
    }

    // ---------------------------------------------------------------
    // INSERT STATEMENT
    // ---------------------------------------------------------------
    @Override
    public Void visitInsert_statement(PlSqlParser.Insert_statementContext ctx) {
        StatementInfo si = new StatementInfo();
        si.setType("INSERT");
        si.setLine(getStartLine(ctx));
        si.setLineEnd(getStopLine(ctx));
        si.setSqlText(getOriginalText(ctx, 200));

        String prevDml = currentDmlOperation;
        List<String> prevTables = currentDmlTables;
        currentDmlOperation = "INSERT";
        currentDmlTables = new ArrayList<>();

        Void result = visitChildren(ctx);

        si.setTables(new ArrayList<>(currentDmlTables));
        addStatement(si);

        // Fallback: scan INSERT text for pkg.fn(args) patterns
        extractFunctionCallsFromSqlText(getOriginalText(ctx, 5000), getStartLine(ctx));

        currentDmlOperation = prevDml;
        currentDmlTables = prevTables;
        return result;
    }

    // ---------------------------------------------------------------
    // UPDATE STATEMENT
    // ---------------------------------------------------------------
    @Override
    public Void visitUpdate_statement(PlSqlParser.Update_statementContext ctx) {
        StatementInfo si = new StatementInfo();
        si.setType("UPDATE");
        si.setLine(getStartLine(ctx));
        si.setLineEnd(getStopLine(ctx));
        si.setSqlText(getOriginalText(ctx, 200));

        String prevDml = currentDmlOperation;
        List<String> prevTables = currentDmlTables;
        currentDmlOperation = "UPDATE";
        currentDmlTables = new ArrayList<>();

        Void result = visitChildren(ctx);

        si.setTables(new ArrayList<>(currentDmlTables));
        addStatement(si);

        // Fallback: scan UPDATE text for pkg.fn(args) patterns
        extractFunctionCallsFromSqlText(getOriginalText(ctx, 5000), getStartLine(ctx));

        currentDmlOperation = prevDml;
        currentDmlTables = prevTables;
        return result;
    }

    // ---------------------------------------------------------------
    // DELETE STATEMENT
    // ---------------------------------------------------------------
    @Override
    public Void visitDelete_statement(PlSqlParser.Delete_statementContext ctx) {
        StatementInfo si = new StatementInfo();
        si.setType("DELETE");
        si.setLine(getStartLine(ctx));
        si.setLineEnd(getStopLine(ctx));
        si.setSqlText(getOriginalText(ctx, 200));

        String prevDml = currentDmlOperation;
        List<String> prevTables = currentDmlTables;
        currentDmlOperation = "DELETE";
        currentDmlTables = new ArrayList<>();

        Void result = visitChildren(ctx);

        si.setTables(new ArrayList<>(currentDmlTables));
        addStatement(si);

        // Fallback: scan DELETE text for pkg.fn(args) patterns
        extractFunctionCallsFromSqlText(getOriginalText(ctx, 5000), getStartLine(ctx));

        currentDmlOperation = prevDml;
        currentDmlTables = prevTables;
        return result;
    }

    // ---------------------------------------------------------------
    // MERGE STATEMENT
    // ---------------------------------------------------------------
    @Override
    public Void visitMerge_statement(PlSqlParser.Merge_statementContext ctx) {
        StatementInfo si = new StatementInfo();
        si.setType("MERGE");
        si.setLine(getStartLine(ctx));
        si.setLineEnd(getStopLine(ctx));
        si.setSqlText(getOriginalText(ctx, 200));

        String prevDml = currentDmlOperation;
        List<String> prevTables = currentDmlTables;
        String prevMergeTarget = currentMergeTargetTable;
        String prevMergeTargetSchema = currentMergeTargetSchema;
        currentDmlOperation = "MERGE";
        currentDmlTables = new ArrayList<>();

        // Extract target table name from selected_tableview(0) for branch attribution
        currentMergeTargetTable = null;
        currentMergeTargetSchema = null;
        if (ctx.selected_tableview() != null && !ctx.selected_tableview().isEmpty()) {
            PlSqlParser.Selected_tableviewContext stv = ctx.selected_tableview(0);
            if (stv.tableview_name() != null) {
                PlSqlParser.Tableview_nameContext tvn = stv.tableview_name();
                if (tvn.identifier() != null) {
                    String name = cleanId(tvn.identifier().getText());
                    if (tvn.id_expression() != null) {
                        currentMergeTargetSchema = name;
                        currentMergeTargetTable = cleanId(tvn.id_expression().getText());
                    } else {
                        currentMergeTargetTable = name;
                    }
                }
            }
        }

        Void result = visitChildren(ctx);

        si.setTables(new ArrayList<>(currentDmlTables));
        addStatement(si);

        // Fallback: scan MERGE text for pkg.fn(args) patterns
        extractFunctionCallsFromSqlText(getOriginalText(ctx, 5000), getStartLine(ctx));

        currentDmlOperation = prevDml;
        currentDmlTables = prevTables;
        currentMergeTargetTable = prevMergeTarget;
        currentMergeTargetSchema = prevMergeTargetSchema;
        return result;
    }

    // ---------------------------------------------------------------
    // MERGE branch clauses — emit dedicated TableOperationInfo per branch
    // ---------------------------------------------------------------
    @Override
    public Void visitMerge_update_clause(PlSqlParser.Merge_update_clauseContext ctx) {
        if (currentMergeTargetTable != null) {
            TableOperationInfo toi = new TableOperationInfo();
            toi.setTableName(currentMergeTargetTable);
            toi.setSchema(currentMergeTargetSchema);
            toi.setOperation("UPDATE");
            toi.setMergeClause("MATCHED");
            toi.setLine(getStartLine(ctx));
            addTableOperation(toi);
        }
        return visitChildren(ctx);
    }

    @Override
    public Void visitMerge_insert_clause(PlSqlParser.Merge_insert_clauseContext ctx) {
        if (currentMergeTargetTable != null) {
            TableOperationInfo toi = new TableOperationInfo();
            toi.setTableName(currentMergeTargetTable);
            toi.setSchema(currentMergeTargetSchema);
            toi.setOperation("INSERT");
            toi.setMergeClause("NOT_MATCHED");
            toi.setLine(getStartLine(ctx));
            addTableOperation(toi);
        }
        return visitChildren(ctx);
    }

    // ---------------------------------------------------------------
    // TRUNCATE TABLE
    // ---------------------------------------------------------------
    @Override
    public Void visitTruncate_table(PlSqlParser.Truncate_tableContext ctx) {
        StatementInfo si = new StatementInfo();
        si.setType("TRUNCATE");
        si.setLine(getStartLine(ctx));
        si.setLineEnd(getStopLine(ctx));
        si.setSqlText(getOriginalText(ctx, 200));

        String prevDml = currentDmlOperation;
        List<String> prevTables = currentDmlTables;
        currentDmlOperation = "TRUNCATE";
        currentDmlTables = new ArrayList<>();

        Void result = visitChildren(ctx);

        si.setTables(new ArrayList<>(currentDmlTables));
        addStatement(si);

        currentDmlOperation = prevDml;
        currentDmlTables = prevTables;
        return result;
    }

    // ---------------------------------------------------------------
    // TABLEVIEW_NAME - extract table references
    // ---------------------------------------------------------------
    @Override
    public Void visitTableview_name(PlSqlParser.Tableview_nameContext ctx) {
        // Only record table operations when inside a DML context
        if (currentDmlOperation != null) {
            String tableName = null;
            String schemaName = null;
            String dbLink = null;

            if (ctx.identifier() != null) {
                tableName = cleanId(ctx.identifier().getText());

                if (ctx.id_expression() != null) {
                    // schema.table pattern
                    schemaName = tableName;
                    tableName = cleanId(ctx.id_expression().getText());
                }

                if (ctx.link_name() != null) {
                    dbLink = ctx.link_name().getText().toUpperCase();
                    ParsedObject po = currentObject();
                    if (po != null) {
                        po.getDependencies().getDbLinks().add(dbLink);
                    }
                }

                if (tableName != null && !isDualTable(tableName)
                        && (schemaName != null || !queryAliases.contains(tableName))) {
                    // Add to DML tables list
                    String fullName = schemaName != null ? schemaName + "." + tableName : tableName;
                    if (currentDmlTables != null && !currentDmlTables.contains(fullName)) {
                        currentDmlTables.add(fullName);
                    }

                    // Create table operation
                    TableOperationInfo toi = new TableOperationInfo();
                    toi.setTableName(tableName);
                    toi.setSchema(schemaName);
                    toi.setOperation(currentDmlOperation);
                    toi.setLine(getStartLine(ctx));
                    toi.setDbLink(dbLink);

                    // Try to get alias from parent table_ref_aux
                    String alias = findTableAlias(ctx);
                    toi.setAlias(alias);

                    addTableOperation(toi);
                }
            }
        }

        return visitChildren(ctx);
    }

    // ---------------------------------------------------------------
    // FROM CLAUSE — detect implicit (comma) joins
    // ---------------------------------------------------------------
    @Override
    public Void visitFrom_clause(PlSqlParser.From_clauseContext ctx) {
        if (ctx.table_ref_list() == null) return visitChildren(ctx);

        List<PlSqlParser.Table_refContext> tableRefs = ctx.table_ref_list().table_ref();
        if (tableRefs == null || tableRefs.size() < 2) return visitChildren(ctx);

        // Extract direct table names from each top-level table_ref before visiting
        // (to avoid mixing with subquery tables added during visitChildren)
        List<String[]> commaTableNames = new ArrayList<>(); // [tableName, schema, alias, line]
        for (PlSqlParser.Table_refContext tref : tableRefs) {
            if (tref.table_ref_aux() != null && tref.table_ref_aux().table_ref_aux_internal() != null) {
                List<String> names = extractTableNamesFromContext(tref.table_ref_aux().table_ref_aux_internal());
                if (!names.isEmpty()) {
                    String fullName = names.get(0);
                    String schema = null;
                    String tableName = fullName;
                    if (fullName.contains(".")) {
                        schema = fullName.substring(0, fullName.indexOf('.'));
                        tableName = fullName.substring(fullName.indexOf('.') + 1);
                    }
                    String alias = null;
                    if (tref.table_ref_aux().table_alias() != null) {
                        alias = cleanId(tref.table_ref_aux().table_alias().getText());
                    }
                    String line = String.valueOf(getStartLine(tref));
                    commaTableNames.add(new String[]{tableName, schema, alias, line});
                }
            }
        }

        Void result = visitChildren(ctx);

        // After visiting, attach implicit joins to the first table's TableOperationInfo
        if (commaTableNames.size() < 2) return result;

        // Find the TableOperationInfo for the first table
        SubprogramInfo sub = currentSubprogram();
        List<TableOperationInfo> opsList = null;
        if (sub != null) {
            opsList = sub.getTableOperations();
        } else {
            ParsedObject po = currentObject();
            if (po != null) opsList = po.getTableOperations();
        }
        if (opsList == null || opsList.isEmpty()) return result;

        // Find the table op matching the first comma-separated table name and line
        String firstTable = commaTableNames.get(0)[0];
        String firstLine = commaTableNames.get(0)[3];
        TableOperationInfo baseOp = null;
        for (int i = opsList.size() - 1; i >= 0; i--) {
            TableOperationInfo op = opsList.get(i);
            if (op.getTableName() != null && op.getTableName().equalsIgnoreCase(firstTable)
                    && String.valueOf(op.getLine()).equals(firstLine)) {
                baseOp = op;
                break;
            }
        }
        if (baseOp == null) return result;

        for (int i = 1; i < commaTableNames.size(); i++) {
            String[] info = commaTableNames.get(i);
            JoinInfo ji = new JoinInfo();
            ji.setJoinType("IMPLICIT");
            ji.setJoinedTable(info[1] != null ? info[1] + "." + info[0] : info[0]);
            ji.setJoinedTableAlias(info[2]);
            ji.setLine(Integer.parseInt(info[3]));
            baseOp.getJoins().add(ji);
        }

        return result;
    }

    // ---------------------------------------------------------------
    // JOIN CLAUSE
    // ---------------------------------------------------------------
    @Override
    public Void visitJoin_clause(PlSqlParser.Join_clauseContext ctx) {
        JoinInfo ji = new JoinInfo();
        ji.setLine(getStartLine(ctx));
        ji.setJoinType(determineJoinType(ctx));

        if (ctx.join_on_part() != null && !ctx.join_on_part().isEmpty()) {
            ji.setCondition(getOriginalText(ctx.join_on_part(0), 200));
        } else if (ctx.join_using_part() != null && !ctx.join_using_part().isEmpty()) {
            ji.setCondition(getOriginalText(ctx.join_using_part(0), 200));
        }

        if (ctx.table_ref_aux() != null) {
            List<String> joinedTables = extractTableNamesFromContext(ctx.table_ref_aux());
            if (!joinedTables.isEmpty()) {
                String jtRef = joinedTables.get(0);
                if (jtRef.contains(".")) {
                    ji.setJoinedTable(jtRef);
                } else {
                    ji.setJoinedTable(jtRef);
                }
            }
            if (ctx.table_ref_aux().table_alias() != null) {
                ji.setJoinedTableAlias(cleanId(ctx.table_ref_aux().table_alias().getText()));
            }
        }

        attachJoinToLastTableOp(ji);
        return visitChildren(ctx);
    }

    private String determineJoinType(PlSqlParser.Join_clauseContext ctx) {
        if (ctx.CROSS() != null && ctx.APPLY() != null) return "CROSS APPLY";
        if (ctx.OUTER() != null && ctx.APPLY() != null) return "OUTER APPLY";
        if (ctx.CROSS() != null) return "CROSS";
        if (ctx.NATURAL() != null) return "NATURAL";
        if (ctx.outer_join_type() != null) {
            PlSqlParser.Outer_join_typeContext ojt = ctx.outer_join_type();
            if (ojt.FULL() != null) return "FULL OUTER";
            if (ojt.LEFT() != null) return "LEFT OUTER";
            if (ojt.RIGHT() != null) return "RIGHT OUTER";
        }
        if (ctx.INNER() != null) return "INNER";
        return "INNER";
    }

    private void attachJoinToLastTableOp(JoinInfo ji) {
        SubprogramInfo sub = currentSubprogram();
        List<TableOperationInfo> ops = null;
        if (sub != null) {
            ops = sub.getTableOperations();
        } else {
            ParsedObject po = currentObject();
            if (po != null) ops = po.getTableOperations();
        }
        if (ops != null && !ops.isEmpty()) {
            ops.get(ops.size() - 1).getJoins().add(ji);
        }
    }

    // ---------------------------------------------------------------
    // EXECUTE IMMEDIATE
    // ---------------------------------------------------------------
    @Override
    public Void visitExecute_immediate(PlSqlParser.Execute_immediateContext ctx) {
        StatementInfo si = new StatementInfo();
        si.setType("EXECUTE_IMMEDIATE");
        si.setLine(getStartLine(ctx));
        si.setLineEnd(getStopLine(ctx));
        si.setSqlText(getOriginalText(ctx, 200));
        addStatement(si);

        DynamicSqlInfo dsi = new DynamicSqlInfo();
        dsi.setType("EXECUTE_IMMEDIATE");
        dsi.setLine(getStartLine(ctx));

        if (ctx.expression() != null) {
            String exprText = getOriginalText(ctx.expression(), 500);
            dsi.setSqlExpression(exprText);
            extractTablesFromDynamicSql(exprText, getStartLine(ctx));
        }

        if (ctx.using_clause() != null) {
            extractUsingVariables(ctx.using_clause(), dsi);
        }

        addDynamicSql(dsi);

        return visitChildren(ctx);
    }

    // ---------------------------------------------------------------
    // OPEN FOR STATEMENT (dynamic cursor)
    // ---------------------------------------------------------------
    @Override
    public Void visitOpen_for_statement(PlSqlParser.Open_for_statementContext ctx) {
        StatementInfo si = new StatementInfo();
        si.setType("OPEN_CURSOR");
        si.setLine(getStartLine(ctx));
        si.setLineEnd(getStopLine(ctx));
        si.setSqlText(getOriginalText(ctx, 200));
        addStatement(si);

        if (ctx.select_statement() != null) {
            // Static OPEN FOR SELECT — extract tables
            String prevDml = currentDmlOperation;
            List<String> prevTables = currentDmlTables;
            currentDmlOperation = "SELECT";
            currentDmlTables = new ArrayList<>();
            Void result = visitChildren(ctx);
            si.setTables(new ArrayList<>(currentDmlTables));
            currentDmlOperation = prevDml;
            currentDmlTables = prevTables;
            return result;
        } else if (ctx.expression() != null) {
            DynamicSqlInfo dsi = new DynamicSqlInfo();
            dsi.setType("OPEN_FOR");
            dsi.setLine(getStartLine(ctx));
            String exprText = getOriginalText(ctx.expression(), 500);
            dsi.setSqlExpression(exprText);

            if (ctx.using_clause() != null) {
                extractUsingVariables(ctx.using_clause(), dsi);
            }

            addDynamicSql(dsi);
            extractTablesFromDynamicSql(exprText, getStartLine(ctx));
        }

        return visitChildren(ctx);
    }

    // ---------------------------------------------------------------
    // OPEN STATEMENT (explicit cursor)
    // ---------------------------------------------------------------
    @Override
    public Void visitOpen_statement(PlSqlParser.Open_statementContext ctx) {
        StatementInfo si = new StatementInfo();
        si.setType("OPEN_CURSOR");
        si.setLine(getStartLine(ctx));
        si.setLineEnd(getStopLine(ctx));
        si.setSqlText(getOriginalText(ctx, 200));
        addStatement(si);
        return visitChildren(ctx);
    }

    // ---------------------------------------------------------------
    // FETCH STATEMENT
    // ---------------------------------------------------------------
    @Override
    public Void visitFetch_statement(PlSqlParser.Fetch_statementContext ctx) {
        StatementInfo si = new StatementInfo();
        si.setLine(getStartLine(ctx));
        si.setLineEnd(getStopLine(ctx));
        si.setSqlText(getOriginalText(ctx, 200));

        if (ctx.BULK() != null) {
            si.setType("BULK_COLLECT");
            List<PlSqlParser.Variable_or_collectionContext> vars = ctx.variable_or_collection();
            if (vars != null && !vars.isEmpty()) {
                si.setCollectionName(cleanId(vars.get(0).getText()));
            }
            if (ctx.LIMIT() != null) {
                if (ctx.numeric() != null) {
                    si.setLimitValue(ctx.numeric().getText());
                } else if (vars != null && vars.size() > 1) {
                    si.setLimitValue(cleanId(vars.get(vars.size() - 1).getText()));
                }
            }
        } else {
            si.setType("FETCH");
        }

        addStatement(si);
        return visitChildren(ctx);
    }

    // ---------------------------------------------------------------
    // CLOSE STATEMENT
    // ---------------------------------------------------------------
    @Override
    public Void visitClose_statement(PlSqlParser.Close_statementContext ctx) {
        StatementInfo si = new StatementInfo();
        si.setType("CLOSE_CURSOR");
        si.setLine(getStartLine(ctx));
        si.setLineEnd(getStopLine(ctx));
        si.setSqlText(getOriginalText(ctx, 200));
        addStatement(si);
        return visitChildren(ctx);
    }

    // ---------------------------------------------------------------
    // IF STATEMENT
    // ---------------------------------------------------------------
    @Override
    public Void visitIf_statement(PlSqlParser.If_statementContext ctx) {
        StatementInfo si = new StatementInfo();
        si.setType("IF");
        si.setLine(getStartLine(ctx));
        si.setLineEnd(getStopLine(ctx));
        addStatement(si);
        return visitChildren(ctx);
    }

    // ---------------------------------------------------------------
    // LOOP STATEMENT (handles LOOP, WHILE, FOR, cursor FOR)
    // ---------------------------------------------------------------
    @Override
    public Void visitLoop_statement(PlSqlParser.Loop_statementContext ctx) {
        StatementInfo si = new StatementInfo();
        si.setLine(getStartLine(ctx));
        si.setLineEnd(getStopLine(ctx));

        if (ctx.WHILE() != null) {
            si.setType("WHILE_LOOP");
        } else if (ctx.FOR() != null) {
            // Check if it is a cursor FOR loop or numeric FOR loop
            if (ctx.cursor_loop_param() != null) {
                PlSqlParser.Cursor_loop_paramContext clp = ctx.cursor_loop_param();
                if (clp.cursor_name() != null || clp.select_statement() != null) {
                    si.setType("CURSOR_FOR_LOOP");

                    // Record cursor info for inline cursor FOR loops
                    if (clp.select_statement() != null) {
                        CursorInfo ci = new CursorInfo();
                        ci.setLine(getStartLine(clp));
                        ci.setLineEnd(getStopLine(clp));
                        ci.setForLoop(true);
                        ci.setQuery(getOriginalText(clp.select_statement(), 10000));
                        if (clp.record_name() != null) {
                            ci.setName("(FOR " + getIdentifierText(clp.record_name()) + ")");
                        }
                        List<String> tables = extractTableNamesFromContext(clp.select_statement());
                        ci.setTables(tables);
                        addCursor(ci);

                        // Promote cursor tables to directTables as SELECT operations
                        for (String tbl : tables) {
                            TableOperationInfo toi = new TableOperationInfo();
                            String schema = null;
                            String name = tbl;
                            if (tbl.contains(".")) {
                                String[] parts = tbl.split("\\.", 2);
                                schema = parts[0];
                                name = parts[1];
                            }
                            toi.setTableName(name);
                            toi.setSchema(schema);
                            toi.setOperation("SELECT");
                            toi.setLine(getStartLine(clp));
                            addTableOperation(toi);
                        }
                    }
                } else {
                    si.setType("FOR_LOOP");
                }
            } else {
                si.setType("FOR_LOOP");
            }
        } else {
            si.setType("LOOP");
        }

        loopNestingDepth++;
        si.setNestingDepth(loopNestingDepth);
        addStatement(si);
        Void result = visitChildren(ctx);

        // Fallback: scan cursor FOR loop query for pkg.fn(args) patterns
        if (ctx.cursor_loop_param() != null && ctx.cursor_loop_param().select_statement() != null) {
            String queryText = getOriginalText(ctx.cursor_loop_param().select_statement(), 5000);
            extractFunctionCallsFromSqlText(queryText, getStartLine(ctx));
        }

        loopNestingDepth--;
        return result;
    }

    // ---------------------------------------------------------------
    // FORALL STATEMENT
    // ---------------------------------------------------------------
    @Override
    public Void visitForall_statement(PlSqlParser.Forall_statementContext ctx) {
        StatementInfo si = new StatementInfo();
        si.setType("FORALL");
        si.setLine(getStartLine(ctx));
        si.setLineEnd(getStopLine(ctx));
        si.setSqlText(getOriginalText(ctx, 200));

        if (ctx.index_name() != null) {
            si.setIndexVariable(cleanId(ctx.index_name().getText()));
        }
        if (ctx.bounds_clause() != null) {
            si.setBoundsExpression(getOriginalText(ctx.bounds_clause(), 200));
        }
        if (ctx.data_manipulation_language_statements() != null) {
            PlSqlParser.Data_manipulation_language_statementsContext dml =
                    ctx.data_manipulation_language_statements();
            if (dml.insert_statement() != null) si.setDmlType("INSERT");
            else if (dml.update_statement() != null) si.setDmlType("UPDATE");
            else if (dml.delete_statement() != null) si.setDmlType("DELETE");
            else if (dml.merge_statement() != null) si.setDmlType("MERGE");
        } else if (ctx.execute_immediate() != null) {
            si.setDmlType("EXECUTE_IMMEDIATE");
        }

        loopNestingDepth++;
        si.setNestingDepth(loopNestingDepth);
        addStatement(si);
        Void result = visitChildren(ctx);
        loopNestingDepth--;
        return result;
    }

    // ---------------------------------------------------------------
    // RETURN STATEMENT
    // ---------------------------------------------------------------
    @Override
    public Void visitReturn_statement(PlSqlParser.Return_statementContext ctx) {
        StatementInfo si = new StatementInfo();
        si.setType("RETURN");
        si.setLine(getStartLine(ctx));
        si.setLineEnd(getStopLine(ctx));
        addStatement(si);
        return visitChildren(ctx);
    }

    // ---------------------------------------------------------------
    // GOTO STATEMENT
    // ---------------------------------------------------------------
    @Override
    public Void visitGoto_statement(PlSqlParser.Goto_statementContext ctx) {
        StatementInfo si = new StatementInfo();
        si.setType("GOTO");
        si.setLine(getStartLine(ctx));
        si.setLineEnd(getStopLine(ctx));
        addStatement(si);
        return visitChildren(ctx);
    }

    // ---------------------------------------------------------------
    // RAISE STATEMENT
    // ---------------------------------------------------------------
    @Override
    public Void visitRaise_statement(PlSqlParser.Raise_statementContext ctx) {
        StatementInfo si = new StatementInfo();
        si.setType("RAISE");
        si.setLine(getStartLine(ctx));
        si.setLineEnd(getStopLine(ctx));
        addStatement(si);
        return visitChildren(ctx);
    }

    // ---------------------------------------------------------------
    // EXIT STATEMENT
    // ---------------------------------------------------------------
    @Override
    public Void visitExit_statement(PlSqlParser.Exit_statementContext ctx) {
        StatementInfo si = new StatementInfo();
        si.setType("EXIT");
        si.setLine(getStartLine(ctx));
        si.setLineEnd(getStopLine(ctx));
        addStatement(si);
        return visitChildren(ctx);
    }

    // ---------------------------------------------------------------
    // COMMIT STATEMENT
    // ---------------------------------------------------------------
    @Override
    public Void visitCommit_statement(PlSqlParser.Commit_statementContext ctx) {
        StatementInfo si = new StatementInfo();
        si.setType("COMMIT");
        si.setLine(getStartLine(ctx));
        si.setLineEnd(getStopLine(ctx));
        addStatement(si);
        return visitChildren(ctx);
    }

    // ---------------------------------------------------------------
    // ROLLBACK STATEMENT
    // ---------------------------------------------------------------
    @Override
    public Void visitRollback_statement(PlSqlParser.Rollback_statementContext ctx) {
        StatementInfo si = new StatementInfo();
        si.setType("ROLLBACK");
        si.setLine(getStartLine(ctx));
        si.setLineEnd(getStopLine(ctx));
        addStatement(si);
        return visitChildren(ctx);
    }

    // ---------------------------------------------------------------
    // SAVEPOINT STATEMENT
    // ---------------------------------------------------------------
    @Override
    public Void visitSavepoint_statement(PlSqlParser.Savepoint_statementContext ctx) {
        StatementInfo si = new StatementInfo();
        si.setType("SAVEPOINT");
        si.setLine(getStartLine(ctx));
        si.setLineEnd(getStopLine(ctx));
        if (ctx.savepoint_name() != null && ctx.savepoint_name().identifier() != null) {
            si.setSavepointName(cleanId(ctx.savepoint_name().identifier().getText()));
        }
        addStatement(si);
        return visitChildren(ctx);
    }

    // ---------------------------------------------------------------
    // IMPLICIT CURSOR ATTRIBUTES — SQL%ROWCOUNT, SQL%FOUND, etc.
    // Detected via other_function: cursor_name PERCENT_*
    // ---------------------------------------------------------------
    @Override
    public Void visitOther_function(PlSqlParser.Other_functionContext ctx) {
        if (ctx.cursor_name() != null && (
                ctx.PERCENT_ROWCOUNT() != null
                || ctx.PERCENT_FOUND() != null
                || ctx.PERCENT_NOTFOUND() != null
                || ctx.PERCENT_ISOPEN() != null)) {

            String cursorName = ctx.cursor_name().getText().toUpperCase();
            String attr = ctx.PERCENT_ROWCOUNT() != null ? "%ROWCOUNT"
                    : ctx.PERCENT_FOUND() != null ? "%FOUND"
                    : ctx.PERCENT_NOTFOUND() != null ? "%NOTFOUND"
                    : "%ISOPEN";

            StatementInfo si = new StatementInfo();
            si.setType("CURSOR_ATTRIBUTE");
            si.setLine(getStartLine(ctx));
            si.setLineEnd(getStopLine(ctx));
            si.setSqlText(cursorName + attr);
            addStatement(si);
        }
        return visitChildren(ctx);
    }

    // ---------------------------------------------------------------
    // PIPE ROW STATEMENT
    // ---------------------------------------------------------------
    @Override
    public Void visitPipe_row_statement(PlSqlParser.Pipe_row_statementContext ctx) {
        StatementInfo si = new StatementInfo();
        si.setType("PIPE_ROW");
        si.setLine(getStartLine(ctx));
        si.setLineEnd(getStopLine(ctx));
        addStatement(si);
        return visitChildren(ctx);
    }

    // ---------------------------------------------------------------
    // ASSIGNMENT STATEMENT
    // ---------------------------------------------------------------
    @Override
    public Void visitAssignment_statement(PlSqlParser.Assignment_statementContext ctx) {
        StatementInfo si = new StatementInfo();
        si.setType("ASSIGNMENT");
        si.setLine(getStartLine(ctx));
        si.setLineEnd(getStopLine(ctx));
        addStatement(si);
        return visitChildren(ctx);
    }

    // ---------------------------------------------------------------
    // EXCEPTION HANDLER
    // ---------------------------------------------------------------
    @Override
    public Void visitException_handler(PlSqlParser.Exception_handlerContext ctx) {
        ExceptionHandlerInfo ehi = new ExceptionHandlerInfo();
        ehi.setLine(getStartLine(ctx));
        ehi.setLineEnd(getStopLine(ctx));

        // Exception names (can be multiple with OR)
        List<String> names = new ArrayList<>();
        if (ctx.exception_name() != null) {
            for (PlSqlParser.Exception_nameContext en : ctx.exception_name()) {
                names.add(cleanId(en.getText()));
            }
        }
        ehi.setExceptionName(String.join(" OR ", names));

        // Count statements in the handler
        if (ctx.seq_of_statements() != null) {
            ehi.setStatementsCount(countStatements(ctx.seq_of_statements()));
        }

        addExceptionHandler(ehi);

        ExceptionHandlerInfo prevHandler = currentExceptionHandler;
        currentExceptionHandler = ehi;
        Void result = visitChildren(ctx);
        currentExceptionHandler = prevHandler;
        return result;
    }

    // ---------------------------------------------------------------
    // CALL STATEMENT (CALL pkg.proc())
    // ---------------------------------------------------------------
    @Override
    public Void visitCall_statement(PlSqlParser.Call_statementContext ctx) {
        if (ctx.routine_name() != null && !ctx.routine_name().isEmpty()) {
            String fullName = ctx.routine_name(0).getText();
            if (ctx.routine_name().size() > 1) {
                StringBuilder sb = new StringBuilder(fullName);
                for (int i = 1; i < ctx.routine_name().size(); i++) {
                    sb.append(".").append(ctx.routine_name(i).getText());
                }
                fullName = sb.toString();
            }
            int argCount = 0;
            if (ctx.function_argument() != null && !ctx.function_argument().isEmpty()) {
                PlSqlParser.Function_argumentContext fa = ctx.function_argument(0);
                if (fa.argument() != null) {
                    argCount = fa.argument().size();
                }
            }

            CallInfo ci = classifyCall(fullName, argCount, getStartLine(ctx));
            addCall(ci);

            // Check for RAISE_APPLICATION_ERROR
            String upperName = fullName.toUpperCase();
            if ("RAISE_APPLICATION_ERROR".equals(upperName)) {
                StatementInfo rae = new StatementInfo();
                rae.setType("RAISE_APPLICATION_ERROR");
                rae.setLine(getStartLine(ctx));
                rae.setLineEnd(getStopLine(ctx));
                extractRaiseApplicationErrorArgs(ctx, rae);
                addStatement(rae);
            }

            // Check for DBMS_SQL references
            if (upperName.startsWith("DBMS_SQL.")) {
                DynamicSqlInfo dsi = new DynamicSqlInfo();
                dsi.setType("DBMS_SQL");
                dsi.setLine(getStartLine(ctx));
                if (upperName.endsWith(".PARSE")) {
                    String sqlArg = extractNthCallArg(ctx, 1);
                    dsi.setSqlExpression(sqlArg != null ? sqlArg : fullName);
                    if (sqlArg != null) {
                        extractTablesFromDynamicSql(sqlArg, getStartLine(ctx));
                    }
                } else {
                    dsi.setSqlExpression(fullName);
                }
                addDynamicSql(dsi);
            }
        }
        return visitChildren(ctx);
    }

    /**
     * Extract the numeric error code and string message from a RAISE_APPLICATION_ERROR call.
     * Expected signature: RAISE_APPLICATION_ERROR(error_code, message [, keeperrors])
     */
    private void extractRaiseApplicationErrorArgs(PlSqlParser.Call_statementContext ctx, StatementInfo si) {
        if (ctx.function_argument() == null || ctx.function_argument().isEmpty()) return;
        PlSqlParser.Function_argumentContext fa = ctx.function_argument(0);
        if (fa.argument() == null || fa.argument().size() < 2) return;

        // First argument: error code (numeric literal, possibly negative)
        String codeText = fa.argument(0).getText().trim();
        try {
            si.setErrorCode(Integer.parseInt(codeText.replace(" ", "")));
        } catch (NumberFormatException ignored) {
            // not a simple literal; store as-is if we can't parse
        }

        // Second argument: message string (strip quotes if it is a simple string literal)
        String msg = fa.argument(1).getText().trim();
        if (msg.startsWith("'") && msg.endsWith("'") && msg.length() >= 2) {
            msg = msg.substring(1, msg.length() - 1).replace("''", "'");
        }
        si.setErrorMessage(msg);
    }

    /**
     * Extract error code + message from RAISE_APPLICATION_ERROR used as a general_element call.
     */
    private void extractRaiseApplicationErrorArgsFromGeneral(PlSqlParser.General_elementContext ctx, StatementInfo si) {
        for (PlSqlParser.General_element_partContext part : ctx.general_element_part()) {
            if (part.function_argument() != null && !part.function_argument().isEmpty()) {
                PlSqlParser.Function_argumentContext fa = part.function_argument(0);
                if (fa.argument() == null || fa.argument().size() < 2) return;

                String codeText = fa.argument(0).getText().trim();
                try {
                    si.setErrorCode(Integer.parseInt(codeText.replace(" ", "")));
                } catch (NumberFormatException ignored) {
                    // not a simple numeric literal
                }

                String msg = fa.argument(1).getText().trim();
                if (msg.startsWith("'") && msg.endsWith("'") && msg.length() >= 2) {
                    msg = msg.substring(1, msg.length() - 1).replace("''", "'");
                }
                si.setErrorMessage(msg);
                return;
            }
        }
    }

    // ---------------------------------------------------------------
    // GENERAL ELEMENT - the main rule for PL/SQL calls and variable refs
    // ---------------------------------------------------------------
    @Override
    public Void visitGeneral_element(PlSqlParser.General_elementContext ctx) {
        // general_element can be:
        // 1) a single general_element_part (simple name or call)
        // 2) general_element '.' general_element_part (dotted: pkg.proc, schema.pkg.proc)
        // 3) '(' general_element ')'

        // Only process if this is a call statement context or assignment LHS
        // We detect calls by checking if any general_element_part has function_argument
        List<PlSqlParser.General_element_partContext> parts = ctx.general_element_part();

        if (parts != null && !parts.isEmpty()) {
            // Collect all part names and check for function arguments
            List<String> partNames = new ArrayList<>();
            boolean hasArgs = false;
            int totalArgs = 0;
            int argsOnPartIndex = -1;

            for (int pi = 0; pi < parts.size(); pi++) {
                PlSqlParser.General_element_partContext part = parts.get(pi);
                if (part.id_expression() != null) {
                    partNames.add(cleanId(part.id_expression().getText()));
                }
                if (part.function_argument() != null && !part.function_argument().isEmpty()) {
                    hasArgs = true;
                    argsOnPartIndex = pi;
                    PlSqlParser.Function_argumentContext fa = part.function_argument(0);
                    if (fa.argument() != null) {
                        totalArgs = fa.argument().size();
                    }
                }
            }

            // collection(index).field: args on non-last part means subscript, not call
            if (hasArgs && argsOnPartIndex >= 0 && argsOnPartIndex < parts.size() - 1) {
                return visitChildren(ctx);
            }

            // Also check if there is a nested general_element (recursive dot notation)
            boolean nestedHasArgs = false;
            if (ctx.general_element() != null && parts.size() > 0) {
                // This is the dotted form: general_element '.' general_element_part+
                List<String> nestedNames = collectGeneralElementParts(ctx.general_element());
                List<String> allNames = new ArrayList<>(nestedNames);
                allNames.addAll(partNames);
                partNames = allNames;

                if (!hasArgs) {
                    nestedHasArgs = hasAnyFunctionArgument(ctx.general_element());
                    hasArgs = nestedHasArgs;
                }
            }

            // nested general_element has args but direct parts don't: also collection access
            if (nestedHasArgs && argsOnPartIndex < 0 && ctx.general_element() != null
                    && parts.size() > 0 && partNames.size() >= 2) {
                return visitChildren(ctx);
            }

            if (hasArgs && !partNames.isEmpty()) {
                // This is a function/procedure call
                String fullName = String.join(".", partNames);
                CallInfo ci = classifyCall(fullName, totalArgs, getStartLine(ctx));
                addCall(ci);

                // Check for RAISE_APPLICATION_ERROR used as expression (non-CALL path)
                if (partNames.size() == 1 && "RAISE_APPLICATION_ERROR".equals(partNames.get(0))) {
                    StatementInfo rae = new StatementInfo();
                    rae.setType("RAISE_APPLICATION_ERROR");
                    rae.setLine(getStartLine(ctx));
                    rae.setLineEnd(getStopLine(ctx));
                    extractRaiseApplicationErrorArgsFromGeneral(ctx, rae);
                    addStatement(rae);
                }

                // Check for DBMS_SQL references
                if (partNames.size() >= 2 && "DBMS_SQL".equals(partNames.get(0))) {
                    DynamicSqlInfo dsi = new DynamicSqlInfo();
                    dsi.setType("DBMS_SQL");
                    dsi.setLine(getStartLine(ctx));
                    if ("PARSE".equals(partNames.get(partNames.size() - 1))) {
                        String sqlArg = extractNthArgument(ctx, 1);
                        dsi.setSqlExpression(sqlArg != null ? sqlArg : fullName);
                        if (sqlArg != null) {
                            extractTablesFromDynamicSql(sqlArg, getStartLine(ctx));
                        }
                    } else {
                        dsi.setSqlExpression(fullName);
                    }
                    addDynamicSql(dsi);
                }
            } else if (!hasArgs && partNames.size() >= 2) {
                // No parentheses = not a function call
                String lastPart = partNames.get(partNames.size() - 1);

                // Check for sequence references: seq.NEXTVAL, schema.seq.NEXTVAL,
                // seq.CURRVAL, schema.seq.CURRVAL
                if ("NEXTVAL".equals(lastPart) || "CURRVAL".equals(lastPart)) {
                    addSequenceReference(partNames, lastPart, getStartLine(ctx));
                } else {
                    // Could be an external package variable reference: PKG_NAME.VAR_NAME
                    // Only if the first part is not the current package
                    String firstPart = partNames.get(0);

                    // Check if this is inside an assignment_statement or expression context
                    // to avoid false positives from grammar ambiguity
                    if (firstPart != null && currentPackageName != null
                        && !firstPart.equals(currentPackageName)
                        && !isKnownKeyword(firstPart)
                        && isLikelyPackageVarRef(ctx)) {

                        String ref = String.join(".", partNames);
                        addExternalPackageVarRef(ref);

                        // Add to dependencies
                        ParsedObject po = currentObject();
                        if (po != null) {
                            po.getDependencies().getPackages().add(firstPart);
                        }
                    }
                }
            }
        }

        return visitChildren(ctx);
    }

    // ---------------------------------------------------------------
    // Helper: classify a call as INTERNAL, EXTERNAL, BUILTIN, or DYNAMIC
    // ---------------------------------------------------------------
    private CallInfo classifyCall(String fullName, int argCount, int line) {
        CallInfo ci = new CallInfo();
        ci.setLine(line);
        ci.setArguments(argCount);

        String[] parts = fullName.split("\\.");
        if (parts.length == 1) {
            // Simple name - could be internal or external
            String name = cleanId(parts[0]);
            ci.setName(name);

            if (currentPackageSubprograms.contains(name)) {
                ci.setType("INTERNAL");
            } else if (isBuiltinFunction(name)) {
                ci.setType("BUILTIN");
            } else {
                ci.setType("EXTERNAL");
            }
        } else if (parts.length == 2) {
            // pkg.proc or schema.proc
            String pkg = cleanId(parts[0]);
            String name = cleanId(parts[1]);
            ci.setPackageName(pkg);
            ci.setName(name);

            if (currentPackageName != null && pkg.equals(currentPackageName)) {
                ci.setType("INTERNAL");
            } else if (BUILTIN_PACKAGES.contains(pkg)) {
                ci.setType("BUILTIN");
                // Track package dependency
                ParsedObject po = currentObject();
                if (po != null) {
                    po.getDependencies().getPackages().add(pkg);
                }
            } else {
                ci.setType("EXTERNAL");
                // Track package dependency
                ParsedObject po = currentObject();
                if (po != null) {
                    po.getDependencies().getPackages().add(pkg);
                }
            }
        } else if (parts.length >= 3) {
            // schema.pkg.proc
            String schema = cleanId(parts[0]);
            String pkg = cleanId(parts[1]);
            String name = cleanId(parts[2]);
            ci.setSchema(schema);
            ci.setPackageName(pkg);
            ci.setName(name);

            if (BUILTIN_PACKAGES.contains(pkg)) {
                ci.setType("BUILTIN");
            } else {
                ci.setType("EXTERNAL");
            }
            ParsedObject po = currentObject();
            if (po != null) {
                po.getDependencies().getPackages().add(pkg);
            }
        }

        // Detect sequence references (*.NEXTVAL, *.CURRVAL)
        if (ci.getName() != null &&
            ("NEXTVAL".equals(ci.getName()) || "CURRVAL".equals(ci.getName()))) {
            ParsedObject po = currentObject();
            if (po != null && ci.getPackageName() != null) {
                po.getDependencies().getSequences().add(ci.getPackageName());
            }
        }

        return ci;
    }

    // ---------------------------------------------------------------
    // Helper: parse qualified name like schema.name or just name
    // ---------------------------------------------------------------
    private void parseQualifiedName(ParsedObject po, String fullName) {
        if (fullName == null) return;
        String[] parts = fullName.split("\\.");
        if (parts.length == 1) {
            po.setName(cleanId(parts[0]));
        } else if (parts.length >= 2) {
            po.setSchema(cleanId(parts[0]));
            po.setName(cleanId(parts[1]));
        }
    }

    // ---------------------------------------------------------------
    // Helper: extract table names from a context by walking its tableview_name children
    // ---------------------------------------------------------------
    private List<String> extractTableNamesFromContext(ParserRuleContext ctx) {
        List<String> tables = new ArrayList<>();
        collectTableviewNames(ctx, tables);
        return tables;
    }

    private void collectTableviewNames(ParseTree node, List<String> tables) {
        if (node instanceof PlSqlParser.Tableview_nameContext) {
            PlSqlParser.Tableview_nameContext tvn = (PlSqlParser.Tableview_nameContext) node;
            if (tvn.identifier() != null) {
                String schema = null;
                String tblName = cleanId(tvn.identifier().getText());
                if (tvn.id_expression() != null) {
                    schema = tblName;
                    tblName = cleanId(tvn.id_expression().getText());
                }
                String fullName = schema != null ? schema + "." + tblName : tblName;
                if (tblName != null && !isDualTable(tblName) && !tables.contains(fullName)
                        && (schema != null || !queryAliases.contains(tblName))) {
                    tables.add(fullName);
                }
            }
            return;
        }
        for (int i = 0; i < node.getChildCount(); i++) {
            collectTableviewNames(node.getChild(i), tables);
        }
    }

    // ---------------------------------------------------------------
    // Helper: find table alias from parent table_ref_aux
    // ---------------------------------------------------------------
    private String findTableAlias(PlSqlParser.Tableview_nameContext ctx) {
        // Walk up to find table_ref_aux which may have a table_alias
        ParserRuleContext parent = ctx.getParent();
        while (parent != null) {
            if (parent instanceof PlSqlParser.Table_ref_auxContext) {
                PlSqlParser.Table_ref_auxContext tra = (PlSqlParser.Table_ref_auxContext) parent;
                if (tra.table_alias() != null) {
                    return cleanId(tra.table_alias().getText());
                }
                break;
            }
            if (parent instanceof PlSqlParser.General_table_refContext) {
                PlSqlParser.General_table_refContext gtr = (PlSqlParser.General_table_refContext) parent;
                if (gtr.table_alias() != null) {
                    return cleanId(gtr.table_alias().getText());
                }
                break;
            }
            if (parent instanceof PlSqlParser.Selected_tableviewContext) {
                PlSqlParser.Selected_tableviewContext stv = (PlSqlParser.Selected_tableviewContext) parent;
                if (stv.table_alias() != null) {
                    return cleanId(stv.table_alias().getText());
                }
                break;
            }
            parent = parent.getParent();
        }
        return null;
    }

    // ---------------------------------------------------------------
    // Helper: check if this is a direct DML statement (not a subquery)
    // ---------------------------------------------------------------
    private boolean isDirectDmlStatement(PlSqlParser.Select_statementContext ctx) {
        ParserRuleContext parent = ctx.getParent();
        // If parent is data_manipulation_language_statements, it's a direct DML
        if (parent instanceof PlSqlParser.Data_manipulation_language_statementsContext) {
            return true;
        }
        // If parent is cursor_loop_param, it's a cursor FOR loop - handled there
        if (parent instanceof PlSqlParser.Cursor_loop_paramContext) {
            return false;
        }
        // If parent is cursor_declaration, skip
        if (parent instanceof PlSqlParser.Cursor_declarationContext) {
            return false;
        }
        // If parent is open_for_statement, skip (handled there)
        if (parent instanceof PlSqlParser.Open_for_statementContext) {
            return false;
        }
        // If parent is subquery_basic_elements or subquery, it's a subquery
        if (parent instanceof PlSqlParser.Subquery_basic_elementsContext) {
            return false;
        }
        // If inside insert (select), skip
        if (parent instanceof PlSqlParser.Single_table_insertContext) {
            return false;
        }
        // Default: treat as a direct statement if parent is certain statement types
        return false;
    }

    // ---------------------------------------------------------------
    // Helper: extract USING clause variables
    // ---------------------------------------------------------------
    private void extractUsingVariables(PlSqlParser.Using_clauseContext ctx, DynamicSqlInfo dsi) {
        if (ctx.using_element() != null) {
            for (PlSqlParser.Using_elementContext ue : ctx.using_element()) {
                String text = ue.getText();
                if (text != null && text.length() > 200) {
                    text = text.substring(0, 200);
                }
                dsi.getUsingVariables().add(text);
            }
        }
    }

    // ---------------------------------------------------------------
    // Helper: count statements in a seq_of_statements
    // ---------------------------------------------------------------
    private int countStatements(PlSqlParser.Seq_of_statementsContext ctx) {
        if (ctx == null) return 0;
        int count = 0;
        if (ctx.statement() != null) {
            count = ctx.statement().size();
        }
        return count;
    }

    // ---------------------------------------------------------------
    // Helper: collect general_element parts recursively
    // ---------------------------------------------------------------
    private List<String> collectGeneralElementParts(PlSqlParser.General_elementContext ctx) {
        List<String> names = new ArrayList<>();
        if (ctx == null) return names;

        // Recursive case: general_element '.' general_element_part+
        if (ctx.general_element() != null) {
            names.addAll(collectGeneralElementParts(ctx.general_element()));
        }

        // Base case: single general_element_part
        if (ctx.general_element_part() != null) {
            for (PlSqlParser.General_element_partContext part : ctx.general_element_part()) {
                if (part.id_expression() != null) {
                    names.add(cleanId(part.id_expression().getText()));
                }
            }
        }

        return names;
    }

    // ---------------------------------------------------------------
    // Helper: check if any general_element_part has function arguments
    // ---------------------------------------------------------------
    private boolean hasAnyFunctionArgument(PlSqlParser.General_elementContext ctx) {
        if (ctx == null) return false;
        if (ctx.general_element_part() != null) {
            for (PlSqlParser.General_element_partContext part : ctx.general_element_part()) {
                if (part.function_argument() != null && !part.function_argument().isEmpty()) {
                    return true;
                }
            }
        }
        if (ctx.general_element() != null) {
            return hasAnyFunctionArgument(ctx.general_element());
        }
        return false;
    }

    // ---------------------------------------------------------------
    // Helper: check if a table name is DUAL
    // ---------------------------------------------------------------
    private boolean isDualTable(String name) {
        return "DUAL".equals(name) || "SYS.DUAL".equals(name);
    }

    // ---------------------------------------------------------------
    // Helper: check if a name is a known keyword (to avoid false package refs)
    // ---------------------------------------------------------------
    private boolean isKnownKeyword(String name) {
        // Common PL/SQL constructs that look like pkg.var but aren't
        Set<String> keywords = new HashSet<>(Arrays.asList(
            "SQL", "SQLCODE", "SQLERRM", "SYSDATE", "SYSTIMESTAMP",
            "USER", "UID", "TRUE", "FALSE", "NULL"
        ));
        return keywords.contains(name);
    }

    // ---------------------------------------------------------------
    // Helper: check if general_element is likely a package var reference
    // ---------------------------------------------------------------
    private boolean isLikelyPackageVarRef(PlSqlParser.General_elementContext ctx) {
        // Check if this is in an expression context that suggests variable usage
        // rather than a type reference, etc.
        ParserRuleContext parent = ctx.getParent();
        if (parent == null) return false;

        // Walk up to find a statement-level context
        while (parent != null) {
            if (parent instanceof PlSqlParser.Assignment_statementContext) return true;
            if (parent instanceof PlSqlParser.ExpressionContext) return true;
            if (parent instanceof PlSqlParser.ConditionContext) return true;
            if (parent instanceof PlSqlParser.If_statementContext) return true;
            if (parent instanceof PlSqlParser.Return_statementContext) return true;
            if (parent instanceof PlSqlParser.StatementContext) return true;
            // Stop at declaration-level contexts
            if (parent instanceof PlSqlParser.Variable_declarationContext) return false;
            if (parent instanceof PlSqlParser.Type_specContext) return false;
            if (parent instanceof PlSqlParser.Cursor_declarationContext) return false;
            parent = parent.getParent();
        }
        return false;
    }

    // ---------------------------------------------------------------
    // Helper: check if a simple name is a builtin function
    // ---------------------------------------------------------------
    private boolean isBuiltinFunction(String name) {
        Set<String> builtins = new HashSet<>(Arrays.asList(
            "NVL", "NVL2", "DECODE", "TO_CHAR", "TO_DATE", "TO_NUMBER",
            "TO_TIMESTAMP", "TRUNC", "ROUND", "SUBSTR", "INSTR", "LENGTH",
            "UPPER", "LOWER", "TRIM", "LTRIM", "RTRIM", "REPLACE", "LPAD",
            "RPAD", "CHR", "ASCII", "CONCAT", "INITCAP", "TRANSLATE",
            "COALESCE", "NULLIF", "GREATEST", "LEAST", "ABS", "CEIL",
            "FLOOR", "MOD", "POWER", "SQRT", "SIGN", "SIN", "COS", "TAN",
            "SYSDATE", "SYSTIMESTAMP", "ADD_MONTHS", "MONTHS_BETWEEN",
            "LAST_DAY", "NEXT_DAY", "EXTRACT", "REGEXP_SUBSTR",
            "REGEXP_INSTR", "REGEXP_REPLACE", "REGEXP_LIKE", "REGEXP_COUNT",
            "LISTAGG", "XMLAGG", "SYS_CONTEXT", "USERENV",
            "RAISE_APPLICATION_ERROR", "SYS_GUID", "RAWTOHEX", "HEXTORAW",
            "UTL_RAW", "ROWIDTOCHAR", "CHARTOROWID",
            "CAST", "MULTISET", "COLLECT", "TABLE",
            "COUNT", "SUM", "AVG", "MIN", "MAX",
            "RANK", "DENSE_RANK", "ROW_NUMBER", "LEAD", "LAG",
            "FIRST_VALUE", "LAST_VALUE", "NTILE",
            "DUMP", "VSIZE", "ORA_HASH"
        ));
        return builtins.contains(name);
    }

    // ---------------------------------------------------------------
    // Dynamic SQL: extract table names from string literals
    // ---------------------------------------------------------------
    private static final Pattern DYN_SQL_TABLE_PATTERN = Pattern.compile(
            "(?i)(?:FROM|INTO|UPDATE|MERGE\\s+INTO|TRUNCATE\\s+TABLE|INSERT\\s+INTO|DELETE\\s+FROM|JOIN)\\s+"
                    + "([A-Z_][A-Z0-9_$#]*(?:\\.[A-Z_][A-Z0-9_$#]*)?)");

    private void extractTablesFromDynamicSql(String expr, int line) {
        if (expr == null) return;
        Set<String> seen = new HashSet<>();

        // Strategy 1: join all string literal fragments (handles simple cases)
        String stripped = extractStringLiterals(expr);
        if (!stripped.isEmpty()) {
            runDynSqlPattern(stripped, line, seen);
        }

        // Strategy 2: run each fragment independently (handles concatenated SQL)
        List<String> fragments = extractStringLiteralList(expr);
        for (String fragment : fragments) {
            if (!fragment.isEmpty()) {
                runDynSqlPattern(fragment, line, seen);
            }
        }
    }

    private void runDynSqlPattern(String text, int line, Set<String> seen) {
        Matcher m = DYN_SQL_TABLE_PATTERN.matcher(text);
        while (m.find()) {
            String tablRef = m.group(1).toUpperCase();
            if (isDualTable(tablRef) || isBuiltinFunction(tablRef)) continue;
            if (seen.contains(tablRef)) continue;
            seen.add(tablRef);

            String schema = null;
            String tblName = tablRef;
            if (tablRef.contains(".")) {
                String[] parts = tablRef.split("\\.", 2);
                schema = parts[0];
                tblName = parts[1];
            }
            TableOperationInfo toi = new TableOperationInfo();
            toi.setTableName(tblName);
            toi.setSchema(schema);
            toi.setOperation("DYNAMIC");
            toi.setLine(line);
            addTableOperation(toi);
        }
    }

    private String extractStringLiterals(String expr) {
        StringBuilder sb = new StringBuilder();
        for (String frag : extractStringLiteralList(expr)) {
            sb.append(frag).append(' ');
        }
        return sb.toString();
    }

    private List<String> extractStringLiteralList(String expr) {
        List<String> fragments = new ArrayList<>();
        int i = 0;
        while (i < expr.length()) {
            if (expr.charAt(i) == '\'') {
                int end = expr.indexOf('\'', i + 1);
                while (end != -1 && end + 1 < expr.length() && expr.charAt(end + 1) == '\'') {
                    end = expr.indexOf('\'', end + 2);
                }
                if (end != -1) {
                    fragments.add(expr.substring(i + 1, end));
                    i = end + 1;
                } else {
                    break;
                }
            } else {
                i++;
            }
        }
        return fragments;
    }

    private String extractNthArgument(PlSqlParser.General_elementContext ctx, int n) {
        for (PlSqlParser.General_element_partContext part : ctx.general_element_part()) {
            if (part.function_argument() != null && !part.function_argument().isEmpty()) {
                PlSqlParser.Function_argumentContext fa = part.function_argument(0);
                if (fa.argument() != null && fa.argument().size() > n) {
                    return getOriginalText(fa.argument(n), 500);
                }
            }
        }
        return null;
    }

    private String extractNthCallArg(PlSqlParser.Call_statementContext ctx, int n) {
        if (ctx.function_argument() != null && !ctx.function_argument().isEmpty()) {
            PlSqlParser.Function_argumentContext fa = ctx.function_argument(0);
            if (fa.argument() != null && fa.argument().size() > n) {
                return getOriginalText(fa.argument(n), 500);
            }
        }
        return null;
    }

    // ---------------------------------------------------------------
    // SQL-embedded function call detection (fallback for error recovery)
    // ---------------------------------------------------------------

    /**
     * Regex that matches dotted function calls in SQL text:
     *   [schema.]package.function_name(...)
     * Captures: group(1)=full dotted prefix, group(2)=opening paren
     * Deliberately excludes single-name calls (handled by standard_function/general_element),
     * and string literals to avoid false positives.
     */
    private static final Pattern SQL_EMBEDDED_CALL_PATTERN = Pattern.compile(
            "(?i)\\b([A-Z_][A-Z0-9_$#]*(?:\\.[A-Z_][A-Z0-9_$#]*)+)\\s*\\(");

    /**
     * Set of SQL keywords and builtin function names that should NOT be treated
     * as package-qualified function calls when they appear before parentheses.
     */
    private static final Set<String> SQL_NON_CALL_IDENTIFIERS = new HashSet<>(Arrays.asList(
        "SYS.DUAL", "SYS.STANDARD"
    ));

    /**
     * Scan SQL text for package.function(args) patterns that may have been missed
     * by the tree walk due to parse error recovery. Only adds calls not already
     * detected (deduplication by upper-case full name).
     *
     * @param sqlText   the original SQL text (cursor query, SELECT statement, etc.)
     * @param baseLine  the starting line of the SQL text for approximate line tracking
     */
    private void extractFunctionCallsFromSqlText(String sqlText, int baseLine) {
        if (sqlText == null || sqlText.isEmpty()) return;

        // Collect already-detected call full names for dedup
        Set<String> alreadyDetected = new HashSet<>();
        SubprogramInfo sub = currentSubprogram();
        List<CallInfo> existingCalls;
        if (sub != null) {
            existingCalls = sub.getCalls();
        } else {
            ParsedObject po = currentObject();
            existingCalls = po != null ? po.getCalls() : Collections.emptyList();
        }
        for (CallInfo ci : existingCalls) {
            StringBuilder key = new StringBuilder();
            if (ci.getSchema() != null) key.append(ci.getSchema()).append('.');
            if (ci.getPackageName() != null) key.append(ci.getPackageName()).append('.');
            key.append(ci.getName());
            alreadyDetected.add(key.toString().toUpperCase());
        }

        // Strip string literals and comments to avoid false positives
        String stripped = stripCommentsAndLiterals(sqlText);

        Matcher m = SQL_EMBEDDED_CALL_PATTERN.matcher(stripped);
        while (m.find()) {
            String dottedName = m.group(1).toUpperCase();

            // Skip known non-call patterns
            if (SQL_NON_CALL_IDENTIFIERS.contains(dottedName)) continue;

            // Skip sequence references (*.NEXTVAL, *.CURRVAL)
            if (dottedName.endsWith(".NEXTVAL") || dottedName.endsWith(".CURRVAL")) continue;

            // Skip if already detected by tree walk
            if (alreadyDetected.contains(dottedName)) continue;

            // Count arguments (approximate: count commas at top-level inside parens)
            int argCount = countTopLevelArgs(stripped, m.end() - 1);

            // Classify and add the call
            CallInfo ci = classifyCall(dottedName, argCount, baseLine);
            addCall(ci);
            alreadyDetected.add(dottedName);
        }
    }

    /**
     * Replace the contents of string literals and comments with spaces
     * to avoid regex false positives in the fallback scanner.
     */
    private String stripCommentsAndLiterals(String text) {
        StringBuilder sb = new StringBuilder(text.length());
        int i = 0;
        while (i < text.length()) {
            char c = text.charAt(i);
            // Single-line comment: -- to end of line
            if (c == '-' && i + 1 < text.length() && text.charAt(i + 1) == '-') {
                while (i < text.length() && text.charAt(i) != '\n') {
                    sb.append(' ');
                    i++;
                }
            }
            // Multi-line comment: /* ... */
            else if (c == '/' && i + 1 < text.length() && text.charAt(i + 1) == '*') {
                sb.append("  ");
                i += 2;
                while (i < text.length()) {
                    if (text.charAt(i) == '*' && i + 1 < text.length() && text.charAt(i + 1) == '/') {
                        sb.append("  ");
                        i += 2;
                        break;
                    }
                    sb.append(' ');
                    i++;
                }
            }
            // String literal: '...'
            else if (c == '\'') {
                sb.append('\'');
                i++;
                while (i < text.length()) {
                    if (text.charAt(i) == '\'') {
                        if (i + 1 < text.length() && text.charAt(i + 1) == '\'') {
                            sb.append("  ");
                            i += 2;
                        } else {
                            sb.append('\'');
                            i++;
                            break;
                        }
                    } else {
                        sb.append(' ');
                        i++;
                    }
                }
            } else {
                sb.append(c);
                i++;
            }
        }
        return sb.toString();
    }

    /**
     * Count the number of top-level arguments inside parentheses starting at position
     * openParenIdx. Returns 0 for empty parens, 1 for no commas, etc.
     */
    private int countTopLevelArgs(String text, int openParenIdx) {
        if (openParenIdx < 0 || openParenIdx >= text.length()
                || text.charAt(openParenIdx) != '(') return 0;
        int depth = 0;
        int commas = 0;
        boolean hasContent = false;
        for (int i = openParenIdx; i < text.length(); i++) {
            char c = text.charAt(i);
            if (c == '(') {
                depth++;
            } else if (c == ')') {
                depth--;
                if (depth == 0) break;
            } else if (c == ',' && depth == 1) {
                commas++;
            } else if (depth == 1 && !Character.isWhitespace(c)) {
                hasContent = true;
            }
        }
        return hasContent ? commas + 1 : 0;
    }
}
