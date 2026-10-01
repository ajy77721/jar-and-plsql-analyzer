package com.plsql.parser.visitor;

import com.plsql.parser.PlSqlLexer;
import com.plsql.parser.PlSqlParser;
import com.plsql.parser.PlSqlParserBaseVisitor;
import com.plsql.parser.model.*;

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
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
    // Names declared via `TYPE <name> IS TABLE OF/VARRAY/RECORD/REF CURSOR` in the current
    // object. A collection type used as a constructor (`l_n := NESTED_T();`) is syntactically
    // indistinguishable from a function call, so without this the visitor recorded a phantom
    // EXTERNAL call/dependency on a type that is declared locally in the very same package.
    // Shares the exact lifecycle of currentPackageSubprograms (cleared at the same points).
    private final Set<String> declaredTypeNames = new HashSet<>();
    // Bug #1 fix: stack of locally-declared nested subprogram name sets, one frame per
    // enclosing procedure/function body that has nested procedure_body/function_body
    // declare_specs. Pushed on entry to a body with nested subprograms, popped on exit,
    // so a local subprogram's visibility is scoped to its declaring body only (not visible
    // to sibling subprograms outside that scope).
    private final Deque<Set<String>> localSubprogramStack = new ArrayDeque<>();
    private String currentPackageName = null;

    // Track the current DML operation for table-operation pairing
    private String currentDmlOperation = null;

    // Track tables collected for the current DML statement.
    // PERF: a parallel Set drives O(1) deduplication; the List preserves insertion order for
    // StatementInfo.tables which the UI relies on.
    private List<String> currentDmlTables = null;
    private Set<String> currentDmlTablesSet = null;

    // Track CTE (WITH clause) names and inline view aliases to avoid false-positive table refs
    private final Set<String> queryAliases = new HashSet<>();

    // FOR-loop record names currently in scope (cursor FOR loops: `FOR r IN (...) LOOP`). A dotted
    // reference qualified by one of these (r.field) is a record-field access, never a package-global
    // read; the package-global capture drops it. A stack because loops nest and the same record name
    // can be reused by sibling loops.
    private final Deque<Set<String>> loopRecordStack = new ArrayDeque<>();

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
    // Option A: per-subprogram VARCHAR2 variable → accumulated string value map.
    // Tracks assignments like:  v_sql := 'SELECT * FROM ' || p_tbl_name;
    // Pushed/popped with each subprogram scope so sibling procs don't bleed.
    // Key = upper-cased variable name.  Value = concatenated known string fragments.
    // ---------------------------------------------------------------
    private final Deque<Map<String, StringBuilder>> varValueStack = new ArrayDeque<>();

    // Option B: map of subprogram-name → raw source text, populated as we enter each
    // function/procedure body so that EXECUTE IMMEDIATE in another proc can scan it.
    // Scoped to the current package body parse (cleared on package entry).
    // Bug #3 fix: bare-name-keyed cache used only by the dynamic-SQL cross-procedure text-scan
    // fallback path. Was Map<String,String> — two overloads of the same procedure/function name
    // would unconditionally clobber each other's cached source. Now keyed the same (by bare
    // uppercased name, since the exact overload being called usually isn't determinable from a
    // plain variable/function-name string at the call site) but stores ALL overloads' source
    // texts in a list, tried in declaration order until one yields a match (see
    // scanAllSubprogramSourcesForReturnStrings below) — no overload's source is silently dropped.
    private final Map<String, List<String>> subprogramSourceMap = new LinkedHashMap<>();

    private void addSubprogramSource(String bareName, String src) {
        if (bareName == null || src == null) return;
        subprogramSourceMap.computeIfAbsent(bareName.toUpperCase(), k -> new ArrayList<>()).add(src);
    }

    // Bug #3 fix: try every cached overload's source text (in declaration order) for the given
    // bare name until one produces a match, instead of only ever having (at most) one source
    // text available per bare name.
    private boolean scanAllSubprogramSourcesForReturnStrings(String bareName, int line) {
        if (bareName == null) return false;
        List<String> sources = subprogramSourceMap.get(bareName.toUpperCase());
        if (sources == null) return false;
        for (String src : sources) {
            if (scanSourceForReturnStrings(src, line)) return true;
        }
        return false;
    }

    // Gap 1: Package-level CONSTANT map (cleared per pkg spec/body; populated from spec CONSTANT decls).
    private final Map<String, String> pkgConstantMap = new LinkedHashMap<>();
    // Gap 4: Cross-package qualified constant registry (pkgName -> constName -> value; entire parse lifetime).
    private final Map<String, Map<String, String>> globalConstRegistry = new LinkedHashMap<>();
    // Gap 2: Package-body global variable accumulator (cleared per pkg body; tracks g_sql := '...' outside subprograms).
    private final Map<String, StringBuilder> pkgGlobalVarMap = new LinkedHashMap<>();
    // Gap 2: Set of known package-level variable names (spec or body); used to route subprogram-scope
    // assignments of package globals back to pkgGlobalVarMap for cross-procedure resolution.
    private final Set<String> pkgGlobalVarNames = new HashSet<>();
    // Gap 2: Cross-package global var name registry (pkgName -> set of global var names; entire parse lifetime).
    private final Map<String, Set<String>> globalVarNamesRegistry = new LinkedHashMap<>();

    // Gap 10: Branch depth counter — incremented when entering IF/ELSIF/ELSE branch bodies.
    // When > 0, variable assignments ACCUMULATE (union semantics: all branches are possible at runtime).
    // When == 0 (sequential code), fresh assignments REPLACE the accumulator.
    private int branchDepth = 0;

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
        // Suppress false-positive "calls" that are really collection/varray/associative-array
        // element access, e.g. l_cids(i). Syntactically identical to a call NAME(args), but NAME
        // is a declared variable in scope (not a procedure) — PL/SQL name resolution binds it to
        // the variable.
        if ((ci.getPackageName() == null || ci.getPackageName().isEmpty())
                && isDeclaredVariableInScope(ci.getName())) {
            return;
        }
        // BUG-CT-048: the same false positive also happens for QUALIFIED names of the form
        // qualifier.method(), e.g. v_subject_param.EXTEND() — a collection/object method call on
        // a declared local variable/parameter/package variable/global variable, not a real call to
        // an external package named after that variable. Only applies when the "qualifier" is
        // exactly the declared variable's own name (a true two-part qualifier.method reference,
        // i.e. no schema prefix) — a genuinely schema-qualified call (schema.pkg.proc) is left
        // alone since a variable cannot be a schema.
        if (ci.getSchema() == null && ci.getPackageName() != null && !ci.getPackageName().isEmpty()
                && isDeclaredVariableInScope(ci.getPackageName())) {
            return;
        }
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

    /**
     * True if {@code name} is a cursor already declared in the current subprogram/object scope,
     * per the {@link CursorInfo} list built up as cursor declarations are visited (not a parallel
     * name set — reuses the same tracking {@link #addCursor} populates).
     */
    private boolean isDeclaredCursorInScope(String name) {
        if (name == null || name.isEmpty()) return false;
        SubprogramInfo sub = currentSubprogram();
        if (sub != null && nameInCursorList(name, sub.getCursors())) return true;
        ParsedObject po = currentObject();
        if (po != null && nameInCursorList(name, po.getCursors())) return true;
        return false;
    }

    private static boolean nameInCursorList(String name, java.util.List<CursorInfo> cursors) {
        if (cursors == null) return false;
        for (CursorInfo c : cursors) {
            if (c.getName() != null && c.getName().equalsIgnoreCase(name)) return true;
        }
        return false;
    }

    /** True if {@code name} is a variable declared in the current subprogram/package/object scope. */
    private boolean isDeclaredVariableInScope(String name) {
        if (name == null || name.isEmpty()) return false;
        if (isLocalVariableOrParamInScope(name)) return true;
        ParsedObject po = currentObject();
        if (po != null) {
            if (nameInVarList(name, po.getPackageVariables())) return true;
            if (nameInVarList(name, po.getGlobalVariables())) return true;
        }
        return false;
    }

    /**
     * True if {@code name} is a LOCAL variable or a formal parameter of the subprogram currently
     * being visited — i.e. it shadows any same-named package global at this point in the code.
     * Distinct from {@link #isDeclaredVariableInScope}, which also counts package/global variables
     * (a bare reference to one of those IS a package-global usage, not a shadowing local).
     */
    private boolean isLocalVariableOrParamInScope(String name) {
        if (name == null || name.isEmpty()) return false;
        SubprogramInfo sub = currentSubprogram();
        if (sub != null && nameInVarList(name, sub.getLocalVariables())) return true;
        // A collection/varray-typed formal parameter indexed as p_arr(1) is syntactically identical
        // to a call, exactly like a local variable (see addCall's comment above) — but parameters
        // live in their own list (SubprogramInfo.getParameters()), separate from getLocalVariables(),
        // so without this check an indexed parameter was misclassified as an EXTERNAL call to a
        // procedure named after the parameter. Confirmed as a real false positive before this check
        // existed (§201).
        if (sub != null && nameInParamList(name, sub.getParameters())) return true;
        return false;
    }

    private static boolean nameInVarList(String name, java.util.List<VariableInfo> vars) {
        if (vars == null) return false;
        for (VariableInfo vi : vars) {
            if (vi.getName() != null && vi.getName().equalsIgnoreCase(name)) return true;
        }
        return false;
    }

    private static boolean nameInParamList(String name, java.util.List<ParameterInfo> params) {
        if (params == null) return false;
        for (ParameterInfo pi : params) {
            if (pi.getName() != null && pi.getName().equalsIgnoreCase(name)) return true;
        }
        return false;
    }

    /**
     * The DML statement currently being visited, so {@link #addTableOperation} can record its SQL.
     *
     * <p>A field rather than a parameter because {@code addTableOperation} is called from a dozen
     * places, several of them nested clauses (a MERGE branch, a subquery) that do not hold the
     * enclosing statement's context. Set and restored around each DML visit, so a nested statement
     * cannot leak its text to the outer one.
     */
    private ParserRuleContext currentDmlCtx;

    /**
     * The full source text of a context — <b>not</b> truncated.
     *
     * <p>Deliberately distinct from {@code getOriginalText(ctx, 200)}, which exists for display and
     * cuts at 200 characters. A truncated statement does not parse, so using it here would have
     * produced no column detail at all for Oracle.
     */
    private String fullText(ParserRuleContext ctx) {
        if (ctx == null || ctx.start == null || ctx.stop == null) return null;
        try {
            return ctx.start.getInputStream().getText(new org.antlr.v4.runtime.misc.Interval(
                    ctx.start.getStartIndex(), ctx.stop.getStopIndex()));
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * True when {@code ctx} actually spans {@code line}.
     *
     * <p>The one condition that makes attaching a statement's text to an operation legitimate: the
     * statement must contain the line the operation was found on. A line of 0 or a context with no
     * position answers false, because an unknown position cannot be shown to be inside anything.
     */
    private boolean spansLine(ParserRuleContext ctx, int line) {
        if (ctx == null || ctx.start == null || ctx.stop == null || line <= 0) return false;
        return line >= ctx.start.getLine() && line <= ctx.stop.getLine();
    }

    /**
     * Adds an operation whose owning statement is known at the call site.
     *
     * <p>Several call sites already hold the exact context the table was read from — a cursor's
     * {@code select_statement}, a {@code select_only_statement} — and then discarded it, leaving
     * {@code addTableOperation} to guess from the sticky {@code currentDmlCtx}. Passing it removes
     * the guess: these operations get the statement they actually came from.
     *
     * <p>This matters because tightening the guess (Sep05-14) correctly stopped attaching unrelated
     * SQL, and that exposed how many Oracle operations had never had a source of their own — detail
     * on the table screen fell from 98% to 38%. The carry-over had been hiding that by filling the
     * gap with the wrong statement, which is why the fix and this belong together: one stops the
     * false answers, the other supplies the true ones.
     */
    private void addTableOperation(TableOperationInfo toi, ParserRuleContext stmtCtx) {
        if (toi.getSqlText() == null && stmtCtx != null) {
            toi.setSqlText(fullText(stmtCtx));
        }
        addTableOperation(toi);
    }

    private void addTableOperation(TableOperationInfo toi) {
        // Sep05-14: only attach the current statement's SQL if that statement really contains this
        // operation.
        //
        // `currentDmlCtx` is assigned when a DML statement is entered and never cleared, which made
        // it a sticky "last DML seen" for everything that followed. Any later operation without
        // text of its own inherited an unrelated statement, and the search screen showed an
        // ERP_PRODUCT row whose SQL read `UPDATE erp_mrp_run SET status = 'DONE'` — a statement
        // that does not mention that table. A blank cell is a gap; this was a confidently wrong
        // answer that a reader cannot tell apart from a correct one.
        //
        // The check goes here rather than restoring the field in each of the five visit methods.
        // That was tried first and is the worse option: those methods have up to fourteen early
        // returns each, so a restore would have to be a try/finally around every one of them, and
        // any future return added past it would silently reintroduce the leak. This encodes the
        // actual requirement instead — text belongs to an operation only if it covers its line —
        // so it holds however the traversal changes.
        if (toi.getSqlText() == null && spansLine(currentDmlCtx, toi.getLine())) {
            toi.setSqlText(fullText(currentDmlCtx));
        }
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

    private void addExternalPackageVarRefDetail(String pkgName, String varName, int line,
            PackageVarRefInfo.AccessType accessType) {
        PackageVarRefInfo info = new PackageVarRefInfo(pkgName, varName, line, accessType);
        SubprogramInfo sub = currentSubprogram();
        if (sub != null) {
            sub.getExternalPackageVarRefDetails().add(info);
        } else {
            ParsedObject po = currentObject();
            if (po != null) {
                po.getExternalPackageVarRefDetails().add(info);
            }
        }
    }

    /**
     * A package-level variable referenced WITHOUT a qualifier, from inside its OWNING package
     * (e.g. {@code g_last_context := p_ctx;} in {@code erp_pkg_planner.set_context}). The owning
     * package is the qualifier the source omitted, so the reference is recorded as
     * {@code <owningPkg>.<name>} — the same shape a cross-package reference uses, which is what the
     * estate globals facet keys on.
     *
     * <p>Fires only when {@code name} is a DECLARED package-level variable of the package currently
     * being parsed and is not shadowed by a local variable/parameter of the enclosing subprogram.
     * Bare locals, parameters, loop indices and record variables therefore never become refs.
     */
    private void captureBarePackageGlobalRef(String name, PlSqlParser.General_elementContext ctx) {
        if (name == null || name.isEmpty()) return;
        if (currentPackageName == null) return;                       // not inside a package at all
        String upper = name.toUpperCase();
        if (!pkgGlobalVarNames.contains(upper)) return;               // not a declared package global
        if (isLocalVariableOrParamInScope(name)) return;              // shadowed by a local/param
        if (!isLikelyPackageVarRef(ctx)) return;                      // not a value-usage context
        addExternalPackageVarRef(currentPackageName + "." + name);
        addExternalPackageVarRefDetail(currentPackageName, name, getStartLine(ctx),
                classifyPackageVarAccess(ctx));
    }

    /**
     * True when {@code qualifier} provably is NOT a package name in this context, so a dotted
     * reference {@code qualifier.field} is a record-field or alias-column access rather than a
     * package-global read. Grammar-first: the disqualifiers are the scope/alias facts the visitor
     * already tracks (declared variables and parameters, cursor names, FOR-loop records, FROM/JOIN
     * table aliases and CTE names), never a text heuristic.
     */
    private boolean isNonPackageQualifier(String qualifier, PlSqlParser.General_elementContext ctx) {
        if (qualifier == null || qualifier.isEmpty()) return true;
        String q = qualifier.toUpperCase();
        if (isKnownKeyword(qualifier)) return true;
        // A declared local/parameter/package/global variable (record variables included).
        if (isDeclaredVariableInScope(qualifier)) return true;
        // A cursor declared in the current subprogram/object.
        if (isDeclaredCursorInScope(qualifier)) return true;
        // A FOR-loop record: FOR r IN (...) LOOP → r.field is a field access.
        for (Set<String> frame : loopRecordStack) {
            if (frame.contains(q)) return true;
        }
        // A table / inline-view / CTE alias bound anywhere in this parse (the query-block pre-scan
        // registers every FROM alias before the select list is visited, so this covers select-list,
        // WHERE and GROUP BY references too).
        if (queryAliases.contains(q)) return true;
        // A type declared in this object (a record/collection type name is not a package).
        if (declaredTypeNames.contains(q)) return true;
        // A table alias bound in the enclosing statement's FROM/JOIN — the ON-condition case, where
        // the qualifier sits inside the join_clause and no query-block pre-scan has run for it.
        PlSqlParser.From_clauseContext fc = enclosingFromClause(ctx);
        if (fc != null && buildAliasMap(fc).containsKey(q)) return true;
        return false;
    }

    /**
     * True when a two-part reference {@code qualifier.var} resolves to a package-global variable.
     * The qualifier has already been proven not to be a variable/record/alias
     * (see {@link #isNonPackageQualifier}). Resolution is grammar-first, in priority order:
     *
     * <ol>
     *   <li>the qualifier is a package parsed earlier in this unit AND {@code var} is one of its
     *       declared globals (the cross-package-in-one-file case);</li>
     *   <li>the qualifier is the package currently being parsed AND {@code var} is one of its own
     *       declared globals (the {@code erp_pkg_planner.g_last_context} self-reference case);</li>
     *   <li>the cross-FILE fallback: {@code var} carries the package-global naming convention
     *       ({@code g_} prefix) and the qualifier looks like a package (the conventional {@code PKG}
     *       token). The referenced package's declaration is not visible at this layer in a per-file
     *       parse, so the naming convention is the only signal available — and it is exactly what
     *       separates the real globals ({@code g_last_context}) from the record fields / alias
     *       columns that share the {@code X.Y} shape.</li>
     * </ol>
     */
    private boolean qualifierResolvesToPackage(String qualifier, String varName) {
        if (qualifier == null || varName == null) return false;
        String q = qualifier.toUpperCase();
        String v = varName.toUpperCase();
        Set<String> knownGlobals = globalVarNamesRegistry.get(q);
        if (knownGlobals != null && knownGlobals.contains(v)) return true;
        if (currentPackageName != null && q.equals(currentPackageName.toUpperCase())
                && pkgGlobalVarNames.contains(v)) {
            return true;
        }
        return v.startsWith("G_") && q.contains("PKG");
    }

    /**
     * Determine whether a general_element resolved as PKG.VAR is being written to
     * (it is the assignment target of an Assignment_statementContext) or read
     * (used within an expression / condition / return / etc.). Mirrors the same
     * assignment-target-vs-expression distinction used elsewhere for pkgGlobalVarMap.
     */
    private PackageVarRefInfo.AccessType classifyPackageVarAccess(PlSqlParser.General_elementContext ctx) {
        ParserRuleContext parent = ctx.getParent();
        if (parent instanceof PlSqlParser.Assignment_statementContext) {
            PlSqlParser.Assignment_statementContext assign = (PlSqlParser.Assignment_statementContext) parent;
            if (assign.general_element() == ctx) {
                return PackageVarRefInfo.AccessType.WRITE;
            }
            return PackageVarRefInfo.AccessType.READ;
        }
        while (parent != null) {
            if (parent instanceof PlSqlParser.ExpressionContext
                || parent instanceof PlSqlParser.ConditionContext
                || parent instanceof PlSqlParser.If_statementContext
                || parent instanceof PlSqlParser.Return_statementContext) {
                return PackageVarRefInfo.AccessType.READ;
            }
            parent = parent.getParent();
        }
        return PackageVarRefInfo.AccessType.UNKNOWN;
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
     * Like {@link #getOriginalText(ParserRuleContext, int)} but blanks out (replaces with
     * spaces) the text spans of any procedure_body/function_body declared directly in
     * {@code declSpecs} before truncating. Used only to build the text buffer fed to the
     * fallback {@code extractFunctionCallsFromSqlText} scanner for a function/procedure body:
     * without this, that scanner would re-scan text belonging to a nested subprogram that the
     * main tree-walk (visitChildren, called before the fallback scan) already visited and
     * recorded calls for under the nested subprogram's OWN SubprogramInfo -- re-detecting the
     * same call targets as bogus duplicates stamped with the OUTER body's start line, since by
     * the time the fallback scan runs, currentSubprogram() has already popped back to the outer
     * body and its own call list has no record of what the (already-popped) nested subprogram
     * detected.
     */
    private String getOriginalTextExcludingNested(ParserRuleContext ctx, int maxLen,
            PlSqlParser.Seq_of_declare_specsContext declSpecs) {
        if (ctx == null || ctx.start == null || ctx.stop == null) return null;
        try {
            int outerStart = ctx.start.getStartIndex();
            int outerStop = ctx.stop.getStopIndex();
            String text = ctx.start.getInputStream().getText(
                new org.antlr.v4.runtime.misc.Interval(outerStart, outerStop));
            List<ParserRuleContext> nestedBodies = collectNestedSubprogramBodies(declSpecs);
            if (!nestedBodies.isEmpty()) {
                char[] chars = text.toCharArray();
                for (ParserRuleContext nested : nestedBodies) {
                    if (nested.start == null || nested.stop == null) continue;
                    int from = nested.start.getStartIndex() - outerStart;
                    int to = nested.stop.getStopIndex() - outerStart; // inclusive
                    if (from < 0) from = 0;
                    if (to >= chars.length) to = chars.length - 1;
                    for (int i = from; i <= to; i++) {
                        if (chars[i] != '\n') chars[i] = ' ';
                    }
                }
                text = new String(chars);
            }
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
        boolean wasQuoted = text.startsWith("\"") && text.endsWith("\"") && text.length() >= 2;
        if (wasQuoted) {
            text = text.substring(1, text.length() - 1);
        }
        // Oracle semantics: a double-quoted identifier is case-sensitive and preserved exactly;
        // an unquoted identifier folds to uppercase.
        return wasQuoted ? text : text.toUpperCase();
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
                String singleName = cleanId(idParts.get(0).getText());
                // A single-identifier X%ROWTYPE is ambiguous: it could be `table%ROWTYPE` (a real
                // table dependency) or `cursor_name%ROWTYPE` (a row shaped like a cursor's SELECT
                // list, not a table reference at all). If X is already a cursor tracked in the
                // current scope, it is the latter — skip recording it as a fake table dependency.
                if (isDeclaredCursorInScope(singleName)) {
                    return;
                }
                tableName = singleName;
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
        declaredTypeNames.clear();
        // Gap 1: clear per-package constant map for this spec
        pkgConstantMap.clear();
        pkgGlobalVarNames.clear();

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

        // Gap 4: register this package's constants in the global cross-package registry
        if (po.getName() != null && !pkgConstantMap.isEmpty()) {
            globalConstRegistry.put(po.getName().toUpperCase(), new LinkedHashMap<>(pkgConstantMap));
        }
        // Gap 2: register this package's global variable names for cross-package lookup
        if (po.getName() != null && !pkgGlobalVarNames.isEmpty()) {
            globalVarNamesRegistry.put(po.getName().toUpperCase(), new HashSet<>(pkgGlobalVarNames));
        }

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
        declaredTypeNames.clear();
        subprogramSourceMap.clear(); // Option B: fresh per-package scope
        // Gap 2: clear global var map for this body; carry over pkg global var names from spec
        pkgGlobalVarMap.clear();
        pkgGlobalVarNames.clear();
        if (po.getName() != null) {
            Set<String> specVarNames = globalVarNamesRegistry.get(po.getName().toUpperCase());
            if (specVarNames != null) pkgGlobalVarNames.addAll(specVarNames);
        }
        // Gap 1: if the spec was parsed first for this package name, reuse its pkgConstantMap.
        // If the body's name matches globalConstRegistry (spec parsed earlier), reload constants.
        if (po.getName() != null) {
            Map<String, String> specConsts = globalConstRegistry.get(po.getName().toUpperCase());
            if (specConsts != null) {
                pkgConstantMap.clear();
                pkgConstantMap.putAll(specConsts);
            } else {
                pkgConstantMap.clear();
            }
        } else {
            pkgConstantMap.clear();
        }

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

        // Option A: push fresh var-value map for this scope
        varValueStack.push(new LinkedHashMap<>());
        // Option B: store source text indexed by name so other procs can scan it
        String procSrc = getOriginalText(ctx, 20000);
        if (si.getName() != null && procSrc != null) {
            addSubprogramSource(si.getName(), procSrc);
        }
        PlSqlParser.Seq_of_declare_specsContext declSpecs = ctx.seq_of_declare_specs();

        // Gap 10: reset branch depth for this new procedure scope
        int savedBranchDepth = branchDepth;
        branchDepth = 0;
        // Bug #1 fix: pre-scan this body's own declare_specs for nested local
        // procedure/function names and push them as an in-scope frame so calls made
        // from within this body (or its nested statements) resolve as INTERNAL.
        localSubprogramStack.push(collectNestedSubprogramNames(declSpecs));
        pushSubprogram(si);
        Void result = visitChildren(ctx);
        // Fallback: scan procedure body text for SQL-embedded pkg.fn(args) calls. Nested
        // procedure_body/function_body spans are blanked out first: the tree walk above already
        // visited them and recorded their calls under their OWN SubprogramInfo, so scanning that
        // text again here would re-detect the same targets as bogus duplicates stamped with this
        // outer procedure's start line (see getOriginalTextExcludingNested).
        extractFunctionCallsFromSqlText(getOriginalTextExcludingNested(ctx, 20000, declSpecs), getStartLine(ctx));
        popContext();
        localSubprogramStack.pop(); // Bug #1 fix: local subprograms go out of scope here
        varValueStack.poll(); // Option A: pop scope
        branchDepth = savedBranchDepth; // Gap 10: restore parent scope's branch depth
        return result;
    }

    // Bug #1 fix: scan a seq_of_declare_specs for directly-nested procedure_body/function_body
    // names (does not recurse into further-nested bodies; each body pushes its own frame).
    private Set<String> collectNestedSubprogramNames(PlSqlParser.Seq_of_declare_specsContext declSpecs) {
        Set<String> names = new HashSet<>();
        for (ParserRuleContext body : collectNestedSubprogramBodies(declSpecs)) {
            if (body instanceof PlSqlParser.Procedure_bodyContext) {
                PlSqlParser.Procedure_bodyContext pb = (PlSqlParser.Procedure_bodyContext) body;
                if (pb.identifier() != null) names.add(cleanId(pb.identifier().getText()));
            } else if (body instanceof PlSqlParser.Function_bodyContext) {
                PlSqlParser.Function_bodyContext fb = (PlSqlParser.Function_bodyContext) body;
                if (fb.identifier() != null) names.add(cleanId(fb.identifier().getText()));
            }
        }
        return names;
    }

    // Fallback-scanner fix: directly-nested procedure_body/function_body contexts declared in
    // this body's own declare section (does not recurse into further-nested bodies; each body's
    // own visit handles its own nested declare_specs). Used both to compute in-scope local names
    // (above) and to blank out their source-text spans before the fallback SQL-embedded-call
    // scanner runs on the outer body's text (see getOriginalTextExcludingNested) -- otherwise
    // that scanner would re-detect calls the tree walk already recorded under the NESTED
    // subprogram's own SubprogramInfo, re-emitting them as bogus duplicates stamped with the
    // OUTER body's start line.
    private List<ParserRuleContext> collectNestedSubprogramBodies(PlSqlParser.Seq_of_declare_specsContext declSpecs) {
        List<ParserRuleContext> bodies = new ArrayList<>();
        if (declSpecs == null) return bodies;
        for (PlSqlParser.Declare_specContext ds : declSpecs.declare_spec()) {
            if (ds.procedure_body() != null) bodies.add(ds.procedure_body());
            if (ds.function_body() != null) bodies.add(ds.function_body());
        }
        return bodies;
    }

    // ---------------------------------------------------------------
    // PROCEDURE SPEC (forward declaration in package spec) — PT-1 fix
    // ---------------------------------------------------------------
    @Override
    public Void visitProcedure_spec(PlSqlParser.Procedure_specContext ctx) {
        // Only add a forward-declaration SubprogramInfo when we are at the package-spec
        // top level (i.e. the ParsedObject is a PACKAGE_SPEC and not inside a subprogram body).
        // PT-1: This populates ParsedObject.subprograms for spec-only declarations so that
        // the UI and downstream analysis see the declared procedures even when no body is parsed.
        ParsedObject po = currentObject();
        if (po != null && "PACKAGE_SPEC".equals(po.getType()) && !insideSubprogram()) {
            SubprogramInfo si = new SubprogramInfo();
            si.setType("PROCEDURE");
            si.setLineStart(getStartLine(ctx));
            si.setLineEnd(getStopLine(ctx));
            si.setForwardDeclaration(true);
            if (ctx.identifier() != null) {
                si.setName(getIdentifierText(ctx.identifier()));
            }
            if (ctx.parameter() != null) {
                for (PlSqlParser.ParameterContext p : ctx.parameter()) {
                    si.getParameters().add(extractParameter(p));
                }
            }
            po.getSubprograms().add(si);
        }
        return visitChildren(ctx);
    }

    // ---------------------------------------------------------------
    // FUNCTION SPEC (forward declaration in package spec) — PT-1 fix
    // ---------------------------------------------------------------
    @Override
    public Void visitFunction_spec(PlSqlParser.Function_specContext ctx) {
        // Only add a forward-declaration SubprogramInfo when we are at the package-spec
        // top level (i.e. the ParsedObject is a PACKAGE_SPEC and not inside a subprogram body).
        // PT-1: This populates ParsedObject.subprograms for spec-only declarations so that
        // the UI and downstream analysis see the declared functions even when no body is parsed.
        ParsedObject po = currentObject();
        if (po != null && "PACKAGE_SPEC".equals(po.getType()) && !insideSubprogram()) {
            SubprogramInfo si = new SubprogramInfo();
            si.setType("FUNCTION");
            si.setLineStart(getStartLine(ctx));
            si.setLineEnd(getStopLine(ctx));
            si.setForwardDeclaration(true);
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
            po.getSubprograms().add(si);
        }
        return visitChildren(ctx);
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
        // Bug (unique-concepts audit item 2): a package-body FUNCTION ... RETURN <type> PIPELINED
        // IS ... was always reported as pipelined=false even though the grammar's function_body
        // rule matches the PIPELINED token fine - nothing ever read it onto SubprogramInfo.
        if (!ctx.PIPELINED().isEmpty()) {
            si.setPipelined(true);
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

        // Option A: push fresh var-value map for this scope
        varValueStack.push(new LinkedHashMap<>());
        // Option B: store source text indexed by name so other procs can scan it
        String fnSrc = getOriginalText(ctx, 20000);
        if (si.getName() != null && fnSrc != null) {
            addSubprogramSource(si.getName(), fnSrc);
        }
        PlSqlParser.Seq_of_declare_specsContext declSpecs = ctx.seq_of_declare_specs();

        // Gap 10: reset branch depth for this new function scope
        int savedBranchDepth = branchDepth;
        branchDepth = 0;
        // Bug #1 fix: pre-scan this body's own declare_specs for nested local
        // procedure/function names and push them as an in-scope frame.
        localSubprogramStack.push(collectNestedSubprogramNames(declSpecs));
        pushSubprogram(si);
        Void result = visitChildren(ctx);
        // Fallback: scan function body text for SQL-embedded pkg.fn(args) calls. Nested
        // procedure_body/function_body spans are blanked out first: the tree walk above already
        // visited them and recorded their calls under their OWN SubprogramInfo, so scanning that
        // text again here would re-detect the same targets as bogus duplicates stamped with this
        // outer function's start line (see getOriginalTextExcludingNested).
        extractFunctionCallsFromSqlText(getOriginalTextExcludingNested(ctx, 20000, declSpecs), getStartLine(ctx));
        popContext();
        localSubprogramStack.pop(); // Bug #1 fix: local subprograms go out of scope here
        varValueStack.poll(); // Option A: pop scope
        branchDepth = savedBranchDepth; // Gap 10: restore parent scope's branch depth
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
        declaredTypeNames.clear();
        subprogramSourceMap.clear(); // Option B: fresh scope for standalone proc

        results.add(po);
        // Option A: push fresh var-value map for standalone procedure scope
        varValueStack.push(new LinkedHashMap<>());
        // Option B: store source for cross-proc analysis
        String standaloneProcSrc = getOriginalText(ctx, 20000);
        if (po.getName() != null && standaloneProcSrc != null) {
            addSubprogramSource(po.getName(), standaloneProcSrc);
        }
        PlSqlParser.Seq_of_declare_specsContext declSpecs = ctx.seq_of_declare_specs();
        // Gap 10: reset branch depth
        int savedBranchDepth = branchDepth;
        branchDepth = 0;
        pushObject(po);
        Void result = visitChildren(ctx);
        // Fallback: scan standalone procedure text for SQL-embedded pkg.fn(args) calls. Nested
        // procedure_body/function_body spans are blanked out first: the tree walk above already
        // visited them and recorded their calls under their OWN SubprogramInfo, so scanning that
        // text again here would re-detect the same targets as bogus duplicates stamped with this
        // outer procedure's start line (see getOriginalTextExcludingNested).
        extractFunctionCallsFromSqlText(getOriginalTextExcludingNested(ctx, 20000, declSpecs), getStartLine(ctx));
        popContext();
        varValueStack.poll(); // Option A: pop scope
        branchDepth = savedBranchDepth;
        return result;
    }

    // ---------------------------------------------------------------
    // CREATE TABLE (standalone) — Bug (temp-table audit): no visitCreate_table override existed
    // at all, so a standalone CREATE TABLE (temp or permanent) produced zero objects/table-ops.
    // Oracle PL/SQL cannot express static DDL inside a procedure/package body (only via
    // EXECUTE IMMEDIATE, handled separately by extractTablesFromDynamicSql), so create_table only
    // ever appears as its own top-level unit_statement — this addition cannot affect anything
    // that currently works around CREATE TABLE reached through a procedure/package body, because
    // no such path exists. Kept minimal: a ParsedObject of type TABLE (so at least the table name
    // — and, via getName's underlying text, whether it was a GLOBAL/PRIVATE TEMPORARY table — is
    // visible), plus a DDL-flagged TableOperationInfo so the same enrichment machinery used for
    // dynamic CREATE TABLE also populates getDdl() here.
    // ---------------------------------------------------------------
    @Override
    public Void visitCreate_table(PlSqlParser.Create_tableContext ctx) {
        ParsedObject po = new ParsedObject();
        po.setType("TABLE");
        po.setLineStart(getStartLine(ctx));
        po.setLineEnd(getStopLine(ctx));

        String tableName = ctx.table_name() != null ? ctx.table_name().getText() : null;
        String schema = ctx.schema_name() != null ? ctx.schema_name().getText() : null;
        po.setName(tableName);
        if (schema != null) po.setSchema(schema);

        results.add(po);
        pushObject(po);

        if (tableName != null) {
            TableOperationInfo toi = new TableOperationInfo();
            toi.setTableName(tableName);
            toi.setSchema(schema);
            toi.setLine(getStartLine(ctx));
            toi.setOperation("DDL");
            toi.setSqlText(fullText(ctx));
            addTableOperation(toi);
        }

        Void result = visitChildren(ctx);
        popContext();
        return result;
    }

    // ---------------------------------------------------------------
    // CREATE SYNONYM (standalone) — Bug (temp-table/hint/synonym audit #11): no
    // visitCreate_synonym override existed, so `CREATE [OR REPLACE] [PUBLIC] SYNONYM
    // syn FOR [schema.]target [@dblink]` produced zero objects/table-ops even though the
    // grammar (create_synonym) already parses it cleanly. Later usage of the synonym name
    // as a plain table (e.g. INSERT INTO syn ...) already works via the normal table-op
    // capture and is untouched here. This adds a minimal, additive capture: a ParsedObject
    // of type SYNONYM (name = synonym name, schema = synonym's own owning schema, null for
    // PUBLIC synonyms), plus a single DDL-flagged TableOperationInfo recording the resolved
    // target (tableName/schema/dbLink) and the PUBLIC-ness via objectType, with alias set to
    // the synonym name so callers can map synonym -> target without a new model class.
    // ---------------------------------------------------------------
    @Override
    public Void visitCreate_synonym(PlSqlParser.Create_synonymContext ctx) {
        boolean isPublic = ctx.PUBLIC() != null;
        String synonymName = ctx.synonym_name() != null ? ctx.synonym_name().getText() : null;
        String targetName = ctx.schema_object_name() != null ? ctx.schema_object_name().getText() : null;
        String dbLink = ctx.link_name() != null ? ctx.link_name().getText() : null;

        // Disambiguate the (up to two) optional schema_name occurrences: the synonym's own
        // owning schema appears before the FOR keyword, the target's schema appears after it.
        String ownSchema = null;
        String targetSchema = null;
        if (ctx.FOR() != null) {
            int forIndex = ctx.FOR().getSymbol().getTokenIndex();
            for (PlSqlParser.Schema_nameContext sc : ctx.schema_name()) {
                if (sc.getStart().getTokenIndex() < forIndex) {
                    ownSchema = sc.getText();
                } else {
                    targetSchema = sc.getText();
                }
            }
        }

        ParsedObject po = new ParsedObject();
        po.setType("SYNONYM");
        po.setLineStart(getStartLine(ctx));
        po.setLineEnd(getStopLine(ctx));
        po.setName(synonymName);
        if (!isPublic && ownSchema != null) {
            po.setSchema(ownSchema);
        }
        results.add(po);
        pushObject(po);

        if (targetName != null) {
            TableOperationInfo toi = new TableOperationInfo();
            toi.setTableName(targetName);
            toi.setSchema(targetSchema);
            toi.setDbLink(dbLink);
            toi.setLine(getStartLine(ctx));
            toi.setOperation("DDL");
            toi.setObjectType(isPublic ? "PUBLIC SYNONYM" : "SYNONYM");
            toi.setAlias(synonymName);
            toi.setSqlText(fullText(ctx));
            addTableOperation(toi);
        }

        Void result = visitChildren(ctx);
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
        // Same PIPELINED-not-captured bug as visitFunction_body, for standalone functions.
        if (!ctx.PIPELINED().isEmpty()) {
            po.setPipelined(true);
        }

        if (ctx.parameter() != null) {
            for (PlSqlParser.ParameterContext p : ctx.parameter()) {
                po.getParameters().add(extractParameter(p));
            }
        }

        currentPackageName = null;
        currentPackageSubprograms.clear();
        declaredTypeNames.clear();
        subprogramSourceMap.clear(); // Option B: fresh scope for standalone function

        results.add(po);
        // Option A: push fresh var-value map for standalone function scope
        varValueStack.push(new LinkedHashMap<>());
        // Option B: store source for cross-proc analysis
        String standaloneFnSrc = getOriginalText(ctx, 20000);
        if (po.getName() != null && standaloneFnSrc != null) {
            addSubprogramSource(po.getName(), standaloneFnSrc);
        }
        PlSqlParser.Seq_of_declare_specsContext declSpecs = ctx.seq_of_declare_specs();
        // Gap 10: reset branch depth
        int savedBranchDepth = branchDepth;
        branchDepth = 0;
        pushObject(po);
        Void result = visitChildren(ctx);
        // Fallback: scan standalone function text for SQL-embedded pkg.fn(args) calls. Nested
        // procedure_body/function_body spans are blanked out first: the tree walk above already
        // visited them and recorded their calls under their OWN SubprogramInfo, so scanning that
        // text again here would re-detect the same targets as bogus duplicates stamped with this
        // outer function's start line (see getOriginalTextExcludingNested).
        extractFunctionCallsFromSqlText(getOriginalTextExcludingNested(ctx, 20000, declSpecs), getStartLine(ctx));
        popContext();
        varValueStack.poll(); // Option A: pop scope
        branchDepth = savedBranchDepth;
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

        // Level: FOR EACH ROW on a simple DML trigger → ROW; a compound trigger fires at
        // statement level (its own per-timing-point sections carry their own level).
        if (ctx.simple_dml_trigger() != null) {
            po.setTriggerLevel(ctx.simple_dml_trigger().for_each_row() != null ? "ROW" : "STATEMENT");
        } else if (ctx.compound_dml_trigger() != null) {
            po.setTriggerLevel("STATEMENT");
        }

        // WHEN (...) condition — the condition text only, never the wrapping WHEN ( ).
        if (ctx.trigger_when_clause() != null && ctx.trigger_when_clause().condition() != null) {
            po.setTriggerWhenClause(getOriginalText(ctx.trigger_when_clause().condition(), 4000));
        }

        // Trigger body source, sliced from the same input stream already parsed — never a re-parse.
        if (ctx.trigger_body() != null) {
            po.setTriggerBody(getOriginalText(ctx.trigger_body(), 20000));
        }

        currentPackageName = null;
        currentPackageSubprograms.clear();
        declaredTypeNames.clear();

        results.add(po);
        pushObject(po);
        Void result = visitChildren(ctx);
        popContext();
        return result;
    }

    // ---------------------------------------------------------------
    // CREATE SEQUENCE
    // ---------------------------------------------------------------
    @Override
    public Void visitCreate_sequence(PlSqlParser.Create_sequenceContext ctx) {
        ParsedObject po = new ParsedObject();
        po.setType("SEQUENCE");
        po.setLineStart(getStartLine(ctx));
        po.setLineEnd(getStopLine(ctx));

        if (ctx.sequence_name() != null) {
            parseQualifiedName(po, ctx.sequence_name().getText());
        }

        if (ctx.sequence_spec() != null) {
            for (PlSqlParser.Sequence_specContext spec : ctx.sequence_spec()) {
                if (spec.sequence_start_clause() != null
                        && spec.sequence_start_clause().UNSIGNED_INTEGER() != null) {
                    po.setSequenceStartWith(
                            Long.parseLong(spec.sequence_start_clause().UNSIGNED_INTEGER().getText()));
                } else if (spec.INCREMENT() != null && spec.UNSIGNED_INTEGER() != null) {
                    po.setSequenceIncrementBy(Long.parseLong(spec.UNSIGNED_INTEGER().getText()));
                } else if (spec.CACHE() != null && spec.UNSIGNED_INTEGER() != null) {
                    po.setSequenceCache(Long.parseLong(spec.UNSIGNED_INTEGER().getText()));
                } else if (spec.CYCLE() != null) {
                    po.setSequenceCycle(true);
                } else if (spec.NOCYCLE() != null) {
                    po.setSequenceCycle(false);
                }
            }
        }

        currentPackageName = null;
        currentPackageSubprograms.clear();
        declaredTypeNames.clear();

        results.add(po);
        pushObject(po);
        Void result = visitChildren(ctx);
        popContext();
        return result;
    }

    // ---------------------------------------------------------------
    // CREATE TYPE / CREATE TYPE BODY
    //
    // The grammar (create_type -> type_definition | type_body) has always accepted these, but no
    // visitor override existed, so an object type, a collection type and a TYPE BODY all parsed
    // with errors=[] and produced nothing at all. Both branches now build a ParsedObject the same
    // way CREATE TRIGGER / CREATE VIEW / CREATE SEQUENCE do.
    // ---------------------------------------------------------------
    @Override
    public Void visitCreate_type(PlSqlParser.Create_typeContext ctx) {
        ParsedObject po = new ParsedObject();
        po.setLineStart(getStartLine(ctx));
        po.setLineEnd(getStopLine(ctx));

        if (ctx.type_definition() != null) {
            po.setType("TYPE");
            PlSqlParser.Type_definitionContext td = ctx.type_definition();
            if (td.type_name() != null) {
                parseQualifiedName(po, td.type_name().getText());
            }
            PlSqlParser.Object_type_defContext otd = td.object_type_def();
            if (otd != null) {
                if (otd.object_as_part() != null) {
                    PlSqlParser.Object_as_partContext oap = otd.object_as_part();
                    if (oap.OBJECT() != null) {
                        po.setTypeCategory("OBJECT");
                    } else if (oap.nested_table_type_def() != null) {
                        po.setTypeCategory("NESTED_TABLE");
                        if (oap.nested_table_type_def().type_spec() != null) {
                            po.setTypeElementType(
                                    cleanId(oap.nested_table_type_def().type_spec().getText()));
                        }
                    } else if (oap.varray_type_def() != null) {
                        po.setTypeCategory("VARRAY");
                        if (oap.varray_type_def().type_spec() != null) {
                            po.setTypeElementType(
                                    cleanId(oap.varray_type_def().type_spec().getText()));
                        }
                    }
                } else if (otd.object_under_part() != null) {
                    po.setTypeCategory("UNDER");
                    if (otd.object_under_part().type_spec() != null) {
                        po.setTypeElementType(cleanId(otd.object_under_part().type_spec().getText()));
                    }
                }
                for (PlSqlParser.Modifier_clauseContext mc : otd.modifier_clause()) {
                    boolean negated = mc.NOT() != null;
                    if (mc.FINAL() != null) {
                        po.setTypeFinal(!negated);
                    } else if (mc.INSTANTIABLE() != null) {
                        po.setTypeInstantiable(!negated);
                    }
                }
            }
        } else {
            // TYPE BODY — name/subprograms are filled in by visitType_body below.
            po.setType("TYPE_BODY");
        }

        currentPackageName = null;
        currentPackageSubprograms.clear();
        declaredTypeNames.clear();

        results.add(po);
        pushObject(po);
        Void result = visitChildren(ctx);
        popContext();
        return result;
    }

    /**
     * TYPE BODY — names the enclosing TYPE_BODY ParsedObject and pre-registers its MEMBER/STATIC
     * subprogram names so calls between them classify as INTERNAL, exactly as
     * {@code visitCreate_package_body} does for a package body.
     */
    @Override
    public Void visitType_body(PlSqlParser.Type_bodyContext ctx) {
        ParsedObject po = currentObject();
        if (po != null && "TYPE_BODY".equals(po.getType()) && ctx.type_name() != null) {
            parseQualifiedName(po, ctx.type_name().getText());
            currentPackageName = po.getName();
        }
        currentPackageSubprograms.clear();
        declaredTypeNames.clear();
        for (PlSqlParser.Type_body_elementsContext el : ctx.type_body_elements()) {
            collectTypeMemberNames(el);
        }
        Void result = visitChildren(ctx);
        currentPackageName = null;
        currentPackageSubprograms.clear();
        declaredTypeNames.clear();
        return result;
    }

    private void collectTypeMemberNames(ParseTree node) {
        if (node instanceof PlSqlParser.Func_decl_in_typeContext) {
            PlSqlParser.Func_decl_in_typeContext f = (PlSqlParser.Func_decl_in_typeContext) node;
            if (f.function_name() != null) currentPackageSubprograms.add(cleanId(f.function_name().getText()));
        } else if (node instanceof PlSqlParser.Proc_decl_in_typeContext) {
            PlSqlParser.Proc_decl_in_typeContext p = (PlSqlParser.Proc_decl_in_typeContext) node;
            if (p.procedure_name() != null) currentPackageSubprograms.add(cleanId(p.procedure_name().getText()));
        } else if (node instanceof PlSqlParser.Overriding_function_specContext) {
            PlSqlParser.Overriding_function_specContext f = (PlSqlParser.Overriding_function_specContext) node;
            if (f.function_name() != null) currentPackageSubprograms.add(cleanId(f.function_name().getText()));
        } else if (node instanceof PlSqlParser.Overriding_procedure_specContext) {
            PlSqlParser.Overriding_procedure_specContext p = (PlSqlParser.Overriding_procedure_specContext) node;
            if (p.procedure_name() != null) currentPackageSubprograms.add(cleanId(p.procedure_name().getText()));
        }
        for (int i = 0; i < node.getChildCount(); i++) {
            collectTypeMemberNames(node.getChild(i));
        }
    }

    /** MEMBER/STATIC FUNCTION inside a TYPE BODY — a real subprogram, same as a package function. */
    @Override
    public Void visitFunc_decl_in_type(PlSqlParser.Func_decl_in_typeContext ctx) {
        SubprogramInfo si = new SubprogramInfo();
        si.setType("FUNCTION");
        si.setLineStart(getStartLine(ctx));
        si.setLineEnd(getStopLine(ctx));
        if (ctx.function_name() != null) {
            si.setName(cleanId(ctx.function_name().getText()));
        }
        if (ctx.type_spec() != null) {
            si.setReturnType(getTypeSpecText(ctx.type_spec()));
        }
        for (PlSqlParser.Type_elements_parameterContext p : ctx.type_elements_parameter()) {
            si.getParameters().add(extractTypeElementsParameter(p));
        }
        return visitTypeMemberBody(ctx, si, ctx.seq_of_declare_specs());
    }

    /** MEMBER/STATIC PROCEDURE inside a TYPE BODY. */
    @Override
    public Void visitProc_decl_in_type(PlSqlParser.Proc_decl_in_typeContext ctx) {
        SubprogramInfo si = new SubprogramInfo();
        si.setType("PROCEDURE");
        si.setLineStart(getStartLine(ctx));
        si.setLineEnd(getStopLine(ctx));
        if (ctx.procedure_name() != null) {
            si.setName(cleanId(ctx.procedure_name().getText()));
        }
        for (PlSqlParser.Type_elements_parameterContext p : ctx.type_elements_parameter()) {
            si.getParameters().add(extractTypeElementsParameter(p));
        }
        return visitTypeMemberBody(ctx, si, ctx.seq_of_declare_specs());
    }

    /** CONSTRUCTOR FUNCTION inside a TYPE BODY. */
    @Override
    public Void visitConstructor_declaration(PlSqlParser.Constructor_declarationContext ctx) {
        SubprogramInfo si = new SubprogramInfo();
        si.setType("CONSTRUCTOR");
        si.setLineStart(getStartLine(ctx));
        si.setLineEnd(getStopLine(ctx));
        if (ctx.function_name() != null) {
            si.setName(cleanId(ctx.function_name().getText()));
        }
        for (PlSqlParser.Type_elements_parameterContext p : ctx.type_elements_parameter()) {
            si.getParameters().add(extractTypeElementsParameter(p));
        }
        return visitTypeMemberBody(ctx, si, ctx.seq_of_declare_specs());
    }

    /**
     * OVERRIDING MEMBER FUNCTION inside a TYPE BODY. The grammar routes these through
     * {@code overriding_subprogram_spec} rather than {@code func_decl_in_type}, so without this
     * override the subprogram — and every table operation in its body — was dropped silently
     * (clean parse, empty errors, missing member).
     */
    @Override
    public Void visitOverriding_function_spec(PlSqlParser.Overriding_function_specContext ctx) {
        SubprogramInfo si = new SubprogramInfo();
        si.setType("FUNCTION");
        si.setLineStart(getStartLine(ctx));
        si.setLineEnd(getStopLine(ctx));
        if (ctx.function_name() != null) {
            si.setName(cleanId(ctx.function_name().getText()));
        }
        if (ctx.type_spec() != null) {
            si.setReturnType(getTypeSpecText(ctx.type_spec()));
        }
        for (PlSqlParser.Type_elements_parameterContext p : ctx.type_elements_parameter()) {
            si.getParameters().add(extractTypeElementsParameter(p));
        }
        return visitTypeMemberBody(ctx, si, ctx.seq_of_declare_specs());
    }

    /** OVERRIDING MEMBER PROCEDURE inside a TYPE BODY — see {@link #visitOverriding_function_spec}. */
    @Override
    public Void visitOverriding_procedure_spec(PlSqlParser.Overriding_procedure_specContext ctx) {
        SubprogramInfo si = new SubprogramInfo();
        si.setType("PROCEDURE");
        si.setLineStart(getStartLine(ctx));
        si.setLineEnd(getStopLine(ctx));
        if (ctx.procedure_name() != null) {
            si.setName(cleanId(ctx.procedure_name().getText()));
        }
        for (PlSqlParser.Type_elements_parameterContext p : ctx.type_elements_parameter()) {
            si.getParameters().add(extractTypeElementsParameter(p));
        }
        return visitTypeMemberBody(ctx, si, ctx.seq_of_declare_specs());
    }

    /**
     * Shared registration/traversal for a TYPE BODY member subprogram: attach it to the enclosing
     * TYPE_BODY ParsedObject, then traverse its body inside a subprogram scope so every call,
     * statement and table operation lands on the SubprogramInfo (mirrors visitFunction_body).
     */
    private Void visitTypeMemberBody(ParserRuleContext ctx, SubprogramInfo si,
                                     PlSqlParser.Seq_of_declare_specsContext declSpecs) {
        ParsedObject po = currentObject();
        if (po != null) {
            po.getSubprograms().add(si);
        }
        varValueStack.push(new LinkedHashMap<>());
        String src = getOriginalText(ctx, 20000);
        if (si.getName() != null && src != null) {
            addSubprogramSource(si.getName(), src);
        }
        int savedBranchDepth = branchDepth;
        branchDepth = 0;
        localSubprogramStack.push(collectNestedSubprogramNames(declSpecs));
        pushSubprogram(si);
        Void result = visitChildren(ctx);
        // Fallback: scan the method body text for SQL-embedded pkg.fn(args) calls. Nested
        // procedure_body/function_body spans are blanked out first: the tree walk above already
        // visited them and recorded their calls under their OWN SubprogramInfo, so scanning that
        // text again here would re-detect the same targets as bogus duplicates stamped with this
        // outer method's start line (see getOriginalTextExcludingNested).
        extractFunctionCallsFromSqlText(getOriginalTextExcludingNested(ctx, 20000, declSpecs), getStartLine(ctx));
        popContext();
        localSubprogramStack.pop();
        varValueStack.poll();
        branchDepth = savedBranchDepth;
        return result;
    }

    private ParameterInfo extractTypeElementsParameter(PlSqlParser.Type_elements_parameterContext ctx) {
        ParameterInfo pi = new ParameterInfo();
        pi.setLine(getStartLine(ctx));
        if (ctx.parameter_name() != null) {
            pi.setName(getIdentifierText(ctx.parameter_name()));
        }
        boolean hasIn = ctx.IN() != null;
        boolean hasOut = ctx.OUT() != null;
        if (hasIn && hasOut) {
            pi.setDirection("IN OUT");
        } else if (hasOut) {
            pi.setDirection("OUT");
        } else {
            pi.setDirection("IN");
        }
        if (ctx.type_spec() != null) {
            pi.setDataType(getTypeSpecText(ctx.type_spec()));
        }
        return pi;
    }

    // ---------------------------------------------------------------
    // ALTER TABLE
    //
    // Standalone ALTER TABLE parsed cleanly but captured nothing: visitTableview_name only records
    // anything when currentDmlOperation != null, and nothing under alter_table ever set it. So the
    // altered table vanished, and — the expensive part — so did every
    // ADD CONSTRAINT ... FOREIGN KEY ... REFERENCES edge, which is the one place a schema states a
    // table-to-table relationship outright.
    //
    // Captured as: one ALTER_TABLE ParsedObject for the altered table, carrying an "ALTER"
    // TableOperationInfo for itself plus, for each foreign key, a "REFERENCE" TableOperationInfo
    // for the target table and a JoinInfo edge (the model's existing two-table relationship
    // representation, as used for query joins) linking the two.
    // ---------------------------------------------------------------
    @Override
    public Void visitAlter_table(PlSqlParser.Alter_tableContext ctx) {
        if (ctx.tableview_name() == null) {
            return visitChildren(ctx);
        }
        ParsedObject po = new ParsedObject();
        po.setType("ALTER_TABLE");
        po.setLineStart(getStartLine(ctx));
        po.setLineEnd(getStopLine(ctx));
        parseQualifiedName(po, ctx.tableview_name().getText());

        currentPackageName = null;
        currentPackageSubprograms.clear();
        declaredTypeNames.clear();

        results.add(po);
        pushObject(po);

        TableOperationInfo altered = new TableOperationInfo();
        altered.setTableName(po.getName());
        altered.setSchema(po.getSchema());
        altered.setOperation("ALTER");
        altered.setObjectType("TABLE");
        altered.setLine(getStartLine(ctx));
        addTableOperation(altered, ctx);

        // Foreign keys: capture BOTH sides as a real lineage edge.
        for (PlSqlParser.References_clauseContext rc : collectReferencesClauses(ctx)) {
            if (rc.tableview_name() == null) continue;
            String refFull = cleanId(rc.tableview_name().getText());
            String refSchema = null;
            String refTable = refFull;
            int dot = refFull.indexOf('.');
            if (dot > 0) {
                refSchema = refFull.substring(0, dot);
                refTable = refFull.substring(dot + 1);
            }

            TableOperationInfo refOp = new TableOperationInfo();
            refOp.setTableName(refTable);
            refOp.setSchema(refSchema);
            refOp.setOperation("REFERENCE");
            refOp.setObjectType("TABLE");
            refOp.setLine(getStartLine(rc));
            addTableOperation(refOp, ctx);

            JoinInfo edge = new JoinInfo();
            edge.setJoinType("FOREIGN KEY");
            edge.setLeftTable(po.getName());
            edge.setJoinedTable(refTable);
            edge.setLine(getStartLine(rc));
            edge.setCondition(foreignKeyCondition(rc, po.getName(), refTable));
            altered.getJoins().add(edge);

            // Additionally record the same relationship structurally, in the dialect-neutral
            // shape the other four dialects bridge into. The JoinInfo edge above stays exactly
            // as it was — this is a parallel capture, not a replacement: the edge is a rendered
            // string for lineage display, while ForeignKeyInfo keeps both column lists intact.
            ForeignKeyInfo fk = new ForeignKeyInfo();
            fk.setConstraintName(foreignKeyConstraintName(rc));
            fk.setTable(po.getName());
            fk.setSchema(po.getSchema());
            fk.setReferencedTable(refTable);
            fk.setReferencedSchema(refSchema);
            fk.setColumns(foreignKeyChildColumns(rc));
            fk.setReferencedColumns(parenColumnNames(rc.paren_column_list()));
            fk.setLine(getStartLine(rc));
            po.getForeignKeys().add(fk);
        }

        Void result = visitChildren(ctx);
        popContext();
        return result;
    }

    /** All REFERENCES clauses anywhere under an ALTER TABLE (inline or out-of-line FK forms). */
    private List<PlSqlParser.References_clauseContext> collectReferencesClauses(ParseTree node) {
        List<PlSqlParser.References_clauseContext> out = new ArrayList<>();
        collectReferencesClauses(node, out);
        return out;
    }

    private void collectReferencesClauses(ParseTree node,
                                          List<PlSqlParser.References_clauseContext> out) {
        if (node instanceof PlSqlParser.References_clauseContext) {
            out.add((PlSqlParser.References_clauseContext) node);
        }
        for (int i = 0; i < node.getChildCount(); i++) {
            collectReferencesClauses(node.getChild(i), out);
        }
    }

    /** Renders "CHILD(col,...) -> PARENT(col,...)" from the FK's column lists where available. */
    private String foreignKeyCondition(PlSqlParser.References_clauseContext rc,
                                       String childTable, String refTable) {
        String childCols = null;
        ParserRuleContext parent = rc.getParent();
        if (parent instanceof PlSqlParser.Foreign_key_clauseContext) {
            PlSqlParser.Foreign_key_clauseContext fk = (PlSqlParser.Foreign_key_clauseContext) parent;
            if (fk.paren_column_list() != null) childCols = fk.paren_column_list().getText();
        } else if (parent instanceof PlSqlParser.Out_of_line_ref_constraintContext) {
            // FOREIGN KEY '(' regular_id+ ')' references_clause — columns are bare regular_ids.
            PlSqlParser.Out_of_line_ref_constraintContext orc =
                    (PlSqlParser.Out_of_line_ref_constraintContext) parent;
            List<String> cols = new ArrayList<>();
            for (PlSqlParser.Regular_idContext rid : orc.regular_id()) {
                cols.add(cleanId(rid.getText()));
            }
            if (!cols.isEmpty()) childCols = "(" + String.join(",", cols) + ")";
        }
        String refCols = rc.paren_column_list() != null ? rc.paren_column_list().getText() : "";
        return childTable + (childCols != null ? childCols : "") + " -> " + refTable + refCols;
    }

    /** The child-side column names of an FK, for the structured {@link ForeignKeyInfo} capture. */
    private List<String> foreignKeyChildColumns(PlSqlParser.References_clauseContext rc) {
        List<String> cols = new ArrayList<>();
        ParserRuleContext parent = rc.getParent();
        if (parent instanceof PlSqlParser.Foreign_key_clauseContext) {
            cols.addAll(parenColumnNames(
                    ((PlSqlParser.Foreign_key_clauseContext) parent).paren_column_list()));
        } else if (parent instanceof PlSqlParser.Out_of_line_ref_constraintContext) {
            for (PlSqlParser.Regular_idContext rid
                    : ((PlSqlParser.Out_of_line_ref_constraintContext) parent).regular_id()) {
                cols.add(cleanId(rid.getText()));
            }
        }
        return cols;
    }

    /** Splits a "(a, b)" paren_column_list into individual cleaned column names. */
    private List<String> parenColumnNames(PlSqlParser.Paren_column_listContext pcl) {
        List<String> cols = new ArrayList<>();
        if (pcl == null || pcl.column_list() == null) return cols;
        for (PlSqlParser.Column_nameContext cn : pcl.column_list().column_name()) {
            cols.add(cleanId(cn.getText()));
        }
        return cols;
    }

    /** The CONSTRAINT name carrying this FK, if the statement named one. */
    private String foreignKeyConstraintName(PlSqlParser.References_clauseContext rc) {
        ParserRuleContext parent = rc.getParent();
        if (parent instanceof PlSqlParser.Out_of_line_ref_constraintContext) {
            PlSqlParser.Constraint_nameContext cn =
                    ((PlSqlParser.Out_of_line_ref_constraintContext) parent).constraint_name();
            return cn != null ? cleanId(cn.getText()) : null;
        }
        if (parent instanceof PlSqlParser.Foreign_key_clauseContext
                && parent.getParent() instanceof PlSqlParser.Out_of_line_constraintContext) {
            PlSqlParser.Constraint_nameContext cn =
                    ((PlSqlParser.Out_of_line_constraintContext) parent.getParent()).constraint_name();
            return cn != null ? cleanId(cn.getText()) : null;
        }
        return null;
    }

    // ---------------------------------------------------------------
    // CREATE INDEX
    // ---------------------------------------------------------------
    @Override
    public Void visitCreate_index(PlSqlParser.Create_indexContext ctx) {
        ParsedObject po = new ParsedObject();
        po.setType("INDEX");
        po.setLineStart(getStartLine(ctx));
        po.setLineEnd(getStopLine(ctx));

        if (ctx.index_name() != null) {
            parseQualifiedName(po, ctx.index_name().getText());
        }

        po.setIndexUnique(ctx.UNIQUE() != null);
        po.setIndexBitmap(ctx.BITMAP() != null);

        currentPackageName = null;
        currentPackageSubprograms.clear();
        declaredTypeNames.clear();

        results.add(po);
        pushObject(po);

        PlSqlParser.Table_index_clauseContext tic = ctx.table_index_clause();
        if (tic != null) {
            if (tic.tableview_name() != null) {
                TableOperationInfo toi = new TableOperationInfo();
                toi.setTableName(cleanId(tic.tableview_name().getText()));
                toi.setOperation("CREATE INDEX");
                toi.setLine(getStartLine(ctx));
                addTableOperation(toi, ctx);
            }
            for (PlSqlParser.Index_exprContext ie : tic.index_expr()) {
                po.getIndexColumns().add(cleanId(ie.getText()));
            }
        }

        Void result = visitChildren(ctx);
        popContext();
        return result;
    }

    // ---------------------------------------------------------------
    // EXCEPTION DECLARATION (e.g. "e_custom EXCEPTION;")
    // ---------------------------------------------------------------
    @Override
    public Void visitException_declaration(PlSqlParser.Exception_declarationContext ctx) {
        // Bug 5 fix: previously silently dropped (no override existed at all). Record the
        // declared custom exception in localVariables (dataType="EXCEPTION") so it isn't lost,
        // matching how PRAGMA EXCEPTION_INIT's error-code binding is now similarly recorded.
        if (ctx.identifier() != null) {
            VariableInfo vi = new VariableInfo();
            vi.setLine(getStartLine(ctx));
            vi.setName(getIdentifierText(ctx.identifier()));
            vi.setDataType("EXCEPTION");
            addVariable(vi);
        }
        return visitChildren(ctx);
    }

    // ---------------------------------------------------------------
    // VARIABLE DECLARATION
    // ---------------------------------------------------------------
    @Override
    public Void visitVariable_declaration(PlSqlParser.Variable_declarationContext ctx) {
        // Hardening: ANTLR error-recovery on unrelated broken syntax nearby can still produce a
        // Variable_declarationContext, but one that carries a recognition exception (ctx.exception
        // != null). Registering such a "phantom" variable into scope-tracking (isDeclaredVariableInScope)
        // has previously caused false suppression of real calls in sibling procedures/declarations
        // that happen to share a name with the corrupted fragment. Skip registering scope for these;
        // still allow other, non-scope-tracking side effects only when the declaration is clean.
        if (ctx.exception != null) {
            return visitChildren(ctx);
        }

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

        // Gap 1/2: track string-valued declarations for dynamic SQL resolution.
        if (vi.getName() != null) {
            String varUpper = vi.getName().toUpperCase();
            boolean insideSub = !varValueStack.isEmpty();
            // Gap 2: track all package-level variable names (to route cross-proc assignments)
            if (!insideSub && !vi.isConstant()) {
                pkgGlobalVarNames.add(varUpper);
            }
            if (ctx.default_value_part() != null) {
                String initExpr = getOriginalText(ctx.default_value_part(), 2000);
                String fragments = (initExpr != null) ? extractStringLiterals(initExpr) : "";
                if (!fragments.isEmpty()) {
                    if (insideSub) {
                        // Inside a subprogram: seed the local scope (Gap A: declaration init)
                        Map<String, StringBuilder> scope = varValueStack.peek();
                        scope.put(varUpper, new StringBuilder(fragments));
                    } else if (vi.isConstant()) {
                        // Gap 1: package-level CONSTANT → store in pkgConstantMap
                        pkgConstantMap.put(varUpper, fragments);
                    } else {
                        // Gap 2: package-level variable (mutable) → store in pkgGlobalVarMap
                        pkgGlobalVarMap.computeIfAbsent(varUpper, k -> new StringBuilder()).append(fragments);
                    }
                }
            }
        }

        return visitChildren(ctx);
    }

    // ---------------------------------------------------------------
    // PARAMETER (for procedures/functions)
    // ---------------------------------------------------------------
    private ParameterInfo extractParameter(PlSqlParser.ParameterContext ctx) {
        ParameterInfo pi = new ParameterInfo();
        pi.setLine(getStartLine(ctx));

        // Hardening: same rationale as visitVariable_declaration — a parameter context flagged by
        // ANTLR error recovery (ctx.exception != null) may be a corrupted/phantom parameter from
        // unrelated broken syntax nearby. Do not register its name for scope-tracking purposes.
        if (ctx.exception != null) {
            return pi;
        }

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

            // Extract table names from cursor query FROM-clause only (name-only list for CursorInfo).
            // Using extractTableRefsWithLines ensures we only collect FROM-clause tables, not
            // WHERE-subquery tables which would cause false CURSOR_REF entries in globalTables.
            List<TableOperationInfo> tableRefs = extractTableRefsWithLines(ctx.select_statement());
            ci.setTables(tableRefs.stream()
                    .map(TableOperationInfo::getTableName)
                    .distinct()
                    .collect(java.util.stream.Collectors.toList()));

            // Promote cursor tables to SELECT operations using each reference's own line,
            // not the cursor declaration start line (which would be wrong for subquery tables).
            for (TableOperationInfo toi : tableRefs) {
                toi.setOperation("SELECT");
                addTableOperation(toi, ctx.select_statement());
            }

            // Bug fix: WHERE-clause subqueries inside a cursor were previously dropped
            // entirely, because the whole select_statement subtree is skipped below (to
            // avoid duplicate FROM-clause table ops). Reuse the exact same subquery-descent
            // mechanism the plain top-level DML statement path uses (visitSubquery /
            // visitTableview_name, gated on currentDmlOperation) by visiting each top-level
            // WHERE clause found in the cursor's query — FROM-clause tables are already
            // captured above, so this only picks up subquery tables nested in WHERE.
            List<PlSqlParser.Where_clauseContext> cursorWhereClauses = new ArrayList<>();
            collectTopLevelWhereClauses(ctx.select_statement(), cursorWhereClauses);
            if (!cursorWhereClauses.isEmpty()) {
                String prevDml = currentDmlOperation;
                currentDmlOperation = "SELECT";
                for (PlSqlParser.Where_clauseContext wc : cursorWhereClauses) {
                    visit(wc);
                }
                currentDmlOperation = prevDml;
            }
        }

        // Check if it is a ref cursor (no query = just declaration)
        if (ctx.select_statement() == null && ctx.RETURN() == null) {
            ci.setRefCursor(false); // simple forward declaration
        }

        addCursor(ci);

        // Visit only non-query children (cursor parameters, etc.).
        // The select_statement is already fully processed by extractTableRefsWithLines above.
        // Calling visitChildren on it would also fire visitTableview_name for every table inside
        // the cursor query (via visitSubquery → currentDmlOperation="SELECT"), producing a
        // duplicate TableOperationInfo for every table that appears in the cursor body.
        for (int i = 0; i < ctx.getChildCount(); i++) {
            if (ctx.select_statement() != null && ctx.getChild(i) == ctx.select_statement()) continue;
            visit(ctx.getChild(i));
        }

        // Extract joins from cursor query (visitJoin_clause is skipped above to avoid
        // duplicate table ops, so we walk the AST separately for join extraction)
        if (ctx.select_statement() != null) {
            extractJoinsFromSelect(ctx.select_statement());
        }

        // Fallback: scan cursor query text for pkg.fn(args) patterns
        if (ctx.select_statement() != null) {
            String queryText = getOriginalText(ctx.select_statement(), 10000);
            extractFunctionCallsFromSqlText(queryText, getStartLine(ctx));
        }

        return null;
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
            } else {
                // Standalone procedure/function — no subprogram scope; store on the object.
                ParsedObject po = currentObject();
                if (po != null) po.setPragmaAutonomousTransaction(true);
            }
        } else if (ctx.EXCEPTION_INIT() != null) {
            // Bug 4 fix: PRAGMA EXCEPTION_INIT(e_custom, -20001) — record the exception-name to
            // error-code binding as a StatementInfo so it isn't silently discarded.
            StatementInfo si = new StatementInfo();
            si.setType("EXCEPTION_INIT");
            si.setLine(getStartLine(ctx));
            si.setLineEnd(getStopLine(ctx));
            if (ctx.exception_name() != null) {
                si.setSqlText(cleanId(ctx.exception_name().getText()));
            }
            if (ctx.numeric_negative() != null) {
                try {
                    si.setErrorCode(-Integer.parseInt(ctx.numeric_negative().numeric().getText().trim()));
                } catch (NumberFormatException ignore) {
                    // leave errorCode unset if it doesn't parse as a plain integer
                }
            }
            addStatement(si);
        }
        return visitChildren(ctx);
    }

    // ---------------------------------------------------------------
    // SELECT STATEMENT
    // ---------------------------------------------------------------
    @Override
    public Void visitSelect_statement(PlSqlParser.Select_statementContext ctx) {
        // Recorded so addTableOperation can attach this statement's SQL. The field is sticky by
        // design — a nested DML takes over while it is visited — and the guard against it
        // outliving its statement lives in addTableOperation, which checks the line span.
        currentDmlCtx = ctx;
        // Only record as a statement if we are at the DML statement level
        // (not inside a cursor declaration or subquery)
        if (isDirectDmlStatement(ctx)) {
            StatementInfo si = new StatementInfo();
            si.setType("SELECT");
            si.setLine(getStartLine(ctx));
            si.setLineEnd(getStopLine(ctx));
            si.setSqlText(getOriginalText(ctx, 200));

            String prevDml = currentDmlOperation;
            List<String> prevTables = currentDmlTables; Set<String> prevTablesSet = currentDmlTablesSet;
            currentDmlOperation = "SELECT";
            currentDmlTables = new ArrayList<>(); currentDmlTablesSet = new HashSet<>();

            Void result = visitChildren(ctx);

            si.setTables(new ArrayList<>(currentDmlTables));
            addStatement(si);

            // Fallback: scan SELECT text for pkg.fn(args) patterns
            // that the tree walk may have missed due to parse error recovery
            String selectText = getOriginalText(ctx, 5000);
            extractFunctionCallsFromSqlText(selectText, getStartLine(ctx));

            currentDmlOperation = prevDml;
            currentDmlTables = prevTables; currentDmlTablesSet = prevTablesSet;
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

        // Emit a SUBQUERY statement for each nested/embedded SELECT (the top-level
        // SELECT of a DML statement is already recorded via visitSelect_statement).
        if (isEmbeddedSubquery(ctx)) {
            StatementInfo sq = new StatementInfo();
            sq.setType("SUBQUERY");
            sq.setLine(getStartLine(ctx));
            sq.setLineEnd(getStopLine(ctx));
            sq.setSqlText(getOriginalText(ctx, 200));
            addStatement(sq);
        }

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
        // Emit a CTE statement for each WITH subquery-factoring clause
        StatementInfo cte = new StatementInfo();
        cte.setType("CTE");
        cte.setLine(getStartLine(ctx));
        cte.setLineEnd(getStopLine(ctx));
        cte.setSqlText(getOriginalText(ctx, 200));
        addStatement(cte);
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
            // Bug fix (phantom alias-star table): register EVERY FROM-clause table alias here,
            // not just inline-view aliases, and do it BEFORE the select list is visited. The
            // grammar visits selected_list before from_clause in document order
            // (query_block: SELECT selected_list into_clause? from_clause? ...), so
            // `SELECT o.*, c.name FROM orders o JOIN customers c ...` would otherwise hit
            // select_list_elements: tableview_name '.' ASTERISK for "o" before "o" was ever
            // added to queryAliases, causing visitTableview_name to record a phantom ORDERS-alias
            // table named "O". Pre-registering all FROM-clause aliases up front fixes this while
            // reusing the same recursive descent (and thus the same nested-subquery scoping) that
            // already existed for inline-view aliases.
            if (tra.table_alias() != null) {
                String alias = cleanId(tra.table_alias().getText());
                if (alias != null) {
                    queryAliases.add(alias);
                }
            }
        }
        for (int i = 0; i < node.getChildCount(); i++) {
            preCollectInlineViewAliases(node.getChild(i));
        }
    }

    /**
     * Recursively scans {@code node} for any {@code subquery_factoring_clause} (i.e. every
     * {@code WITH cte_name (...) AS (...)} declaration, including chained/nested ones) and adds
     * each CTE's own name to {@link #queryAliases} up-front. Used by callers (e.g.
     * {@link #visitLoop_statement}) that must populate the self-reference suppression set BEFORE
     * running a raw tree pre-scan like {@link #extractTableRefsWithLines}, since that pre-scan
     * does not go through the normal {@code visitSubquery_factoring_clause} visitor path.
     */
    private void preRegisterCteNames(ParseTree node) {
        if (node instanceof PlSqlParser.Subquery_factoring_clauseContext) {
            PlSqlParser.Subquery_factoring_clauseContext sfc =
                    (PlSqlParser.Subquery_factoring_clauseContext) node;
            if (sfc.query_name() != null && sfc.query_name().identifier() != null) {
                queryAliases.add(cleanId(sfc.query_name().identifier().getText()));
            }
        }
        for (int i = 0; i < node.getChildCount(); i++) {
            preRegisterCteNames(node.getChild(i));
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
        // Recorded so addTableOperation can attach this statement's SQL. The field is sticky by
        // design — a nested DML takes over while it is visited — and the guard against it
        // outliving its statement lives in addTableOperation, which checks the line span.
        currentDmlCtx = ctx;
        StatementInfo si = new StatementInfo();
        si.setType("INSERT");
        si.setLine(getStartLine(ctx));
        si.setLineEnd(getStopLine(ctx));
        si.setSqlText(getOriginalText(ctx, 200));

        String prevDml = currentDmlOperation;
        List<String> prevTables = currentDmlTables; Set<String> prevTablesSet = currentDmlTablesSet;
        currentDmlOperation = "INSERT";
        currentDmlTables = new ArrayList<>(); currentDmlTablesSet = new HashSet<>();

        Void result = visitChildren(ctx);

        si.setTables(new ArrayList<>(currentDmlTables));
        addStatement(si);

        // Fallback: scan INSERT text for pkg.fn(args) patterns
        extractFunctionCallsFromSqlText(getOriginalText(ctx, 5000), getStartLine(ctx));

        currentDmlOperation = prevDml;
        currentDmlTables = prevTables; currentDmlTablesSet = prevTablesSet;
        return result;
    }

    // ---------------------------------------------------------------
    // UPDATE STATEMENT
    // ---------------------------------------------------------------
    @Override
    public Void visitUpdate_statement(PlSqlParser.Update_statementContext ctx) {
        // Recorded so addTableOperation can attach this statement's SQL. The field is sticky by
        // design — a nested DML takes over while it is visited — and the guard against it
        // outliving its statement lives in addTableOperation, which checks the line span.
        currentDmlCtx = ctx;
        StatementInfo si = new StatementInfo();
        si.setType("UPDATE");
        si.setLine(getStartLine(ctx));
        si.setLineEnd(getStopLine(ctx));
        si.setSqlText(getOriginalText(ctx, 200));

        String prevDml = currentDmlOperation;
        List<String> prevTables = currentDmlTables; Set<String> prevTablesSet = currentDmlTablesSet;
        currentDmlOperation = "UPDATE";
        currentDmlTables = new ArrayList<>(); currentDmlTablesSet = new HashSet<>();

        Void result = visitChildren(ctx);

        si.setTables(new ArrayList<>(currentDmlTables));
        addStatement(si);

        // Fallback: scan UPDATE text for pkg.fn(args) patterns
        extractFunctionCallsFromSqlText(getOriginalText(ctx, 5000), getStartLine(ctx));

        currentDmlOperation = prevDml;
        currentDmlTables = prevTables; currentDmlTablesSet = prevTablesSet;
        return result;
    }

    // ---------------------------------------------------------------
    // DELETE STATEMENT
    // ---------------------------------------------------------------
    @Override
    public Void visitDelete_statement(PlSqlParser.Delete_statementContext ctx) {
        // Recorded so addTableOperation can attach this statement's SQL. The field is sticky by
        // design — a nested DML takes over while it is visited — and the guard against it
        // outliving its statement lives in addTableOperation, which checks the line span.
        currentDmlCtx = ctx;
        StatementInfo si = new StatementInfo();
        si.setType("DELETE");
        si.setLine(getStartLine(ctx));
        si.setLineEnd(getStopLine(ctx));
        si.setSqlText(getOriginalText(ctx, 200));

        String prevDml = currentDmlOperation;
        List<String> prevTables = currentDmlTables; Set<String> prevTablesSet = currentDmlTablesSet;
        currentDmlOperation = "DELETE";
        currentDmlTables = new ArrayList<>(); currentDmlTablesSet = new HashSet<>();

        Void result = visitChildren(ctx);

        si.setTables(new ArrayList<>(currentDmlTables));
        addStatement(si);

        // Fallback: scan DELETE text for pkg.fn(args) patterns
        extractFunctionCallsFromSqlText(getOriginalText(ctx, 5000), getStartLine(ctx));

        currentDmlOperation = prevDml;
        currentDmlTables = prevTables; currentDmlTablesSet = prevTablesSet;
        return result;
    }

    // ---------------------------------------------------------------
    // MERGE STATEMENT
    // ---------------------------------------------------------------
    @Override
    public Void visitMerge_statement(PlSqlParser.Merge_statementContext ctx) {
        // Recorded so addTableOperation can attach this statement's SQL. The field is sticky by
        // design — a nested DML takes over while it is visited — and the guard against it
        // outliving its statement lives in addTableOperation, which checks the line span.
        currentDmlCtx = ctx;
        StatementInfo si = new StatementInfo();
        si.setType("MERGE");
        si.setLine(getStartLine(ctx));
        si.setLineEnd(getStopLine(ctx));
        si.setSqlText(getOriginalText(ctx, 200));

        String prevDml = currentDmlOperation;
        List<String> prevTables = currentDmlTables; Set<String> prevTablesSet = currentDmlTablesSet;
        String prevMergeTarget = currentMergeTargetTable;
        String prevMergeTargetSchema = currentMergeTargetSchema;
        currentDmlOperation = "MERGE";
        currentDmlTables = new ArrayList<>(); currentDmlTablesSet = new HashSet<>();

        // Extract target table name from selected_tableview(0) for branch attribution
        currentMergeTargetTable = null;
        currentMergeTargetSchema = null;
        if (ctx.selected_tableview() != null && !ctx.selected_tableview().isEmpty()) {
            PlSqlParser.Selected_tableviewContext stv = ctx.selected_tableview(0);
            if (stv.tableview_name() != null) {
                PlSqlParser.Tableview_nameContext tvn = stv.tableview_name();
                if (tvn.identifier() != null) {
                    String name = cleanId(tvn.identifier().getText());
                    List<PlSqlParser.Id_expressionContext> tvnParts = tvn.id_expression();
                    if (tvnParts != null && !tvnParts.isEmpty()) {
                        currentMergeTargetSchema = name;
                        currentMergeTargetTable = cleanId(tvnParts.get(tvnParts.size() - 1).getText());
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
        currentDmlTables = prevTables; currentDmlTablesSet = prevTablesSet;
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
    // CASE statement / CASE expression — emit a "CASE" StatementInfo.
    // Overriding the top-level case_statement / case_expression rules
    // (rather than the searched/simple sub-alternatives) avoids double counting.
    // ---------------------------------------------------------------
    @Override
    public Void visitCase_statement(PlSqlParser.Case_statementContext ctx) {
        StatementInfo si = new StatementInfo();
        si.setType("CASE");
        si.setLine(getStartLine(ctx));
        si.setLineEnd(getStopLine(ctx));
        si.setSqlText(getOriginalText(ctx, 200));
        addStatement(si);
        return visitChildren(ctx);
    }

    @Override
    public Void visitCase_expression(PlSqlParser.Case_expressionContext ctx) {
        StatementInfo si = new StatementInfo();
        si.setType("CASE");
        si.setLine(getStartLine(ctx));
        si.setLineEnd(getStopLine(ctx));
        si.setSqlText(getOriginalText(ctx, 200));
        addStatement(si);
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
        List<String> prevTables = currentDmlTables; Set<String> prevTablesSet = currentDmlTablesSet;
        currentDmlOperation = "TRUNCATE";
        currentDmlTables = new ArrayList<>(); currentDmlTablesSet = new HashSet<>();

        Void result = visitChildren(ctx);

        si.setTables(new ArrayList<>(currentDmlTables));
        addStatement(si);

        currentDmlOperation = prevDml;
        currentDmlTables = prevTables; currentDmlTablesSet = prevTablesSet;
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

                List<PlSqlParser.Id_expressionContext> idParts = ctx.id_expression();
                if (idParts != null && !idParts.isEmpty()) {
                    // schema[.pkg].table / schema.package.function pattern
                    schemaName = tableName;
                    tableName = cleanId(idParts.get(idParts.size() - 1).getText());
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

                    // FP-PL3: skip package names that are mistaken for table refs.
                    // When code like PKG_NAME.PROCEDURE() appears, the grammar may
                    // surface PKG_NAME as a tableview_name. Reject it if it matches
                    // a known builtin package, the current package, or any subprogram
                    // declared in the current package.
                    String fullRef = schemaName != null ? schemaName + "." + tableName : tableName;
                    boolean isPackageRef = BUILTIN_PACKAGES.contains(tableName)
                            || (currentPackageName != null && tableName.equals(currentPackageName))
                            || currentPackageSubprograms.contains(tableName);
                    if (isPackageRef) {
                        return visitChildren(ctx);
                    }

                    // FP-PL2: system / data-dictionary tables — skip silently.
                    // Exception: INSERT/UPDATE/DELETE targets are never system views (Oracle read-only
                    // data-dictionary views like ALL_OBJECTS cannot be modified). Applying the filter
                    // to write operations causes false negatives for user tables named ALL_*.
                    boolean isWrite = "INSERT".equals(currentDmlOperation)
                            || "UPDATE".equals(currentDmlOperation)
                            || "DELETE".equals(currentDmlOperation)
                            || "MERGE".equals(currentDmlOperation);
                    if (!isWrite && (isSystemTable(tableName) || isSystemTable(fullRef))) {
                        return visitChildren(ctx);
                    }

                    // Add to DML tables list (PERF: O(1) Set lookup instead of O(n) List.contains)
                    String fullName = schemaName != null ? schemaName + "." + tableName : tableName;
                    if (currentDmlTables != null && currentDmlTablesSet != null
                            && currentDmlTablesSet.add(fullName)) {
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

                    // FP-PL5: add alias to queryAliases so subsequent references to the
                    // alias letter/word are not mistakenly recorded as table names.
                    if (alias != null && !alias.isEmpty()) {
                        queryAliases.add(alias);
                    }

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
        List<String[]> commaTableNames = collectCommaTableRefs(ctx);

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

        attachImplicitCommaJoins(ctx, commaTableNames, baseOp);

        return result;
    }

    /**
     * Collect the top-level (comma-separated) table refs of a FROM clause as
     * {@code [tableName, schema, alias, line]}, ignoring inline views and TABLE(...) items.
     */
    private List<String[]> collectCommaTableRefs(PlSqlParser.From_clauseContext ctx) {
        List<String[]> commaTableNames = new ArrayList<>();
        if (ctx == null || ctx.table_ref_list() == null) return commaTableNames;
        List<PlSqlParser.Table_refContext> tableRefs = ctx.table_ref_list().table_ref();
        if (tableRefs == null) return commaTableNames;
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
        return commaTableNames;
    }

    /**
     * Record one IMPLICIT join per extra comma-separated table, then promote any of them that a
     * WHERE-clause {@code (+)} marks as an outer join. Shared by {@link #visitFrom_clause} and by
     * the cursor path, which never reaches visitFrom_clause because it deliberately skips
     * re-visiting the SELECT body (see {@code visitCursor_declaration}) — without this the exact
     * same comma-join query written inside a CURSOR silently lost both its implicit joins and its
     * old-style outer-join types, falling through to the generic text analyser's plain INNER.
     */
    private void attachImplicitCommaJoins(PlSqlParser.From_clauseContext fc,
                                          List<String[]> commaTableNames,
                                          TableOperationInfo baseOp) {
        if (fc == null || baseOp == null || commaTableNames == null || commaTableNames.size() < 2) return;
        for (int i = 1; i < commaTableNames.size(); i++) {
            String[] info = commaTableNames.get(i);
            String joinedTable = info[1] != null ? info[1] + "." + info[0] : info[0];
            int line = Integer.parseInt(info[3]);
            // Idempotent: a path that both visits the FROM clause and calls this explicitly must
            // not record the same implicit join twice.
            boolean already = false;
            for (JoinInfo existing : baseOp.getJoins()) {
                if (existing.getLine() == line && joinedTable.equalsIgnoreCase(existing.getJoinedTable())) {
                    already = true;
                    break;
                }
            }
            if (already) continue;
            JoinInfo ji = new JoinInfo();
            ji.setJoinType("IMPLICIT");
            ji.setJoinedTable(joinedTable);
            ji.setJoinedTableAlias(info[2]);
            ji.setLine(line);
            baseOp.getJoins().add(ji);
        }

        // Old-style Oracle outer join: promote IMPLICIT joins whose WHERE-clause
        // equijoin condition carries an outer_join_sign ('(+)') on one side to the
        // correct LEFT/RIGHT OUTER join type, and populate condition/leftTable.
        applyOldStyleOuterJoinSigns(fc, baseOp);
    }

    // ---------------------------------------------------------------
    // OLD-STYLE (+) OUTER JOIN — the grammar's outer_join_sign ('(' '+' ')')
    // is attached to an atom in the WHERE clause but was previously never
    // consulted by the visitor, so `a.id = b.a_id(+)` produced a generic
    // IMPLICIT join with no join-type/condition/leftTable info. This scans the
    // enclosing query_block's WHERE clause for equijoin conditions that carry
    // an outer_join_sign on one side and reclassifies the matching IMPLICIT
    // join (matched by joined-table alias) accordingly. Handles both left-side
    // and right-side (+) placement, and multiple (+) conditions ANDed together.
    // ---------------------------------------------------------------
    private void applyOldStyleOuterJoinSigns(PlSqlParser.From_clauseContext fc, TableOperationInfo baseOp) {
        if (baseOp.getJoins() == null || baseOp.getJoins().isEmpty()) return;
        ParseTree parent = fc.getParent();
        if (!(parent instanceof PlSqlParser.Query_blockContext)) return;
        PlSqlParser.Query_blockContext qb = (PlSqlParser.Query_blockContext) parent;
        PlSqlParser.Where_clauseContext wc = qb.where_clause();
        if (wc == null || wc.condition() == null) return;

        Map<String, String> aliasMap = buildAliasMap(fc);

        List<PlSqlParser.Relational_expressionContext> equalities = new ArrayList<>();
        collectOuterJoinEqualities(wc.condition(), equalities);

        for (PlSqlParser.Relational_expressionContext eq : equalities) {
            PlSqlParser.Relational_expressionContext left = eq.relational_expression(0);
            PlSqlParser.Relational_expressionContext right = eq.relational_expression(1);
            boolean leftHasSign = containsOuterJoinSign(left);
            boolean rightHasSign = containsOuterJoinSign(right);
            if (leftHasSign == rightHasSign) continue; // need exactly one side marked

            java.util.LinkedHashSet<String> leftAliases = extractAliasRefs(left);
            java.util.LinkedHashSet<String> rightAliases = extractAliasRefs(right);
            String aliasWithSign = leftHasSign
                    ? (leftAliases.isEmpty() ? null : leftAliases.iterator().next())
                    : (rightAliases.isEmpty() ? null : rightAliases.iterator().next());
            String aliasWithoutSign = leftHasSign
                    ? (rightAliases.isEmpty() ? null : rightAliases.iterator().next())
                    : (leftAliases.isEmpty() ? null : leftAliases.iterator().next());
            if (aliasWithSign == null || aliasWithoutSign == null) continue;

            String condText = getOriginalText(eq, 200);
            String preservedAlias = aliasWithoutSign; // the alias without (+) is always preserved
            String preservedTable = aliasMap.get(preservedAlias);

            // Prefer matching the join whose joined-table alias is the one carrying the
            // (+) (the join target itself is optional => LEFT OUTER). Only fall back to
            // matching via the non-signed alias (join target is preserved, the OTHER
            // alias — e.g. the base table — is optional => RIGHT OUTER) when no join
            // target carries the sign, e.g. `a.id(+) = b.a_id` where the base table a
            // itself is the optional side.
            JoinInfo target = null;
            String joinType = null;
            for (JoinInfo ji : baseOp.getJoins()) {
                if (aliasWithSign.equalsIgnoreCase(ji.getJoinedTableAlias())) {
                    target = ji;
                    joinType = "LEFT OUTER";
                    break;
                }
            }
            if (target == null) {
                for (JoinInfo ji : baseOp.getJoins()) {
                    if (aliasWithoutSign.equalsIgnoreCase(ji.getJoinedTableAlias())) {
                        target = ji;
                        joinType = "RIGHT OUTER";
                        break;
                    }
                }
            }
            if (target == null) continue;

            target.setLeftTableAlias(preservedAlias);
            target.setLeftTable(preservedTable);
            target.setCondition(condText);
            target.setJoinType(joinType);
        }
    }

    private boolean containsOuterJoinSign(ParseTree node) {
        if (node instanceof PlSqlParser.Outer_join_signContext) return true;
        for (int i = 0; i < node.getChildCount(); i++) {
            if (containsOuterJoinSign(node.getChild(i))) return true;
        }
        return false;
    }

    private void collectOuterJoinEqualities(ParseTree node, List<PlSqlParser.Relational_expressionContext> out) {
        if (node instanceof PlSqlParser.Relational_expressionContext) {
            PlSqlParser.Relational_expressionContext rec = (PlSqlParser.Relational_expressionContext) node;
            if (rec.relational_expression().size() == 2 && rec.relational_operator() != null
                    && "=".equals(rec.relational_operator().getText())
                    && containsOuterJoinSign(rec)) {
                out.add(rec);
            }
        }
        for (int i = 0; i < node.getChildCount(); i++) {
            collectOuterJoinEqualities(node.getChild(i), out);
        }
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
                ji.setJoinedTable(joinedTables.get(0));
            } else {
                // JOIN TABLE(pkg.fn(...)) alias ON ... — a table-valued-function result has no
                // tableview_name, so there is no real table to name here. Fall back to the alias
                // (the only stable identifier the ON condition can refer to) and record the
                // underlying function call as a dependency. This mirrors exactly what the cursor
                // path already does in extractJoinFromCursorClause; without it, an ANSI JOIN onto
                // a TVF in a plain SELECT produced a JoinInfo with joinedTable == null.
                // Deliberately NOT recorded as a TableOperationInfo: the alias is not a table and
                // injecting it into tableOperations would corrupt the table-dependency model.
                if (ctx.table_ref_aux().table_alias() != null) {
                    ji.setJoinedTable(cleanId(ctx.table_ref_aux().table_alias().getText()));
                }
                // NOTE: unlike the cursor path (extractJoinFromCursorClause), we must NOT call
                // extractCallsFromTableRefAux here. This visitor still descends via
                // visitChildren below, which already records the TABLE(pkg.fn(...)) call; doing
                // it here too emits a second, duplicate CallInfo for the same physical call site.
            }
            if (ctx.table_ref_aux().table_alias() != null) {
                ji.setJoinedTableAlias(cleanId(ctx.table_ref_aux().table_alias().getText()));
            }
        }

        resolveJoinSides(ctx, ji);
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
    // JOIN SIDE RESOLUTION
    // Build an alias->table map for the query block that owns this join
    // (FROM base table + every JOIN target), then resolve BOTH sides of the
    // join from the aliases referenced in its ON condition. The left side is
    // NOT defaulted to the FROM base table — it is the "other" alias in the ON
    // predicate (the one that is not the newly joined table's alias).
    // ---------------------------------------------------------------
    private void resolveJoinSides(PlSqlParser.Join_clauseContext ctx, JoinInfo ji) {
        PlSqlParser.From_clauseContext fc = enclosingFromClause(ctx);
        // Stable per-query identifier so joins from different statements that share a
        // base table are not lumped together downstream.
        ji.setQueryId(fc != null ? getStartLine(fc) : getStartLine(ctx));

        Map<String, String> aliasMap = buildAliasMap(fc);
        String joinedAlias = ji.getJoinedTableAlias(); // already upper-cased via cleanId
        // Grammar-based: read the aliases straight off the ON condition's parse tree instead of
        // text-scanning JoinInfo.getCondition() (which is also length-truncated for display).
        // A USING(...) join carries no qualified references, exactly as the old text scan found none.
        if (ji.getCondition() == null) return;
        PlSqlParser.ConditionContext onCond =
                (ctx.join_on_part() != null && !ctx.join_on_part().isEmpty())
                        ? ctx.join_on_part(0).condition() : null;
        if (onCond == null) return;

        // When the joined table carries no alias, references to it inside the ON condition are
        // written with its own (possibly schema-qualified) NAME — `JOIN T_B ON T_B.a_id = a.id`.
        // buildAliasMap deliberately maps every table name to itself so such unqualified-alias
        // references resolve, which means the joined table's own name would otherwise be picked
        // as the LEFT side, making the join point at itself. Excluded here, exactly as an alias
        // is. Only applied when there is no alias: an aliased self-join (`JOIN T x ON x.id =
        // T.pid`) legitimately has the bare table name on the left.
        String joinedBareName = null;
        if (joinedAlias == null && ji.getJoinedTable() != null) {
            String jt = ji.getJoinedTable();
            joinedBareName = (jt.contains(".") ? jt.substring(jt.lastIndexOf('.') + 1) : jt).toUpperCase();
        }
        for (String a : extractAliasRefs(onCond)) {
            if (joinedAlias != null && a.equalsIgnoreCase(joinedAlias)) continue;
            if (joinedBareName != null && a.equalsIgnoreCase(joinedBareName)) continue;
            String t = aliasMap.get(a);
            if (t != null) {
                ji.setLeftTableAlias(a);
                ji.setLeftTable(t);
                return;
            }
        }
    }

    private PlSqlParser.From_clauseContext enclosingFromClause(ParserRuleContext ctx) {
        ParseTree p = ctx;
        while (p != null && !(p instanceof PlSqlParser.From_clauseContext)) {
            p = p.getParent();
        }
        return (PlSqlParser.From_clauseContext) p;
    }

    private Map<String, String> buildAliasMap(PlSqlParser.From_clauseContext fc) {
        Map<String, String> m = new HashMap<>();
        if (fc == null || fc.table_ref_list() == null) return m;
        for (PlSqlParser.Table_refContext tr : fc.table_ref_list().table_ref()) {
            if (tr.table_ref_aux() != null) addAlias(m, tr.table_ref_aux());
            if (tr.join_clause() != null) {
                for (PlSqlParser.Join_clauseContext jc : tr.join_clause()) {
                    addAlias(m, jc.table_ref_aux());
                }
            }
        }
        return m;
    }

    private void addAlias(Map<String, String> m, PlSqlParser.Table_ref_auxContext aux) {
        if (aux == null) return;
        List<String> names = extractTableNamesFromContext(
                aux.table_ref_aux_internal() != null ? aux.table_ref_aux_internal() : aux);
        if (names.isEmpty()) return;
        String tbl = names.get(0);
        if (tbl.contains(".")) tbl = tbl.substring(tbl.indexOf('.') + 1);
        tbl = tbl.toUpperCase();
        String alias = aux.table_alias() != null ? cleanId(aux.table_alias().getText()) : null;
        if (alias != null) m.put(alias, tbl);
        // Also map the table name to itself so unaliased column references resolve.
        m.put(tbl, tbl);
    }

    /**
     * Collect, in source order, every qualifier name that appears to the LEFT of a dot in a
     * condition — i.e. the table aliases referenced by {@code a.id = b.a_id}.
     *
     * <p>Grammar-based (no regex): the qualified references are real parse-tree nodes
     * ({@code general_element: general_element ('.' general_element_part)+}, plus
     * {@code column_name}/{@code tableview_name}, both {@code identifier ('.' id_expression)*}),
     * so the qualifiers are read straight off the tree. Every component except the last is
     * emitted, mirroring the previous "identifier immediately followed by a dot" scan for
     * multi-part names such as {@code SCHEMA.TABLE.COLUMN}. Unlike a text scan, identifiers
     * inside string literals can never be mistaken for aliases.
     */
    private java.util.LinkedHashSet<String> extractAliasRefs(ParseTree cond) {
        java.util.LinkedHashSet<String> s = new java.util.LinkedHashSet<>();
        collectDottedQualifiers(cond, s);
        return s;
    }

    private void collectDottedQualifiers(ParseTree t, Set<String> out) {
        if (t == null) return;
        if (t instanceof PlSqlParser.General_elementContext
                && !(t.getParent() instanceof PlSqlParser.General_elementContext)) {
            List<PlSqlParser.General_element_partContext> parts = new ArrayList<>();
            flattenGeneralElement((PlSqlParser.General_elementContext) t, parts);
            for (int i = 0; i + 1 < parts.size(); i++) {
                PlSqlParser.General_element_partContext p = parts.get(i);
                if (p.id_expression() != null) out.add(cleanId(p.id_expression().getText()).toUpperCase());
            }
        } else if (t instanceof PlSqlParser.Column_nameContext) {
            PlSqlParser.Column_nameContext cn = (PlSqlParser.Column_nameContext) t;
            if (!cn.id_expression().isEmpty() && cn.identifier() != null) {
                out.add(cleanId(cn.identifier().getText()).toUpperCase());
                for (int i = 0; i + 1 < cn.id_expression().size(); i++) {
                    out.add(cleanId(cn.id_expression(i).getText()).toUpperCase());
                }
            }
        } else if (t instanceof PlSqlParser.Tableview_nameContext) {
            PlSqlParser.Tableview_nameContext tv = (PlSqlParser.Tableview_nameContext) t;
            if (!tv.id_expression().isEmpty() && tv.identifier() != null) {
                out.add(cleanId(tv.identifier().getText()).toUpperCase());
                for (int i = 0; i + 1 < tv.id_expression().size(); i++) {
                    out.add(cleanId(tv.id_expression(i).getText()).toUpperCase());
                }
            }
        }
        for (int i = 0; i < t.getChildCount(); i++) {
            collectDottedQualifiers(t.getChild(i), out);
        }
    }

    /** Flatten the left-recursive {@code general_element} chain into its dotted parts, in order. */
    private void flattenGeneralElement(PlSqlParser.General_elementContext ge,
                                       List<PlSqlParser.General_element_partContext> out) {
        if (ge == null) return;
        if (ge.general_element() != null) flattenGeneralElement(ge.general_element(), out);
        out.addAll(ge.general_element_part());
    }

    // ---------------------------------------------------------------
    // SINGLE TABLE INSERT — fix INSERT INTO t SELECT * FROM s so that
    // the SELECT source table is recorded as SELECT, not INSERT.
    // ---------------------------------------------------------------
    @Override
    public Void visitSingle_table_insert(PlSqlParser.Single_table_insertContext ctx) {
        // The target table (INSERT INTO ...) is already captured by visitTableview_name
        // under currentDmlOperation = "INSERT" (set by visitInsert_statement).
        // For INSERT ... SELECT ..., the select_statement source must be visited under
        // currentDmlOperation = "SELECT" so the FROM-clause tables get the right label.
        if (ctx.select_statement() != null) {
            // Visit the insert_into_clause under INSERT context (target table)
            if (ctx.insert_into_clause() != null) {
                visit(ctx.insert_into_clause());
            }
            // Switch to SELECT for the source query
            String prevDml = currentDmlOperation;
            currentDmlOperation = "SELECT";
            visit(ctx.select_statement());
            currentDmlOperation = prevDml;
            // Visit remaining children (error_logging_clause etc.) if any
            for (int i = 0; i < ctx.getChildCount(); i++) {
                ParseTree child = ctx.getChild(i);
                if (child != ctx.insert_into_clause() && child != ctx.select_statement()) {
                    visit(child);
                }
            }
            return null;
        }
        return visitChildren(ctx);
    }

    // ---------------------------------------------------------------
    // MULTI TABLE INSERT — INSERT ALL INTO t1 ... INTO t2 ... SELECT *
    // Each INTO target is a WRITE; the trailing SELECT source is READ.
    // ---------------------------------------------------------------
    @Override
    public Void visitMulti_table_insert(PlSqlParser.Multi_table_insertContext ctx) {
        // Visit each insert-into target under INSERT context
        String prevDml = currentDmlOperation;
        currentDmlOperation = "INSERT";
        if (ctx.multi_table_element() != null) {
            for (PlSqlParser.Multi_table_elementContext elem : ctx.multi_table_element()) {
                visit(elem);
            }
        }
        if (ctx.conditional_insert_clause() != null) {
            visit(ctx.conditional_insert_clause());
        }
        // The source SELECT must be visited under SELECT context
        if (ctx.select_statement() != null) {
            currentDmlOperation = "SELECT";
            visit(ctx.select_statement());
        }
        currentDmlOperation = prevDml;
        return null;
    }

    // ---------------------------------------------------------------
    // SUBTYPE DECLARATION — SUBTYPE t IS orders%ROWTYPE → captures orders
    // ---------------------------------------------------------------
    @Override
    public Void visitSubtype_declaration(PlSqlParser.Subtype_declarationContext ctx) {
        if (ctx.type_spec() != null) {
            extractTypeSpecTableRef(ctx.type_spec(), getStartLine(ctx));
        }
        return visitChildren(ctx);
    }

    // ---------------------------------------------------------------
    // CREATE VIEW — captures table dependencies from the SELECT body.
    // ---------------------------------------------------------------
    @Override
    public Void visitCreate_view(PlSqlParser.Create_viewContext ctx) {
        ParsedObject po = new ParsedObject();
        po.setType("VIEW");
        po.setLineStart(getStartLine(ctx));
        po.setLineEnd(getStopLine(ctx));

        // View name: ctx.v is the Id_expressionContext for the view name
        if (ctx.v != null) {
            po.setName(cleanId(ctx.v.getText()));
        }
        // Schema prefix: ctx.schema_name() contains the schema if qualified (schema.view_name)
        if (ctx.schema_name() != null) {
            po.setSchema(cleanId(ctx.schema_name().getText()));
        }

        currentPackageName = null;
        currentPackageSubprograms.clear();
        declaredTypeNames.clear();

        results.add(po);
        pushObject(po);

        // Extract static table references from the VIEW SELECT body
        if (ctx.select_only_statement() != null) {
            List<TableOperationInfo> tableRefs =
                    extractTableRefsWithLines(ctx.select_only_statement());
            for (TableOperationInfo toi : tableRefs) {
                toi.setOperation("SELECT");
                addTableOperation(toi, ctx.select_only_statement()); // also populates po.getDependencies().getTables()
            }
        }

        // Visit only non-query children (e.g. WITH CHECK OPTION clause, etc.).
        // The select_only_statement body is already fully processed by extractTableRefsWithLines
        // above; calling visitChildren on it would also fire visitTableview_name for every table
        // inside the view body (via visitSubquery -> currentDmlOperation), producing a duplicate
        // TableOperationInfo for every table that appears in the view's FROM clause. Same pattern
        // as visitCursor_declaration's skip-the-already-extracted-query-subtree fix above.
        for (int i = 0; i < ctx.getChildCount(); i++) {
            if (ctx.select_only_statement() != null && ctx.getChild(i) == ctx.select_only_statement()) continue;
            visit(ctx.getChild(i));
        }
        popContext();
        return null;
    }

    // CREATE MATERIALIZED VIEW — captures table dependencies from the
    // SELECT body just like CREATE VIEW (Fix A6 analog).
    // ---------------------------------------------------------------
    @Override
    public Void visitCreate_materialized_view(PlSqlParser.Create_materialized_viewContext ctx) {
        ParsedObject po = new ParsedObject();
        po.setType("MATERIALIZED_VIEW");
        po.setLineStart(getStartLine(ctx));
        po.setLineEnd(getStopLine(ctx));

        // MV name: from tableview_name()
        if (ctx.tableview_name() != null) {
            PlSqlParser.Tableview_nameContext tvn = ctx.tableview_name();
            if (tvn.identifier() != null) {
                String name = cleanId(tvn.identifier().getText());
                List<PlSqlParser.Id_expressionContext> parts = tvn.id_expression();
                if (parts != null && !parts.isEmpty()) {
                    po.setSchema(name);
                    po.setName(cleanId(parts.get(parts.size() - 1).getText()));
                } else {
                    po.setName(name);
                }
            }
        }

        currentPackageName = null;
        currentPackageSubprograms.clear();
        declaredTypeNames.clear();

        results.add(po);
        pushObject(po);

        // Extract static table references from the MV SELECT body
        if (ctx.select_only_statement() != null) {
            String prevDml = currentDmlOperation;
            List<String> prevTables = currentDmlTables; Set<String> prevTablesSet = currentDmlTablesSet;
            currentDmlOperation = "SELECT";
            currentDmlTables = new ArrayList<>(); currentDmlTablesSet = new HashSet<>();

            List<TableOperationInfo> tableRefs =
                    extractTableRefsWithLines(ctx.select_only_statement());
            for (TableOperationInfo toi : tableRefs) {
                toi.setOperation("SELECT");
                addTableOperation(toi, ctx.select_only_statement());
            }
            // Skip re-visiting the already-extracted select body — see the identical fix/comment
            // in visitCreate_view above (and visitCursor_declaration) for why visitChildren here
            // would double-count every FROM-clause table.
            for (int i = 0; i < ctx.getChildCount(); i++) {
                if (ctx.getChild(i) == ctx.select_only_statement()) continue;
                visit(ctx.getChild(i));
            }

            currentDmlOperation = prevDml;
            currentDmlTables = prevTables; currentDmlTablesSet = prevTablesSet;
        } else {
            visitChildren(ctx);
        }

        popContext();
        return null;
    }

    /**
     * Walk a cursor SELECT statement and attach JoinInfo to the base table op.
     * extractTableRefsWithLines already registered ALL table ops (base + join targets)
     * before this is called, so attachJoinToLastTableOp would pick the wrong op.
     * Instead we find the first table in the FROM clause by name+line and attach there.
     */
    private void extractJoinsFromSelect(ParserRuleContext ctx) {
        if (ctx == null) return;
        // Find the base table op: first tableview_name in the FROM clause
        TableOperationInfo baseOp = findBaseTableOpFromSelect(ctx);
        collectJoinsFromNode(ctx, baseOp);
        // Comma-separated (implicit) joins and old-style (+) outer joins live in the FROM/WHERE
        // clauses, not in a join_clause, so collectJoinsFromNode cannot see them.
        PlSqlParser.From_clauseContext fc = findFirstFromClause(ctx);
        attachImplicitCommaJoins(fc, collectCommaTableRefs(fc), baseOp);
    }

    private TableOperationInfo findBaseTableOpFromSelect(ParserRuleContext ctx) {
        // Walk to the first from_clause → table_ref_list → first table_ref
        PlSqlParser.From_clauseContext fromClause = findFirstFromClause(ctx);
        if (fromClause == null || fromClause.table_ref_list() == null) return null;
        List<PlSqlParser.Table_refContext> tableRefs = fromClause.table_ref_list().table_ref();
        if (tableRefs == null || tableRefs.isEmpty()) return null;

        PlSqlParser.Table_refContext firstRef = tableRefs.get(0);
        List<String> names = new ArrayList<>();
        if (firstRef.table_ref_aux() != null && firstRef.table_ref_aux().table_ref_aux_internal() != null) {
            names = extractTableNamesFromContext(firstRef.table_ref_aux().table_ref_aux_internal());
        }
        if (names.isEmpty()) return null;

        String fullName = names.get(0);
        String baseName = fullName.contains(".") ? fullName.substring(fullName.indexOf('.') + 1) : fullName;
        int baseLine = getStartLine(firstRef);

        SubprogramInfo sub = currentSubprogram();
        List<TableOperationInfo> ops = sub != null ? sub.getTableOperations() : null;
        if (ops == null) {
            ParsedObject po = currentObject();
            if (po != null) ops = po.getTableOperations();
        }
        if (ops == null) return null;

        // Match by name and line (exact), or fall back to name-only match
        for (int i = ops.size() - 1; i >= 0; i--) {
            TableOperationInfo op = ops.get(i);
            if (op.getTableName() != null && op.getTableName().equalsIgnoreCase(baseName)
                    && op.getLine() == baseLine) {
                return op;
            }
        }
        // Fallback: name only
        for (int i = ops.size() - 1; i >= 0; i--) {
            TableOperationInfo op = ops.get(i);
            if (op.getTableName() != null && op.getTableName().equalsIgnoreCase(baseName)) {
                return op;
            }
        }
        return null;
    }

    private PlSqlParser.From_clauseContext findFirstFromClause(ParseTree node) {
        if (node instanceof PlSqlParser.From_clauseContext) {
            return (PlSqlParser.From_clauseContext) node;
        }
        // Don't descend into subqueries — only the top-level FROM matters
        if (node instanceof PlSqlParser.SubqueryContext && node.getParent() != null
                && !(node.getParent() instanceof PlSqlParser.Select_only_statementContext)) {
            return null;
        }
        for (int i = 0; i < node.getChildCount(); i++) {
            PlSqlParser.From_clauseContext fc = findFirstFromClause(node.getChild(i));
            if (fc != null) return fc;
        }
        return null;
    }

    private void collectJoinsFromNode(ParseTree node, TableOperationInfo baseOp) {
        if (node == null) return;
        if (node instanceof PlSqlParser.Selected_listContext
                || node instanceof PlSqlParser.Where_clauseContext
                || node instanceof PlSqlParser.Having_clauseContext
                || node instanceof PlSqlParser.Order_by_clauseContext
                || node instanceof PlSqlParser.Group_by_clauseContext
                || node instanceof PlSqlParser.Hierarchical_query_clauseContext
                || node instanceof PlSqlParser.Start_partContext) {
            return;
        }
        if (node instanceof PlSqlParser.Join_clauseContext) {
            extractJoinFromCursorClause((PlSqlParser.Join_clauseContext) node, baseOp);
            return;
        }
        for (int i = 0; i < node.getChildCount(); i++) {
            collectJoinsFromNode(node.getChild(i), baseOp);
        }
    }

    private void extractJoinFromCursorClause(PlSqlParser.Join_clauseContext ctx, TableOperationInfo baseOp) {
        JoinInfo ji = new JoinInfo();
        ji.setJoinType(determineJoinType(ctx));
        ji.setLine(getStartLine(ctx));
        if (ctx.join_on_part() != null && !ctx.join_on_part().isEmpty()) {
            ji.setCondition(getOriginalText(ctx.join_on_part(0), 200));
        } else if (ctx.join_using_part() != null && !ctx.join_using_part().isEmpty()) {
            ji.setCondition(getOriginalText(ctx.join_using_part(0), 200));
        }
        if (ctx.table_ref_aux() != null) {
            List<String> joinedTables = extractTableNamesFromContext(ctx.table_ref_aux());
            if (!joinedTables.isEmpty()) {
                ji.setJoinedTable(joinedTables.get(0));
            } else {
                // TABLE(fn()) in the join — use alias as joined table name and extract the call
                if (ctx.table_ref_aux().table_alias() != null) {
                    ji.setJoinedTable(cleanId(ctx.table_ref_aux().table_alias().getText()));
                }
                extractCallsFromTableRefAux(ctx.table_ref_aux());
            }
            if (ctx.table_ref_aux().table_alias() != null) {
                ji.setJoinedTableAlias(cleanId(ctx.table_ref_aux().table_alias().getText()));
            }
        }
        resolveJoinSides(ctx, ji);
        if (baseOp != null) {
            baseOp.getJoins().add(ji);
        } else {
            attachJoinToLastTableOp(ji);
        }
    }

    private void extractCallsFromTableRefAux(ParserRuleContext ctx) {
        if (ctx == null) return;
        for (int i = 0; i < ctx.getChildCount(); i++) {
            ParseTree child = ctx.getChild(i);
            if (child instanceof PlSqlParser.Table_collection_expressionContext) {
                extractCallsFromTableCollectionExpression(
                        (PlSqlParser.Table_collection_expressionContext) child);
                return;
            }
            if (child instanceof ParserRuleContext) {
                extractCallsFromTableRefAux((ParserRuleContext) child);
            }
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

            // Primary extraction: string literals present in the expression itself
            boolean tablesFound = extractTablesFromDynamicSqlTracked(exprText, getStartLine(ctx));

            // Option A: expression is a plain variable — look up accumulated value in scope
            if (!tablesFound) {
                String varName = extractSimpleExprVar(exprText);
                if (varName != null) {
                    // A1. Local scope (procedure variable assignment)
                    if (!varValueStack.isEmpty()) {
                        Map<String, StringBuilder> scope = varValueStack.peek();
                        StringBuilder acc = scope != null ? scope.get(varName) : null;
                        if (acc != null && acc.length() > 0) {
                            // acc contains already-stripped SQL text (no quotes) — run pattern directly
                            tablesFound = runDynSqlPatternTracked(acc.toString(), getStartLine(ctx));
                        }
                    }
                    // A2. Package-level CONSTANT (Gap 1)
                    if (!tablesFound) {
                        String constVal = pkgConstantMap.get(varName);
                        if (constVal != null && !constVal.isEmpty()) {
                            tablesFound = runDynSqlPatternTracked(constVal, getStartLine(ctx));
                            if (tablesFound) dsi.setCrossProcSourceHint("PKG_CONSTANT");
                        }
                    }
                    // A3. Package-level mutable variable (Gap 2)
                    if (!tablesFound) {
                        StringBuilder globalAcc = pkgGlobalVarMap.get(varName);
                        if (globalAcc != null && globalAcc.length() > 0) {
                            tablesFound = runDynSqlPatternTracked(globalAcc.toString(), getStartLine(ctx));
                            if (tablesFound) dsi.setCrossProcSourceHint("PKG_GLOBAL_VAR");
                        }
                    }
                    // Option B: check if varName matches a known function in the same package
                    // (rare — usually the func name appears directly in EXECUTE IMMEDIATE)
                    if (!tablesFound) {
                        boolean found = scanAllSubprogramSourcesForReturnStrings(varName, getStartLine(ctx));
                        if (found) {
                            tablesFound = true;
                            dsi.setCrossProcSourceHint(varName);
                        }
                    }
                    // Option C: still nothing — flag unresolved
                    if (!tablesFound) {
                        dsi.setUnresolvedVarName(varName);
                    }
                } else {
                    // A4. Qualified constant: PKG.CONST → look up const in pkgConstantMap or globalConstRegistry
                    String qualifiedConst = extractQualifiedConstName(exprText);
                    if (qualifiedConst != null && !tablesFound) {
                        tablesFound = resolveQualifiedConst(qualifiedConst, getStartLine(ctx));
                    }
                    // Option B: expression is a function call — try to find the function body
                    if (!tablesFound) {
                        String funcName = extractSimpleCallName(exprText);
                        if (funcName != null) {
                            boolean found = scanAllSubprogramSourcesForReturnStrings(funcName, getStartLine(ctx));
                            if (found) {
                                dsi.setCrossProcSourceHint(funcName);
                            }
                        }
                    }
                }
            }
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
            List<String> prevTables = currentDmlTables; Set<String> prevTablesSet = currentDmlTablesSet;
            currentDmlOperation = "SELECT";
            currentDmlTables = new ArrayList<>(); currentDmlTablesSet = new HashSet<>();
            Void result = visitChildren(ctx);
            si.setTables(new ArrayList<>(currentDmlTables));
            currentDmlOperation = prevDml;
            currentDmlTables = prevTables; currentDmlTablesSet = prevTablesSet;
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

            boolean tablesFound = extractTablesFromDynamicSqlTracked(exprText, getStartLine(ctx));

            // Option A: plain variable — look up accumulated value
            if (!tablesFound) {
                String varName = extractSimpleExprVar(exprText);
                if (varName != null) {
                    // A1. Local scope
                    if (!varValueStack.isEmpty()) {
                        Map<String, StringBuilder> scope = varValueStack.peek();
                        StringBuilder acc = scope != null ? scope.get(varName) : null;
                        if (acc != null && acc.length() > 0) {
                            tablesFound = runDynSqlPatternTracked(acc.toString(), getStartLine(ctx));
                        }
                    }
                    // A2. Package-level CONSTANT (Gap 1)
                    if (!tablesFound) {
                        String constVal = pkgConstantMap.get(varName);
                        if (constVal != null && !constVal.isEmpty()) {
                            tablesFound = runDynSqlPatternTracked(constVal, getStartLine(ctx));
                            if (tablesFound) dsi.setCrossProcSourceHint("PKG_CONSTANT");
                        }
                    }
                    // A3. Package-level mutable variable (Gap 2)
                    if (!tablesFound) {
                        StringBuilder globalAcc = pkgGlobalVarMap.get(varName);
                        if (globalAcc != null && globalAcc.length() > 0) {
                            tablesFound = runDynSqlPatternTracked(globalAcc.toString(), getStartLine(ctx));
                            if (tablesFound) dsi.setCrossProcSourceHint("PKG_GLOBAL_VAR");
                        }
                    }
                    if (!tablesFound) {
                        boolean found = scanAllSubprogramSourcesForReturnStrings(varName, getStartLine(ctx));
                        if (found) {
                            tablesFound = true;
                            dsi.setCrossProcSourceHint(varName);
                        }
                    }
                    if (!tablesFound) {
                        dsi.setUnresolvedVarName(varName);
                    }
                } else {
                    // A4. Qualified constant: PKG.CONST
                    String qualifiedConst = extractQualifiedConstName(exprText);
                    if (qualifiedConst != null) {
                        tablesFound = resolveQualifiedConst(qualifiedConst, getStartLine(ctx));
                    }
                    // Option B: function call expression
                    if (!tablesFound) {
                        String funcName = extractSimpleCallName(exprText);
                        if (funcName != null) {
                            boolean found = scanAllSubprogramSourcesForReturnStrings(funcName, getStartLine(ctx));
                            if (found) {
                                dsi.setCrossProcSourceHint(funcName);
                            }
                        }
                    }
                }
            }

            addDynamicSql(dsi);
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
    // IF STATEMENT — full implementation is below (Gap 10: branch multi-value)
    // (see visitIf_statement near the EXCEPTION HANDLER section)

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
                        // Bug fix (2026-09-17): extractTableRefsWithLines() below walks the raw
                        // parse tree directly (bypassing the normal visitChildren traversal), so
                        // for an inline cursor FOR loop (`FOR r IN (WITH cte AS (...) SELECT ...)
                        // LOOP`) it used to run BEFORE visitSubquery_factoring_clause ever had a
                        // chance to populate queryAliases with the CTE's own name — leaving the
                        // self-reference guard empty and causing the CTE name to be misrecorded
                        // as an external table. Pre-register every CTE name declared anywhere in
                        // this select statement first so the guard is already populated.
                        preRegisterCteNames(clp.select_statement());

                        // FROM-clause only — avoids collecting WHERE-subquery tables into ci.tables
                        List<TableOperationInfo> tableRefs = extractTableRefsWithLines(clp.select_statement());
                        ci.setTables(tableRefs.stream()
                                .map(TableOperationInfo::getTableName)
                                .distinct()
                                .collect(java.util.stream.Collectors.toList()));
                        addCursor(ci);

                        // Promote cursor tables to SELECT operations using each reference's own line
                        for (TableOperationInfo toi : tableRefs) {
                            toi.setOperation("SELECT");
                            addTableOperation(toi, clp.select_statement());
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

        // For inline cursor FOR loops, skip visiting the select_statement inside
        // cursor_loop_param — it was already fully processed by extractTableRefsWithLines above.
        // Visiting it again via visitChildren would fire visitTableview_name for every table in
        // the cursor query (through visitSubquery), creating duplicate TableOperationInfo entries.
        PlSqlParser.Cursor_loop_paramContext clpForSkip =
                (ctx.FOR() != null && ctx.cursor_loop_param() != null
                        && ctx.cursor_loop_param().select_statement() != null)
                        ? ctx.cursor_loop_param() : null;

        // A cursor FOR loop's record (FOR r IN (...) LOOP) is a record variable scoped to the loop
        // body: r.field is a field access, not a package-global read. Track it while the body is
        // visited so the package-global capture can drop it.
        boolean pushedLoopRecord = ctx.FOR() != null
                && ctx.cursor_loop_param() != null
                && ctx.cursor_loop_param().record_name() != null;
        if (pushedLoopRecord) {
            Set<String> frame = new HashSet<>();
            String rec = cleanId(ctx.cursor_loop_param().record_name().getText());
            if (rec != null) frame.add(rec.toUpperCase());
            loopRecordStack.push(frame);
        }

        Void result;
        if (clpForSkip != null) {
            // Visit all loop children except the inline cursor select_statement
            for (int i = 0; i < ctx.getChildCount(); i++) {
                ParseTree child = ctx.getChild(i);
                if (child == clpForSkip) {
                    // Visit cursor_loop_param children except its select_statement
                    for (int j = 0; j < clpForSkip.getChildCount(); j++) {
                        ParseTree clpChild = clpForSkip.getChild(j);
                        if (clpChild != clpForSkip.select_statement()) visit(clpChild);
                    }
                } else {
                    visit(child);
                }
            }
            result = null;
        } else {
            result = visitChildren(ctx);
        }

        // Extract joins from inline cursor FOR loop query
        if (ctx.cursor_loop_param() != null && ctx.cursor_loop_param().select_statement() != null) {
            extractJoinsFromSelect(ctx.cursor_loop_param().select_statement());
        }

        // Fallback: scan cursor FOR loop query for pkg.fn(args) patterns
        if (ctx.cursor_loop_param() != null && ctx.cursor_loop_param().select_statement() != null) {
            String queryText = getOriginalText(ctx.cursor_loop_param().select_statement(), 10000);
            extractFunctionCallsFromSqlText(queryText, getStartLine(ctx));
        }

        if (pushedLoopRecord) {
            loopRecordStack.pop();
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
        // Bug 15 fix: distinguish bare RAISE; (re-raise, no exception_name) from RAISE <name>;
        // by capturing the raised exception's name in sqlText when present.
        if (ctx.exception_name() != null) {
            si.setSqlText(cleanId(ctx.exception_name().getText()));
        }
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
        // Bug 13 fix: ROLLBACK TO SAVEPOINT <name> must capture the target savepoint name,
        // same as plain SAVEPOINT already does.
        if (ctx.savepoint_name() != null && ctx.savepoint_name().identifier() != null) {
            si.setSavepointName(cleanId(ctx.savepoint_name().identifier().getText()));
        }
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

        // Option A/Gap2: track string-variable assignments so EXECUTE IMMEDIATE v_sql can be resolved.
        // We only care about simple assignments to a plain variable (no dot / no subscript).
        if (ctx.general_element() != null && ctx.expression() != null) {
            String varName = extractSimpleVarName(ctx.general_element());
            if (varName != null) {
                String rhs = getOriginalText(ctx.expression(), 2000);
                if (rhs != null) {
                    String fragments = extractStringLiterals(rhs);
                    if (!fragments.isEmpty()) {
                        boolean isConcatAssign = isConcatAssignRhs(rhs, varName);
                        // Gap 10: inside an IF/ELSIF/ELSE branch, accumulate rather than replace
                        // (all branches are possible at runtime → union semantics for static analysis)
                        boolean shouldAccumulate = isConcatAssign || branchDepth > 0;
                        if (!varValueStack.isEmpty()) {
                            // Local (subprogram) scope update
                            Map<String, StringBuilder> scope = varValueStack.peek();
                            if (shouldAccumulate) {
                                StringBuilder acc = scope.computeIfAbsent(varName, k -> new StringBuilder());
                                acc.append(fragments);
                            } else {
                                scope.put(varName, new StringBuilder(fragments));
                            }
                        }
                        // Gap 2: if the variable is a known package-level global, also update pkgGlobalVarMap
                        // (so that cross-procedure lookups from a different subprogram scope find the value).
                        // ALWAYS accumulate for the global map — multiple different procedures may each assign
                        // a different value to the same global (e.g. SET_A writes TABLE_A, SET_B writes TABLE_B).
                        // At runtime any of them could have run before the consumer proc, so union semantics.
                        if (pkgGlobalVarNames.contains(varName)) {
                            pkgGlobalVarMap.computeIfAbsent(varName, k -> new StringBuilder()).append(fragments);
                        }
                    }
                }
            }
        }

        return visitChildren(ctx);
    }

    /**
     * If the general_element is a simple identifier (no dots, no subscript), return its
     * upper-cased name; otherwise return null.
     */
    private String extractSimpleVarName(PlSqlParser.General_elementContext ctx) {
        if (ctx == null) return null;
        // Must have no nested general_element (no dots like pkg.var)
        if (ctx.general_element() != null) return null;
        List<PlSqlParser.General_element_partContext> parts = ctx.general_element_part();
        if (parts == null || parts.size() != 1) return null;
        PlSqlParser.General_element_partContext part = parts.get(0);
        // Must have no function arguments (not a call)
        if (part.function_argument() != null && !part.function_argument().isEmpty()) return null;
        if (part.id_expression() == null) return null;
        return cleanId(part.id_expression().getText());
    }

    // ---------------------------------------------------------------
    // IF STATEMENT (Gap 10: branch multi-value accumulation)
    // ---------------------------------------------------------------
    /**
     * Gap 10: When inside an IF statement, treat all branch assignments as ACCUMULATE
     * (not reset), so that EXECUTE IMMEDIATE v_sql after the IF sees all possible values.
     *
     * branchDepth > 0 signals to visitAssignment_statement that it should append rather
     * than reset the variable accumulator.
     */
    @Override
    public Void visitIf_statement(PlSqlParser.If_statementContext ctx) {
        StatementInfo si = new StatementInfo();
        si.setType("IF");
        si.setLine(getStartLine(ctx));
        si.setLineEnd(getStopLine(ctx));
        addStatement(si);
        branchDepth++;
        Void result = visitChildren(ctx);
        branchDepth--;
        return result;
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

        // Bug 12 fix: if this handler is visited while another handler's body is being walked
        // (i.e. this handler's EXCEPTION block is nested inside an enclosing handler's protected
        // block), record that enclosing handler as the parent so the nesting relationship is
        // reconstructible instead of both landing as flat siblings.
        ehi.setParentHandler(currentExceptionHandler);

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

            // Collection/record constructor for a type declared in this same object
            // (e.g. `l_n := NESTED_T();`) — not a call to any external program unit.
            if (hasArgs && partNames.size() == 1
                    && declaredTypeNames.contains(partNames.get(0).toUpperCase())) {
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
                    // Could be an external package variable reference: PKG_NAME.VAR_NAME.
                    // Grammar-first: the qualifier must resolve to a real PACKAGE. A qualifier
                    // that is a declared variable/record, a FOR-loop record, a cursor, or a
                    // FROM/JOIN table alias is NOT a package — DROP it (those produced the
                    // 100%-false-positive record/alias rows in the globals facet). And the pair
                    // must match a DECLARED global (known-package registry or the owning package's
                    // own globals), or carry the package-global naming convention for the
                    // cross-file case.
                    String firstPart = partNames.get(0);
                    String varPart = partNames.get(partNames.size() - 1);

                    // Check if this is inside an assignment_statement or expression context
                    // to avoid false positives from grammar ambiguity
                    if (firstPart != null
                        && !isNonPackageQualifier(firstPart, ctx)
                        && qualifierResolvesToPackage(firstPart, varPart)
                        && isLikelyPackageVarRef(ctx)) {

                        String ref = String.join(".", partNames);
                        addExternalPackageVarRef(ref);
                        addExternalPackageVarRefDetail(firstPart, varPart, getStartLine(ctx),
                            classifyPackageVarAccess(ctx));

                        // Add to dependencies (a package referencing its OWN global is not a
                        // dependency edge — don't create a package self-loop).
                        ParsedObject po = currentObject();
                        if (po != null && (currentPackageName == null
                                || !firstPart.equalsIgnoreCase(currentPackageName))) {
                            po.getDependencies().getPackages().add(firstPart);
                        }
                    }
                }
            } else if (!hasArgs && partNames.size() == 1) {
                // A bare (unqualified) package-global reference from inside its OWNING package:
                // `g_last_context := p_ctx;`. The package qualifier is implicit; record it as
                // <owningPkg>.<name> so it joins the same usage stream as a cross-package ref.
                captureBarePackageGlobalRef(partNames.get(0), ctx);
            }
        }

        return visitChildren(ctx);
    }

    // ---------------------------------------------------------------
    // Helper: classify a call as INTERNAL, EXTERNAL, BUILTIN, or DYNAMIC
    // ---------------------------------------------------------------
    // Bug #1 fix: is `name` a locally-declared nested subprogram in scope at the current
    // call site (i.e. declared in the enclosing body being visited, or one of ITS enclosing
    // bodies)? Checked innermost-to-outermost via the frame stack.
    @Override
    public Void visitType_declaration(PlSqlParser.Type_declarationContext ctx) {
        if (ctx.identifier() != null) {
            declaredTypeNames.add(cleanId(ctx.identifier().getText()).toUpperCase());
        }
        return visitChildren(ctx);
    }

    private boolean isLocallyDeclaredSubprogram(String name) {
        for (Set<String> frame : localSubprogramStack) {
            if (frame.contains(name)) return true;
        }
        return false;
    }

    private CallInfo classifyCall(String fullName, int argCount, int line) {
        CallInfo ci = new CallInfo();
        ci.setLine(line);
        ci.setArguments(argCount);

        String[] parts = fullName.split("\\.");
        if (parts.length == 1) {
            // Simple name - could be internal or external
            String name = cleanId(parts[0]);
            ci.setName(name);

            if (currentPackageSubprograms.contains(name) || isLocallyDeclaredSubprogram(name)) {
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
                List<PlSqlParser.Id_expressionContext> tvnParts2 = tvn.id_expression();
                if (tvnParts2 != null && !tvnParts2.isEmpty()) {
                    schema = tblName;
                    tblName = cleanId(tvnParts2.get(tvnParts2.size() - 1).getText());
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

    /**
     * Walk the AST and collect each table reference with its exact source line number.
     * Unlike extractTableNamesFromContext, this preserves per-reference line positions
     * so that cursor/subquery table promotions record the FROM clause line, not the
     * enclosing cursor declaration start line.
     * Returns list of TableOperationInfo stubs (schema, name, line) with operation left blank.
     *
     * Only descends into FROM-clause and JOIN-clause table references. Skips selected_list,
     * where_clause, having_clause, order_by_clause, hierarchical_query_clause, etc. so that
     * alias names used in SELECT lists (A.*) and WHERE column refs (C.COL) are never mistaken
     * for physical table references.
     */
    private List<TableOperationInfo> extractTableRefsWithLines(ParserRuleContext ctx) {
        List<TableOperationInfo> refs = new ArrayList<>();
        collectFromClauseTablesWithLines(ctx, refs, new HashSet<>());
        return refs;
    }

    /**
     * Collects every top-level {@code WHERE} clause under {@code node} without descending
     * into a WHERE clause once found — visiting it later (via the normal visitor, which
     * recurses into any subquery it contains) will itself discover any WHERE clauses nested
     * further inside subqueries. This lets a caller re-run the standard subquery-descent
     * mechanism (visitSubquery / visitTableview_name) over just the WHERE clauses of a
     * select_statement it otherwise wants to skip (e.g. a cursor's query, whose FROM-clause
     * tables were already collected separately).
     */
    private void collectTopLevelWhereClauses(ParseTree node, List<PlSqlParser.Where_clauseContext> out) {
        if (node == null) return;
        if (node instanceof PlSqlParser.Where_clauseContext) {
            out.add((PlSqlParser.Where_clauseContext) node);
            return;
        }
        for (int i = 0; i < node.getChildCount(); i++) {
            collectTopLevelWhereClauses(node.getChild(i), out);
        }
    }

    /**
     * Restricted AST walker: enters only structural nodes on the path to FROM-clause
     * tableview_name references. Entry points accepted: select_statement,
     * select_only_statement, subquery, subquery_basic_elements, query_block,
     * subquery_operation_part (UNION/INTERSECT/MINUS branches),
     * from_clause, table_ref_list, table_ref, table_ref_aux, table_ref_aux_internal variants,
     * dml_table_expression_clause, join_clause.
     * All other node types are ignored (pruned), preventing false positives from
     * WHERE / SELECT-list / CONNECT BY / HAVING / ORDER BY subtrees.
     */
    private void collectFromClauseTablesWithLines(ParseTree node,
            List<TableOperationInfo> refs, Set<String> seen) {
        if (node == null) return;

        // ── Leaf: actual table reference ──────────────────────────────────────────
        if (node instanceof PlSqlParser.Dml_table_expression_clauseContext) {
            PlSqlParser.Dml_table_expression_clauseContext dtec =
                    (PlSqlParser.Dml_table_expression_clauseContext) node;
            if (dtec.tableview_name() != null) {
                // Normal table/view reference (incl. table-valued function) — collect it
                recordTableviewName(dtec.tableview_name(), refs, seen);
            } else if (dtec.select_statement() != null) {
                // Inline view: ( SELECT ... ) — recurse into its own FROM clauses
                collectFromClauseTablesWithLines(dtec.select_statement(), refs, seen);
            } else if (dtec.subquery() != null) {
                // LATERAL ( subquery ) — recurse into its own FROM clauses
                collectFromClauseTablesWithLines(dtec.subquery(), refs, seen);
            } else if (dtec.table_collection_expression() != null) {
                // TABLE(fn(...)) — extract the function call so it appears as a dependency
                extractCallsFromTableCollectionExpression(dtec.table_collection_expression());
            }
            return;
        }

        // ── Pruning: skip non-FROM subtrees entirely ───────────────────────────
        if (node instanceof PlSqlParser.Selected_listContext
                || node instanceof PlSqlParser.Where_clauseContext
                || node instanceof PlSqlParser.Having_clauseContext
                || node instanceof PlSqlParser.Order_by_clauseContext
                || node instanceof PlSqlParser.Group_by_clauseContext
                || node instanceof PlSqlParser.Hierarchical_query_clauseContext
                || node instanceof PlSqlParser.Start_partContext) {
            return; // do not descend
        }

        // ── Transparent: descend into structural nodes ──────────────────────────
        for (int i = 0; i < node.getChildCount(); i++) {
            collectFromClauseTablesWithLines(node.getChild(i), refs, seen);
        }
    }

    private void recordTableviewName(PlSqlParser.Tableview_nameContext tvn,
            List<TableOperationInfo> refs, Set<String> seen) {
        if (tvn == null || tvn.identifier() == null) return;
        String schema = null;
        String tblName = cleanId(tvn.identifier().getText());
        List<PlSqlParser.Id_expressionContext> parts = tvn.id_expression();
        if (parts != null && !parts.isEmpty()) {
            schema = tblName;
            tblName = cleanId(parts.get(parts.size() - 1).getText());
        }
        String fullName = schema != null ? schema + "." + tblName : tblName;
        int line = getStartLine(tvn);
        String dedup = fullName + "@" + line;
        if (tblName != null && !isDualTable(tblName) && !seen.contains(dedup)
                && (schema != null || !queryAliases.contains(tblName))) {
            seen.add(dedup);
            TableOperationInfo toi = new TableOperationInfo();
            toi.setTableName(tblName);
            toi.setSchema(schema);
            toi.setLine(line);
            // Carry the FROM-clause alias through, matching what visitTableview_name already
            // records for non-cursor statements. Previously every table op derived from a cursor
            // declaration (which goes through this pre-scan instead of the normal visitor) had
            // alias == null, so the same table looked different depending on where it was
            // declared. Purely additive: the field was unconditionally null on this path before.
            toi.setAlias(findTableAlias(tvn));
            refs.add(toi);
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
    // Helper: check if a name refers to an Oracle system / data-dictionary view.
    // These are never real application tables and should be skipped (FP-PL2).
    // ---------------------------------------------------------------
    private static final java.util.regex.Pattern SYSTEM_TABLE_PATTERN =
            java.util.regex.Pattern.compile(
                "(?i)^(?:"
                + "ALL_[A-Z0-9_]+"          // ALL_ data-dictionary views
                + "|DBA_[A-Z0-9_]+"         // DBA_ data-dictionary views
                + "|USER_[A-Z0-9_]+"        // USER_ data-dictionary views
                + "|V\\$[A-Z0-9_]+"         // V$ dynamic performance views
                + "|GV\\$[A-Z0-9_]+"        // GV$ global dynamic views
                + "|SYS\\.[A-Z0-9_$]+"      // SYS. qualified objects
                + "|SYSTEM\\.[A-Z0-9_$]+"   // SYSTEM. schema objects
                + ")$");

    private boolean isSystemTable(String name) {
        if (name == null || name.isEmpty()) return false;
        return SYSTEM_TABLE_PATTERN.matcher(name).matches();
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
        return BUILTIN_FUNCTIONS.contains(name);
    }

    // ---------------------------------------------------------------
    // Authoritative, comprehensive set of Oracle SQL / PL-SQL engine
    // built-in scalar, aggregate, analytic, conversion, XML and
    // miscellaneous function names (upper-cased). Any UNQUALIFIED call
    // whose name matches here is classified BUILTIN and never emitted as a
    // user-routine call edge. Deliberately excludes generic words that could
    // plausibly be user-routine names (e.g. VALUE, LEVEL, DATE, SET, KEY).
    // NOTE: a HashSet is used (not Set.of) so accidental duplicate literals
    // are collapsed silently rather than throwing; still kept deduped below.
    // ---------------------------------------------------------------
    private static final Set<String> BUILTIN_FUNCTIONS = new HashSet<>(Arrays.asList(
        // --- NULL-handling / conditional ---
        "NVL", "NVL2", "NULLIF", "COALESCE", "DECODE", "NANVL", "LNNVL",
        "GREATEST", "LEAST",
        // --- Conversion ---
        "TO_CHAR", "TO_DATE", "TO_NUMBER", "TO_TIMESTAMP", "TO_TIMESTAMP_TZ",
        "TO_CLOB", "TO_NCLOB", "TO_BLOB", "TO_LOB", "TO_BINARY_DOUBLE",
        "TO_BINARY_FLOAT", "TO_DSINTERVAL", "TO_YMINTERVAL", "TO_MULTI_BYTE",
        "TO_SINGLE_BYTE", "CAST", "CONVERT", "TREAT", "VALIDATE_CONVERSION",
        "NUMTODSINTERVAL", "NUMTOYMINTERVAL", "CHARTOROWID", "ROWIDTOCHAR",
        "ROWIDTONCHAR", "HEXTORAW", "RAWTOHEX", "RAWTONHEX", "UNISTR",
        "SCN_TO_TIMESTAMP", "TIMESTAMP_TO_SCN", "BIN_TO_NUM", "COMPOSE",
        "DECOMPOSE", "ASCIISTR",
        // --- Character / string ---
        "SUBSTR", "SUBSTRB", "SUBSTRC", "SUBSTR2", "SUBSTR4", "INSTR",
        "INSTRB", "INSTRC", "INSTR2", "INSTR4", "LENGTH", "LENGTHB",
        "LENGTHC", "LENGTH2", "LENGTH4", "LPAD", "RPAD", "LTRIM", "RTRIM",
        "TRIM", "REPLACE", "TRANSLATE", "UPPER", "LOWER", "INITCAP",
        "NLS_UPPER", "NLS_LOWER", "NLS_INITCAP", "NLSSORT", "CONCAT",
        "REGEXP_REPLACE", "REGEXP_SUBSTR", "REGEXP_INSTR", "REGEXP_COUNT",
        "REGEXP_LIKE", "ASCII", "ASCIISTR", "CHR", "NCHR", "SOUNDEX",
        "VSIZE", "DUMP",
        // --- Numeric / math ---
        "ABS", "CEIL", "FLOOR", "ROUND", "TRUNC", "MOD", "REMAINDER",
        "POWER", "SQRT", "EXP", "LN", "LOG", "SIGN", "SIN", "COS", "TAN",
        "ASIN", "ACOS", "ATAN", "ATAN2", "SINH", "COSH", "TANH", "WIDTH_BUCKET",
        "BITAND", "ORA_HASH", "STANDARD_HASH",
        // --- Date / time ---
        "SYSDATE", "SYSTIMESTAMP", "CURRENT_DATE", "CURRENT_TIMESTAMP",
        "LOCALTIMESTAMP", "DBTIMEZONE", "SESSIONTIMEZONE", "ADD_MONTHS",
        "MONTHS_BETWEEN", "LAST_DAY", "NEXT_DAY", "EXTRACT", "NEW_TIME",
        "ROUND_TIES_TO_EVEN", "FROM_TZ", "TZ_OFFSET", "SYS_EXTRACT_UTC",
        "ORA_DST_AFFECTED", "ORA_DST_CONVERT", "ORA_DST_ERROR",
        // --- Aggregate ---
        "COUNT", "SUM", "AVG", "MIN", "MAX", "STDDEV", "STDDEV_POP",
        "STDDEV_SAMP", "VARIANCE", "VAR_POP", "VAR_SAMP", "MEDIAN",
        "LISTAGG", "CORR", "COVAR_POP", "COVAR_SAMP", "CUME_DIST",
        "PERCENT_RANK", "PERCENTILE_CONT", "PERCENTILE_DISC", "GROUPING",
        "GROUPING_ID", "GROUP_ID", "REGR_SLOPE", "REGR_INTERCEPT",
        "REGR_COUNT", "REGR_R2", "REGR_AVGX", "REGR_AVGY", "REGR_SXX",
        "REGR_SYY", "REGR_SXY", "COLLECT", "APPROX_COUNT_DISTINCT",
        // --- Analytic / window ---
        "ROW_NUMBER", "RANK", "DENSE_RANK", "LAG", "LEAD", "NTILE",
        "FIRST_VALUE", "LAST_VALUE", "NTH_VALUE", "RATIO_TO_REPORT",
        "FIRST", "LAST", "LISTAGG",
        // --- Environment / identity ---
        "USER", "UID", "SYS_GUID", "SYS_CONTEXT", "USERENV", "SYS_TYPEID",
        "SYS_CONNECT_BY_PATH", "SYS_XMLGEN", "SYS_XMLAGG", "ORA_HASH",
        "USERENV", "SYSCONTEXT",
        // --- XML ---
        "XMLELEMENT", "XMLAGG", "XMLATTRIBUTES", "XMLCONCAT", "XMLFOREST",
        "XMLPARSE", "XMLPI", "XMLCOMMENT", "XMLSERIALIZE", "XMLQUERY",
        "XMLTABLE", "XMLCAST", "XMLROOT", "XMLTYPE", "EXTRACTVALUE",
        "EXISTSNODE", "UPDATEXML", "XMLISVALID", "XMLSEQUENCE",
        // --- JSON ---
        "JSON_VALUE", "JSON_QUERY", "JSON_OBJECT", "JSON_ARRAY",
        "JSON_OBJECTAGG", "JSON_ARRAYAGG", "JSON_TABLE", "JSON_EXISTS",
        "JSON_MERGEPATCH", "JSON_SERIALIZE", "JSON_SCALAR",
        // --- LOB / raw / hashing ---
        "RAWTOHEX", "HEXTORAW", "EMPTY_CLOB", "EMPTY_BLOB",
        // --- PL/SQL supplied / misc ---
        "RAISE_APPLICATION_ERROR", "SQLCODE", "SQLERRM", "DEREF",
        "MULTISET", "POWERMULTISET", "CARDINALITY", "TABLE"
    ));

    // ---------------------------------------------------------------
    // Dynamic SQL: extract table names from string literals
    // ---------------------------------------------------------------
    // Bug (dynamic GTT audit): "CREATE\s+TABLE" required CREATE and TABLE to be adjacent, so
    // `EXECUTE IMMEDIATE 'CREATE GLOBAL TEMPORARY TABLE gtt_dyn (...)'` — where GLOBAL TEMPORARY
    // sits between the two keywords — never matched this alternative at all, silently skipping
    // the whole dynamic-DDL-extraction path for GTTs (unlike a plain dynamic CREATE TABLE, which
    // already worked). `(?:GLOBAL\s+TEMPORARY\s+)?` tolerates the optional GTT qualifier.
    private static final Pattern DYN_SQL_TABLE_PATTERN = Pattern.compile(
            "(?i)(?:FROM|INTO|UPDATE|MERGE\\s+INTO|TRUNCATE\\s+TABLE|INSERT\\s+INTO|DELETE\\s+FROM|JOIN"
                    + "|CREATE\\s+(?:GLOBAL\\s+TEMPORARY\\s+)?TABLE|CREATE\\s+OR\\s+REPLACE\\s+VIEW|CREATE\\s+VIEW|ALTER\\s+TABLE"
                    + "|DROP\\s+TABLE|CREATE\\s+INDEX|DROP\\s+INDEX)\\s+"
                    + "([A-Z_][A-Z0-9_$#]*(?:\\.[A-Z_][A-Z0-9_$#]*)?)");

    /**
     * ING-019 (§122) — the DDL verbs among {@link #DYN_SQL_TABLE_PATTERN}'s alternation. Oracle
     * PL/SQL cannot express DDL directly in a procedure body (only via {@code EXECUTE IMMEDIATE}),
     * so this is the only place a dynamic-SQL table reference needs to be told apart as DDL rather
     * than the generic {@code DYNAMIC} bucket every other dynamic reference falls into.
     *
     * <p>{@code CREATE INDEX}/{@code DROP INDEX} are matched by the pattern above but excluded here
     * deliberately: {@link #DYN_SQL_TABLE_PATTERN}'s capture group for those two is the *index*
     * name, not a table, and {@code SqlAnalyzer.extractDdl}'s own {@code CreateIndex} branch reads
     * the table off the statement's own {@code ON <table>} clause, not off this match — routing an
     * index name through as a table operation would mislabel it. Left as {@code DYNAMIC} (matching
     * their pre-existing behaviour) rather than mis-attributed as DDL against the wrong object.
     */
    private static final Pattern DYN_SQL_DDL_VERB = Pattern.compile(
            "(?i)^(?:CREATE\\s+(?:GLOBAL\\s+TEMPORARY\\s+)?TABLE|CREATE\\s+OR\\s+REPLACE\\s+VIEW|CREATE\\s+VIEW|ALTER\\s+TABLE"
                    + "|DROP\\s+TABLE|TRUNCATE\\s+TABLE)$");

    // ---------------------------------------------------------------
    // Grammar-first recursive dynamic-SQL parse.
    //
    // The literal handed to EXECUTE IMMEDIATE / DBMS_SQL.PARSE is real SQL, so it is re-parsed
    // through this same Oracle grammar (sql_script) and its table targets are read off the nested
    // parse tree by the very visitor the static path uses. The legacy DYN_SQL_TABLE_PATTERN regex
    // survives only as a recovery path for literals the grammar cannot attribute a table to
    // (concatenation fragments, `DROP TABLE` — which has no visitor rule yet — wrapped source …),
    // and every use of it is logged once and counted.
    // ---------------------------------------------------------------

    /** Hard cap on nested dynamic-SQL literals (a literal containing dynamic SQL containing …). */
    private static final int DYN_PARSE_MAX_DEPTH = 3;

    /** Literals larger than this are never handed to the grammar — an absurd payload, regex only. */
    private static final int DYN_PARSE_MAX_CHARS = 64 * 1024;

    /** Per-thread recursion depth, so a nested dynamic-SQL literal can never loop forever. */
    private static final ThreadLocal<Integer> DYN_PARSE_DEPTH = ThreadLocal.withInitial(() -> 0);

    /**
     * Number of dynamic-SQL literals the recursive grammar parse could not attribute a table to,
     * i.e. the number of times the legacy regex ran. Exposed so the regex-retirement rate of the
     * regex → grammar migration is observable (should fall to ~0 on real corpora).
     */
    public static final java.util.concurrent.atomic.AtomicLong DYN_SQL_REGEX_FALLBACKS =
            new java.util.concurrent.atomic.AtomicLong();

    private static final java.util.concurrent.atomic.AtomicBoolean DYN_FALLBACK_LOGGED =
            new java.util.concurrent.atomic.AtomicBoolean();

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

        // Bug 1 (§DML-audit): a variable/expression sat right where a table name would begin
        // (e.g. `'...FROM ' || v_prefix || '_orders'`) — Strategies 1/2 above never bridge that
        // gap into a fabricated name (see DYN_SQL_VAR_BREAK), so record it explicitly as
        // unresolvable dynamic SQL rather than silently dropping it.
        if (DYN_SQL_UNRESOLVED_PATTERN.matcher(stripped).find() && !seen.contains(DYNAMIC_UNRESOLVABLE_TABLE)) {
            seen.add(DYNAMIC_UNRESOLVABLE_TABLE);
            TableOperationInfo toi = new TableOperationInfo();
            toi.setTableName(DYNAMIC_UNRESOLVABLE_TABLE);
            toi.setLine(line);
            toi.setOperation("DYNAMIC");
            addTableOperation(toi);
        }
    }

    private void runDynSqlPattern(String text, int line, Set<String> seen) {
        if (text == null || text.isEmpty()) return;

        // Grammar first: recursively parse the literal through this same Oracle grammar and read
        // the table targets off the nested parse tree, exactly as the static path does.
        List<TableOperationInfo> grammarOps = nestedParseDynamicSqlTables(text);
        if (!grammarOps.isEmpty()) {
            emitGrammarTableOps(grammarOps, text, line, seen);
            return;
        }

        // Recovery path: the literal is not a statement this grammar can attribute a table to
        // (a concatenation fragment, an Oracle `DROP TABLE` the visitor has no rule for, wrapped
        // source …). Logged once and counted so the regex retirement rate is measurable.
        long fallbackNo = DYN_SQL_REGEX_FALLBACKS.incrementAndGet();
        if (DYN_FALLBACK_LOGGED.compareAndSet(false, true)) {
            System.err.println("[PlSqlAnalysisVisitor] dynamic-SQL recursive parse produced no table "
                    + "target; using legacy regex fallback (fallback #" + fallbackNo + ")");
        }

        Matcher m = DYN_SQL_TABLE_PATTERN.matcher(text);
        while (m.find()) {
            String tablRef = m.group(1).toUpperCase();
            if (isDualTable(tablRef) || isBuiltinFunction(tablRef)) continue;
            if (isSystemTable(tablRef)) continue; // suppress ALL_, DBA_, USER_, V$, GV$, SYS. tables
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
            toi.setLine(line);

            // ING-019 (§122) — checked against the verb *at this match's own position*, not the
            // whole fragment: text can hold more than one dynamic-SQL statement concatenated
            // together (Strategy 1 above joins every string literal in the expression), so a
            // whole-string anchor would misclassify every match after the first. `sqlText` is set
            // only for the DDL case: it is what lets `TableOperationEnricher.enrich()`'s existing
            // `SqlAnalyzer.analyze()` call (already wired for every table operation with non-blank
            // `sqlText` — no new plumbing needed) reach `extractDdl()` and populate `getDdl()` with
            // the real action/objectType/line — DYNAMIC operations deliberately carry no `sqlText`
            // today and this leaves that alone.
            String verb = m.group(0).substring(0, m.group(0).length() - m.group(1).length()).trim();
            if (DYN_SQL_DDL_VERB.matcher(verb).matches()) {
                toi.setOperation("DDL");
                // Oracle's dynamic-SQL string may not end at the match (Strategy 1 above joins
                // every literal in the expression); a trailing `;` never belongs in an
                // EXECUTE IMMEDIATE string (Oracle rejects it), so it is the one safe, unambiguous
                // boundary for "this statement ends here" without guessing at SQL structure.
                String stmtText = text.substring(m.start());
                int semi = stmtText.indexOf(';');
                if (semi >= 0) stmtText = stmtText.substring(0, semi);
                toi.setSqlText(stmtText.trim());
            } else {
                toi.setOperation("DYNAMIC");
            }
            addTableOperation(toi);
        }
    }

    /**
     * Recursively parse a dynamic-SQL literal through this same Oracle grammar and return every
     * table operation the nested analysis recorded. Returns an empty list when the literal is not
     * a statement this grammar can attribute a table to, or when a guard (depth/size) fires —
     * callers then run the legacy regex as a recovery path.
     *
     * <p>Schema-level DDL ({@code CREATE}/{@code ALTER}/{@code DROP …}) parses as a top-level
     * {@code sql_script} unit and is captured by the object visitors. DML and PL/SQL blocks are
     * only recorded inside a program unit, so they are wrapped in a synthetic procedure body —
     * the same technique {@link com.plsql.parser.flow.TriggerDiscoverer} uses for a trigger body —
     * which leaves the statements inside it untouched.
     */
    private List<TableOperationInfo> nestedParseDynamicSqlTables(String sqlText) {
        if (sqlText == null || sqlText.isBlank()) return List.of();
        if (sqlText.length() > DYN_PARSE_MAX_CHARS) return List.of();
        int depth = DYN_PARSE_DEPTH.get();
        if (depth >= DYN_PARSE_MAX_DEPTH) return List.of();
        DYN_PARSE_DEPTH.set(depth + 1);
        try {
            String trimmed = sqlText.stripLeading();
            boolean schemaDdl = startsWithKeyword(trimmed, "CREATE")
                    || startsWithKeyword(trimmed, "ALTER")
                    || startsWithKeyword(trimmed, "DROP");
            List<TableOperationInfo> ops = schemaDdl
                    ? parseDynamicSourceForTables(sqlText)
                    : List.of();
            if (ops.isEmpty()) {
                // A dynamic-SQL literal never carries its own terminator (Oracle rejects a
                // trailing `;`), but a statement inside a block needs one — normalize to exactly
                // one so a PL/SQL block literal (`BEGIN … END;`) is not doubled up either.
                String stmt = sqlText.stripTrailing();
                if (stmt.endsWith(";")) stmt = stmt.substring(0, stmt.length() - 1);
                ops = parseDynamicSourceForTables(
                        "CREATE OR REPLACE PROCEDURE JAR$DYN_SQL_BODY IS\nBEGIN\n"
                                + stmt + ";\nEND;\n");
            }
            // A schema-DDL literal that did not yield a table from the wrapped form may still be
            // capturable at top level (and vice-versa) — try the other shape once.
            if (ops.isEmpty() && !schemaDdl) {
                ops = parseDynamicSourceForTables(sqlText);
            }
            return ops;
        } catch (Throwable t) {
            return List.of();
        } finally {
            DYN_PARSE_DEPTH.set(depth);
        }
    }

    private static boolean startsWithKeyword(String text, String keyword) {
        return text.length() >= keyword.length()
                && text.regionMatches(true, 0, keyword, 0, keyword.length())
                && (text.length() == keyword.length()
                        || !Character.isLetterOrDigit(text.charAt(keyword.length())));
    }

    /**
     * Parse one source string with the Oracle grammar and harvest every table operation the
     * visitor recorded. Any lexer/parser error makes the result untrusted (empty), so a partial
     * error-recovered tree can never fabricate table targets.
     */
    private List<TableOperationInfo> parseDynamicSourceForTables(String source) {
        List<String> errors = new ArrayList<>();
        try {
            PlSqlLexer lexer = new PlSqlLexer(CharStreams.fromString(source));
            lexer.removeErrorListeners();
            lexer.addErrorListener(new DynParseErrorListener(errors));
            PlSqlParser parser = new PlSqlParser(new CommonTokenStream(lexer));
            parser.removeErrorListeners();
            parser.addErrorListener(new DynParseErrorListener(errors));
            ParseTree tree = parser.sql_script();
            if (!errors.isEmpty()) return List.of();
            PlSqlAnalysisVisitor sub = new PlSqlAnalysisVisitor();
            sub.visit(tree);
            List<TableOperationInfo> ops = new ArrayList<>();
            for (ParsedObject po : sub.getResults()) {
                collectTableOpsForDynamic(po, ops);
            }
            return ops;
        } catch (Throwable t) {
            return List.of();
        }
    }

    private static void collectTableOpsForDynamic(ParsedObject po, List<TableOperationInfo> out) {
        if (po == null) return;
        if (po.getTableOperations() != null) out.addAll(po.getTableOperations());
        if (po.getSubprograms() != null) {
            for (SubprogramInfo sub : po.getSubprograms()) {
                if (sub.getTableOperations() != null) out.addAll(sub.getTableOperations());
            }
        }
    }

    /**
     * Map the nested visitor's table operations onto the dynamic-SQL output shape: the same
     * {@link TableOperationInfo} fields, with DML/SELECT references landing in the generic
     * {@code DYNAMIC} bucket and schema changes in {@code DDL} (carrying {@code sqlText} so the
     * existing enrichment populates {@code getDdl()}) — byte-identical to what the regex produced,
     * but with the table targets read off the parse tree.
     */
    private void emitGrammarTableOps(List<TableOperationInfo> ops, String text, int line,
                                     Set<String> seen) {
        for (TableOperationInfo src : ops) {
            String tableName = src.getTableName();
            if (tableName == null || tableName.isEmpty()) continue;
            // "REFERENCE" rows are foreign-key lineage edges, not targets of the dynamic statement.
            if ("REFERENCE".equalsIgnoreCase(src.getOperation())) continue;
            String schema = src.getSchema();
            String fullRef = (schema != null ? schema + "." + tableName : tableName).toUpperCase();
            if (isDualTable(fullRef) || isBuiltinFunction(fullRef)) continue;
            if (isSystemTable(fullRef)) continue; // suppress ALL_, DBA_, USER_, V$, GV$, SYS. tables
            if (!seen.add(fullRef)) continue;

            String op = src.getOperation() == null ? "" : src.getOperation().toUpperCase();
            boolean dml = op.equals("SELECT") || op.equals("INSERT") || op.equals("UPDATE")
                    || op.equals("DELETE") || op.equals("MERGE");
            TableOperationInfo toi = new TableOperationInfo();
            toi.setTableName(tableName);
            toi.setSchema(schema);
            toi.setLine(line);
            if (dml) {
                toi.setOperation("DYNAMIC");
            } else {
                toi.setOperation("DDL");
                String stmt = src.getSqlText();
                toi.setSqlText(stmt != null && !stmt.isBlank() ? stmt.trim() : text.trim());
            }
            addTableOperation(toi);
        }
    }

    /** Collects ANTLR syntax errors so a nested dynamic-SQL parse can be trusted only when clean. */
    private static final class DynParseErrorListener extends BaseErrorListener {
        private final List<String> errors;

        DynParseErrorListener(List<String> errors) {
            this.errors = errors;
        }

        @Override
        public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol,
                                int line, int charPositionInLine, String msg,
                                RecognitionException e) {
            errors.add(msg);
        }
    }

    /**
     * If exprText looks like a qualified constant reference (PKG.CONST or just CONST with no parens
     * and a single dot), return the constant name (last part) upper-cased. Returns null otherwise.
     */
    private String extractQualifiedConstName(String exprText) {
        if (exprText == null) return null;
        String t = exprText.trim().toUpperCase();
        // Must match: IDENTIFIER.IDENTIFIER (no parens — otherwise it's a method call)
        if (!t.matches("[A-Z_][A-Z0-9_$#]*\\.[A-Z_][A-Z0-9_$#]*")) return null;
        return t; // Return full qualified name for lookup
    }

    /**
     * Resolve a qualified constant PKG.CONST or just CONST from globalConstRegistry or pkgConstantMap.
     * Returns true if at least one table operation was added.
     */
    private boolean resolveQualifiedConst(String qualifiedName, int line) {
        if (qualifiedName == null) return false;
        int dot = qualifiedName.indexOf('.');
        if (dot > 0) {
            String pkgName = qualifiedName.substring(0, dot);
            String constName = qualifiedName.substring(dot + 1);
            // Look up in globalConstRegistry (cross-package constants from spec)
            Map<String, String> registry = globalConstRegistry.get(pkgName);
            if (registry != null) {
                String val = registry.get(constName);
                if (val != null && !val.isEmpty()) {
                    return runDynSqlPatternTracked(val, line);
                }
            }
            // Fall back to current pkgConstantMap if same package
            String val = pkgConstantMap.get(constName);
            if (val != null && !val.isEmpty()) {
                return runDynSqlPatternTracked(val, line);
            }
        }
        return false;
    }

    /**
     * Run DYN_SQL_TABLE_PATTERN directly on already-stripped SQL text (no single-quote delimiters).
     * Used when the accumulated variable value has already had its string literals extracted.
     * Returns true if at least one table operation was added.
     */
    private boolean runDynSqlPatternTracked(String rawSql, int line) {
        if (rawSql == null || rawSql.isEmpty()) return false;
        int before = countCurrentTableOps();
        runDynSqlPattern(rawSql, line, new HashSet<>());
        return countCurrentTableOps() > before;
    }

    /**
     * Detect whether the RHS of an assignment is a concat-assign:
     *   v_sql := v_sql || '...'  (the variable being assigned is referenced in the RHS)
     * vs a fresh assignment:
     *   v_sql := '...'
     * Heuristic: the RHS (original source text) starts with the variable name followed by optional
     * whitespace and "||". Case-insensitive because identifiers in PL/SQL are case-insensitive.
     */
    private boolean isConcatAssignRhs(String rhs, String varName) {
        if (rhs == null || varName == null) return false;
        String t = rhs.trim().toUpperCase();
        String v = varName.toUpperCase();
        // Check: RHS starts with VAR_NAME followed by whitespace/|| (concat-assign)
        if (!t.startsWith(v)) return false;
        String rest = t.substring(v.length()).trim();
        return rest.startsWith("||");
    }

    /**
     * Sentinel inserted between two literal fragments in {@link #extractStringLiterals} whose gap
     * (Bug 1, §DML-audit) held a non-literal expression (a variable/function-call operand of
     * {@code ||}) rather than just the concatenation operator itself. It deliberately breaks
     * {@link #DYN_SQL_TABLE_PATTERN}'s {@code \s+([A-Z_]...)}: no identifier character follows it,
     * so a keyword sitting at the end of one fragment (e.g. {@code ...FROM }) can never bridge into
     * the next fragment's text (e.g. {@code _orders}) to fabricate a table name that was actually
     * built at runtime from a variable. Chosen as a control character so it can never appear in
     * real PL/SQL source text and never collides with a legitimate identifier character.
     */
    private static final String DYN_SQL_VAR_BREAK = "\u0001";

    /**
     * Bug 1 (§DML-audit) — matches {@code <keyword> <break>} so a dynamic-SQL table reference that
     * was interrupted by a variable right where the table name would begin can be recorded as
     * unresolvable instead of silently dropped.
     */
    private static final Pattern DYN_SQL_UNRESOLVED_PATTERN = Pattern.compile(
            "(?i)(?:FROM|INTO|UPDATE|MERGE\\s+INTO|TRUNCATE\\s+TABLE|INSERT\\s+INTO|DELETE\\s+FROM|JOIN)\\s+"
                    + DYN_SQL_VAR_BREAK);

    /** Sentinel table name used when dynamic SQL builds a table name from a variable/expression and
     * cannot be resolved statically — deliberately distinct from any real table name so callers can
     * tell "unresolvable" apart from a genuine (if oddly-named) table. */
    public static final String DYNAMIC_UNRESOLVABLE_TABLE = "UNRESOLVABLE_DYNAMIC";

    private String extractStringLiterals(String expr) {
        StringBuilder sb = new StringBuilder();
        int i = 0;
        int prevEnd = -1; // index just after the closing quote of the previous fragment
        while (i < expr.length()) {
            if (expr.charAt(i) == '\'') {
                int end = expr.indexOf('\'', i + 1);
                while (end != -1 && end + 1 < expr.length() && expr.charAt(end + 1) == '\'') {
                    end = expr.indexOf('\'', end + 2);
                }
                if (end != -1) {
                    String frag = expr.substring(i + 1, end);
                    if (prevEnd >= 0) {
                        String gap = expr.substring(prevEnd, i).replace("||", " ").trim();
                        sb.append(gap.isEmpty() ? " " : DYN_SQL_VAR_BREAK);
                    }
                    sb.append(frag);
                    prevEnd = end + 1;
                    i = end + 1;
                } else {
                    break;
                }
            } else {
                i++;
            }
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
     * Line tolerance used when deciding whether an existing CallInfo (recorded by the whole-body/
     * cursor-text catch-all scan) and a candidate call found by the TABLE(fn(...)) collection
     * expression scan (allowRepeats=true) refer to the SAME physical call site rather than two
     * genuinely distinct occurrences. Both {@code baseLine} values are approximate (start of the
     * enclosing statement/expression, not the exact token), so two scans of the same call a line
     * or two apart must still be recognised as one occurrence and merged, not double-counted.
     */
    private static final int TABLE_CALL_REPEAT_LINE_TOLERANCE = 2;

    /**
     * Extract function calls from a TABLE(expr) or TABLE(subquery) expression.
     * Used when TABLE(fn(...)) appears in a FROM or JOIN clause so the pipelined
     * function call is recorded as a dependency even though it has no tableview_name.
     */
    private void extractCallsFromTableCollectionExpression(
            PlSqlParser.Table_collection_expressionContext tce) {
        if (tce == null) return;
        // TABLE(expr) — the expression may be a package.function(args) call
        // Use the text-regex fallback to pick it up
        if (tce.expression() != null) {
            String exprText = getOriginalText(tce.expression(), 2000);
            // allowRepeats=true: each TABLE(fn(...)) collection expression is a distinct,
            // independent occurrence of the call (e.g. the same package function invoked
            // via TABLE(...) in several different cursor FOR loops within one subprogram),
            // so it must not be suppressed just because the same function name was already
            // recorded elsewhere in this subprogram.
            extractFunctionCallsFromSqlText(exprText, getStartLine(tce), true);
            // BUGFIX: SQL_EMBEDDED_CALL_PATTERN only matches dotted/qualified names
            // (pkg.fn(...)), so an unqualified TABLE(fn(...)) call (no package prefix)
            // falls through both that regex and the normal tree walk (which never fires
            // inside table_collection_expression), silently dropping the call entirely.
            // Explicitly check for the unqualified case here.
            extractUnqualifiedCallFromTableCollectionExpr(tce.expression(), getStartLine(tce));
        } else if (tce.subquery() != null) {
            String subText = getOriginalText(tce.subquery(), 2000);
            extractFunctionCallsFromSqlText(subText, getStartLine(tce), true);
        }
    }

    /**
     * Detect and record an unqualified function call name (e.g. SOME_PIPELINED_FUNC(p1, p2))
     * used as the argument of TABLE(...). SQL_EMBEDDED_CALL_PATTERN requires at least one dot
     * (schema/package-qualified), so a bare function name is otherwise never picked up from
     * inside a table_collection_expression. Mirrors the dedup/line-tolerance behavior of
     * extractFunctionCallsFromSqlText(..., allowRepeats=true).
     *
     * <p>Grammar-based (no regex): the call is read off the parse tree — the leading
     * {@code general_element} of the TABLE(...) expression, whose last
     * {@code general_element_part} carries the {@code function_argument} and whose earlier
     * parts (if any) are the package/schema qualifier. The argument count comes from the
     * grammar's own {@code argument} list rather than from counting commas in text, and the
     * dotted name is immune to whitespace around the dots.
     */
    private void extractUnqualifiedCallFromTableCollectionExpr(
            PlSqlParser.ExpressionContext expr, int baseLine) {
        if (expr == null) return;
        PlSqlParser.General_elementContext ge = leadingGeneralElement(expr);
        if (ge == null) return;
        List<PlSqlParser.General_element_partContext> parts = new ArrayList<>();
        flattenGeneralElement(ge, parts);
        if (parts.isEmpty()) return;
        // The call itself is the LAST dotted part (the one carrying the argument list); every
        // earlier part is its package/schema qualifier. Handling the qualified shape here too —
        // not just the bare one — because SQL_EMBEDDED_CALL_PATTERN requires the dots to be
        // written WITHOUT surrounding whitespace, so `TABLE(PKG . MY_FN(x))` matched nothing.
        // On the cursor path the SELECT body is deliberately not re-visited, so the tree walk
        // never recovered it either and the dependency was dropped purely because of spacing.
        PlSqlParser.General_element_partContext part = parts.get(parts.size() - 1);
        if (part.id_expression() == null || part.function_argument().isEmpty()) return;

        StringBuilder dotted = new StringBuilder();
        for (int i = 0; i + 1 < parts.size(); i++) {
            PlSqlParser.General_element_partContext q = parts.get(i);
            if (q.id_expression() == null) return; // not a plain dotted name — leave it alone
            dotted.append(cleanId(q.id_expression().getText()).toUpperCase()).append('.');
        }
        String name = cleanId(part.id_expression().getText()).toUpperCase();
        String fullName = dotted + name;
        if (SQL_NON_CALL_IDENTIFIERS.contains(fullName) || SQL_NON_CALL_IDENTIFIERS.contains(name)) return;
        if (fullName.endsWith(".NEXTVAL") || fullName.endsWith(".CURRVAL")) return;

        SubprogramInfo sub = currentSubprogram();
        List<CallInfo> existingCalls;
        if (sub != null) {
            existingCalls = sub.getCalls();
        } else {
            ParsedObject po = currentObject();
            existingCalls = po != null ? po.getCalls() : Collections.emptyList();
        }
        for (CallInfo ci : existingCalls) {
            if (Math.abs(ci.getLine() - baseLine) > TABLE_CALL_REPEAT_LINE_TOLERANCE) continue;
            StringBuilder key = new StringBuilder();
            if (ci.getSchema() != null) key.append(ci.getSchema()).append('.');
            if (ci.getPackageName() != null) key.append(ci.getPackageName()).append('.');
            key.append(ci.getName());
            if (fullName.equalsIgnoreCase(key.toString())) {
                return; // already recorded at this call site
            }
        }

        int argCount = part.function_argument(0).argument().size();
        CallInfo ci = classifyCall(fullName, argCount, baseLine);
        addCall(ci);
    }

    /**
     * The {@code general_element} that begins at the very first token of {@code expr}, if any.
     * This is the grammar equivalent of anchoring a scan to the start of the expression text.
     */
    private PlSqlParser.General_elementContext leadingGeneralElement(ParserRuleContext expr) {
        ParseTree node = expr;
        while (node != null) {
            if (node instanceof PlSqlParser.General_elementContext) {
                return (PlSqlParser.General_elementContext) node;
            }
            if (!(node instanceof ParserRuleContext) || node.getChildCount() == 0) return null;
            node = node.getChild(0);
        }
        return null;
    }

    /**
     * Scan SQL text for package.function(args) patterns that may have been missed
     * by the tree walk due to parse error recovery. Only adds calls not already
     * detected (deduplication by upper-case full name).
     *
     * @param sqlText   the original SQL text (cursor query, SELECT statement, etc.)
     * @param baseLine  the starting line of the SQL text for approximate line tracking
     */
    private void extractFunctionCallsFromSqlText(String sqlText, int baseLine) {
        extractFunctionCallsFromSqlText(sqlText, baseLine, false);
    }

    /**
     * @param allowRepeats when false (default/whole-body catch-all usage), a call name
     *                     already recorded anywhere in the current subprogram/object is
     *                     skipped — this is the original parse-error-recovery behavior,
     *                     used when scanning a large body of text that the tree walk may
     *                     have already covered elsewhere.
     *                     When true (used for distinct TABLE(fn(...)) collection expression
     *                     occurrences), only calls already recorded at this exact source
     *                     line are treated as duplicates, so repeated invocations of the
     *                     same function from separate TABLE(...) occurrences each get their
     *                     own CallInfo/line instead of being collapsed into one.
     */
    private void extractFunctionCallsFromSqlText(String sqlText, int baseLine, boolean allowRepeats) {
        if (sqlText == null || sqlText.isEmpty()) return;

        // Collect already-detected call full names for dedup.
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
            // BUGFIX (§264/§265 dup-edge): allowRepeats intentionally lets the same target be
            // recorded again from a genuinely separate TABLE(fn(...)) occurrence elsewhere in the
            // subprogram (different FOR loop, clearly different source line). But line numbers fed
            // in here are only approximate (baseLine is the start of the enclosing statement/
            // expression being text-scanned, not the exact call-site line), so the SAME physical
            // call — first picked up by the whole-body/cursor-text catch-all scan, then picked up
            // again by extractCallsFromTableCollectionExpression's TABLE(...) scan of the nested
            // expression a line or two later — was treated as "different line" and re-emitted as a
            // second CallInfo/edge for the identical (from,to) pair (e.g. EXPLODE_PRODUCTS,
            // SET_CONTEXT, COMPUTE_TOTAL, OPEN_DEMAND_CURSOR), which later collided as duplicate
            // am_call_edge_unique docs once both nodeIds resolved to the same Mongo entity ref.
            // A small tolerance treats those near-identical lines as the same occurrence while
            // still treating occurrences several lines apart (distinct FOR loops/statements) as
            // genuine repeats.
            if (allowRepeats && Math.abs(ci.getLine() - baseLine) > TABLE_CALL_REPEAT_LINE_TOLERANCE) continue;
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

            // Skip Oracle old-style outer-join markers: `b.acct_id(+)` is a column reference
            // tagged with the outer-join sign, NOT a call to package B's function ACCT_ID.
            // The regex cannot tell them apart (both look like `name.name(`), so inspect what
            // actually follows the opening paren.
            if (isOuterJoinSignAt(stripped, m.end() - 1)) continue;

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
     * True when the parenthesised group starting at {@code openParenIdx} is exactly the Oracle
     * old-style outer-join sign {@code (+)}, allowing internal whitespace (Oracle accepts
     * {@code ( + )}). Used to stop the SQL-embedded-call regex from mistaking the outer-join
     * marker on a qualified column (e.g. {@code b.acct_id(+)}) for a package function call.
     */
    private boolean isOuterJoinSignAt(String text, int openParenIdx) {
        if (text == null || openParenIdx < 0 || openParenIdx >= text.length()
                || text.charAt(openParenIdx) != '(') {
            return false;
        }
        int i = openParenIdx + 1;
        while (i < text.length() && Character.isWhitespace(text.charAt(i))) i++;
        if (i >= text.length() || text.charAt(i) != '+') return false;
        i++;
        while (i < text.length() && Character.isWhitespace(text.charAt(i))) i++;
        return i < text.length() && text.charAt(i) == ')';
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
     * Like extractTablesFromDynamicSql but returns true if at least one table was recorded.
     */
    private boolean extractTablesFromDynamicSqlTracked(String expr, int line) {
        if (expr == null) return false;
        // Count table ops before
        int before = countCurrentTableOps();
        extractTablesFromDynamicSql(expr, line);
        return countCurrentTableOps() > before;
    }

    private int countCurrentTableOps() {
        SubprogramInfo sub = currentSubprogram();
        if (sub != null) return sub.getTableOperations().size();
        ParsedObject po = currentObject();
        return po != null ? po.getTableOperations().size() : 0;
    }

    /**
     * If exprText (trimmed) is a plain identifier (no parens, no dots), return it upper-cased.
     * This detects EXECUTE IMMEDIATE v_sql where v_sql is a variable.
     */
    private String extractSimpleExprVar(String exprText) {
        if (exprText == null) return null;
        String t = exprText.trim();
        if (t.matches("[A-Za-z_][A-Za-z0-9_$#]*")) {
            return t.toUpperCase();
        }
        return null;
    }

    /**
     * If exprText looks like a simple function call (optionally package-qualified, with parens),
     * return the upper-cased function name (last dotted component). Returns null otherwise.
     */
    private String extractSimpleCallName(String exprText) {
        if (exprText == null) return null;
        String t = exprText.trim();
        // Match: [pkg.]fn(...)  — optional package prefix, mandatory parens
        int paren = t.indexOf('(');
        if (paren <= 0) return null;
        String namePart = t.substring(0, paren).trim();
        if (!namePart.matches("[A-Za-z_][A-Za-z0-9_.]*")) return null;
        // Return last component only (the function name within its package)
        int dot = namePart.lastIndexOf('.');
        String funcName = dot >= 0 ? namePart.substring(dot + 1) : namePart;
        return funcName.toUpperCase();
    }

    /**
     * Scan a subprogram's source text for RETURN statement string literals and extract
     * any table references from them. Returns true if at least one table was extracted.
     * This powers Option B: when EXECUTE IMMEDIATE calls a function whose body we have.
     */
    private boolean scanSourceForReturnStrings(String src, int line) {
        if (src == null) return false;
        // Simple regex: find RETURN followed by a quoted string expression
        Pattern returnPattern = Pattern.compile(
                "(?i)\\bRETURN\\s+('(?:[^']|'')*'(?:\\s*\\|\\|\\s*'(?:[^']|'')*')*)");
        Matcher m = returnPattern.matcher(src);
        boolean found = false;
        while (m.find()) {
            String retExpr = m.group(1);
            int before = countCurrentTableOps();
            extractTablesFromDynamicSql(retExpr, line);
            if (countCurrentTableOps() > before) found = true;
        }
        return found;
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
