// ============================================================
// MySqlParser.g4
// Parser grammar for MySQL stored procedures / functions.
// Entry point: compilationUnit
// Each section is responsible for one structural concern:
//   1. Routine definition (CREATE PROCEDURE / FUNCTION)
//   2. Declarations       (DECLARE var / cursor / handler)
//   3. Compound block     (BEGIN … END)
//   4. Control-flow       (IF / CASE / WHILE / LOOP / REPEAT)
//   5. DML statements     (SELECT / INSERT / UPDATE / DELETE)
//   6. Cursor ops         (OPEN / FETCH / CLOSE)
//   7. Dynamic SQL        (PREPARE / EXECUTE / DEALLOCATE)
//   8. Expressions        (simplified, enough to parse all above)
// ============================================================
parser grammar MySqlParser;

options { tokenVocab = MySqlLexer; }

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 1 — Entry point & routine definition
// ─────────────────────────────────────────────────────────────────────────────

compilationUnit
    : (createRoutine | createTrigger | createView | createIndex | alterTable SEMI?) EOF
    ;

createRoutine
    : CREATE definerClause?
      routineType=(PROCEDURE | FUNCTION)
      routineName
      LPAREN paramList? RPAREN
      returnsClause?
      routineCharacteristic*
      compoundBlock
      SEMI?
    ;

// ─────────────────────────────────────────────────────────────────────────────
// CREATE TRIGGER
//   CREATE [DEFINER=...] TRIGGER name {BEFORE|AFTER} {INSERT|UPDATE|DELETE}
//     ON table FOR EACH ROW [{FOLLOWS|PRECEDES} other_trigger] body
//
// The body deliberately reuses the routine grammar (compoundBlock / routineStatement),
// so DML, CALLs and NEW./OLD. column references inside a trigger are captured by exactly
// the same visitor logic that already handles procedure bodies — no parallel statement
// grammar to keep in sync. A single-statement body (no BEGIN…END) is legal MySQL and is
// covered by the routineStatement alternative.
// ─────────────────────────────────────────────────────────────────────────────
createTrigger
    : CREATE definerClause? TRIGGER triggerName
      triggerTiming=(BEFORE | AFTER)
      triggerEvent=(INSERT | UPDATE | DELETE)
      ON tableRef
      FOR EACH ROW
      triggerOrder?
      triggerBody
      SEMI?
    ;

triggerName  : (schemaRef DOT)? objectId ;

triggerOrder : (FOLLOWS | PRECEDES) triggerName ;

triggerBody
    : compoundBlock
    | routineStatement
    ;

// ─────────────────────────────────────────────────────────────────────────────
// CREATE VIEW
//   CREATE [OR REPLACE] [ALGORITHM = {UNDEFINED|MERGE|TEMPTABLE}] [DEFINER=...]
//     [SQL SECURITY {DEFINER|INVOKER}] VIEW name [(cols)] AS select
//     [WITH [CASCADED|LOCAL] CHECK OPTION]
//
// The defining query reuses selectStatement, so the view's underlying tables (FROM,
// JOINs, subqueries, CTEs) are discovered by the existing fromClause/joinClause visitor
// logic rather than a second table-extraction implementation.
// ─────────────────────────────────────────────────────────────────────────────
createView
    : CREATE (OR REPLACE)? viewAlgorithm? definerClause? viewSecurity? VIEW viewName
      (LPAREN columnList RPAREN)?
      AS selectStatement
      viewCheckOption?
      SEMI?
    ;

viewName        : (schemaRef DOT)? objectId ;
viewAlgorithm   : ALGORITHM EQ simpleId ;
viewSecurity    : SQL_KW SECURITY (DEFINER | ID) ;
viewCheckOption : WITH (CASCADED | LOCAL)? CHECK OPTION ;

// ─────────────────────────────────────────────────────────────────────────────
// CREATE INDEX
//   CREATE [UNIQUE|FULLTEXT|SPATIAL] INDEX name [USING {BTREE|HASH}]
//     ON table (col [ASC|DESC], ...)
//
// Wired as its own compilationUnit alternative so a standalone CREATE INDEX script
// parses cleanly instead of failing with "no viable alternative" at the top level.
// The indexed table is recorded through the same tableRef rule used by DML, so the
// index's target shows up in the existing table-lineage machinery.
// ─────────────────────────────────────────────────────────────────────────────
createIndex
    : CREATE indexKind=(UNIQUE | FULLTEXT | SPATIAL)? INDEX indexName
      indexTypeClause?
      ON tableRef
      LPAREN indexColumn (COMMA indexColumn)* RPAREN
      SEMI?
    ;

indexName       : (schemaRef DOT)? objectId ;
indexTypeClause : USING indexType=(BTREE | HASH) ;
indexColumn     : objectId direction=(ASC | DESC)? ;

// ─────────────────────────────────────────────────────────────────────────────
// ALTER TABLE
//   ALTER TABLE t action [, action ...]
//
// Before this rule, EVERY form of ALTER TABLE fell into the unknownStatement catch-all
// (whose comment named it explicitly). It was at least logged as "unsupported statement
// skipped" rather than lost silently, but nothing was captured — including the single
// most valuable fact any DDL statement carries:
//
//     ALTER TABLE orders ADD CONSTRAINT fk FOREIGN KEY (customer_id)
//                            REFERENCES customers(id)
//
// is a declared, authoritative orders → customers dependency edge. Inferring that edge
// from DML is guesswork; here the database states it outright, and it was being thrown
// away. So the FOREIGN KEY … REFERENCES form is parsed precisely (both table names and
// both column lists are structural), as are the other named-constraint ADD forms and
// the DROP forms. Everything else — MODIFY / CHANGE / RENAME / ENGINE= / partitioning —
// is absorbed by the permissive alterOtherAction tail, the same trade viewCheckOption's
// and triggerBody's permissive tails make: an uncommon sub-form must not fail the parse
// of the statement (or, inside a routine body, of its neighbours), but nothing is read
// out of its tokens and reported as a finding.
//
// Wired into BOTH compilationUnit (standalone DDL scripts) and routineStatement (ALTER
// TABLE inside a procedure body), because both occur in the wild.
// ─────────────────────────────────────────────────────────────────────────────
alterTable
    : ALTER TABLE tableRef alterAction (COMMA alterAction)*
    ;

alterAction
    // The constraint alternative MUST precede the column alternative. Because every new
    // keyword here is also in reservedAsId (it has to be, or identifiers named `constraint`
    // /`foreign` regress), `ADD CONSTRAINT fk_c FOREIGN KEY …` is a viable prefix of
    // `ADD COLUMN? columnDef` — column `constraint` of type `fk_c` — and the column
    // alternative, if listed first, swallowed it and left FOREIGN KEY … unparsed. Measured:
    // every named-constraint form fell back to the catch-all with the FK edge still lost.
    : ADD tableConstraint                                    # alterAddConstraint
    | ADD COLUMN? columnDef                                  # alterAddColumn
    // DROP PRIMARY KEY / FOREIGN KEY x / CONSTRAINT x / CHECK x / INDEX x must precede
    // the plain DROP-column alternative, otherwise `DROP PRIMARY KEY` would be read as
    // dropping a column named `primary`.
    | DROP PRIMARY KEY                                       # alterDropPrimaryKey
    | DROP FOREIGN KEY objectId                              # alterDropForeignKey
    | DROP dropKind=(CONSTRAINT | CHECK | INDEX | KEY) objectId  # alterDropNamedConstraint
    | DROP COLUMN? objectId                                  # alterDropColumn
    | alterOtherAction                                       # alterOther
    ;

// The ADD-constraint forms. The CONSTRAINT keyword and the constraint name are both
// optional in MySQL (`ADD FOREIGN KEY (x) REFERENCES t(y)` is legal), hence the
// (CONSTRAINT objectId?)? prefix shared by all four.
tableConstraint
    : constraintName? FOREIGN KEY objectId?
      LPAREN columnList RPAREN
      REFERENCES tableRef (LPAREN columnList RPAREN)?
      referenceOption*                                       # fkTableConstraint
    | constraintName? PRIMARY KEY (LPAREN columnList RPAREN)?  # pkTableConstraint
    | constraintName? UNIQUE (INDEX | KEY)? objectId?
      LPAREN columnList RPAREN                               # uniqueTableConstraint
    | constraintName? CHECK LPAREN expr RPAREN               # checkTableConstraint
    ;

constraintName : CONSTRAINT objectId? ;

// ON DELETE/UPDATE RESTRICT | CASCADE | SET NULL | NO ACTION | SET DEFAULT.
// RESTRICT/CASCADE/ACTION are not tokens in this lexer (they lex as ID), so they are
// matched via simpleId rather than by adding three more keywords nothing else needs.
referenceOption
    : ON (DELETE | UPDATE) ( SET (NULL_LIT | DEFAULT)
                           | NO simpleId
                           | simpleId )
    ;

// Permissive tail for alter actions this grammar does not model structurally. Bounded
// by COMMA (next action) and SEMI (end of statement) so it can never run past the
// statement it belongs to.
alterOtherAction
    : ~(COMMA | SEMI) ( ~(COMMA | SEMI) )*
    ;

definerClause
    : DEFINER EQ definerValue
    ;

definerValue
    : CURRENT_USER
    | simpleId (AT_SIGN simpleId)?
    | SQ_STRING (AT_SIGN SQ_STRING)?
    ;

routineName
    : (schemaRef DOT)? objectId
    ;

// ─────────────────────────────────────────────────────────────────────────────
// Parameters
// ─────────────────────────────────────────────────────────────────────────────

paramList
    : param (COMMA param)*
    ;

param
    : direction=(IN | OUT | INOUT)? paramName dataType
    ;

paramName : objectId ;

returnsClause
    : RETURNS dataType
    ;

// ─────────────────────────────────────────────────────────────────────────────
// Routine characteristics
// ─────────────────────────────────────────────────────────────────────────────

routineCharacteristic
    : LANGUAGE SQL_KW
    | NOT? DETERMINISTIC
    | CONTAINS SQL_KW
    | NO SQL_KW
    | READS SQL_KW DATA
    | MODIFIES SQL_KW DATA
    | SQL_KW SECURITY (DEFINER | ID)
    | COMMENT_KW SQ_STRING
    ;

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 2 — Data types
// ─────────────────────────────────────────────────────────────────────────────

dataType
    : typeName typeArgs? typeModifier*
    ;

typeName
    : simpleId
    | FUNCTION    // edge-case: type named FUNCTION
    ;

typeArgs
    : LPAREN typeArg (COMMA typeArg)* RPAREN
    ;

typeArg
    : INT_NUM
    | DEC_NUM
    | SQ_STRING
    | simpleId
    ;

typeModifier
    : UNSIGNED
    | SIGNED
    | ZEROFILL
    | CHARACTER SET simpleId
    | CHARSET simpleId
    | COLLATE simpleId
    ;

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 3 — Compound block (BEGIN … END)
// ─────────────────────────────────────────────────────────────────────────────

compoundBlock
    : blockLabel?
      BEGIN
      declareSection*
      blockBody?
      END
      blockEndLabel?
      SEMI?
    ;

blockLabel    : simpleId COLON ;
blockEndLabel : simpleId ;

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 4 — Declarations inside a block
// ─────────────────────────────────────────────────────────────────────────────

declareSection
    : declareVar     SEMI
    | declareCursor  SEMI
    | declareHandler            // action statement carries its own trailing SEMI
    | declareCondition SEMI
    ;

// DECLARE name[, name] type [DEFAULT expr]
declareVar
    : DECLARE varNameList dataType (DEFAULT expr)?
    ;

varNameList
    : objectId (COMMA objectId)*
    ;

// DECLARE cursorName CURSOR FOR selectStatement
declareCursor
    : DECLARE objectId CURSOR FOR selectStatement
    ;

// DECLARE {CONTINUE|EXIT|UNDO} HANDLER FOR conditionList statement
declareHandler
    : DECLARE handlerKind HANDLER FOR handlerConditionList routineStatement
    ;

handlerKind : CONTINUE | EXIT | UNDO ;

handlerConditionList
    : handlerCondition (COMMA handlerCondition)*
    ;

handlerCondition
    : SQLSTATE VALUE? SQ_STRING   # handlerSqlState
    | SQLWARNING                  # handlerSqlWarning
    | NOT FOUND                   # handlerNotFound
    | SQLEXCEPTION                # handlerSqlException
    | simpleId                    # handlerNamedCondition
    ;

// DECLARE condName CONDITION FOR {SQLSTATE str | errorCode}
declareCondition
    : DECLARE simpleId CONDITION FOR conditionValue
    ;

conditionValue
    : SQLSTATE VALUE? SQ_STRING
    | INT_NUM
    ;

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 5 — Block body (sequence of statements)
// ─────────────────────────────────────────────────────────────────────────────

blockBody
    : routineStatement+
    ;

routineStatement
    : compoundBlock                         # stmtCompound
    | ifStatement                           # stmtIf
    | caseStatement                         # stmtCase
    | whileStatement                        # stmtWhile
    | loopStatement                         # stmtLoop
    | repeatStatement                       # stmtRepeat
    | callStatement              SEMI       # stmtCall
    | setStatement               SEMI       # stmtSet
    | openCursorStatement        SEMI       # stmtOpen
    | fetchCursorStatement       SEMI       # stmtFetch
    | closeCursorStatement       SEMI       # stmtClose
    | returnStatement            SEMI       # stmtReturn
    | leaveStatement             SEMI       # stmtLeave
    | iterateStatement           SEMI       # stmtIterate
    | prepareStatement           SEMI       # stmtPrepare
    | executeStatement           SEMI       # stmtExecute
    | deallocateStatement        SEMI       # stmtDeallocate
    | signalStatement            SEMI       # stmtSignal
    | resignalStatement          SEMI       # stmtResignal
    | getDiagnosticsStatement    SEMI       # stmtGetDiagnostics
    | selectStatement            SEMI?      # stmtSelect
    | insertStatement            SEMI?      # stmtInsert
    | replaceStatement           SEMI?      # stmtReplace
    | updateStatement            SEMI?      # stmtUpdate
    | deleteStatement            SEMI?      # stmtDelete
    | createTmpTableStatement    SEMI?      # stmtCreateTmpTable
    | dropTmpTableStatement      SEMI?      # stmtDropTmpTable
    | truncateStatement          SEMI?      # stmtTruncate
    | alterTable                 SEMI?      # stmtAlterTable
    | transactionStatement       SEMI?      # stmtTransaction
    // A bare ';' is a legal empty statement (e.g. stray/doubled terminators like ';;;').
    // Must come before unknownStatement so an empty statement isn't swallowed by the catch-all.
    | SEMI                                  # stmtEmpty
    // Must stay LAST. ANTLR takes the lowest-numbered alternative that matches, so every rule above
    // still wins; this only fires when nothing else could parse the statement at all.
    | unknownStatement           SEMI       # stmtUnknown
    ;

// ─────────────────────────────────────────────────────────────────────────────
// The catch-all.
//
// Without it, one statement this grammar does not know fails the enclosing body and the WHOLE
// procedure yields nothing — zero tables, zero operations, no error. A procedure writing to eight
// tables reported writing to none, and the analysis looked complete. That was measured for TRUNCATE
// and, before this rule, also held for LOCK TABLE, CREATE INDEX, ALTER TABLE, GRANT and MERGE.
//
// Adding a rule per keyword is whack-a-mole: the next unknown statement brings the failure straight
// back. Skipping the statement instead makes the degradation bounded and local — we lose that one
// statement, never its neighbours.
//
// BEGIN and END are excluded so the catch-all cannot swallow a block terminator and run past the end
// of the routine. SEMI is excluded because it is the terminator this rule stops at.
// ─────────────────────────────────────────────────────────────────────────────
unknownStatement
    // Block-continuation keywords are excluded because they never START a statement.
    //
    // Without them the catch-all swallowed ELSE and ELSEIF branches whole — and everything inside
    // them. Measured on the real MySQL analysis: 160 skipped statements, 94 ELSE and 66 ELSEIF,
    // each carrying an INSERT INTO erp_audit_log that was silently lost. That is worse than the
    // whole-body failure this rule was added to prevent, because it looks like a successful parse.
    //
    // DB2's otherStatement has carried the same exclusion list for the same reason (its comment
    // cites the reserve_stock bug); the MySQL rule was written without it.
    : ~(SEMI | BEGIN | END | ELSE | ELSEIF | THEN | WHEN | DO | UNTIL | LOOP | REPEAT | WHILE | IF | CASE | LPAREN | OVER)
      ( ~(SEMI | BEGIN | END) )*
    ;

// ─────────────────────────────────────────────────────────────────────────────
// Transaction-control statements (MySQL)
//   COMMIT [WORK] [AND [NO] CHAIN] [[NO] RELEASE]
//   ROLLBACK [WORK] TO [SAVEPOINT] name          (rollback to savepoint)
//   ROLLBACK [WORK] [AND [NO] CHAIN] [[NO] RELEASE]
//   SAVEPOINT name
//   RELEASE SAVEPOINT name
// ─────────────────────────────────────────────────────────────────────────────
transactionStatement
    : START TRANSACTION                                      # txnStart
    | COMMIT WORK? (AND NO? CHAIN)? (NO? RELEASE)?           # txnCommit
    | ROLLBACK WORK? TO SAVEPOINT? objectId                  # txnRollbackTo
    | ROLLBACK WORK? (AND NO? CHAIN)? (NO? RELEASE)?         # txnRollback
    | SAVEPOINT objectId                                     # txnSavepoint
    | RELEASE SAVEPOINT objectId                             # txnReleaseSavepoint
    ;

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 6 — Control-flow statements
// ─────────────────────────────────────────────────────────────────────────────

ifStatement
    : IF expr THEN blockBody?
      elseifClause*
      elseClause?
      END IF SEMI?
    ;

elseifClause : ELSEIF expr THEN blockBody? ;
elseClause   : ELSE blockBody? ;

caseStatement
    : CASE expr?
        caseWhenClause+
        (ELSE blockBody?)?
      END CASE SEMI?
    ;

caseWhenClause : WHEN expr THEN blockBody? ;

whileStatement
    : blockLabel? WHILE expr DO blockBody? END WHILE blockEndLabel? SEMI?
    ;

loopStatement
    : blockLabel? LOOP blockBody? END LOOP blockEndLabel? SEMI?
    ;

repeatStatement
    : blockLabel? REPEAT blockBody? UNTIL expr END REPEAT blockEndLabel? SEMI?
    ;

leaveStatement   : LEAVE simpleId ;
iterateStatement : ITERATE simpleId ;
returnStatement  : RETURN expr ;

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 7 — CALL statement (procedure invocation — key for call-tree)
// ─────────────────────────────────────────────────────────────────────────────

callStatement
    : CALL routineName LPAREN callArgList? RPAREN
    ;

callArgList
    : callArg (COMMA callArg)*
    ;

callArg
    : expr
    | AT_VAR ASSIGN_OP expr   // OUT param pass-by-user-var: @v := val
    ;

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 8 — SET statement
// ─────────────────────────────────────────────────────────────────────────────

setStatement
    : SET setAssignment (COMMA setAssignment)*
    ;

setAssignment
    : setTarget (EQ | ASSIGN_OP) expr
    ;

setTarget
    : AT_VAR
    | AT_SIGN AT_SIGN simpleId DOT simpleId   // @@global.var
    | simpleId (DOT simpleId)?                // column, or table.column (multi-table UPDATE SET)
    ;

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 9 — Cursor operations
// ─────────────────────────────────────────────────────────────────────────────

openCursorStatement
    : OPEN simpleId
    ;

closeCursorStatement
    : CLOSE simpleId
    ;

fetchCursorStatement
    : FETCH (NEXT? FROM)? simpleId INTO fetchTargetList
    ;

fetchTargetList
    : fetchTarget (COMMA fetchTarget)*
    ;

fetchTarget : AT_VAR | simpleId ;

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 10 — Dynamic SQL
// ─────────────────────────────────────────────────────────────────────────────

prepareStatement
    : PREPARE simpleId FROM dynamicSqlSource
    ;

dynamicSqlSource
    : AT_VAR
    | SQ_STRING
    | DQ_STRING
    ;

executeStatement
    : EXECUTE simpleId (USING executeVarList)?
    ;

executeVarList
    : executeVar (COMMA executeVar)*
    ;

executeVar : AT_VAR | simpleId ;

deallocateStatement
    : (DEALLOCATE | DROP) PREPARE simpleId
    ;

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 11 — SIGNAL / RESIGNAL
// ─────────────────────────────────────────────────────────────────────────────

signalStatement
    : SIGNAL signalConditionValue signalSetClause?
    ;

resignalStatement
    : RESIGNAL signalConditionValue? signalSetClause?
    ;

signalConditionValue
    : SQLSTATE VALUE? SQ_STRING
    | simpleId
    ;

signalSetClause
    : SET signalItem (COMMA signalItem)*
    ;

signalItem : simpleId EQ expr ;

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 11b — GET DIAGNOSTICS
//   GET [CURRENT | STACKED] DIAGNOSTICS
//        { statement-info-item-list
//        | CONDITION condition-number condition-info-item-list }
//   The info-item names (NUMBER, ROW_COUNT, RETURNED_SQLSTATE, MESSAGE_TEXT,
//   MYSQL_ERRNO, CLASS_ORIGIN, …) are ordinary identifiers, matched via simpleId.
// ─────────────────────────────────────────────────────────────────────────────
getDiagnosticsStatement
    : GET (CURRENT | STACKED)? DIAGNOSTICS
      ( CONDITION conditionNumber diagnosticsItem (COMMA diagnosticsItem)*
      | diagnosticsItem (COMMA diagnosticsItem)* )
    ;

conditionNumber
    : INT_NUM
    | AT_VAR
    | simpleId
    ;

diagnosticsItem
    : diagnosticsTarget EQ simpleId
    ;

diagnosticsTarget
    : AT_VAR
    | simpleId
    ;

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 12 — SELECT statement  (with CTE, JOINs, UNION, INTO)
// ─────────────────────────────────────────────────────────────────────────────

selectStatement
    : withClause? selectCore unionTail* intoClause?
    ;

withClause
    : WITH RECURSIVE? cteDefinition (COMMA cteDefinition)*
    ;

// The optional column list — `WITH RECURSIVE tree (id, parent_id, lvl) AS (...)` — is how most
// real recursive CTEs are written, since naming the columns up front is the readable way to line
// the anchor and recursive branches up. Omitting it from the rule meant every such CTE failed to
// parse and the entire statement, including the JOINs in its recursive branch and the final
// SELECT, was dropped as unsupported.
cteDefinition
    : simpleId (LPAREN columnList RPAREN)? AS LPAREN selectStatement RPAREN
    ;

selectCore
    : SELECT selectModifier* selectItems
      intoClause?
      fromClause?
      whereClause?
      groupByClause?
      havingClause?
      windowClause?
      orderByClause?
      limitClause?
    ;

// The exact set MySQL allows here — no catch-all.
//
// This rule previously ended in a bare `ID` "to catch SQL_CALC_FOUND_ROWS etc.", and that
// alternative was reachable by the NAME OF A FUNCTION. `SELECT fa(1), fb(2) FROM t` parsed
// as modifier `fa`, select item `(1)`, select item `fb(2)`: no syntax error, the FROM table
// still reported, and the first select expression quietly replaced by its own argument list.
// The visible damage was to the call graph — every `SELECT my_func(x) ...` lost its
// my_func edge, and only the SECOND and later functions in a select list were ever
// recorded — but the mangled expression was wrong for any consumer, not just call edges.
// Measured: `SELECT calc_total(o.id) FROM orders o WHERE fn_is_active(o.id) = 1` reported
// fn_is_active and not calc_total.
//
// Spelling the modifiers out as tokens removes the rival parse entirely; there is no
// remaining position in which an arbitrary identifier may precede the select list.
selectModifier
    : ALL | DISTINCT | DISTINCTROW | HIGH_PRI | STRAIGHT_JOIN
    | SQL_SMALL_RESULT | SQL_BIG_RESULT | SQL_BUFFER_RESULT
    | SQL_NO_CACHE | SQL_CALC_FOUND_ROWS
    ;

selectItems
    : STAR
    | selectItem (COMMA selectItem)*
    ;

selectItem
    : STAR                                         # selectWildcard
    | tableRef DOT STAR                            # selectTableWildcard
    | expr (AS? alias)?                            # selectExpr
    ;

alias : simpleId | SQ_STRING | DQ_STRING ;

fromClause
    : FROM tableReferences
    ;

tableReferences
    : tableReference (COMMA tableReference)*
    ;

tableReference
    : tableAtom joinClause*
    ;

tableAtom
    // JSON_TABLE must precede atomTable. JSON_TABLE is its own token (not a simpleId), so
    // the two cannot actually collide, but keeping the table-function form first documents
    // the intent and matches how the joined/constraint alternatives are ordered elsewhere.
    : jsonTable (AS? alias)? (LPAREN columnList RPAREN)?             # atomJsonTable
    | tableRef partitionClause? (AS? alias)?                         # atomTable
    | LPAREN tableReferences RPAREN                                  # atomParenRef
    | LPAREN selectStatement RPAREN (AS? alias)?                     # atomSubquery
    // LATERAL derived table (MySQL 8.0.14+). Structurally identical to atomSubquery — the
    // keyword only changes the scoping rules, not the shape — but without the alternative the
    // LATERAL token had nothing to match and the whole SELECT, base tables included, was lost.
    // Handled by the same visitChildren recursion that already discovers a derived table's
    // inner FROM, so `payments` inside the lateral subquery is recorded exactly as before.
    | LATERAL LPAREN selectStatement RPAREN (AS? alias)?             # atomLateral
    ;

// ─────────────────────────────────────────────────────────────────────────────
// JSON_TABLE — MySQL 8.0's only table function.
//
//   JSON_TABLE(json_doc, '$.path' COLUMNS (col_def [, col_def] ...)) [AS] alias
//
// Before this rule there was no table-function form in tableAtom at all, so a FROM clause
// containing JSON_TABLE did not match, the statement fell into unknownStatement, and every
// ORDINARY table in the same query went unrecorded along with it — measured: a SELECT over
// `orders o, JSON_TABLE(o.payload, ...) jt` reported neither `orders` nor the query.
//
// The column spec is modelled structurally rather than skipped permissively because its
// NESTED PATH form recurses, and a balanced-paren catch-all cannot tell where the COLUMNS
// list ends versus where the enclosing JSON_TABLE(...) does.
// ─────────────────────────────────────────────────────────────────────────────
jsonTable
    : JSON_TABLE LPAREN expr COMMA jsonPathLiteral COLUMNS jsonTableColumns RPAREN
    ;

jsonPathLiteral : SQ_STRING | DQ_STRING | AT_VAR ;

jsonTableColumns
    : LPAREN jsonTableColumn (COMMA jsonTableColumn)* RPAREN
    ;

jsonTableColumn
    // `rn FOR ORDINALITY` — the generated row counter.
    : objectId FOR ORDINALITY                                        # jsonColOrdinality
    // `NESTED [PATH] '$.sub[*]' COLUMNS (...)` — recurses into a nested array.
    | NESTED PATH? jsonPathLiteral COLUMNS jsonTableColumns          # jsonColNested
    // `c INT EXISTS PATH '$.x'` and `c INT PATH '$.x' [DEFAULT '..'|NULL|ERROR ON EMPTY|ERROR]`
    | objectId dataType EXISTS? PATH jsonPathLiteral jsonColumnOnError*  # jsonColValue
    ;

jsonColumnOnError
    : (NULL_LIT | ERROR | DEFAULT jsonPathLiteral) ON (EMPTY | ERROR)
    ;

joinClause
    : joinType? JOIN tableAtom joinCondition?
    ;

joinType
    : INNER
    | LEFT  OUTER?
    | RIGHT OUTER?
    | CROSS
    | STRAIGHT_JOIN
    | NATURAL (LEFT | RIGHT)? OUTER? JOIN
    ;

joinCondition
    : ON expr
    | USING LPAREN columnList RPAREN
    ;

// Explicit partition selection: `FROM sales PARTITION (p0, p1) s`, and the same clause on
// INSERT / UPDATE / DELETE. It narrows which partitions of ONE table are read or written —
// the table identity, which is all the lineage cares about, is unchanged — so the partition
// names are recognized and then deliberately not reported as anything. Without the rule the
// clause was unparseable and the whole statement was dropped: the DELETE probe lost its
// WHERE, and the INSERT and UPDATE probes lost the `sales` write entirely.
//
// PARTITION is already in reservedAsId (the window-function clause needs it there), so
// `FROM t PARTITION (...)` is formally ambiguous with a table t aliased `partition` followed
// by a parenthesised expression. Listing partitionClause BEFORE the optional alias resolves
// it the way MySQL does — the greedy first alternative wins.
partitionClause : PARTITION LPAREN columnList RPAREN ;

tableRef     : (schemaRef DOT)? objectId ;
schemaRef    : simpleId ;
columnList   : simpleId (COMMA simpleId)* ;

whereClause    : WHERE expr ;
// `WITH ROLLUP` (legacy MySQL trailing modifier) and the modern `ROLLUP(...)` / `CUBE(...)` /
// `GROUPING SETS(...)` grouping-item forms are recognized as dedicated grammar constructs rather
// than left to fall through to the unknown-statement catch-all (WITH ROLLUP) or be mistaken for
// a plain function call (ROLLUP(...)). Full grouping-set semantics are not modeled — recognizing
// and correctly labeling the construct is enough.
groupByClause    : GROUP BY groupByItem (COMMA groupByItem)* (WITH ROLLUP)? ;
groupByItem      : expr (ASC | DESC)?
                 | groupByModifier
                 ;
groupByModifier  : ROLLUP LPAREN expr (COMMA expr)* RPAREN                 # groupByRollup
                 | CUBE LPAREN expr (COMMA expr)* RPAREN                   # groupByCube
                 | GROUPING SETS LPAREN groupingSetItem (COMMA groupingSetItem)* RPAREN # groupByGroupingSets
                 ;
groupingSetItem  : LPAREN (expr (COMMA expr)*)? RPAREN
                 | expr
                 ;
havingClause   : HAVING expr ;
// Named window definitions: `SELECT SUM(x) OVER w FROM t WINDOW w AS (PARTITION BY g ORDER BY
// id)`. `OVER w` was already accepted, but the WINDOW clause that defines `w` was not, so any
// query using the named form failed entirely. Sits between HAVING and ORDER BY, as in MySQL.
windowClause   : WINDOW windowDefinition (COMMA windowDefinition)* ;
windowDefinition : simpleId AS LPAREN windowSpec RPAREN ;
orderByClause  : ORDER BY orderByItem (COMMA orderByItem)* ;
orderByItem    : expr (ASC | DESC)? ;
limitClause    : LIMIT expr (COMMA expr)? ;

unionTail
    : (UNION | INTERSECT | EXCEPT) (ALL | DISTINCT)? selectCore
    ;

intoClause
    : INTO intoTarget (COMMA intoTarget)*
    | INTO OUTFILE SQ_STRING
    ;

intoTarget : AT_VAR | simpleId ;

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 13 — INSERT / UPDATE / DELETE statements
// ─────────────────────────────────────────────────────────────────────────────

insertStatement
    : INSERT insertPriority? IGNORE? INTO? tableRef partitionClause?
      insertColumns?
      insertSource
      onDuplicateClause?
    ;

// REPLACE [LOW_PRIORITY|DELAYED] [INTO] tbl … — MySQL's delete-then-insert DML. It had no rule
// at all, so it was rejected by the statement dispatcher and reported as
// "unsupported statement skipped": the target table and any REPLACE … SELECT source were lost
// (MySqlTableOpInfo.DmlType.REPLACE already existed in the model but nothing ever produced it).
// Shape is identical to insertStatement minus IGNORE/ON DUPLICATE, which REPLACE does not accept.
replaceStatement
    : REPLACE insertPriority? INTO? tableRef partitionClause?
      insertColumns?
      insertSource
    ;

onDuplicateClause
    : ON DUPLICATE KEY UPDATE setAssignment (COMMA setAssignment)*
    ;

insertPriority : LOW_PRI | DELAYED | HIGH_PRI ;

insertColumns
    : LPAREN columnList RPAREN
    ;

insertSource
    : VALUES insertRowList                              # insertValues
    | selectStatement                                   # insertSelect
    | SET setAssignment (COMMA setAssignment)*          # insertSet
    ;

insertRowList : insertRow (COMMA insertRow)* ;
insertRow     : LPAREN exprOrDefaultList RPAREN ;
exprOrDefaultList : exprOrDefault (COMMA exprOrDefault)* ;
exprOrDefault : expr | DEFAULT ;

// Accepts both the single-table form (UPDATE t SET ...) and MySQL's multi-table
// form (UPDATE t1 JOIN t2 ON ... SET ..., and comma-joins). Reuses tableReferences /
// joinClause from SELECT's FROM so join logic is not duplicated.
updateStatement
    : UPDATE LOW_PRI? IGNORE? tableReferences
      SET setAssignment (COMMA setAssignment)*
      whereClause?
      orderByClause?
      limitClause?
    ;

deleteStatement
    : DELETE LOW_PRI? QUICK? IGNORE? FROM tableRef partitionClause? (AS? alias)?
      whereClause?
      orderByClause?
      limitClause?
    ;

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 14 — CREATE / DROP TEMPORARY TABLE
// ─────────────────────────────────────────────────────────────────────────────

createTmpTableStatement
    : CREATE TEMPORARY? TABLE (IF NOT EXISTS)? tableRef
      ( LPAREN columnDefList RPAREN
      | AS selectStatement
      | LIKE tableRef
      )
    ;

columnDefList : columnDef (COMMA columnDef)* ;

columnDef
    : simpleId dataType columnConstraint*
    ;

columnConstraint
    : NOT NULL_LIT
    | NULL_LIT
    | DEFAULT expr
    | AUTO_INC
    | PRIMARY KEY
    | UNIQUE KEY?
    // Column-level REFERENCES — the same dependency edge the table-level FOREIGN KEY
    // constraint declares, just written inline (`ADD COLUMN customer_id INT REFERENCES
    // customers(id)`). Modelled here so both spellings feed one visitor path.
    | REFERENCES tableRef (LPAREN columnList RPAREN)? referenceOption*
    | COMMENT_KW SQ_STRING
    // Generated / computed columns (MySQL 5.7+), both spellings:
    //   c INT GENERATED ALWAYS AS (a + b) STORED
    //   d VARCHAR(10)          AS (CONCAT(a,'x')) VIRTUAL
    // Absent, the column definition did not match, so the whole CREATE TABLE fell into the
    // unsupported-statement catch-all — the table and every column of it went unrecorded, even
    // though only one column of the several was generated.
    | (GENERATED ALWAYS)? AS LPAREN expr RPAREN (STORED | VIRTUAL)?
    ;

dropTmpTableStatement
    : DROP TEMPORARY? TABLE (IF EXISTS)? tableRef
    ;

// TRUNCATE TABLE t  —  DDL, but it appears inside routine bodies and must parse.
//
// It was absent, and the cost was not "TRUNCATE is missing": no alternative matched, so the
// enclosing body failed and the procedure yielded NOTHING. Measured on a probe procedure —
// with the TRUNCATE, 0 tables and 0 operations; the identical procedure without it, 1 table
// with INSERT and UPDATE. One unsupported statement silently discarded every other statement
// around it.
truncateStatement
    : TRUNCATE TABLE? tableRef
    ;

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 15 — Expressions  (simplified precedence hierarchy)
// ─────────────────────────────────────────────────────────────────────────────

expr
    : orExpr
    ;

orExpr
    : andExpr (OR andExpr)*
    ;

andExpr
    : notExpr (AND notExpr)*
    ;

notExpr
    : NOT notExpr
    | xorExpr
    ;

xorExpr
    : compExpr (XOR compExpr)*
    ;

compExpr
    : addExpr ( compOp addExpr
              | IS NOT? (NULL_LIT | TRUE_LIT | FALSE_LIT | UNKNOWN)
              | NOT? BETWEEN addExpr AND addExpr
              | NOT? IN  LPAREN exprList RPAREN
              | NOT? IN  subqueryExpr
              | NOT? LIKE addExpr
              | NOT? REGEXP addExpr
              )?
    ;

compOp
    : EQ | NULL_SAFE | NEQ | LT | GT | LE | GE
    ;

addExpr
    : mulExpr ((PLUS | MINUS) mulExpr)*
    ;

mulExpr
    : unaryExpr ((STAR | SLASH | PERCENT) unaryExpr)*
    ;

unaryExpr
    : (MINUS | TILDE | EXCL) unaryExpr
    | jsonExpr
    ;

// MySQL's JSON path accessors bind tighter than any arithmetic operator, so they sit between
// unaryExpr and primaryExpr:  `doc->'$.a.b'`,  `doc->>'$.status'`,  `t.doc->>'$.x' = 'OK'`.
jsonExpr
    : primaryExpr ((JSON_EXTRACT_OP | JSON_UNQUOTE_OP) primaryExpr)*
    ;

primaryExpr
    : literal (COLLATE simpleId)?                           # primLiteral
    | AT_VAR                                                # primUserVar
    | CAST LPAREN expr AS castType RPAREN                   # primCast
    | CONVERT LPAREN expr COMMA castType RPAREN             # primConvert
    | VALUES LPAREN columnRef RPAREN                        # primValuesRef
    // MATCH (cols) AGAINST ('text' [IN NATURAL LANGUAGE MODE | IN BOOLEAN MODE
    //                               | WITH QUERY EXPANSION])
    // Must precede functionCall: while MATCH is now its own token and cannot be a
    // functionRef, keeping the fulltext form first mirrors how the other dedicated
    // call-shaped constructs (CAST/CONVERT/VALUES) are ordered above.
    //
    // Before this rule, `MATCH(a,b) AGAINST (...)` parsed as the function call MATCH(a,b)
    // and then died on AGAINST, so the whole statement was dropped — and, worse, `MATCH`
    // was reported as a user-routine CALL edge, a phantom that does not exist. The search
    // string is a plain literal and is never scanned, so text like
    // `'+select * from secret_table'` inside it can contribute no phantom table.
    | MATCH LPAREN exprList RPAREN
      AGAINST LPAREN expr fulltextModifier? RPAREN          # primMatchAgainst
    | functionCall                                          # primFunctionCall
    | columnRef                                             # primColumnRef
    | EXISTS subqueryExpr                                   # primExists
    | subqueryExpr                                          # primSubquery
    | CASE expr? caseExprWhen+ (ELSE expr)? END             # primCaseExpr
    | LPAREN exprList RPAREN                                # primParenExprList
    | LPAREN expr RPAREN                                    # primParenExpr
    ;

// CAST/CONVERT target types: MySQL allows SIGNED/UNSIGNED [INTEGER] in addition
// to normal data types (DECIMAL(p,s), CHAR, DATE, …). Reuses dataType for the latter.
castType
    : (SIGNED | UNSIGNED) simpleId?
    | dataType
    ;

fulltextModifier
    : IN NATURAL LANGUAGE MODE (WITH QUERY EXPANSION)?
    | IN BOOLEAN MODE
    | WITH QUERY EXPANSION
    ;

caseExprWhen : WHEN expr THEN expr ;

subqueryExpr : LPAREN selectStatement RPAREN ;

exprList : expr (COMMA expr)* ;

functionCall
    : functionRef LPAREN (STAR | callArgList)? RPAREN overClause?
    ;

// ── Window / analytic function OVER clause ────────────────────────────────────
//   func(...) OVER ( [PARTITION BY ...] [ORDER BY ...] [frame] )
//   func(...) OVER window_name
overClause
    : OVER (LPAREN windowSpec RPAREN | simpleId)
    ;

windowSpec
    : (PARTITION BY exprList)?
      orderByClause?
      frameClause?
    ;

frameClause
    : (ROWS | RANGE)
      ( BETWEEN frameBound AND frameBound
      | frameBound )
    ;

frameBound
    : UNBOUNDED PRECEDING
    | UNBOUNDED FOLLOWING
    | CURRENT ROW
    | expr PRECEDING
    | expr FOLLOWING
    ;

functionRef
    : (schemaRef DOT)? simpleId
    ;

columnRef
    : (tableRef DOT)? simpleId
    ;

literal
    : INT_NUM
    | DEC_NUM
    | HEX_NUM
    | HEX_NUM_QUOTED
    | BIT_NUM
    | BIT_NUM_QUOTED
    | SQ_STRING
    | DQ_STRING
    | CHARSET_STRING
    | NCHAR_STRING
    | NULL_LIT
    | TRUE_LIT
    | FALSE_LIT
    ;

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 16 — Identifier helpers
// ─────────────────────────────────────────────────────────────────────────────

simpleId
    : ID
    | BACKTICK_ID
    | reservedAsId
    ;

objectId
    : simpleId
    ;

// Keywords allowed as identifiers in MySQL (most keywords are not reserved)
reservedAsId
    : CHARACTER | COMMENT_KW | CONTAINS | DATA | DEFAULT | DEFINER | END | FOUND
    | FUNCTION | IGNORE | INDEX | KEY | LANGUAGE | LEAVE | MODIFIES
    | NO | PROCEDURE | READS | REPLACE | RETURNS | ROW | SECURITY
    | SET | SQL_KW | UNTIL | USE | VALUE | WHILE
    | START | TRANSACTION | DUPLICATE
    | CHARSET | CURRENT | DIAGNOSTICS | GET | STACKED
    // OVER and WINDOW are deliberately NOT here. Unlike the rest of this list they ARE reserved
    // words in MySQL 8.0, and allowing them as bare identifiers made `SUM(x) OVER w` parse as the
    // column SUM(x) aliased `OVER`, and `FROM t WINDOW w AS (...)` parse as the table t aliased
    // `WINDOW` — in both cases the rest of the query then became an unparseable second statement
    // and the whole query was dropped. A column genuinely named `over`/`window` must be
    // backtick-quoted in MySQL 8.0 anyway, and BACKTICK_ID still covers that.
    | PARTITION | PRECEDING | FOLLOWING | UNBOUNDED | ROWS | RANGE
    // Newly-tokenized, non-reserved CREATE TRIGGER keywords. Listing them here keeps
    // identifiers such as `after`, `each` or `trigger_name`'s bare `trigger` usable.
    | TRIGGER | BEFORE | AFTER | EACH | FOLLOWS | PRECEDES
    // Newly-tokenized, non-reserved CREATE VIEW keywords — same reasoning.
    | VIEW | ALGORITHM | CHECK | OPTION | CASCADED | LOCAL
    // Newly-tokenized, non-reserved CREATE INDEX keywords — same reasoning. (INDEX
    // is already listed above; UNIQUE/USING are reserved in MySQL and stay out.)
    | FULLTEXT | SPATIAL | BTREE | HASH
    // Newly-tokenized generated-column keywords — same reasoning.
    | GENERATED | ALWAYS | STORED | VIRTUAL
    // Newly-tokenized ALTER TABLE keywords — same reasoning again. Without these, a
    // routine body containing a column, variable or alias named `add`, `column`,
    // `constraint`, `references`, `foreign` or `alter` would stop lexing as ID and
    // regress bodies that parsed before ALTER TABLE support was added.
    | ALTER | ADD | COLUMN | CONSTRAINT | FOREIGN | REFERENCES
    // Newly-tokenized JSON_TABLE column-spec keywords. Every one of these is non-reserved
    // in MySQL, and `path`, `columns`, `error`, `empty` and `nested` are all plausible real
    // column names, so they must remain usable as identifiers. JSON_TABLE itself is absent
    // on purpose: it IS reserved in MySQL 8.0, and letting it be a bare identifier would
    // make `FROM JSON_TABLE(...)` ambiguous with a table literally named json_table.
    | COLUMNS | PATH | NESTED | ORDINALITY | EMPTY | ERROR
    // Newly-tokenized fulltext-search keywords, all non-reserved in MySQL. BOOLEAN is the
    // load-bearing one: it is a data type name, resolved through simpleId, so leaving it
    // out would break every `DECLARE flag BOOLEAN` in the estate.
    | BOOLEAN | MODE | QUERY | EXPANSION
    // The two non-reserved SELECT modifiers. The other four (DISTINCTROW, SQL_SMALL_RESULT,
    // SQL_BIG_RESULT, SQL_CALC_FOUND_ROWS) are reserved in MySQL and stay out.
    | SQL_BUFFER_RESULT | SQL_NO_CACHE
    ;
