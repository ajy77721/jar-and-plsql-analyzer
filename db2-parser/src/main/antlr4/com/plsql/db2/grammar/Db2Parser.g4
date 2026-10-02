// ============================================================
// Db2Parser.g4
// Parser grammar for IBM DB2 SQL PL stored procedures / functions.
// Entry point: compilationUnit
// tokenVocab = Db2Lexer
// ============================================================
parser grammar Db2Parser;

options { tokenVocab = Db2Lexer; }

// ─────────────────────────────────────────────────────────────────────────────
// Entry point
// ─────────────────────────────────────────────────────────────────────────────

compilationUnit
    : (routineDefinition | createTrigger | createView | createSequence | createIndex
       | alterTable | createType | createModule | alterModule | createVariable)? EOF
    ;

// ─────────────────────────────────────────────────────────────────────────────
// CREATE TYPE / CREATE DISTINCT TYPE
//
//   CREATE TYPE name AS (attr type, …) MODE DB2SQL      -- structured type
//   CREATE DISTINCT TYPE name AS <base-type> [WITH COMPARISONS]
//
// The two forms are told apart by what follows AS: a parenthesised attribute list means a
// structured type, a bare type name means a distinct (strong-typed alias) type. They can't
// be confused, because `dataType` starts with an `id` and can never start with LPAREN.
// MODE / DB2SQL / COMPARISONS deliberately get no lexer tokens of their own — they are
// matched through the existing `id` rule (MODE and COMPARISONS are perfectly ordinary
// column names), keeping the reserved-word set as small as the rest of this grammar does.
// ─────────────────────────────────────────────────────────────────────────────

createType
    : CREATE (OR REPLACE)? DISTINCT? TYPE routineName AS
      ( LPAREN typeAttribute (COMMA typeAttribute)* RPAREN typeModeClause?
      | dataType )
      (WITH id)? SEMI?
    ;

typeAttribute
    : id dataType
    ;

// MODE DB2SQL — the only mode DB2 defines, but matched generically as two identifiers.
typeModeClause
    : id id
    ;

// ─────────────────────────────────────────────────────────────────────────────
// ALTER TABLE
//
//   ALTER TABLE name
//     { ADD [COLUMN] col type [NOT NULL] [DEFAULT v]
//     | ADD CONSTRAINT [name] FOREIGN KEY (cols) REFERENCES table [(cols)] [rules]
//     | ADD CONSTRAINT [name] PRIMARY KEY (cols)
//     | ADD CONSTRAINT [name] UNIQUE (cols)
//     | ADD CONSTRAINT [name] CHECK ( expr )
//     | DROP CONSTRAINT name
//     | DROP [COLUMN] name
//     | <anything else> }+
//
// The alternatives are ordered so the more specific constraint forms are tried before the
// generic column forms (DROP CONSTRAINT before DROP [COLUMN], since CONSTRAINT is also a
// legal `id`). The FOREIGN KEY … REFERENCES form is spelled out precisely because it is the
// one that carries real lineage: both the altered table and the referenced table become
// table ops. alterActionOther is a permissive tail-eater for the long list of less common
// sub-forms (ALTER COLUMN … SET DATA TYPE, ACTIVATE NOT LOGGED INITIALLY, PCTFREE, …) so an
// exotic clause degrades to "altered table captured, clause ignored" instead of losing the
// whole statement to a parse error. It deliberately cannot start with ADD/DROP, so it never
// steals a well-formed action from the alternatives above it.
// ─────────────────────────────────────────────────────────────────────────────

alterTable
    : ALTER TABLE tableRef (COMMA? alterAction)+ SEMI?
    ;

alterAction
    : ADD CONSTRAINT? id? FOREIGN KEY LPAREN idList RPAREN
      REFERENCES tableRef (LPAREN idList RPAREN)? fkRule*        # alterAddForeignKey
    | ADD CONSTRAINT? id? PRIMARY KEY LPAREN idList RPAREN       # alterAddPrimaryKey
    | ADD CONSTRAINT? id? UNIQUE LPAREN idList RPAREN            # alterAddUnique
    | ADD CONSTRAINT? id? CHECK LPAREN expr RPAREN               # alterAddCheck
    // The trailing REFERENCES clause is the column-level spelling of a foreign key
    // (ADD COLUMN c INT REFERENCES parent(id)); it declares exactly the same relationship
    // as the table-level ADD CONSTRAINT … FOREIGN KEY form above, with the added column
    // itself as the single source column.
    | ADD COLUMN? id dataType (NOT? NULL_KW)? (DEFAULT defaultVal)?
      (REFERENCES tableRef (LPAREN idList RPAREN)? fkRule*)?     # alterAddColumn
    | DROP CONSTRAINT id                                         # alterDropConstraint
    | DROP (PRIMARY KEY | FOREIGN KEY id | UNIQUE id | CHECK id) # alterDropOther
    | DROP COLUMN? id                                            # alterDropColumn
    | alterActionOther                                           # alterOther
    ;

// ON DELETE / ON UPDATE referential actions trailing a FOREIGN KEY clause.
fkRule
    : ON (DELETE | UPDATE) (NO id | id id? )
    ;

alterActionOther
    : ~(SEMI | EOF | ADD | DROP) (~(SEMI | EOF | ADD | DROP))*
    ;

// ─────────────────────────────────────────────────────────────────────────────
// CREATE INDEX
//
//   CREATE [UNIQUE] INDEX name ON table (col [ASC|DESC], …)
//
// The indexed table is reported as a table op (SELECT-flavoured, since an index is a
// read-path object) so lineage sees the dependency, and the key columns are captured
// in order with their sort direction.
// ─────────────────────────────────────────────────────────────────────────────

createIndex
    : CREATE UNIQUE? INDEX routineName ON tableRef
      LPAREN indexColumn (COMMA indexColumn)* RPAREN SEMI?
    ;

indexColumn
    : id (ASC | DESC)?
    ;

// ─────────────────────────────────────────────────────────────────────────────
// CREATE SEQUENCE
//
//   CREATE [OR REPLACE] SEQUENCE name [AS datatype]
//     [START WITH n] [INCREMENT BY n] [{MINVALUE n | NO MINVALUE}]
//     [{MAXVALUE n | NO MAXVALUE}] [{CYCLE | NO CYCLE}] [{CACHE n | NO CACHE}]
//     [{ORDER | NO ORDER}]
//
// The options are order-independent in DB2, so they are modelled as a repeated
// `sequenceOption*` rather than a fixed sequence; the visitor reads whichever ones
// are present. Values may be negative (e.g. INCREMENT BY -1), hence sequenceValue's
// optional MINUS. Sequence *references* (NEXT VALUE FOR seq) are a separate,
// already-supported expression form — this rule covers the DDL *definition*.
// ─────────────────────────────────────────────────────────────────────────────

createSequence
    : CREATE (OR REPLACE)? SEQUENCE routineName (AS dataType)? sequenceOption* SEMI?
    ;

sequenceOption
    : START WITH sequenceValue
    | INCREMENT BY sequenceValue
    | MINVALUE sequenceValue
    | MAXVALUE sequenceValue
    | CACHE INT_NUM
    | CYCLE
    | ORDER
    | NO (MINVALUE | MAXVALUE | CYCLE | CACHE | ORDER)
    ;

sequenceValue
    : MINUS? (INT_NUM | DEC_NUM)
    ;

// ─────────────────────────────────────────────────────────────────────────────
// CREATE VIEW
//
//   CREATE [OR REPLACE] VIEW name [(col, …)] AS <select> [WITH CHECK OPTION]
//
// The defining query reuses the existing `selectStatement` rule, so the view's base
// tables and joins are captured by visitFromClause exactly like any other SELECT.
// A trailing WITH CHECK OPTION / WITH CASCADED CHECK OPTION needs no production of
// its own: selectStatement's permissive `selectRest` catch-all already absorbs it as
// opaque trailing tokens (CHECK/OPTION/CASCADED/LOCAL are not lexer keywords), so the
// unit still parses cleanly without extending the reserved-word set.
// ─────────────────────────────────────────────────────────────────────────────

createView
    : CREATE (OR REPLACE)? VIEW routineName (LPAREN idList RPAREN)? AS selectStatement
    ;

// ─────────────────────────────────────────────────────────────────────────────
// CREATE TRIGGER
//
// DB2 trigger DDL is a top-level CREATE form just like a routine, but the entry
// point previously only accepted CREATE PROCEDURE / CREATE FUNCTION, so the token
// right after CREATE was a syntax error and the whole unit yielded nothing —
// no target table, no body DML, no calls.
//
//   CREATE TRIGGER name
//     [NO CASCADE] {BEFORE | AFTER | INSTEAD OF} {INSERT | DELETE | UPDATE [OF cols]}
//     ON table
//     [REFERENCING (OLD|NEW|OLD_TABLE|NEW_TABLE) AS alias ...]
//     [FOR EACH {ROW | STATEMENT}]
//     [WHEN ( condition )]
//     <body>
//
// The body deliberately reuses `bodyBlock` (the SAME rule a procedure/function body
// uses) so DML, CALLs, cursors and handlers inside a trigger are captured by the
// existing visitor logic with no duplication. A single unbraced statement (DB2 allows
// a triggered action without BEGIN…END) is also accepted.
// OLD/NEW/OLD_TABLE/NEW_TABLE are matched via `id` rather than dedicated keywords:
// they are correlation-name introducers only here and are freely used as ordinary
// identifiers elsewhere.
// ─────────────────────────────────────────────────────────────────────────────

createTrigger
    : CREATE (OR REPLACE)? TRIGGER routineName
      triggerTiming
      triggerEvent
      ON tableRef
      triggerReferencing?
      triggerGranularity?
      triggerWhen?
      triggerBody
    ;

triggerTiming
    : NO CASCADE BEFORE
    | BEFORE
    | AFTER
    | INSTEAD OF
    ;

triggerEvent
    : INSERT
    | DELETE
    | UPDATE (OF idList)?
    ;

triggerReferencing
    : REFERENCING triggerCorrelation+
    ;

triggerCorrelation
    : id AS id
    ;

triggerGranularity
    : FOR EACH ROW
    | FOR EACH STATEMENT
    ;

triggerWhen
    : WHEN LPAREN expr RPAREN
    ;

triggerBody
    : bodyBlock
    | statement
    ;

// ─────────────────────────────────────────────────────────────────────────────
// MODULES — DB2's package analog (task #28)
//
//   CREATE MODULE <name> [;]                 -- empty module; members published later
//   CREATE MODULE <name> ; <declarations> END MODULE ;
//   ALTER MODULE <name> PUBLISH FUNCTION|PROCEDURE <member>(...) ... <body>
//   ALTER MODULE <name> PUBLISH VARIABLE <member> <type> [DEFAULT ...]
//   CREATE VARIABLE [schema.]<name> <type> [DEFAULT ...]
//
// A module groups member routines and module-scoped variables. The catalog column
// SYSCAT.ROUTINES.ROUTINEMODULENAME names the owning module for a member routine; the
// member's key becomes MODULE.SUB (mirroring Oracle's PACKAGE.SUB). Module variables are
// the globals facet's declarations (the Oracle package-globals shape).
//
// The module body is parsed permissively: member routines (so their calls/table ops are
// captured) and CREATE VARIABLE declarations (captured as module globals) are matched
// explicitly; anything else is consumed up to the next CREATE/END. END MODULE is optional
// because DB2 also accepts the empty `CREATE MODULE m` header on its own.
// ─────────────────────────────────────────────────────────────────────────────

createModule
    : CREATE (OR REPLACE)? MODULE moduleName=routineName SEMI?
      moduleDeclaration*
      (END MODULE SEMI?)?
    ;

moduleDeclaration
    : createVariable
    | createType
    | routineDefinition
    | ~(END | CREATE | EOF)
    ;

alterModule
    : ALTER MODULE moduleName=routineName alterModuleAction SEMI?
    ;

alterModuleAction
    : PUBLISH? (FUNCTION | PROCEDURE) memberName=routineName
      LPAREN paramList? RPAREN (RETURNS dataType)? routineCharacteristic* bodyBlock?
    | PUBLISH? VARIABLE memberName=routineName dataType (DEFAULT defaultVal)?
    | ADD? VARIABLE memberName=routineName dataType (DEFAULT defaultVal)?
    | ~(SEMI | EOF)+
    ;

createVariable
    : CREATE (OR REPLACE)? VARIABLE routineName dataType (DEFAULT defaultVal)? SEMI?
    ;

// ─────────────────────────────────────────────────────────────────────────────
// Routine definition
// ─────────────────────────────────────────────────────────────────────────────

routineDefinition
    : CREATE (OR REPLACE)? (PROCEDURE | FUNCTION) routineName
      LPAREN paramList? RPAREN
      (RETURNS dataType)?
      routineCharacteristic*
      bodyBlock?
    ;

// NOTE: REPLACE must be defined in Db2Lexer.g4 as a keyword token.

// Permissive catch-all for routine characteristics that appear between
// the parameter list and BEGIN (e.g. LANGUAGE SQL, NOT DETERMINISTIC, etc.)
// Anything that is not BEGIN, EOF, or the start of another construct is consumed.
routineCharacteristic
    : ~(BEGIN | EOF)+
    ;

bodyBlock
    : BEGIN ATOMIC? body END SEMI?
    ;

routineName
    : (id DOT)? id
    ;

id
    : ID
    | DQ_ID
    // Allow selected non-structural keywords to be used as identifiers where the
    // parser expects an id. STRUCTURAL clause keywords (FROM, INTO, JOIN, ON,
    // WHERE, GROUP, ORDER, BY, HAVING, UNION, AS, SELECT, the join-type words,
    // ALL, DISTINCT, WHEN, THEN, ELSE, DO, …) are deliberately EXCLUDED so that
    // an expression stops at a clause boundary instead of swallowing it.
    // INTERSECT and EXCEPT are also deliberately EXCLUDED (set operators, like UNION)
    // so they cannot be misread as a bare table alias in a FROM clause.
    | ATOMIC | CALL | CASE | CAST | CLOSE | CONDITION | CONTINUE | CREATE | CURRENT
    | CURSOR | DEFAULT | EXISTS | EXIT | FETCH | FUNCTION | HANDLER | HOLD
    | IMMEDIATE | INOUT | IN_KW | IS | LANGUAGE | LIKE | LOOP | NEXT | NULL_KW
    | OPEN | OR | OUT_KW | OVER | PARTITION | PREPARE | PROCEDURE | REPEAT
    | REPLACE | RESIGNAL | RETURNS | RETURN | SET | SIGNAL | SQL_KW
    | SQLEXCEPTION | SQLSTATE | SQLWARNING | UNDO | UNTIL | UPDATE | USING
    | VALUE | VALUES | WHILE | WITH
    // Trigger/view DDL keywords. They are only structural inside createTrigger /
    // createView, so they stay available as ordinary identifiers (column names like
    // ROW, STATEMENT, NO, OF are common) — exactly as the keywords above do.
    | AFTER | BEFORE | CASCADE | EACH | INSTEAD | NO | OF | REFERENCING | ROW
    | STATEMENT | TRIGGER | VIEW
    // Sequence/index DDL keywords. Like the trigger/view ones above they are only
    // structural inside createSequence / createIndex, so they remain usable as plain
    // identifiers elsewhere (CACHE, START, INDEX, ORDER-free columns are common names).
    | CACHE | CYCLE | INCREMENT | INDEX | MAXVALUE | MINVALUE | SEQUENCE | START | UNIQUE
    // MERGE: structural only as the first token of mergeStatement; before it became a lexer
    // token it was a plain ID, so it must stay usable as an ordinary identifier here.
    | MERGE
    // ALTER TABLE keywords. Same rationale again: they are structural only inside
    // alterTable, and several of them (TABLE in DECLARE GLOBAL TEMPORARY TABLE, plus
    // KEY/CHECK/TYPE/COLUMN as everyday column names) were plain identifiers before they
    // became lexer tokens, so they must remain usable as identifiers here.
    | ADD | ALTER | CHECK | COLUMN | CONSTRAINT | DROP | FOREIGN | KEY | PRIMARY
    | REFERENCES | TABLE
    // TYPE: structural only inside createType, and a very common column name otherwise.
    | TYPE
    // MODULE: structural only inside createModule/alterModule; VARIABLE/PUBLISH likewise.
    // They stay usable as ordinary identifiers elsewhere (MODULE/VARIABLE are plausible
    // column names, PUBLISH a plausible one).
    | MODULE | VARIABLE | PUBLISH
    ;

// ─────────────────────────────────────────────────────────────────────────────
// Parameters
// ─────────────────────────────────────────────────────────────────────────────

paramList
    : param (COMMA param)*
    ;

// PROCEDURE params carry a mode (IN/OUT/INOUT); FUNCTION params carry none.
// The mode is therefore OPTIONAL so both forms parse.
param
    : (IN_KW | OUT_KW | INOUT)? id dataType (DEFAULT defaultVal)?
    ;

// DB2 type-modifier suffixes: CHAR(n) / VARCHAR(n) may be followed by a
// "FOR BIT DATA" phrase (binary-column marker). BIT and DATA are not reserved
// keywords in this grammar (no dedicated lexer tokens), so they are matched
// here via the existing `id` rule, which already covers plain identifiers.
dataType
    : id (LPAREN INT_NUM (COMMA INT_NUM)? RPAREN)? (FOR id id)?
    ;

defaultVal
    : literal
    | id
    | MINUS INT_NUM
    ;

// ─────────────────────────────────────────────────────────────────────────────
// Body
// ─────────────────────────────────────────────────────────────────────────────

body
    : statement*
    ;

// ─────────────────────────────────────────────────────────────────────────────
// Statements
// ─────────────────────────────────────────────────────────────────────────────

statement
    : declareGlobalTempTable
    | declareConditionStmt
    | declareVariableStmt
    | declareCursorStmt
    | declareHandlerStmt
    | signalStmt
    | resignalStmt
    | callStatement
    | setStatement
    | insertStatement
    | updateStatement
    | deleteStatement
    | selectStatement
    | ifStatement
    | whileStatement
    | forStatement
    | loopStatement
    | repeatStatement
    | executeImmStmt
    | executeStatement
    | prepareStatement
    | openStatement
    | closeStatement
    | fetchStatement
    | returnStatement
    | transactionStatement
    | beginBlock
    | leaveStatement
    | iterateStatement
    | caseStatement
    | mergeStatement
    | otherStatement
    ;

// ─────────────────────────────────────────────────────────────────────────────
// MERGE INTO <target> [AS alias] USING <source> [AS alias] [(cols)] ON … WHEN …
//
// Previously MERGE had no rule at all and fell through `otherStatement`, so the visitor had
// to regex-scan the raw statement text for "MERGE … INTO x" / "USING y" to recover the two
// table ops. This rule exposes both sides as real parse-tree nodes instead. Declared BEFORE
// otherStatement so it wins; a MERGE shape this rule cannot match still falls back to
// otherStatement (and its legacy text scan), so nothing that used to be captured is lost.
// ─────────────────────────────────────────────────────────────────────────────
mergeStatement
    : MERGE INTO? tableRef (AS? id)? USING mergeSource (AS? id)? (LPAREN idList RPAREN)?
      mergeRest SEMI?
    ;

mergeSource
    : LPAREN selectStatement RPAREN   # mergeSourceSubquery
    // (VALUES (…), …) and other non-SELECT parenthesized sources: consumed permissively with
    // balanced parens, and — like a derived table in FROM — not reported as a base table.
    | LPAREN atomFuncArgs RPAREN      # mergeSourceParen
    | tableRef                        # mergeSourceTable
    ;

// Permissive tail (ON condition + the WHEN [NOT] MATCHED THEN UPDATE/INSERT/DELETE branches).
// Same stop set as otherStatement's continuation, so it consumes exactly what otherStatement
// used to consume for a MERGE.
// A CASE expression inside a WHEN MATCHED THEN UPDATE SET (very common in a MERGE) carries its
// own END, which would otherwise terminate this token run mid-statement: the MERGE then ended at
// that END, the enclosing BEGIN…END block was closed by it too, and the orphaned remainder
// ("WHEN NOT MATCHED THEN …") was re-parsed as garbage that yielded a bogus table op literally
// named "SET". Balanced CASE…END pairs are therefore matched as a unit (same technique
// selectRestToken already uses); the CASE alternative must precede the single-token one, which
// would otherwise also match CASE.
mergeRest
    : mergeRestToken*
    ;

// Bug (MERGE capture gap): a subquery in the ON condition or in a WHEN-branch predicate —
// `ON t.id = s.id AND t.id IN (SELECT id FROM filter_tbl)` — was consumed one opaque token at a
// time by the `~(SEMI|BEGIN|END)` catch-all below, so filter_tbl never became a parse-tree node
// and was never captured. Mirrors selectRestToken's identical first alternative (and the derived
// table/subquery alternatives in tableAtom). Declared FIRST so it wins over the single-token
// alternative, which would otherwise also match the LPAREN. A parenthesized group that is NOT a
// SELECT still falls through to the catch-all exactly as before, so MERGE's captured shape is
// unchanged for every statement that has no such subquery.
mergeRestToken
    : LPAREN selectStatement RPAREN
    | CASE mergeRestToken* END
    | ~(SEMI | BEGIN | END)
    ;

// ─────────────────────────────────────────────────────────────────────────────
// Transaction-control statements (DB2 SQL PL)
//   COMMIT [WORK]
//   ROLLBACK [WORK] [TO SAVEPOINT name]
//   SAVEPOINT name [UNIQUE] ON ROLLBACK RETAIN CURSORS ...
//   RELEASE [TO] SAVEPOINT name
// ─────────────────────────────────────────────────────────────────────────────
transactionStatement
    : COMMIT WORK? SEMI?                        # txnCommit
    | ROLLBACK WORK? TO SAVEPOINT? id SEMI?     # txnRollbackTo
    | ROLLBACK WORK? SEMI?                      # txnRollback
    | SAVEPOINT id savepointOpt* SEMI?         # txnSavepoint
    | RELEASE TO? SAVEPOINT id SEMI?           # txnReleaseSavepoint
    ;

// Trailing options after SAVEPOINT name (UNIQUE / ON ROLLBACK RETAIN CURSORS / …)
savepointOpt
    : ~(SEMI | BEGIN | END | DECLARE | CALL | IF | WHILE | FOR | LOOP | REPEAT | PREPARE
        | EXECUTE | OPEN | CLOSE | FETCH | INSERT | UPDATE | DELETE | SELECT | RETURN
        | SET | CREATE)
    ;

// DECLARE id dataType [DEFAULT expr] ;
// Note: must not match CURSOR declarations — we handle those separately
// DECLARE GLOBAL TEMPORARY TABLE SESSION.name (...) ON COMMIT … — consumed permissively to ';'
// DECLARE GLOBAL TEMPORARY TABLE [schema.]name ( … ) ON COMMIT …
// GLOBAL/TEMPORARY/TABLE are plain identifiers (not lexer tokens); the shape
// "DECLARE id id id [schema.]name (" is specific enough not to collide with a
// normal 2-id variable declaration.
declareGlobalTempTable
    : DECLARE id id id id (DOT id)? LPAREN (~SEMI)* SEMI
    ;

declareVariableStmt
    : DECLARE id dataType (DEFAULT expr)? SEMI
    ;

// DECLARE id CONDITION FOR SQLSTATE 'nnnnn' ;
// Binds a custom condition name to a SQLSTATE value so it can later be referenced by name in
// a DECLARE ... HANDLER FOR <name> clause (handlerCond's bare `id` alternative).
declareConditionStmt
    : DECLARE id CONDITION FOR SQLSTATE VALUE? SQ_STRING SEMI?
    ;

// SIGNAL SQLSTATE '75001' [SET MESSAGE_TEXT = 'x' (, MESSAGE_TEXT = ... )*] ;
// SIGNAL <condition-name> [SET ...] ;
signalStmt
    : SIGNAL SQLSTATE VALUE? SQ_STRING (SET id EQ expr (COMMA id EQ expr)*)? SEMI?
    | SIGNAL id (SET id EQ expr (COMMA id EQ expr)*)? SEMI?
    ;

// RESIGNAL ;                                    (bare re-raise)
// RESIGNAL SQLSTATE '75001' SET MESSAGE_TEXT = 'x' ;   (re-raise with new SQLSTATE/message)
// RESIGNAL <condition-name> [SET ...] ;
resignalStmt
    : RESIGNAL SQLSTATE VALUE? SQ_STRING (SET id EQ expr (COMMA id EQ expr)*)? SEMI?
    | RESIGNAL id (SET id EQ expr (COMMA id EQ expr)*)? SEMI?
    | RESIGNAL SEMI?
    ;

// DECLARE id CURSOR [WITH HOLD] FOR selectStatement
// Also: DECLARE id CURSOR [WITH HOLD] FOR <prepared-statement-name>
//   where the name is an identifier produced by a prior PREPARE statement.
declareCursorStmt
    : DECLARE id CURSOR cursorOption* FOR selectStatement SEMI?
    | DECLARE id CURSOR cursorOption* FOR id SEMI
    ;

// DB2 cursor options: WITH HOLD / WITH RETURN [TO CALLER|TO CLIENT] (CALLER/CLIENT are ids)
cursorOption
    : WITH HOLD
    | WITH RETURN (TO id)?
    ;

// DECLARE (CONTINUE|EXIT|UNDO) HANDLER FOR handlerCondList handlerAction
declareHandlerStmt
    : DECLARE (CONTINUE | EXIT | UNDO) HANDLER FOR handlerCondList handlerAction SEMI?
    ;

handlerCondList
    : handlerCond (COMMA handlerCond)*
    ;

handlerCond
    : NOT id       // NOT FOUND
    | SQLEXCEPTION
    | SQLWARNING
    | SQLSTATE VALUE? SQ_STRING
    | id           // condition name
    ;

handlerAction
    : statement
    ;

// CALL [schema.]name(args)
callStatement
    : CALL routineName LPAREN argList? RPAREN SEMI?
    ;

argList
    : expr (COMMA expr)*
    ;

setStatement
    : SET id EQ expr SEMI?     // SET var = expr
    | SET id id SEMI?          // SET SCHEMA <name>
    | SET CURRENT id id SEMI?  // SET CURRENT SCHEMA <name>
    ;

insertStatement
    : INSERT INTO? tableRef stmtTail SEMI?
    ;

updateStatement
    : UPDATE tableRef stmtTail SEMI?
    ;

deleteStatement
    : DELETE FROM? tableRef stmtTail SEMI?
    ;

// Structured SELECT: optional WITH (CTE), one or more UNIONed cores.
// Enough structure to expose FROM / JOIN / CTE via ANTLR contexts (no text scanning).
selectStatement
    : withClause? selectCore ((UNION ALL? | INTERSECT | EXCEPT) selectCore)* SEMI?
    ;

// Bug (recursive-CTE audit): `WITH RECURSIVE anc (…) AS (…)` — accepted by DB2 for i and by
// newer LUW levels — parsed as nothing at all. RECURSIVE is not a lexer keyword (it is an
// ordinary `id`), so cteDef matched it as the CTE *name* and then choked on the real name that
// followed; the whole selectStatement failed and, silently, the ENTIRE routine yielded zero
// tables and zero operations. The optional leading `id` is therefore predicated on the token
// literally being RECURSIVE, and on the NEXT token being neither `AS` nor `(` — so a CTE that is
// itself named "recursive" (`WITH recursive AS (…)` / `WITH recursive (a,b) AS (…)`) still binds
// that token as the CTE name exactly as before.
withClause
    : WITH ({_input.LT(1).getText().equalsIgnoreCase("RECURSIVE")
             && !_input.LT(2).getText().equalsIgnoreCase("AS")
             && !_input.LT(2).getText().equals("(")}? id)?
      cteDef (COMMA cteDef)*
    ;

cteDef
    : id (LPAREN idList RPAREN)? AS LPAREN selectStatement RPAREN
    ;

idList
    : id (COMMA id)*
    ;

selectCore
    : SELECT (ALL | DISTINCT)? selectList
      intoClause?
      fromClause?
      selectRest
    ;

selectList
    : selectItem (COMMA selectItem)*
    ;

selectItem
    : expr (AS? id)?
    ;

intoClause
    : INTO id (COMMA id)*
    ;

fromClause
    : FROM tableReference (COMMA tableReference)*
    ;

tableReference
    : tableAtom joinClause*
    ;

tableAtom
    // Bug (unique-concepts audit item 13): a table-valued function reference in FROM, e.g.
    // XMLTABLE('/path' PASSING v_doc COLUMNS c1 INTEGER PATH '@id', ...) AS x. Its argument list
    // is not a plain expr list (PASSING/COLUMNS/PATH aren't tokens and the COLUMNS list mixes
    // dataTypes in), so plain `functionCall` cannot match it. Without a dedicated alternative,
    // the bare function name (e.g. "XMLTABLE") alone matched `tableRef`/atomTable and was
    // captured as a bogus base table, while everything after the opening `(` — including the
    // real args and the `AS x` alias — fell through into the permissive selectRest catch-all.
    // Tried BEFORE atomTable: since atomTable alone (`tableRef` with no trailing `(AS? id)?`
    // match) still lets the rest of the statement parse successfully via the permissive
    // selectRest catch-all, ANTLR's ambiguity resolution would otherwise always prefer the
    // shorter, earlier-declared atomTable alt over this one. Consumed permissively (balanced
    // parens) and NOT reported as a table op by the visitor (it is a derived/XML source, not a
    // base table).
    // DB2's TABLE(fn(...)) table-function-in-FROM idiom: `TABLE` here is a plain identifier (not
    // a lexer keyword, see the `id` rule), so it also matches the generic atomFunc alt below —
    // but as a bare permissive token run, so the *inner* fn(...) is never parsed as a real
    // functionCall node and its call never reaches visitFunctionCall. Special-cased ahead of the
    // generic atomFunc so the inner call is still captured (and TABLE(...) itself is correctly
    // not reported as a base table, same as atomFunc).
    : {_input.LT(1).getText().equalsIgnoreCase("TABLE")}? id LPAREN functionCall RPAREN (AS? id)? # atomTableFunctionCall
    // Bug (R3 audit): DB2's data-change-table-reference —
    //   SELECT … FROM FINAL TABLE (INSERT INTO t VALUES (…))
    //   SELECT … FROM OLD TABLE (UPDATE t SET …)   /  NEW TABLE (…)
    // FINAL/OLD/NEW are ordinary `id`s (no lexer tokens), so `FINAL` alone matched atomTable and
    // was reported as a BOGUS base table literally named "FINAL"/"OLD", while the parenthesised
    // INSERT/UPDATE/DELETE fell into the permissive selectRest catch-all — so the routine's
    // actual WRITE was invisible to lineage. Parsing the inner DML as a real statement makes the
    // write visible again (ordinary visitChildren recursion reaches it); the data-change
    // reference itself is a derived source and is NOT reported as a base table.
    // Predicated on the introducer word so a plain table/column named e.g. "final" is untouched.
    | {_input.LT(1).getText().equalsIgnoreCase("FINAL")
       || _input.LT(1).getText().equalsIgnoreCase("OLD")
       || _input.LT(1).getText().equalsIgnoreCase("NEW")}?
      id TABLE LPAREN dataChangeStatement RPAREN (AS? id)? (LPAREN idList RPAREN)?  # atomDataChangeTable
    | id LPAREN atomFuncArgs RPAREN (AS? id)?             # atomFunc
    // Bug (temporal-table audit): a period specification on a temporal table —
    // `FROM policy FOR SYSTEM_TIME AS OF '2020-01-01'` — had no rule at all. `tableRef` matched
    // `policy`, the optional alias did not match the FOR that followed, and FOR is excluded from
    // selectRest's catch-all (it is also the cursor/FOR-loop keyword), so selectCore failed, the
    // whole statement failed, and — because otherStatement cannot start with SELECT either — the
    // ENTIRE routine yielded zero tables and zero statements with errors=[]. Same silent
    // total-loss signature as the WITH RECURSIVE and XMLQUERY bugs. Measured, not assumed.
    | tableRef temporalSpec? (AS? id)?                    # atomTable
    | LPAREN selectStatement RPAREN (AS? id)?             # atomSubquery
    // Bug (R3 audit): a VALUES list used as a derived table — `FROM (VALUES (1),(2)) AS v(x)`.
    // VALUES is a lexer token but also a legal `id`, so `(VALUES (1),(2))` reached atomParen →
    // tableReference → atomFunc, which matched only `VALUES (1)` and then could not match the
    // `,(2)` that followed (an INT_NUM is not a tableRef). Nothing else matched either, so the
    // whole statement failed and the ENTIRE routine yielded zero tables and zero statements with
    // errors=[] — the round-2 silent total-loss signature again. Consumed permissively with
    // balanced parens and, being a literal row constructor rather than a base table, not
    // reported as a table op. Declared after atomSubquery (a real `(SELECT …)` still wins).
    | LPAREN VALUES atomFuncArgs RPAREN (AS? id)? (LPAREN idList RPAREN)?  # atomValues
    | LPAREN tableReference (COMMA tableReference)* RPAREN # atomParen
    ;

atomFuncArgs
    : (LPAREN atomFuncArgs RPAREN | ~(LPAREN | RPAREN))*
    ;

// The DML nested inside a data-change-table-reference (FINAL/OLD/NEW TABLE (…)). Reuses the
// SAME rules the top-level statements use, so the write is captured by the existing visitor
// logic with no duplication.
dataChangeStatement
    : (insertStatement | updateStatement | deleteStatement | mergeStatement) dataChangeTail
    ;

// At top level, `UPDATE t SET a = 2` works because updateStatement's stmtTail stops at SET (SET
// starts a statement) and the remainder is then parsed as a separate setStatement. Inside the
// parentheses of a data-change-table-reference there is no "next statement" to absorb it, so the
// leftover clause has to be consumed here or the whole reference fails to match (observed: OLD
// TABLE (UPDATE …) fell back to atomTable and re-introduced the bogus "OLD" base table).
// A nested SELECT is matched as a real selectStatement rather than as opaque tokens, so an
// `INSERT INTO t SELECT … FROM src` inside the parentheses still contributes src to lineage.
dataChangeTail
    : (LPAREN dataChangeTail RPAREN | selectStatement | ~(LPAREN | RPAREN))*
    ;

// ─────────────────────────────────────────────────────────────────────────────
// Temporal-table period specification (DB2 10.1+ system-period / application-period
// temporal tables):
//
//   FOR SYSTEM_TIME   AS OF <v>  |  FROM <v> TO <v>  |  BETWEEN <v> AND <v>
//   FOR BUSINESS_TIME AS OF <v>  |  FROM <v> TO <v>  |  BETWEEN <v> AND <v>
//
// SYSTEM_TIME / BUSINESS_TIME are NOT lexer keywords (they are ordinary `id`s, and are
// perfectly legal column names), so the leading FOR is gated on a semantic predicate that the
// NEXT token is literally one of those two period names. Without the predicate this rule would
// be reachable for any `FROM t FOR …`, which is exactly the shape of the "FOR BIT DATA" type
// suffix and of the cursor/FOR-loop keyword — the predicate keeps it from stealing anything.
//
// The period bounds are a deliberately CLOSED, small set rather than the general `expr` rule:
// `expr` is a greedy `exprAtom+` and would swallow the table's trailing alias (`… AS OF CURRENT
// TIMESTAMP p`) and, in the BETWEEN form, the `AND` separator plus the second bound (AND is
// itself an exprAtom). A bounded temporalValue keeps the alias and both bounds intact.
// ─────────────────────────────────────────────────────────────────────────────
temporalSpec
    : {_input.LT(2).getText().equalsIgnoreCase("SYSTEM_TIME")
       || _input.LT(2).getText().equalsIgnoreCase("BUSINESS_TIME")}?
      FOR id ( AS OF temporalValue
             | FROM temporalValue TO temporalValue
             | BETWEEN temporalValue AND temporalValue )
    ;

// CURRENT TIMESTAMP / CURRENT DATE are spelled `CURRENT id`; CURRENT is a lexer token while
// TIMESTAMP/DATE are ordinary ids.
temporalValue
    : castExpr
    | CURRENT id?
    | functionCall
    | literal
    | id (DOT id)?
    | QMARK
    | COLON id
    ;

joinClause
    : joinType? JOIN tableAtom (ON expr)?
    ;

joinType
    : INNER
    | LEFT OUTER?
    | RIGHT OUTER?
    | FULL OUTER?
    | CROSS
    ;

// Permissive trailing clauses (WHERE / GROUP BY / HAVING / ORDER BY / fetch-first …).
// Stops at UNION, a nested SELECT, or a statement boundary.
selectRest
    : selectRestToken*
    ;

selectRestToken
    : LPAREN selectStatement RPAREN          // WHERE-clause subquery: IN/EXISTS/= (SELECT … FROM …)
    | LPAREN selectRestToken* RPAREN
    | CASE selectRestToken* END
    | ~(SEMI | BEGIN | END | CASE | UNION | SELECT | DECLARE | CALL | IF | WHILE
        | FOR | LOOP | REPEAT | PREPARE | EXECUTE | OPEN | CLOSE | FETCH | INSERT
        | UPDATE | DELETE | RETURN | SET | CREATE | LPAREN | RPAREN)
    ;

// IF expr THEN body [ELSEIF expr THEN body]* [ELSE body] END IF [;]
ifStatement
    : IF expr THEN body
      (ELSEIF expr THEN body)*
      (ELSE body)?
      END IF SEMI?
    ;

// ─────────────────────────────────────────────────────────────────────────────
// CASE as a STATEMENT.
//
//   CASE WHEN cond THEN stmts; [WHEN ...] [ELSE stmts;] END CASE;
//   CASE expr WHEN val  THEN stmts; [WHEN ...] [ELSE stmts;] END CASE;
//
// This grammar already handled CASE...END as an *expression* (see stmtToken and
// selectRestToken), which is a different thing and did not help here: a statement-level CASE
// matched no alternative, so the enclosing body failed and the WHOLE procedure yielded nothing —
// zero tables, zero operations, no error. Measured, not assumed.
//
// otherStatement could not absorb it either, and deliberately so: it stops at the first SEMI, which
// in a CASE lands *inside* the first THEN branch, leaving a dangling `ELSE ...` that no rule can
// start. ELSE/WHEN are excluded from otherStatement on purpose — letting the catch-all swallow a
// branch would silently drop every statement inside it, which is the same class of loss.
//
// So the branches are parsed as real bodies. DML inside a THEN or ELSE is found, rather than being
// skipped along with the construct that contains it.
// ─────────────────────────────────────────────────────────────────────────────
caseStatement
    : CASE expr? caseWhenClause+ (ELSE body)? END CASE SEMI?
    ;

caseWhenClause
    : WHEN expr THEN body
    ;

// WHILE expr DO body END WHILE [;]
whileStatement
    : WHILE expr DO body END WHILE SEMI?
    ;

// FOR var AS cursor DO body END FOR [;]
// Also: FOR label AS cursor-name CURSOR FOR select DO body END FOR
forStatement
    : FOR id AS selectStatement DO body END FOR SEMI?                              // FOR row AS <select> DO … END FOR
    | FOR id (AS id?)? CURSOR? FOR selectStatement DO body END FOR SEMI?
    ;

// DB2 labeled loop: `my_loop: LOOP ... END LOOP my_loop;`. Leading label and
// trailing repeated label after END LOOP are both optional.
loopStatement
    : (id COLON)? LOOP body END LOOP id? SEMI?
    ;

repeatStatement
    : REPEAT body UNTIL expr END REPEAT SEMI?
    ;

// EXECUTE IMMEDIATE must come BEFORE EXECUTE stmt (ANTLR takes first match)
executeImmStmt
    : EXECUTE IMMEDIATE (id | SQ_STRING) SEMI?
    ;

executeStatement
    : EXECUTE id (USING argList)? SEMI?
    ;

prepareStatement
    : PREPARE id FROM (id | SQ_STRING) SEMI?
    ;

openStatement
    : OPEN id SEMI?
    ;

closeStatement
    : CLOSE id SEMI?
    ;

fetchStatement
    : FETCH (NEXT? FROM)? id INTO argList SEMI?
    ;

returnStatement
    : RETURN expr? SEMI?
    ;

// DB2 labeled compound statement: `my_label: BEGIN ... END my_label;`. The
// leading `id COLON` and trailing `id` (repeating the label after END) are both
// optional so plain unlabeled BEGIN...END blocks still parse unchanged.
beginBlock
    : (id COLON)? BEGIN ATOMIC? body END id? SEMI?
    ;

// DB2 `LEAVE <label>` — exits the labeled compound statement or loop.
leaveStatement
    : LEAVE id SEMI?
    ;

// DB2 `ITERATE <label>` — jumps to the next iteration of the labeled loop.
iterateStatement
    : ITERATE id SEMI?
    ;

// Catch-all for unrecognized statements: consume tokens until semicolon or block boundary
otherStatement
    // CREATE-prefixed statements unsupported INSIDE a body (CREATE INDEX / CREATE SEQUENCE /
    // CREATE VARIABLE / CREATE TYPE / CREATE ALIAS …). These are top-level DDL forms with no
    // body-level rule; before this alternative, one of them made the enclosing bodyBlock fail
    // wholesale — the WholeBodyLossGate failure mode (the DML around it vanished). Consumed
    // permissively to the next semicolon; no identity/table-op is recorded (matching the gate's
    // "silently losing an unsupported statement is acceptable, losing its neighbours is not").
    // CREATE stays excluded from the general catch-all below so a malformed CREATE still cannot
    // be swallowed as an arbitrary token run.
    : CREATE ~(SEMI | BEGIN | END)* SEMI?
    | ~(SEMI | BEGIN | END | DECLARE | CALL | IF | WHILE | FOR | LOOP | REPEAT | PREPARE
        | EXECUTE | OPEN | CLOSE | FETCH | INSERT | UPDATE | DELETE | SELECT | RETURN
        | SET | CREATE
        // Block-continuation keywords never START a statement; excluding them keeps the
        // catch-all from swallowing an ELSE/ELSEIF/THEN/… branch of an enclosing IF/CASE/
        // loop (which would drop every CALL/DML inside that branch — see reserve_stock bug).
        | ELSE | ELSEIF | THEN | DO | UNTIL | WHEN)
      (~(SEMI | BEGIN | END))* SEMI?
    | SEMI
    ;

// ─────────────────────────────────────────────────────────────────────────────
// Table reference  (schema.table or just table)
// ─────────────────────────────────────────────────────────────────────────────

// Bug (3-part catalog audit): extended from a hard 2-part cap (id (DOT id)?) to allow a
// 3-part catalog.schema.table reference. extractTableName()/extractTableSchema() below take
// the LAST id as the table name and the second-to-last as the schema, so any earlier
// (catalog) segment is simply not attributed to schema/table rather than mis-splitting.
tableRef
    : id (DOT id)? (DOT id)?
    ;

// ─────────────────────────────────────────────────────────────────────────────
// stmtTail — permissive rule that eats tokens up to next statement boundary.
// Recursively handles balanced parentheses and CASE...END.
// ─────────────────────────────────────────────────────────────────────────────

stmtTail
    : stmtToken*
    ;

stmtToken
    : LPAREN stmtToken* RPAREN
    | CASE stmtToken* END
    // NEXT VALUE FOR seq / id FOR seq (NEXTVAL/PREVVAL FOR) — sequence references nested inside a
    // permissively-consumed clause (e.g. an INSERT ... VALUES(...) tuple). Without these two
    // alternatives here, the standalone FOR token was excluded below (it is also the cursor/
    // numeric FOR-loop keyword), so `NEXT VALUE FOR order_seq` / `NEXTVAL FOR shipment_seq` inside
    // VALUES(...) were swallowed as opaque tokens and never reached visitFunctionCall/
    // recordSequenceRef's sequence-recording path — dropping the sequence reference entirely.
    | NEXT VALUE FOR (id DOT)? id
    | id FOR (id DOT)? id
    | functionCall
    | ~(SEMI | BEGIN | END | CASE | DECLARE | CALL | IF | WHILE | FOR | LOOP
        | REPEAT | PREPARE | EXECUTE | OPEN | CLOSE | FETCH | INSERT | UPDATE
        | DELETE | SELECT | RETURN | SET | CREATE | LPAREN | RPAREN)
    ;

// ─────────────────────────────────────────────────────────────────────────────
// Expression (simplified — enough to parse conditions and SET rhs)
// ─────────────────────────────────────────────────────────────────────────────

expr
    : exprAtom+
    ;

exprAtom
    : castExpr
    | NEXT VALUE FOR (id DOT)? id             // sequence: NEXT VALUE FOR seq
    | id FOR (id DOT)? id                     // sequence (Oracle-compat): NEXTVAL/PREVVAL FOR seq
    // OLAP window clause: `<aggregate> OVER ( [PARTITION BY …] [ORDER BY …] [frame] )`.
    // OVER is a lexer token but also a legal `id`, so `OVER ()` (an empty window spec, as in
    // `COUNT(*) OVER ()`) matched `functionCall` and was recorded as a user-routine call edge
    // literally named "OVER" — a false positive in the call tree. (The non-empty spellings
    // `OVER (PARTITION BY …)` / `OVER (ORDER BY …)` escaped this only by accident: BY and ORDER
    // are not valid expr operands, so functionCall was not viable and the permissive
    // balanced-paren alternative below matched instead.) Declared BEFORE functionCall so every
    // window clause takes this alternative uniformly and none of them reaches visitFunctionCall.
    | OVER LPAREN atomFuncArgs RPAREN
    | functionCall
    // Bug (XML/JSON audit): a built-in whose argument list is NOT a plain expr list —
    // `XMLQUERY('…' PASSING doc AS "d")`, `XMLELEMENT(NAME "x", …)`, `SUBSTR(s FROM 1 FOR 2)`,
    // `LISTAGG(c, ',') WITHIN GROUP (…)`. PASSING/FROM/AS are not expr operands, so functionCall
    // could not match, nothing else could either, and the enclosing selectList → selectCore →
    // statement failed — silently dropping EVERY table of the query (the same failure mode the
    // tableAtom #atomFunc alternative was added for, in FROM position). Declared AFTER
    // functionCall so an ordinary call still matches that rule (and still reaches
    // visitFunctionCall); this permissive, balanced-paren form is only reached when the real
    // rule is not viable. It is deliberately not reported as a call — its contents were never
    // parsed as expressions.
    | id (DOT id)? LPAREN atomFuncArgs RPAREN
    | LPAREN selectStatement RPAREN          // scalar subquery:  SET v = (SELECT … FROM …)
    | LPAREN expr? RPAREN
    | CASE stmtToken* END
    | literal
    // Qualified star, e.g. `SELECT o.*, c.name FROM …`. Without this, expr matched only
    // the `o` and stopped at the DOT, so selectList → selectCore → the whole statement
    // failed to parse — silently, with no syntax error — and every table in the query was
    // lost. Must precede the plain `id (DOT id)*` alternative. (Pre-existing gap, hit by
    // any query using a qualified star; surfaced by the CREATE VIEW work.)
    | id (DOT id)* DOT STAR
    | id (DOT id)*
    | STAR
    | EQ | NEQ | LT | GT | LEQ | GEQ | PLUS | MINUS | SLASH | PERCENT
    | AND | OR | NOT | IN_KW | LIKE | IS | BETWEEN | EXISTS | NULL_KW
    | CURRENT
    | AMPERSAND | PIPE | CARET | TILDE | COLON
    | QMARK                                   // parameter marker in prepared/dynamic SQL
    ;

// CAST ( expr AS dataType ) — reuses the shared dataType rule for the target type.
castExpr
    : CAST LPAREN expr AS dataType RPAREN
    ;

// [schema.]name(args) — a function invocation in an expression.
functionCall
    : id (DOT id)? LPAREN functionArgs? RPAREN
    ;

functionArgs
    : STAR
    | expr (COMMA expr)*
    ;

literal
    : SQ_STRING
    | INT_NUM
    | DEC_NUM
    | NULL_KW
    ;
