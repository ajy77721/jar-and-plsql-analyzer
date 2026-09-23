// ============================================================
// PostgresParser.g4
// Parser grammar for IBM POSTGRES SQL PL stored procedures / functions.
// Entry point: compilationUnit
// tokenVocab = PostgresLexer
// ============================================================
parser grammar PostgresParser;

options { tokenVocab = PostgresLexer; }

// ─────────────────────────────────────────────────────────────────────────────
// Entry point
// ─────────────────────────────────────────────────────────────────────────────

compilationUnit
    : (routineDefinition | doStatement | createTrigger | createView | createSequence
      | createIndex | alterTable | createType | createDomain | createTableTop)? EOF
    ;

// ─────────────────────────────────────────────────────────────────────────────
// CREATE TABLE  (top-level statement)
//
// CREATE TABLE was only ever reachable as a `statement` INSIDE a routine/DO body (the
// `createTableStmt` alternative of `createStatement`). A standalone schema script —
// `CREATE TABLE t (...);` with no enclosing routine — was therefore not a compilationUnit
// alternative at all and failed outright with
// `line 1:7 no viable alternative at input 'CREATETABLE'`, yielding zero table ops. Since
// every other top-level DDL form (SEQUENCE / INDEX / VIEW / TRIGGER / TYPE / DOMAIN /
// ALTER TABLE) is already a compilationUnit alternative, CREATE TABLE was the odd one out.
//
// The rule mirrors the in-body `createTableStmt` exactly and shares `createTableTailItem`
// with it, so both paths accept the same syntax and feed the same visitor logic. The only
// difference is the terminator: a top-level script's final statement often has no trailing
// `;`, so SEMI is optional here (compilationUnit's own EOF closes the parse), whereas the
// in-body form still requires the `;` that separates it from the next statement.
createTableTop
    : CREATE createTempModifier? TABLE (IF NOT EXISTS)? tableRef createTableTailItem* SEMI?
    ;

// ─────────────────────────────────────────────────────────────────────────────
// CREATE TYPE / CREATE DOMAIN  (top-level statements)
//
//   CREATE TYPE name AS ENUM ('a', 'b', ...)          -- enumerated type
//   CREATE TYPE name AS (col type, ...)               -- composite type
//   CREATE TYPE name                                  -- shell type (forward declaration)
//   CREATE DOMAIN name [AS] base_type [constraint ...]
//
// Like a sequence or a trigger, these are pure metadata: no body, no query. So each rule only
// has to capture the object's identity and its shape (the enum's labels, the composite's
// attributes, the domain's base type and CHECK predicate). Before they existed, TYPE/ENUM/DOMAIN
// were not tokens at all and neither statement was a compilationUnit alternative, so either one
// was a hard parse failure at the top level.
createType
    : CREATE TYPE routineName AS ENUM LPAREN SQ_STRING (COMMA SQ_STRING)* RPAREN SEMI?  # createEnumType
    | CREATE TYPE routineName AS LPAREN typeAttribute (COMMA typeAttribute)* RPAREN SEMI? # createCompositeType
    | CREATE TYPE routineName SEMI?                                                     # createShellType
    ;

typeAttribute
    : id dataType
    ;

createDomain
    : CREATE DOMAIN routineName AS? dataType domainConstraint* SEMI?
    ;

// NOT NULL / NULL / DEFAULT / CHECK, in any order and any number (PostgreSQL allows several
// CHECKs). The DEFAULT value is consumed as a permissive token run rather than an `expr`
// because CHECK and CONSTRAINT are both legal `id`s (see the `id` rule), so a greedy `expr`
// would swallow a following CHECK clause as if it were part of the default expression.
domainConstraint
    : (CONSTRAINT id)? CHECK LPAREN expr RPAREN   # domainCheck
    | (CONSTRAINT id)? NOT NULL_KW                # domainNotNull
    | (CONSTRAINT id)? NULL_KW                    # domainNull
    | DEFAULT domainDefaultValue                  # domainDefault
    ;

domainDefaultValue
    : ~(CHECK | CONSTRAINT | NOT | NULL_KW | SEMI | EOF)+
    ;

// ─────────────────────────────────────────────────────────────────────────────
// ALTER TABLE  (top-level statement AND a real `statement` alternative inside a routine body)
//
//   ALTER TABLE [IF EXISTS] [ONLY] table action [, action ...]
//
// Severity note: before this rule existed, ALTER/TABLE/ADD/CONSTRAINT/FOREIGN/REFERENCES were
// not tokens at all, so an ALTER TABLE written INSIDE a procedure/function body matched
// `otherStatement`'s catch-all and was consumed token-by-token with ZERO reported errors — the
// altered table and, worse, the FK-referenced table were both lost silently. `alterTable` is
// therefore listed in `statement` AHEAD of `otherStatement` so that path is now structured.
//
// `alterAction` is precise for the forms whose operands are real lineage (FOREIGN KEY …
// REFERENCES <table> gives both table names) and deliberately permissive elsewhere:
// `alterActionTail`/`alterActionOther` eat up to the next COMMA or the statement terminator, so
// an unrecognized sub-form (ALTER COLUMN … TYPE …, RENAME …, OWNER TO …, SET/RESET options) can
// never turn the whole statement into a parse error.
alterTable
    : ALTER TABLE (IF EXISTS)? tableRef alterAction (COMMA alterAction)* SEMI?
    ;

// Ordering matters here and is deliberate:
//   * the FOREIGN KEY / PRIMARY KEY / UNIQUE / CHECK alternatives come BEFORE `ADD [COLUMN]`,
//     because CONSTRAINT/PRIMARY/UNIQUE/CHECK are all legal `id`s (see the `id` rule) and would
//     otherwise be misread as a new column's name;
//   * `DROP CONSTRAINT` comes BEFORE `DROP [COLUMN]` for exactly the same reason.
alterAction
    : ADD (CONSTRAINT id?)? FOREIGN KEY LPAREN idList RPAREN
      REFERENCES tableRef (LPAREN idList RPAREN)? alterActionTail?   # alterAddForeignKey
    | ADD (CONSTRAINT id?)? PRIMARY KEY LPAREN idList RPAREN alterActionTail?   # alterAddPrimaryKey
    | ADD (CONSTRAINT id?)? UNIQUE LPAREN idList RPAREN alterActionTail?        # alterAddUnique
    | ADD (CONSTRAINT id?)? CHECK LPAREN expr RPAREN alterActionTail?           # alterAddCheck
    | ADD COLUMN? (IF NOT EXISTS)? id dataType alterActionTail?                 # alterAddColumn
    | DROP CONSTRAINT (IF EXISTS)? id alterActionTail?                          # alterDropConstraint
    | DROP COLUMN? (IF EXISTS)? id alterActionTail?                             # alterDropColumn
    | alterActionOther                                                          # alterActionUnknown
    ;

alterActionTail
    : ~(COMMA | SEMI | EOF)+
    ;

alterActionOther
    : ~(COMMA | SEMI | EOF)+
    ;

// ─────────────────────────────────────────────────────────────────────────────
// CREATE INDEX  (top-level statement)
//
//   CREATE [UNIQUE] INDEX [CONCURRENTLY] [IF NOT EXISTS] [name]
//       ON table [USING method] ( elem [, ...] )
//       [INCLUDE (...)] [WITH (...)] [TABLESPACE t]
//       [WHERE predicate]
//
// The index name is OPTIONAL in PostgreSQL (`CREATE INDEX ON orders(status)` lets the server
// generate one), which is why `routineName?` is optional here — ON is a keyword token and can
// never match `id`, so there is no ambiguity between "the name" and "the ON clause".
//
// An index element is a full expression, not just a column: `lower(email)`, `(a + b)` and
// `col DESC NULLS LAST` are all legal, so `indexElem` reuses `expr` and then allows the
// ordering suffix. ASC/DESC are tokens outside the `id` rule, so `expr`'s greedy `exprAtom+`
// stops before them instead of swallowing the sort direction as a column.
//
// `indexTail` permissively consumes INCLUDE/WITH/TABLESPACE between the column list and the
// partial-index predicate; it stops at WHERE so the predicate is still a real `expr` the
// visitor (and any nested function calls in it) can see.
createIndex
    : CREATE UNIQUE? INDEX CONCURRENTLY? (IF NOT EXISTS)? routineName?
      ON tableRef
      (USING id)?
      LPAREN indexElem (COMMA indexElem)* RPAREN
      indexTail?
      (WHERE expr)?
      SEMI?
    ;

indexElem
    : expr (ASC | DESC)?
    ;

indexTail
    : ~(WHERE | SEMI | EOF)+
    ;

// ─────────────────────────────────────────────────────────────────────────────
// CREATE SEQUENCE  (top-level statement)
//
//   CREATE [TEMPORARY] SEQUENCE [IF NOT EXISTS] name
//       [AS datatype]
//       [START [WITH] n] [INCREMENT [BY] n]
//       [MINVALUE n | NO MINVALUE] [MAXVALUE n | NO MAXVALUE]
//       [CYCLE | NO CYCLE] [CACHE n]
//       [OWNED BY table.column | OWNED BY NONE]
//
// A sequence is pure metadata (no body, no query), so like CREATE TRIGGER this rule only has to
// capture the sequence's identity plus its options. PostgreSQL allows the options in ANY order
// and each at most once, which `sequenceOption*` models directly (over-permissive about
// repetition, which is the right trade-off for an analyzer). `sequenceTail` mops up the
// lower-priority tails (OWNED BY …, and anything PostgreSQL adds later) so an unrecognized
// option can never turn the whole statement into a parse error.
createSequence
    : CREATE SEQUENCE (IF NOT EXISTS)? routineName sequenceOption* sequenceTail? SEMI?
    ;

sequenceOption
    : AS dataType                       # seqAsType
    | START WITH? signedNumber          # seqStart
    | INCREMENT BY? signedNumber        # seqIncrement
    | MINVALUE signedNumber             # seqMinValue
    | NO MINVALUE                       # seqNoMinValue
    | MAXVALUE signedNumber             # seqMaxValue
    | NO MAXVALUE                       # seqNoMaxValue
    | CYCLE                             # seqCycle
    | NO CYCLE                          # seqNoCycle
    | CACHE signedNumber                # seqCache
    ;

signedNumber
    : (PLUS | MINUS)? (INT_NUM | DEC_NUM)
    ;

sequenceTail
    : ~(SEMI | EOF)+
    ;

// ─────────────────────────────────────────────────────────────────────────────
// CREATE [MATERIALIZED] VIEW  (top-level statement)
//
//   CREATE [OR REPLACE] [MATERIALIZED] VIEW name [(col, ...)] [WITH (opts)]
//       AS <select>
//       [WITH [CASCADED|LOCAL] CHECK OPTION | WITH [NO] DATA]
//
// The defining query is a real `selectStatement`, so the view's underlying tables and joins
// are captured by exactly the same fromClause/tableReference visitor paths every other SELECT
// uses — nothing view-specific is needed for table extraction.
//
// `viewPreAs` / `viewTail` consume the optional column list, storage options and trailing
// WITH CHECK OPTION / WITH [NO] DATA clauses permissively (same approach as SQL Server's
// `viewHeader : ~AS+`), so no keyword token is needed for any of them.
createView
    : CREATE (OR REPLACE)? MATERIALIZED? VIEW routineName
      viewPreAs?
      AS selectStatement
      viewTail?
      SEMI?
    ;

viewPreAs
    : ~(AS | SEMI | EOF)+
    ;

viewTail
    : ~(SEMI | EOF)+
    ;

// ─────────────────────────────────────────────────────────────────────────────
// CREATE TRIGGER  (top-level statement)
//
//   CREATE [OR REPLACE] [CONSTRAINT] TRIGGER name
//       {BEFORE | AFTER | INSTEAD OF} event [OR event]*
//       ON table
//       [FOR [EACH] {ROW | STATEMENT}]
//       [WHEN (condition)]
//       EXECUTE {FUNCTION | PROCEDURE} func_name (args)
//
// In PostgreSQL a CREATE TRIGGER statement is pure METADATA: it has no body of its own — the
// row-level logic lives in a separately defined trigger FUNCTION (`CREATE FUNCTION ... RETURNS
// TRIGGER AS $$ ... $$`, which already parses via routineDefinition). So this rule only needs
// to capture the trigger's own metadata plus the EXECUTE FUNCTION reference, which is the
// dependency edge from the trigger to the function that actually does the work.
//
// `triggerTail` permissively consumes the optional trailing clauses PostgreSQL allows between
// the target table and EXECUTE (FROM referenced_table, [NOT] DEFERRABLE, INITIALLY …,
// REFERENCING OLD/NEW TABLE AS …) without needing a token for each of them; it stops at FOR,
// WHEN and EXECUTE so the structured clauses below are still matched by their own rules.
createTrigger
    : CREATE (OR REPLACE)? TRIGGER routineName
      triggerTiming
      triggerEvent (OR triggerEvent)*
      ON tableRef
      triggerTail?
      triggerForEach?
      triggerWhen?
      EXECUTE (FUNCTION | PROCEDURE) routineName LPAREN argList? RPAREN SEMI?
    ;

triggerTiming
    : BEFORE
    | AFTER
    | INSTEAD OF
    ;

// UPDATE may name the columns it fires on: `AFTER UPDATE OF col1, col2 ON t`.
triggerEvent
    : INSERT
    | DELETE
    | UPDATE (OF idList)?
    ;

triggerTail
    : ~(FOR | WHEN | EXECUTE | SEMI | EOF)+
    ;

triggerForEach
    : FOR EACH? (ROW | STATEMENT)
    ;

triggerWhen
    : WHEN LPAREN expr RPAREN
    ;

// Anonymous PL/pgSQL code block: DO [LANGUAGE lang] $tag$ <body> $tag$ [;]
// The engine's preprocess() step blanks the dollar-quote delimiters (and any trailing
// LANGUAGE clause after the closing tag) in place before this grammar ever sees the text,
// so by the time it reaches this rule it reads as: DO <bodyBlock>. Reuses bodyBlock (the
// same BEGIN…END rule a routine body uses) so DO-block statements go through the exact
// same visitor paths (table ops, calls, cursors, exception handlers, …) as a routine body.
doStatement
    : DO (LANGUAGE id)? bodyBlock SEMI?
    ;

// ─────────────────────────────────────────────────────────────────────────────
// Routine definition
// ─────────────────────────────────────────────────────────────────────────────

routineDefinition
    : CREATE (OR REPLACE)? (PROCEDURE | FUNCTION) routineName
      LPAREN paramList? RPAREN
      (RETURNS returnType)?
      (routineCharacteristic | preBeginDeclareSection)*
      bodyBlock?
    ;

// NOTE: REPLACE must be defined in PostgresLexer.g4 as a keyword token.

// Permissive catch-all for routine characteristics that appear between
// the parameter list and BEGIN (e.g. LANGUAGE SQL, NOT DETERMINISTIC, etc.)
// Anything that is not BEGIN, EOF, or the start of another construct is consumed.
//
// DECLARE is deliberately excluded here (bug 9 fix): without the exclusion this catch-all
// swallowed a real "DECLARE ... BEGIN" section — the idiomatic PL/pgSQL variable-declaration
// form — whole, silently discarding every variable in it. preBeginDeclareSection (below) is
// tried first (routineDefinition lists it as an alternative to this rule) whenever the next
// token is DECLARE, so this catch-all now only ever fires for genuine non-DECLARE
// characteristics (LANGUAGE, cost/rows clauses, etc.).
routineCharacteristic
    : ~(BEGIN | EOF | DECLARE)+
    ;

// The idiomatic PL/pgSQL "DECLARE ... BEGIN" variable section: a single DECLARE keyword
// followed by one-or-more plain "name type [:= | DEFAULT | = expr];" items (no per-item
// DECLARE, unlike declareVariableStmt used inside a body). Reuses dataType/expr from
// declareVariableStmt rather than duplicating the declaration shape.
preBeginDeclareSection
    : DECLARE preBeginDeclareItem+
    ;

preBeginDeclareItem
    // Bug 5 (§DML-audit): a cursor declared inside the idiomatic one-DECLARE-keyword block
    // (`DECLARE c1 CURSOR FOR SELECT ...; v_cnt INT; BEGIN ...`) has no per-item DECLARE
    // keyword, so declareCursorStmt's `DECLARE id CURSOR ... FOR selectStatement` never
    // matches past the first item — this alternative (tried first) covers that form directly
    // so the cursor's SELECT is captured for table extraction on every path.
    // Bug 4 (§dynamic-cursor-audit): cursor parameters — `cur1 CURSOR (p_id INT) FOR SELECT ...`.
    // The parameter list is optional and sits between the CURSOR keyword and the (also optional)
    // WITH HOLD / FOR clause; reuses paramList (the same rule the routine's own parameter list
    // uses) rather than duplicating the parameter shape.
    : id CURSOR (LPAREN paramList RPAREN)? (WITH HOLD)? FOR selectStatement SEMI
    // Bug 3 (§comment-style-audit): DB2/Oracle-style "CURSOR name FOR ..." word order (keyword
    // before name) is not standard PL/pgSQL — real Postgres only ever places the cursor name
    // first — but source ported from those dialects sometimes keeps this ordering. Accepting it
    // here is unambiguous: the alternative starts with the CURSOR token, which the id-first
    // alternative above and the plain-variable alternative below can never start with, so this
    // cannot swallow or shadow any existing valid declaration.
    | CURSOR id (LPAREN paramList RPAREN)? (WITH HOLD)? FOR selectStatement SEMI
    | id dataType ((DEFAULT | ASSIGN | EQ) expr)? SEMI
    ;

bodyBlock
    : BEGIN ATOMIC? body exceptionSection? END SEMI?
    ;

// PL/pgSQL exception handler section: EXCEPTION WHEN cond [OR cond]* THEN <stmts> ...
// Statements inside (e.g. ROLLBACK) are visited normally, so they are captured.
exceptionSection
    : EXCEPTION exceptionHandler+
    ;

exceptionHandler
    : WHEN handlerCond (OR handlerCond)* THEN statement*
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
    | ATOMIC | CALL | CASE | CAST | CLOSE | CONDITION | CONFLICT | CONTINUE | CREATE | CURRENT
    | CURSOR | DEFAULT | EXISTS | EXIT | FETCH | FUNCTION | HANDLER | HOLD
    | FOREACH | IMMEDIATE | INOUT | IN_KW | IS | LANGUAGE | LIKE | LOOP | MERGE | NEXT | NULL_KW
    | OPEN | OR | OUT_KW | OVER | PARTITION | PREPARE | PROCEDURE | REPEAT
    | REPLACE | RESIGNAL | RETURNS | RETURN | SET | SIGNAL | SQL_KW
    | SQLEXCEPTION | SQLSTATE | SQLWARNING | UNDO | UNTIL | UPDATE
    | VALUE | VALUES | WHILE | WITH
    // CREATE TRIGGER keywords. None of them is structural OUTSIDE the createTrigger rule (which
    // matches them as explicit tokens), and all of them are legal unquoted identifiers in real
    // PostgreSQL, so they are listed here to guarantee that promoting them from ID to keyword
    // tokens cannot regress any existing routine that uses one as a column/table/variable name.
    | AFTER | BEFORE | EACH | INSTEAD | OF | ROW | STATEMENT | TRIGGER
    // CREATE [MATERIALIZED] VIEW keywords — same rationale as the trigger keywords above.
    | MATERIALIZED | VIEW
    // CREATE SEQUENCE keywords — same rationale again. These are structural only inside the
    // createSequence rule; everywhere else they must keep behaving as ordinary identifiers.
    | CACHE | CYCLE | INCREMENT | MAXVALUE | MINVALUE | NO | SEQUENCE | START
    // CREATE INDEX keywords — same rationale. (USING stays excluded, see the note below.)
    | CONCURRENTLY | INDEX | UNIQUE
    // ALTER TABLE keywords — same rationale. Each is structural only inside `alterTable`, and
    // words like "table", "add", "drop", "key", "check", "type" are common column/variable names
    // in ported source, so they must keep working as ordinary identifiers everywhere else.
    | ALTER | TABLE | ADD | DROP | COLUMN | CONSTRAINT | FOREIGN | KEY | PRIMARY
    | REFERENCES | CHECK
    // CREATE TYPE / CREATE DOMAIN keywords — same rationale again.
    | TYPE | ENUM | DOMAIN
    // TRUNCATE was promoted from ID to a keyword token so `truncateStatement` can recognise it;
    // it is listed here so that anything already using "truncate" as a plain identifier
    // (column/variable/function name) keeps parsing exactly as before.
    | TRUNCATE
    // USING is deliberately EXCLUDED (unlike the rest of this permissive list): EXECUTE ... USING
    // and MERGE ... USING are both clause boundaries, and letting expr's `exprAtom+` swallow the
    // USING keyword as a bare identifier atom meant the EXECUTE statement's own USING-bound
    // arguments were absorbed into the preceding dynamic-SQL expr instead of being split out —
    // e.g. `EXECUTE '...' USING 0` parsed as one atom-run ending only at the terminating SEMI.
    ;

// ─────────────────────────────────────────────────────────────────────────────
// Parameters
// ─────────────────────────────────────────────────────────────────────────────

paramList
    : param (COMMA param)*
    ;

// PROCEDURE params carry a mode (IN/OUT/INOUT); FUNCTION params carry none.
// The mode is therefore OPTIONAL so both forms parse.
//
// Real-corpus bug E (§postgres-real-corpus): the parameter NAME was mandatory. In PostgreSQL it
// is optional — a signature may declare a bare type — and `pg_get_functiondef()` emits exactly
// that form for every built-in, e.g.
//     CREATE OR REPLACE FUNCTION pg_catalog.unnest(anyarray)
//     CREATE OR REPLACE FUNCTION pg_catalog.set_config(text, text, boolean)
// Both of these are pulled into the real analysis as callees, and both failed catastrophically
// at the SIGNATURE, not in the body: `id dataType` consumed the single word as the name and then
// demanded a type, so the closing paren (or the following comma) was "extraneous" and the parse
// derailed into the top-level DDL alternatives.
//
// The named form is listed FIRST so it still wins wherever a name is present; the anonymous form
// is reachable only when there is no second word to be the type.
param
    : (IN_KW | OUT_KW | INOUT | variadicKw)? id dataType (DEFAULT defaultVal)?
    | (IN_KW | OUT_KW | INOUT | variadicKw)? dataType (DEFAULT defaultVal)?
    ;

// VARIADIC is not promoted to a lexer keyword (it is a legal identifier); matched by the same
// case-insensitive left-edge ID predicate used for TEMP / INHERITS / ONLY / NATURAL elsewhere.
variadicKw
    : { _input.LT(1).getText().equalsIgnoreCase("VARIADIC") }? ID
    ;

// The RETURNS clause of a function. Real-corpus bug E, second half: `RETURNS SETOF anyelement`
// (every set-returning function, `unnest` included) was not expressible — `dataType` matched
// `SETOF` itself as the type and left `anyelement` dangling. SETOF is deliberately NOT promoted
// to a lexer keyword (same rationale as VARIADIC above); the predicate is on the left edge of the
// optional prefix, so a function genuinely returning a type called "setof" is unaffected.
// `RETURNS TABLE (col type, …)` reuses aliasColumnList, which already models exactly that shape.
returnType
    : setofKw? dataType
    | TABLE aliasColumnList
    ;

setofKw
    : { _input.LT(1).getText().equalsIgnoreCase("SETOF") }? ID
    ;

dataType
    : id (DOT id)? (LPAREN INT_NUM (COMMA INT_NUM)? RPAREN)? (LBRACK INT_NUM? RBRACK)*
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
    : declareVariableStmt
    | declareCursorStmt
    | declareHandlerStmt
    | callStatement
    | performStatement
    | assignStatement
    | setStatement
    | mergeStatement
    | insertStatement
    | updateStatement
    | deleteStatement
    | truncateStatement
    | selectStatement
    | ifStatement
    | caseStatement
    | whileStatement
    | forStatement
    | loopStatement
    | repeatStatement
    | executeImmStmt
    | executeStatement
    | createStatement
    // Must precede otherStatement: ALTER TABLE inside a routine body was previously swallowed
    // by that catch-all with zero errors and zero table capture (see the alterTable rule above).
    | alterTable
    | prepareStatement
    | openStatement
    | closeStatement
    | fetchStatement
    | returnStatement
    | transactionStatement
    | beginBlock
    | raiseStatement
    // Real-corpus bug B (§postgres-real-corpus): `EXIT WHEN NOT FOUND;` — the single most common
    // PL/pgSQL loop-exit idiom — had NO rule at all. EXIT/CONTINUE are members of the permissive
    // `id` list, so a bare `EXIT;` was absorbed by otherStatement's catch-all; but otherStatement
    // deliberately excludes WHEN from BOTH its first-token set and its continuation set (so an
    // IF's ELSE/WHEN branch can never be swallowed), which meant the run stopped dead on the
    // WHEN token and the `WHEN NOT FOUND` remainder had no rule to land in — one hard parse error
    // per cursor loop, in every routine of the real corpus. Listed immediately before
    // otherStatement so it wins the ambiguity with the catch-all while never shadowing
    // assignStatement (`exit := 1;` still needs the ASSIGN token, which this rule cannot match).
    | exitStatement
    | otherStatement
    ;

// PL/pgSQL loop control: `EXIT [label] [WHEN cond];` / `CONTINUE [label] [WHEN cond];`
// WHEN is deliberately excluded from the `id` rule, so the optional label can never swallow the
// WHEN keyword of the guard.
exitStatement
    : (EXIT | CONTINUE) id? (WHEN expr)? SEMI?
    ;

// PL/pgSQL RAISE statement: `RAISE [level] 'format', args...` where level is one of
// DEBUG/LOG/INFO/NOTICE/WARNING/EXCEPTION (or bare RAISE / RAISE '<condition>'). RAISE is not a
// reserved keyword in this grammar (matched as a bare `id`), so without a dedicated rule
// `RAISE EXCEPTION ...` is swallowed by otherStatement's catch-all, which explicitly stops at
// the EXCEPTION token (since EXCEPTION also opens an exceptionSection) -- causing a spurious
// syntax error on the EXCEPTION keyword. Match RAISE explicitly so EXCEPTION can be consumed
// as part of the statement here instead.
raiseStatement
    : {_input.LT(1).getText().equalsIgnoreCase("RAISE")}? id EXCEPTION? (~(SEMI))* SEMI?
    ;

// ─────────────────────────────────────────────────────────────────────────────
// Transaction-control statements (POSTGRES SQL PL)
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
        | SET | CREATE | MERGE)
    ;

// DECLARE id dataType [DEFAULT expr] ;
// Note: must not match CURSOR declarations — we handle those separately
declareVariableStmt
    : DECLARE id dataType ((DEFAULT | ASSIGN | EQ) expr)? SEMI
    ;

// DECLARE id CURSOR [WITH HOLD] FOR selectStatement
// Also: DECLARE id CURSOR [WITH HOLD] FOR <prepared-statement-name>
//   where the name is an identifier produced by a prior PREPARE statement.
declareCursorStmt
    // Bug 4 (§dynamic-cursor-audit): optional parameter list, mirrored from the idiomatic
    // preBeginDeclareItem cursor alternative above (reuses paramList; see comment there).
    : DECLARE id CURSOR (LPAREN paramList RPAREN)? (WITH HOLD)? FOR selectStatement SEMI?
    | DECLARE id CURSOR (LPAREN paramList RPAREN)? (WITH HOLD)? FOR id SEMI
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

// PL/pgSQL PERFORM <call>; — the idiomatic way to invoke a function purely for its side
// effects, discarding the return value (a bare `foo();` is not valid PL/pgSQL; only PROCEDUREs
// use CALL). PERFORM is not a reserved keyword in this grammar (matched as a bare `id`), so
// without a dedicated rule it fell through to otherStatement's opaque catch-all and the call was
// silently dropped from the call graph.
performStatement
    : {_input.LT(1).getText().equalsIgnoreCase("PERFORM")}? id routineName LPAREN argList? RPAREN SEMI?
    ;

argList
    : expr (COMMA expr)*
    ;

// PL/pgSQL assignment:  target := expr ;   (target may be qualified: rec.field)
// The RHS expr is visited so any function call (e.g. erp.fn_total(x)) is captured as a call.
// PL/pgSQL CASE statement (distinct from the CASE expression):
//   CASE [search] WHEN … THEN <stmts> … [ELSE <stmts>] END CASE;
caseStatement
    : CASE expr? (WHEN expr (COMMA expr)* THEN statement*)+ (ELSE statement*)? END CASE SEMI?
    ;

// PL/pgSQL assignment:  target[:=]expr ;   target may be qualified/subscripted (rec.field, arr[i]).
assignStatement
    : id (DOT id)* (LBRACK expr RBRACK)? ASSIGN expr SEMI?
    ;

setStatement
    : SET id EQ expr SEMI?
    ;

// INSERT [INTO] table ...  optionally followed by an ON CONFLICT (upsert) tail.
// The leading stmtTail stops at CONFLICT (excluded from stmtToken), having consumed
// the "ON" keyword; onConflictClause then recognizes the upsert action.
// Bugs 7 & 8 (§DML-audit): `stmtTail`/`stmtToken` deliberately exclude SELECT (so a
// parenthesized subquery elsewhere is handled by the dedicated `subquery` alt, not swallowed
// as opaque tokens) — but that meant an unparenthesized `INSERT INTO t (cols) SELECT ...`
// source select was never consumed by this rule at all: `stmtTail` stopped right after the
// column list, leaving the whole SELECT (and everything after it — the ON CONFLICT tail, the
// RETURNING clause) as leftover text for the parser to resync on independently. That resync
// is what fabricated a phantom `UPDATE` table op named `SET` (recovering at the `UPDATE` token
// inside `DO UPDATE SET ...` and reading `SET` as `updateStatement`'s tableRef, since `id`
// permits `SET` as an identifier) and dropped `onConflictClause`/`returningClause` entirely
// for this form. The optional `selectStatement` here keeps the whole statement — column list,
// source SELECT, ON CONFLICT tail, and RETURNING — inside one parse tree.
insertStatement
    : INSERT INTO? tableRef stmtTail selectStatement? onConflictClause? returningClause? SEMI?
    ;

// RETURNING col [, col ...] | RETURNING * — trailing clause on INSERT/UPDATE/DELETE.
returningClause
    : RETURNING returningItem (COMMA returningItem)*
    ;

returningItem
    : STAR
    | expr (AS? id)?
    ;

// ON CONFLICT [ target ] (DO NOTHING | DO UPDATE SET ...) [RETURNING ...].
// The "ON" was already consumed by the preceding stmtTail; this rule starts at
// the CONFLICT token. Its body is consumed permissively up to the statement end.
onConflictClause
    : CONFLICT conflictToken*
    ;

conflictToken
    : LPAREN conflictToken* RPAREN
    | ~(SEMI | BEGIN | END | LPAREN | RPAREN | RETURNING)
    ;

// MERGE INTO target USING source ON cond WHEN [NOT] MATCHED THEN (UPDATE|INSERT|DELETE) ...
// Recognized as a top-level statement (PG15+). Body consumed permissively to the
// terminating ';'. Parenthesized subqueries (USING (SELECT …)) are exposed as real
// selectStatements so nested table ops / subqueries are still captured.
mergeStatement
    : MERGE mergeToken* SEMI?
    ;

mergeToken
    : subquery
    | LPAREN mergeToken* RPAREN
    | CASE mergeToken* END
    | ~(SEMI | BEGIN | END | CASE | LPAREN | RPAREN)
    ;

updateStatement
    : UPDATE tableRef stmtTail returningClause? SEMI?
    ;

deleteStatement
    : DELETE FROM? tableRef stmtTail returningClause? SEMI?
    ;

// TRUNCATE [TABLE] [ONLY] t [, t2 …] [RESTART IDENTITY] [CASCADE|RESTRICT]
// Previously there was no rule at all: TRUNCATE fell into otherStatement's opaque catch-all,
// so a routine whose only data-changing operation was a TRUNCATE reported zero table ops,
// zero statements and errors=[] — the silent-loss signature, and the truncated table was
// invisible to the dependency graph.
truncateStatement
    : TRUNCATE TABLE? onlyKw? tableRef (COMMA onlyKw? tableRef)* (~(SEMI | BEGIN | END))* SEMI?
    ;

// `ONLY` is not a lexer keyword here (it would arrive as a plain ID and be mistaken for the
// table name), so it is recognised by literal text under a predicate.
onlyKw
    : {_input.LT(1).getText().equalsIgnoreCase("ONLY")}? id
    ;

// Structured SELECT: optional WITH (CTE), one or more UNIONed cores.
// Enough structure to expose FROM / JOIN / CTE via ANTLR contexts (no text scanning).
selectStatement
    : withClause? selectCore ((UNION ALL? | INTERSECT | EXCEPT) selectCore)* SEMI?
    ;

withClause
    : WITH RECURSIVE? cteDef (COMMA cteDef)*
    ;

cteDef
    : id (LPAREN idList RPAREN)? AS LPAREN selectStatement RPAREN
    ;

idList
    : id (COMMA id)*
    ;

selectCore
    : selectKeyword (ALL | DISTINCT)? selectList
      intoClause?
      fromClause?
      selectRest
    ;

// PL/pgSQL's `PERFORM <query>` is *exactly* a SELECT with the SELECT keyword replaced by
// PERFORM (the result is discarded). `performStatement` above only covers the degenerate
// `PERFORM fn(args);` call form; anything with a real query shape — `PERFORM 1 FROM t WHERE …`,
// `PERFORM * FROM a JOIN b …` — previously matched neither performStatement nor selectStatement
// and fell into otherStatement's opaque catch-all, producing ZERO tableOps/statements with
// errors=[] (silent total loss when such a PERFORM was the routine's only query). PERFORM is
// not a reserved token in this lexer, so it arrives as a plain `id` and is gated by a predicate
// on the literal text — nothing else can enter through this alternative.
selectKeyword
    : SELECT
    | {_input.LT(1).getText().equalsIgnoreCase("PERFORM")}? id
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

// Sep05-11: `LATERAL` may precede an atom both in a comma-separated FROM list
// (`FROM a, LATERAL (SELECT …) x`) and after a JOIN (`CROSS JOIN LATERAL (…)`), so it is optional
// in both places. It is a modifier on the atom, not a table, and carries no name of its own.
//
// To be accurate about what each half of this fix does: the *lexer token* is what removes the
// phantom table, by stopping LATERAL from matching `id`. These two rules were measured and made no
// difference to the tables, joins or error count of a LATERAL query. They are kept because
// without them LATERAL would be a token appearing in no parser rule at all, so every LATERAL
// query would fall out of the structured SELECT path into the permissive catch-all — parsing by
// accident rather than by rule, which is how the next defect gets in.
tableReference
    : LATERAL? tableAtom joinClause*
    ;

// Round-3 bug (§WITH ORDINALITY): `FROM srf(...) WITH ORDINALITY [AS] alias(col, ord)`. WITH is a
// real token AND a legal `id`, so the optional bare alias greedily matched the WITH keyword itself;
// ORDINALITY and the column-alias list were then left unconsumed, which ended the fromClause early
// and let `selectRest`'s catch-all swallow everything after it — INCLUDING a following
// `JOIN other_table ON …`, whose table was dropped from the analysis with errors=[] (a SILENT
// capture loss, the most dangerous class). `withOrdinality` is matched ahead of the alias with a
// left-edge predicate (ORDINALITY is a plain ID, not a keyword, so no new reserved word is
// introduced), and `aliasColumnList` consumes the function's column-alias list so the FROM item
// ends where it really ends. Both are attached to `atomFunc` ONLY — the table/subquery atoms are
// untouched.
tableAtom
    : functionCall withOrdinality? tableAtomAlias? aliasColumnList?   # atomFunc
    | tableRef tableAtomAlias?                            # atomTable
    | LPAREN selectStatement RPAREN tableAtomAlias?       # atomSubquery
    | LPAREN tableReference (COMMA tableReference)* RPAREN # atomParen
    ;

// `WITH ORDINALITY` — only valid directly after a set-returning function in FROM. ORDINALITY is
// deliberately NOT promoted to a lexer keyword (it is a perfectly legal column/table name); the
// left-edge predicate checks both tokens before the rule is entered, so the optional
// `withOrdinality?` decision never fires on a plain `WITH`-named alias or on a CTE's WITH.
withOrdinality
    : { _input.LT(1).getType() == WITH
        && _input.LT(2).getText().equalsIgnoreCase("ORDINALITY") }? WITH ID
    ;

// The column-alias list a FROM-item function may carry: `unnest(a) AS t(v, ord)`,
// `json_to_recordset(x) AS r(id int, name text)`. Column types are allowed (RECORD-returning
// functions require them), so each item is `id dataType?`.
aliasColumnList
    : LPAREN id dataType? (COMMA id dataType?)* RPAREN
    ;

// `USING (col, …)` is matched explicitly (as in MySqlParser's joinCondition) rather than being
// left to error recovery: USING is a real keyword token that selectRest deliberately excludes,
// so `JOIN b USING (id)` had no rule to land in. The token run is bounded by the closing paren,
// so it cannot swallow an EXECUTE/MERGE `USING` clause.
joinClause
    : joinType? JOIN LATERAL? tableAtom (ON expr | joinUsing)?
    ;

joinUsing
    : USING LPAREN id (COMMA id)* RPAREN
    ;

// Optional table alias. Written as two alternatives (rather than `AS? id`) so the bare-alias
// form carries a left-edge predicate: without it the greedy optional alias swallows the plain-ID
// keyword NATURAL in `FROM orders NATURAL JOIN cust` (no explicit alias), which downgrades the
// join type. An alias explicitly introduced with AS is unrestricted.
tableAtomAlias
    : AS id
    | { !_input.LT(1).getText().equalsIgnoreCase("NATURAL") }? id
    ;

// NATURAL is not a lexer keyword (it arrives as a plain ID), so `NATURAL JOIN` / `NATURAL LEFT
// JOIN` previously failed to match joinClause and the joined table was silently dropped from the
// FROM list. Matched here with the same case-insensitive ID predicate used for TEMP/GLOBAL above.
naturalKw
    : { _input.LT(1).getText().equalsIgnoreCase("NATURAL") }? ID
    ;

joinType
    : naturalKw (INNER | LEFT OUTER? | RIGHT OUTER? | FULL OUTER?)?
    | INNER
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
    : subquery
    | LPAREN selectRestToken* RPAREN
    | CASE selectRestToken* END
    // Bugs 7 & 8 (§DML-audit): CONFLICT and RETURNING added to the exclusion set. Without
    // them, an unparenthesized `INSERT INTO t (cols) SELECT ... ON CONFLICT ...`/`... RETURNING
    // ...` had its trailing ON CONFLICT / RETURNING clause greedily swallowed as opaque
    // selectRestToken text (stopping only at UPDATE/SET, which are excluded) — leaving
    // insertStatement's own onConflictClause?/returningClause? nothing to match, and the
    // leftover "...DO UPDATE SET ..." text for the parser to resync on independently (which is
    // what fabricated a phantom `UPDATE` table op named `SET`, since `id` permits SET as an
    // identifier).
    | ~(SEMI | BEGIN | END | CASE | UNION | SELECT | DECLARE | CALL | IF | WHILE
        | FOR | LOOP | REPEAT | PREPARE | EXECUTE | OPEN | CLOSE | FETCH | INSERT
        | UPDATE | DELETE | RETURN | SET | CREATE | LPAREN | RPAREN | CONFLICT | RETURNING)
    ;

// IF expr THEN body [ELSEIF expr THEN body]* [ELSE body] END IF [;]
ifStatement
    : IF expr THEN body
      ((ELSEIF | ELSIF) expr THEN body)*
      (ELSE body)?
      END IF SEMI?
    ;

// WHILE expr DO body END WHILE [;]
whileStatement
    : WHILE expr DO body END WHILE SEMI?          // DB2 form
    | WHILE expr LOOP body END LOOP SEMI?         // PL/pgSQL form
    ;

// FOR var AS cursor DO body END FOR [;]
// Also: FOR label AS cursor-name CURSOR FOR select DO body END FOR
forStatement
    : FOR id (AS id?)? CURSOR? FOR selectStatement DO body END FOR SEMI?           // DB2 cursor-for style
    | FOR id (COMMA id)* IN_KW selectStatement LOOP body END LOOP SEMI?            // PL/pgSQL FOR rec IN <query> LOOP … END LOOP
    | FOR id (COMMA id)* IN_KW forExprTail LOOP body END LOOP SEMI?                // PL/pgSQL FOR i IN [REVERSE] 1..N [BY s] LOOP
    | FOREACH id (COMMA id)* IN_KW forExprTail LOOP body END LOOP SEMI?            // PL/pgSQL FOREACH x IN ARRAY … LOOP … END LOOP
    ;

// The integer-range tail. The lexer tokenizes "1..10" loosely (1. then .10), so consume the
// range expression permissively up to LOOP.
forExprTail
    : ~(LOOP | SEMI | BEGIN | END)+
    ;

loopStatement
    : LOOP body END LOOP SEMI?
    ;

repeatStatement
    : REPEAT body UNTIL expr END REPEAT SEMI?
    ;

// EXECUTE IMMEDIATE must come BEFORE EXECUTE stmt (ANTLR takes first match)
executeImmStmt
    : EXECUTE IMMEDIATE (id | SQ_STRING) SEMI?
    ;

executeStatement
    : EXECUTE expr (INTO id (COMMA id)*)? (USING expr (COMMA expr)*)? SEMI?   // PL/pgSQL EXECUTE format(...) [INTO v] [USING a,b]
    ;

// PL/pgSQL / DDL inside a routine body: CREATE [TEMP|TEMPORARY|UNLOGGED] TABLE …, CREATE INDEX …, etc.
// Consumed permissively up to the terminating ';'.
// CREATE TABLE is broken out as its own labelled alternative so the table name comes from a
// real `tableRef` parse-tree node (schema-qualification included) instead of being recovered by
// a regex over the raw statement text. TEMP / TEMPORARY / UNLOGGED are not lexer keywords (they
// arrive as plain ID), so they are matched via a case-insensitive predicate on the ID token —
// that keeps the accepted set byte-for-byte the same as the regex it replaces and prevents
// `CREATE UNIQUE INDEX …` (also ID + …) from being mistaken for a table. Every other CREATE
// variant still falls through to the permissive, opaque `createOtherStmt` alternative exactly
// as before.
createStatement
    : CREATE createTempModifier? TABLE (IF NOT EXISTS)? tableRef createTableTailItem* SEMI  # createTableStmt
    | CREATE (~SEMI)* SEMI                                                                  # createOtherStmt
    ;

// The column list / storage options / constraint tail of a CREATE TABLE, consumed opaquely one
// token at a time. Shared by the in-body `createTableStmt` and the top-level `createTableTop`
// so the two forms can never drift apart. EOF is excluded as well as SEMI (the old `~SEMI` run
// admitted EOF) so the loop can never run off the end of input in the top-level form, where the
// terminating semicolon is optional.
// The `AS <query>` of a CREATE TABLE … AS SELECT was previously consumed by the opaque
// `~(SEMI | EOF)` catch-all, so the SOURCE tables of a CTAS were invisible (only the new table
// was recorded) — MySQL and SQL Server both capture the source of their equivalent
// CREATE TABLE … AS SELECT / SELECT … INTO. `selectCore` (not `selectStatement`) is used so the
// statement's own terminating SEMI is left for createTableStmt to match. A column-definition
// `… GENERATED ALWAYS AS (expr) STORED` cannot enter this alternative because `selectCore`
// requires a leading SELECT/PERFORM keyword.
createTableTailItem
    : inheritsClause
    | AS withClause? selectCore
    | ~(SEMI | EOF)
    ;

// `... ) INHERITS (parent [, parent ...])` — PostgreSQL table inheritance. The child table was
// already captured as a CREATE op, but the parent was swallowed by the opaque tail, so the
// child->parent dependency was lost entirely.
//
// INHERITS is deliberately NOT promoted to a lexer keyword: doing so would make it a reserved
// word that could no longer be used as a column/table name, which is a change in accepted input.
// Instead it is matched by a case-insensitive predicate on the ID token — exactly the technique
// `createTempModifier` already uses for TEMP / TEMPORARY / UNLOGGED in this same rule. The
// predicate is at the left edge of the alternative, so an ID that is not "INHERITS" (or one not
// followed by a parenthesised table list) falls straight through to the opaque `~(SEMI|EOF)`
// alternative and behaves exactly as it did before.
inheritsClause
    : { _input.LT(1).getText().equalsIgnoreCase("INHERITS") }? ID
      LPAREN tableRef (COMMA tableRef)* RPAREN
    ;

createTempModifier
    : createTempScope? { _input.LT(1).getText().equalsIgnoreCase("TEMP")
     || _input.LT(1).getText().equalsIgnoreCase("TEMPORARY")
     || _input.LT(1).getText().equalsIgnoreCase("UNLOGGED") }? ID
    ;

// Postgres accepts the noise words GLOBAL / LOCAL in front of TEMP[ORARY]
// (`CREATE GLOBAL TEMPORARY TABLE …`, `CREATE LOCAL TEMP TABLE …`). Like TEMP itself these are
// plain IDs, so they are matched with the same case-insensitive predicate; anything else in that
// position (e.g. UNIQUE in `CREATE UNIQUE INDEX …`) still falls through to createOtherStmt.
createTempScope
    : { _input.LT(1).getText().equalsIgnoreCase("GLOBAL")
     || _input.LT(1).getText().equalsIgnoreCase("LOCAL") }? ID
    ;

prepareStatement
    : PREPARE id FROM (id | SQ_STRING) SEMI?
    ;

// Bug 2 (§dynamic-cursor-audit): `OPEN cur FOR EXECUTE <expr> [USING ...]` and
// `OPEN cur FOR <select>` — a refcursor opened directly against dynamic SQL or an inline SELECT,
// distinct from opening a previously DECLAREd cursor. Both new alternatives start with the
// unambiguous OPEN id FOR prefix (the plain-OPEN alternative below never has a FOR token after
// id), and EXECUTE vs. a bare selectStatement are themselves distinguished by the EXECUTE
// keyword, so no ambiguity is introduced against the existing plain/parameterized OPEN forms.
// Bug 4 (§dynamic-cursor-audit): `OPEN cur(args)` — opening a cursor declared with parameters
// (see declareCursorStmt/preBeginDeclareItem) passes the actual argument values here.
openStatement
    : OPEN id FOR EXECUTE expr (USING expr (COMMA expr)*)? SEMI?
    | OPEN id FOR selectStatement
    | OPEN id (LPAREN argList? RPAREN)? SEMI?
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

beginBlock
    : BEGIN ATOMIC? body exceptionSection? END SEMI?
    ;

// Catch-all for unrecognized statements: consume tokens until semicolon or block boundary
// ELSE/ELSEIF/THEN/WHEN are excluded from BOTH the first-token set and the continuation set:
// `body : statement*` inside `IF … THEN body (ELSE body)? END IF` would otherwise let
// otherStatement start on the ELSE keyword and swallow the entire ELSE branch up to the next
// semicolon — so `ELSE SELECT count(*) INTO v FROM t_else;` produced no table op, no statement
// and errors=[] (silent loss of every ELSE/ELSIF-branch statement). Excluding them from the
// continuation set as well stops a preceding opaque statement from eating the branch keyword.
otherStatement
    : ~(SEMI | BEGIN | END | DECLARE | CALL | IF | WHILE | FOR | LOOP | REPEAT | PREPARE
        | EXECUTE | OPEN | CLOSE | FETCH | INSERT | UPDATE | DELETE | SELECT | RETURN
        | SET | CREATE | MERGE | EXCEPTION | ELSE | ELSEIF | ELSIF | THEN | WHEN)
      (~(SEMI | BEGIN | END | EXCEPTION | ELSE | ELSEIF | ELSIF | THEN | WHEN))* SEMI?
    | SEMI
    ;

// ─────────────────────────────────────────────────────────────────────────────
// Table reference  (schema.table or just table)
// ─────────────────────────────────────────────────────────────────────────────

// Bug (3-part db.schema.table audit): extended from a hard 2-part cap (id (DOT id)?) to allow a
// 3-part db.schema.table-looking reference. extractTableName()/extractTableSchema() below take
// the LAST id as the table name and the second-to-last as the schema, so an earlier (database)
// segment is simply not attributed to schema/table rather than mis-splitting.
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
    : subquery
    // CONFLICT is (deliberately) a member of the permissive `id` list, so `functionCall`
    // happily matched the `CONFLICT (a)` of an `INSERT … VALUES (…) ON CONFLICT (a) DO …`
    // as if it were a call, swallowing the conflict target and leaving insertStatement's
    // `onConflictClause?` nothing to match: no UPSERT statement was recorded, and the
    // orphaned `DO UPDATE SET a = …` tail was resynced as a separate updateStatement,
    // fabricating a phantom table op named `SET`. Only the parenthesized-target form was
    // affected (bare `ON CONFLICT DO …` has no LPAREN for functionCall to latch onto),
    // which is why it survived the earlier ON CONFLICT audit.
    | {_input.LT(1).getType() != PostgresParser.CONFLICT}? functionCall
    | LPAREN stmtToken* RPAREN
    | CASE stmtToken* END
    | ~(SEMI | BEGIN | END | CASE | DECLARE | CALL | IF | WHILE | FOR | LOOP
        | REPEAT | PREPARE | EXECUTE | OPEN | CLOSE | FETCH | INSERT | UPDATE
        | DELETE | SELECT | RETURN | RETURNING | SET | CREATE | MERGE | CONFLICT | LPAREN | RPAREN)
    ;

// A parenthesized SELECT used as a scalar / IN / EXISTS subquery in an expression
// or a permissive clause tail. Exposes a real selectStatement so the visitor can
// emit a SUBQUERY record and still descend for nested table ops / calls.
// Real-corpus bugs C & D (§postgres-real-corpus): `subquery` is listed FIRST in `exprAtom`,
// `stmtToken`, `selectRestToken` and `mergeToken`, so every one of those decisions has to choose
// between it and a plain parenthesized group on seeing `(`. `selectCore` reaches `selectKeyword`,
// whose PERFORM alternative is `{...equalsIgnoreCase("PERFORM")}? id` — and that predicate is NOT
// on the left edge of the decision (it sits one token past the LPAREN), so ANTLR's ALL(*)
// prediction cannot evaluate it and treats it as TRUE. `( <any identifier> …` therefore looked
// like a viable subquery, and being the lowest-numbered alternative it won; the parser then
// really entered selectStatement, evaluated the predicate for real, and died with
// "no viable alternative at input '<that identifier>'".
//
// That single defect produced BOTH of the remaining real-corpus statement-body errors:
//   * `… || (v_js->>'count')`            — a parenthesized JSONB expression in a CALL argument
//   * `… INSERT (cols) VALUES (nextval('erp.erp_mrp_seq'), …)` inside a MERGE
// and every other `( identifier …)` group with more than one atom in it (`(a + 1)`, `(v_m).amount`
// …). Single-atom groups such as `(v_js)` survived only by accident: selectStatement can also
// match a one-item select list, so the wrong alternative happened to succeed.
//
// The fix is a left-edge predicate — evaluated at the decision start, where LT(1) is the LPAREN
// itself — that admits this rule only when the token after the paren really can begin a query:
// SELECT, a CTE's WITH, or the PERFORM spelling of a query. Everything else now falls through to
// the parenthesized-expression alternative, which is what it always should have matched.
subquery
    : { _input.LT(2).getType() == PostgresParser.SELECT
     || _input.LT(2).getType() == PostgresParser.WITH
     || _input.LT(2).getText().equalsIgnoreCase("PERFORM") }?
      LPAREN selectStatement RPAREN
    ;

// ─────────────────────────────────────────────────────────────────────────────
// Expression (simplified — enough to parse conditions and SET rhs)
// ─────────────────────────────────────────────────────────────────────────────

expr
    : exprAtom+
    ;

exprAtom
    : subquery
    | castExpr
    | functionCall
    | LPAREN expr? RPAREN (DOT id)* (LBRACK expr (COMMA expr)* RBRACK)*
    | CASE stmtToken* END
    | literal
    // Bug 6 (§DML-audit): a qualified wildcard (`c.*`, e.g. `SELECT c.* FROM customers c JOIN
    // ...`) previously had no alternative here — `id (DOT id)*` requires an `id` after each
    // DOT, so it matched only the bare alias and left the `.` / `*` unconsumed, breaking the
    // selectItem/selectList parse and losing the whole FROM (and its JOIN) to error recovery.
    // That is what actually dropped the second CTE's own joined table — not a table-op wiring
    // bug in processTableReference. Tried before the plain-identifier alt so it wins whenever
    // the trailing member is a `*`.
    | id DOT STAR
    | id (DOT id)* (LBRACK expr (COMMA expr)* RBRACK)*
    | LBRACK (expr (COMMA expr)*)? RBRACK
    | STAR
    | EQ | NEQ | LT | GT | LEQ | GEQ | PLUS | MINUS | SLASH | PERCENT
    | CONTAINS_OP | CONTAINED_BY_OP | JSON_EXTRACT_OP | JSON_EXTRACT_TEXT_OP
    // Round-3 (§operator-audit): the newly-tokenized JSON path / key-existence / full-text /
    // bitwise operators must be expression atoms too, otherwise the lexer fix would merely turn a
    // silent character drop into a parse error.
    | JSON_PATH_OP | JSON_PATH_TEXT_OP | JSON_DELETE_OP | HASH
    | TEXT_MATCH_OP | AT_SIGN
    | JSON_EXISTS_ANY_OP | JSON_EXISTS_ALL_OP | QUESTION
    | REGEX_NOT_MATCH_OP | REGEX_NOT_IMATCH_OP
    | AND | OR | NOT | IN_KW | LIKE | IS | BETWEEN | EXISTS | NULL_KW
    | CURRENT
    | AMPERSAND | PIPE | CARET | TILDE | COLON | REGEX_IMATCH_OP
    ;

// CAST ( expr AS dataType ) — reuses the shared dataType rule for the target type.
castExpr
    : CAST LPAREN expr AS dataType RPAREN
    ;

// [schema.]name(args) — a function invocation in an expression. `overClause` is optional so a
// window function (`ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)`) is consumed as part of
// the expr/functionCall itself. Without it, OVER/PARTITION are unmatched by any exprAtom, so expr
// stops right after the `()`, leaving `OVER (...)` unconsumed; that in turn stops selectItem
// mid-selectList, so fromClause is never entered and the query falls into the permissive
// selectRest catch-all — silently dropping the FROM table (e.g. a window function inside a
// derived-table subquery used to drop that subquery's own source table from tableOps).
functionCall
    : id (DOT id)? LPAREN functionArgs? RPAREN overClause?
    ;

overClause
    : OVER LPAREN
        (PARTITION BY expr (COMMA expr)*)?
        (ORDER BY expr (ASC | DESC)? (COMMA expr (ASC | DESC)?)*)?
      RPAREN
    ;

// Real-corpus bug A (§postgres-real-corpus), second half. An AGGREGATE call may carry a
// `DISTINCT`/`ALL` quantifier and a `ORDER BY` sort clause INSIDE its parentheses —
//     COUNT(DISTINCT so.order_id)
//     array_agg(product_id ORDER BY product_id)
// — and neither was expressible here: DISTINCT/ALL/ORDER are structural keywords deliberately
// excluded from `id`, so they are not `exprAtom`s. `functionCall` therefore did not match at all;
// the call collapsed into a bare identifier followed by an unparseable parenthesised group, which
// the surrounding permissive catch-alls then absorbed. With the `subquery` mis-prediction above
// still in place that surfaced as the hard error `line 12:47 no viable alternative at input 'SUM'`
// on the entry procedure's very first query; with it fixed the error is gone but the loss is
// SILENT — `SELECT COUNT(DISTINCT so.order_id) … FROM erp.erp_sales_order so` reported errors=[]
// and ZERO table ops, because fromClause was never reached. Both forms are now part of the call.
functionArgs
    : STAR
    | (ALL | DISTINCT)? expr (COMMA expr)* aggregateOrderBy?
    ;

// `ORDER BY e [ASC|DESC] [, …]` written inside an aggregate's argument list (ordered-set and
// array/string aggregates). Same shape as overClause's ORDER BY, kept separate so it cannot be
// confused with a query's own ORDER BY.
aggregateOrderBy
    : ORDER BY expr (ASC | DESC)? (COMMA expr (ASC | DESC)?)*
    ;

literal
    : SQ_STRING
    | ESQ_STRING
    | INT_NUM
    | DEC_NUM
    | NULL_KW
    ;
