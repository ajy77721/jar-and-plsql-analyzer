// ============================================================
// SqlServerParser.g4
// Parser grammar for SQL Server T-SQL stored procedures / functions.
// Entry point: compilationUnit
// tokenVocab = SqlServerLexer
//
// Design: permissive + robust. Specific rules exist for the constructs we
// extract (EXEC calls, DML table ops, SET/DECLARE, dynamic SQL); everything
// else is consumed by tolerant catch-all rules so real-world T-SQL parses
// without "no viable alternative" failures.
// ============================================================
parser grammar SqlServerParser;

options { tokenVocab = SqlServerLexer; }

// ─────────────────────────────────────────────────────────────────────────────
// Entry point
// ─────────────────────────────────────────────────────────────────────────────

compilationUnit
    : (routineDefinition | triggerDefinition | viewDefinition | createSequence
       | createType | createIndex | alterTable)? EOF
    ;

// ALTER TABLE [schema.]table
//     ADD [COLUMN] col <type> ...
//   | ADD [CONSTRAINT name] FOREIGN KEY (cols) REFERENCES ref_table (cols) [ON DELETE ...]
//   | ADD [CONSTRAINT name] PRIMARY KEY (cols) | UNIQUE (cols) | CHECK (expr)
//   | DROP [COLUMN] col | DROP CONSTRAINT name
//   | ALTER COLUMN ... / anything else
//
// Only the FOREIGN KEY ... REFERENCES form is parsed structurally — it is the one that carries
// real lineage value (two table names and the joining columns). Every other sub-form is eaten
// permissively by alterTableTail, exactly like createSequenceTail / createIndexTail, because the
// altered table itself is already captured structurally by the leading tableRef.
//
// TABLE / ADD / KEY are not reserved tokens in this dialect and are matched positionally as IDs
// (promoting TABLE in particular would break RETURNS TABLE, DECLARE @t TABLE(...) and
// CREATE TYPE ... AS TABLE). Only CONSTRAINT / FOREIGN / REFERENCES were promoted to tokens.
alterTable
    : ALTER tableKw tableRef alterTableClause? SEMI?
    ;

tableKw
    : ID
    ;

alterTableClause
    : addForeignKey
    | alterTableTail
    ;

// ADD <constraint> [, <constraint>]...
//
// One ALTER TABLE may add SEVERAL foreign keys in a single comma-separated ADD list. The rule
// used to inline a single constraint, so the second and later constraints were swallowed by the
// permissive alterTableTail and lost outright. Each constraint is now its own subrule, and the
// visitor records one SqlServerForeignKeyInfo per match.
addForeignKey
    : ID foreignKeyConstraint (COMMA foreignKeyConstraint)* alterTableTail?
    ;

// Trailing referential actions are part of the constraint, not of the tail. Without them the
// greedy alterTableTail (~SEMI+) swallowed "ON DELETE CASCADE" *and* every comma-separated
// sibling constraint after it, so only the first FK of such a list was ever recorded.
foreignKeyConstraint
    : (CONSTRAINT namePart)? FOREIGN ID LPAREN columnNameList RPAREN
      REFERENCES tableRef (LPAREN columnNameList RPAREN)? referentialAction*
    ;

// ON DELETE|UPDATE  CASCADE | NO ACTION | SET NULL | SET DEFAULT
// Both an ON DELETE and an ON UPDATE clause may appear, in either order — hence the `*`.
// CASCADE / NO / ACTION / DEFAULT are not reserved in this dialect and lex as plain IDs;
// SET and NULL are real tokens.
referentialAction
    : ON (DELETE | UPDATE) (SET (NULL_KW | ID) | ID ID?)
    ;

columnNameList
    : namePart (COMMA namePart)*
    ;

alterTableTail
    : ~SEMI+
    ;

// CREATE TYPE [schema.]name FROM <base type> [NOT NULL | NULL]      -- alias type
// CREATE TYPE [schema.]name AS TABLE ( <column defs> )              -- table type (TVP)
//
// Listed BEFORE createIndex in compilationUnit, and anchored on the real TYPE keyword token,
// so `CREATE TYPE ...` can no longer be absorbed by createIndex's permissive
// `indexModifier : ID`. (That absorption is exactly what produced a CreateIndexContext with a
// null tableRef() and an NPE in visitCreateIndex.)
//
// TABLE is not a reserved token in this dialect, so it is matched positionally as an ID
// (tableTypeKw); the column-definition list is consumed as a balanced parenGroup and decoded
// by the visitor, matching how createSequenceTail / createIndexTail are handled.
createType
    : CREATE TYPE routineName FROM dataType createTypeTail? SEMI?
    | CREATE TYPE routineName AS tableTypeKw parenGroup createTypeTail? SEMI?
    ;

tableTypeKw
    : ID
    ;

createTypeTail
    : ~SEMI+
    ;

// CREATE [UNIQUE] [CLUSTERED|NONCLUSTERED] INDEX name ON table (col [ASC|DESC], ...)
//        [INCLUDE (col, ...)] [WHERE ...] [WITH (...)] [ON filegroup]
// UNIQUE / CLUSTERED / NONCLUSTERED lex as plain ID (deliberately not reserved), so they are
// matched positionally by indexModifier; everything after the key-column list is consumed
// permissively and decoded by the visitor.
createIndex
    : CREATE indexModifier* INDEX routineName ON tableRef
      LPAREN indexColumnList RPAREN createIndexTail? SEMI?
    ;

indexModifier
    : ID
    ;

indexColumnList
    : indexColumn (COMMA indexColumn)*
    ;

indexColumn
    : (ID | BRACKET_ID) (ASC | DESC)?
    ;

createIndexTail
    : ~SEMI+
    ;

// CREATE SEQUENCE [schema.]name [AS <type>] [START WITH n] [INCREMENT BY n]
//                 [MINVALUE n | NO MINVALUE] [MAXVALUE n | NO MAXVALUE]
//                 [CYCLE | NO CYCLE] [CACHE [n] | NO CACHE]
// The option list is order-independent and every option is optional, so rather than encoding
// a combinatorial permutation grammar the tail is consumed permissively (same approach as
// viewHeader / triggerHeader above); SqlServerAnalysisVisitor.visitCreateSequence recovers the
// individual option values by regex over the original tail text.
createSequence
    : CREATE SEQUENCE routineName createSequenceTail? SEMI?
    ;

createSequenceTail
    : ~SEMI+
    ;

// CREATE TRIGGER name ON target (AFTER|FOR|INSTEAD OF) events AS body
// (header is consumed permissively; the body statements carry the DML we extract)
triggerDefinition
    : CREATE (OR ALTER)? TRIGGER routineName triggerHeader AS routineBody
    ;

// Trigger header: ON <table> [timing] <event>[, <event>…]
//
// STRUCTURED ALTERNATIVE (regex → grammar migration, batch 2): the target table, timing and
// event list are now real sub-nodes instead of being recovered by three inline regexes over
// `originalText(triggerHeader)`. The visitor reads them off the tree
// (visitTriggerHeader) and only falls back to the legacy regexes — counted — when the
// structured alternative did not match.
//
// The second alternative is the ORIGINAL permissive `~AS+` shape, kept verbatim as a hard
// fallback: any header the structured alternative cannot parse (an unusual vendor extension,
// a malformed header, …) still parses exactly as it always did, so this change can never turn
// a previously-parseable trigger into a parse error. ANTLR prefers the lowest viable
// alternative, so the structured form wins whenever it matches.
//
// INSTEAD / OF / AFTER / BEFORE / FOR are deliberately NOT lexer tokens (they are common
// ordinary identifiers), so the timing keywords are recognised by local semantic predicates
// on the ID token — the same technique applyKw/intersectKw/exceptKw already use. INSERT /
// UPDATE / DELETE ARE tokens, so the event list needs no predicate.
triggerHeader
    : triggerOnTable triggerTiming? triggerEvent (COMMA? triggerEvent)*
    | ~AS+
    ;

triggerOnTable
    : ON tableRef
    ;

triggerTiming
    : {"INSTEAD".equalsIgnoreCase(_input.LT(1).getText())}? ID
      {"OF".equalsIgnoreCase(_input.LT(1).getText())}? ID
    | {"AFTER".equalsIgnoreCase(_input.LT(1).getText())}? ID
    | {"BEFORE".equalsIgnoreCase(_input.LT(1).getText())}? ID
    | {"FOR".equalsIgnoreCase(_input.LT(1).getText())}? ID
    ;

triggerEvent
    : INSERT
    | UPDATE
    | DELETE
    ;

// CREATE VIEW name [WITH ...] AS <select/union>
viewDefinition
    : CREATE (OR ALTER)? VIEW routineName viewHeader? AS routineBody
    ;

viewHeader
    : ~AS+
    ;

// ─────────────────────────────────────────────────────────────────────────────
// Routine definition
// ─────────────────────────────────────────────────────────────────────────────

routineDefinition
    : CREATE (OR ALTER)? routineKw routineName paramBlock? returnsClause? AS routineBody
    ;

routineKw
    : PROCEDURE
    | PROC
    | FUNCTION
    ;

// RETURNS <type>                       (scalar function)
// RETURNS TABLE                        (inline TVF)
// RETURNS @var TABLE( ... )            (multi-statement TVF)
returnsClause
    : RETURNS AT_VAR? dataType
    ;

routineName
    : namePart (DOT namePart)?
    ;

namePart
    : BRACKET_ID
    | ID
    | SP_EXECUTESQL          // re-admitted so a (schema-qualified) sp_executesql still parses
    ;

paramBlock
    : LPAREN (param (COMMA param)*)? RPAREN
    | param (COMMA param)*
    ;

param
    : AT_VAR dataType (EQ defaultVal)? paramModifier*
    ;

// OUTPUT / OUT / READONLY (READONLY tokenises as ID)
paramModifier
    : OUTPUT
    | OUT
    | ID
    ;

// Permissive data type: (schema-qualified) name + optional balanced parenthesised spec.
// Covers INT, DECIMAL(18,2), NVARCHAR(MAX), hr.NewHireList,
// TABLE(Col INT, Col2 DECIMAL(18,2)) ...
dataType
    : (ID | BRACKET_ID) (DOT (ID | BRACKET_ID))? parenGroup?
    ;

// Balanced parenthesised group (handles nested parens).
parenGroup
    : LPAREN (parenGroup | ~(LPAREN | RPAREN))* RPAREN
    ;

defaultVal
    : literal
    | ID
    | MINUS INT_NUM
    | NULL_KW
    ;

// ─────────────────────────────────────────────────────────────────────────────
// Routine body
// ─────────────────────────────────────────────────────────────────────────────

routineBody
    : beginEndBlock
    | statement*
    ;

// ─────────────────────────────────────────────────────────────────────────────
// Statements
// ─────────────────────────────────────────────────────────────────────────────

statement
    : execStatement
    | declareStatement
    | setStatement
    | insertStatement
    | updateStatement
    | deleteStatement
    | selectStatement
    | ifStatement
    | whileStatement
    | tryBlock
    | transactionStatement
    | beginEndBlock
    | createStatement
    | returnStatement
    | labelStatement
    | throwStatement
    | raiserrorStatement
    | otherStatement
    ;

// THROW ;                              -- bare re-raise (only valid inside a CATCH block)
// THROW <errorNumber>, <message>, <state> ;   -- new custom error
// No dedicated rule previously existed; THROW fell into otherStatement's catch-all, so the
// error number/message/state were parsed as opaque tokens and never distinguished from a bare
// re-raise (both just produced a generic "THROW" statement type).
throwStatement
    : THROW (throwArg COMMA throwArg COMMA throwArg)? SEMI?
    ;

throwArg
    : throwArgAtom+
    ;

// Same atom set as exprAtom, minus COMMA (COMMA is the THROW/RAISERROR argument separator here,
// not part of an individual argument's expression).
throwArgAtom
    : parenGroup
    | CASE stmtToken* END
    | literal
    | AT_VAR
    | AT_AT_VAR
    | BRACKET_ID
    | ID
    | STAR
    | DOT
    | EQ | NEQ | LT | GT | LEQ | GEQ | PLUS | MINUS | SLASH | PERCENT
    | AND | OR | NOT | IN | LIKE | IS | BETWEEN | EXISTS | NULL_KW
    | AMPERSAND | PIPE | CARET | TILDE | EXCL
    ;

// RAISERROR ( <msg>, <severity>, <state> [, <arg>...] ) [WITH LOG] ;
// Previously fell into otherStatement's catch-all: parsed cleanly but the message/severity/
// state arguments were never extracted, only the bare keyword-derived type "RAISERROR".
raiserrorStatement
    : RAISERROR LPAREN throwArg (COMMA throwArg)* RPAREN (WITH ID)? SEMI?
    ;

// Standard T-SQL GOTO-label syntax: an identifier immediately followed by ':' marks a
// label that GOTO can jump to (e.g. "DoneLabel:"). Without a dedicated rule, the lexer has
// no token for ':' and a bare label line is a token-recognition error.
labelStatement
    : ID COLON
    ;

// EXEC (@sql)                 → dynamic SQL
// EXEC schema.proc args...    → outbound call
//
// STRUCTURED sp_executesql alternative (batch 2): `EXEC sp_executesql @sql [, N'@p INT'][, @p=…]`
// now has real argument nodes, so the @sql variable, the parameter-signature literal and the bound
// name=value pairs are read off the tree instead of being regex-decoded from stmtTail text. Listed
// before the generic alternative; when its predicate/structure does not match (e.g. a
// schema-qualified `dbo.sp_executesql`, or a literal first argument) the generic alternative parses
// the statement exactly as before and the visitor's counted regex fallback takes over.
execStatement
    : (EXEC | EXECUTE) LPAREN expr? RPAREN SEMI?
    | (EXEC | EXECUTE) SP_EXECUTESQL spExecuteSqlArgs? SEMI?
    | (EXEC | EXECUTE) routineName stmtTail SEMI?
    ;

spExecuteSqlArgs
    : AT_VAR (COMMA spExecuteSqlArg)*
    ;

spExecuteSqlArg
    : N_STRING
    | SQ_STRING
    | AT_VAR EQ spArgValue
    | spArgValue
    ;

// A single sp_executesql argument value. COMMA is deliberately excluded (unlike exprAtom) so the
// comma-separated argument list is not swallowed into one value — `@p1=5, @p2='x'` is two args.
spArgValue
    : spArgAtom+
    ;

spArgAtom
    : parenGroup
    | literal
    | AT_VAR
    | BRACKET_ID
    | ID
    | SP_EXECUTESQL
    | DOT
    | EQ | NEQ | LT | GT | LEQ | GEQ | PLUS | MINUS | SLASH | PERCENT
    | AMPERSAND | PIPE | CARET | TILDE | EXCL
    | NULL_KW
    ;

// DECLARE @a INT, @b DECIMAL(18,2) = 0 ...   (one or more variables / table vars)
// DECLARE cur CURSOR ... FOR ...             (cursor — name is a plain identifier)
declareStatement
    : DECLARE declareItem (COMMA declareItem)* SEMI?
    | DECLARE ID stmtTail SEMI?
    ;

declareItem
    : AT_VAR dataType (EQ expr)?
    ;

// SET @x = expr | SET @x += expr | SET NOCOUNT ON | SET XACT_ABORT ON ...
setStatement
    : SET AT_VAR assignOp expr SEMI?
    | SET stmtTail SEMI?
    ;

assignOp
    : EQ
    | PLUS EQ
    | MINUS EQ
    | STAR EQ
    | SLASH EQ
    | PERCENT EQ
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

selectStatement
    : SELECT stmtTail SEMI?
    ;

ifStatement
    : IF expr (beginEndBlock | statement) (ELSE (beginEndBlock | statement))?
    ;

whileStatement
    : WHILE expr (beginEndBlock | statement)
    ;

beginEndBlock
    : BEGIN statement* END
    ;

tryBlock
    : BEGIN TRY statement* END TRY BEGIN CATCH statement* END CATCH
    ;

// Transaction-control statements — must be matched before beginEndBlock so
// BEGIN TRAN is not mistaken for an (unterminated) BEGIN...END block.
transactionStatement
    : BEGIN (TRAN | TRANSACTION) ID? SEMI?
    | COMMIT (TRAN | TRANSACTION)? ID? SEMI?
    | ROLLBACK (TRAN | TRANSACTION)? ID? SEMI?
    | SAVE (TRAN | TRANSACTION) ID? SEMI?
    ;

// CREATE TABLE #temp( ... ), CREATE INDEX ... (inside a routine body)
//
// STRUCTURED ALTERNATIVES (regex → grammar migration, batch 2):
//   * createSynonym      — CREATE SYNONYM [schema.]name FOR [server.db.]schema.target [@srv]
//   * createTableStatement — CREATE TABLE name ( column-defs ) — enough structure to read
//                            IDENTITY column specs off the tree instead of scanning raw text.
// The generic `CREATE stmtTail` catch-all stays LAST, so anything the two structured forms
// cannot parse still parses exactly as before (and the visitor's regex fallbacks cover it).
createStatement
    : createSynonym
    | createTableStatement
    | CREATE stmtTail SEMI?
    ;

// CREATE SYNONYM [schema.]syn_name FOR [server.database.]schema.target [@linked_server]
// SYNONYM / FOR are deliberately not lexer tokens (both are ordinary identifiers), so they are
// recognised by local semantic predicates on the ID token.
createSynonym
    : CREATE {"SYNONYM".equalsIgnoreCase(_input.LT(1).getText())}? ID
      routineName {"FOR".equalsIgnoreCase(_input.LT(1).getText())}? ID
      tableRef AT_VAR?
    ;

// CREATE TABLE [schema.]name ( <column-defs> ) [tail]
// TABLE is an ordinary identifier in this dialect, so it is matched positionally via a predicate.
createTableStatement
    : CREATE createTableKw tableRef LPAREN createTableItem (COMMA createTableItem)* RPAREN
      createTableTail? SEMI?
    ;

createTableKw
    : {"TABLE".equalsIgnoreCase(_input.LT(1).getText())}? ID
    ;

createTableItem
    : columnDefinition
    ;

columnDefinition
    : namePart dataType columnOption*
    ;

// A column's trailing options. IDENTITY[(seed, increment)] is the one with extraction value, so it
// is modelled explicitly; every other option (NOT NULL, DEFAULT …, CHECK (…), COLLATE …, …) is
// consumed permissively — parenGroup for balanced groups, a single token otherwise — because
// nothing downstream reads them.
columnOption
    : identitySpec
    | parenGroup
    | ~(COMMA | RPAREN)
    ;

// IDENTITY[(seed, increment)] — bare IDENTITY defaults to seed=1, increment=1 per T-SQL semantics.
identitySpec
    : {"IDENTITY".equalsIgnoreCase(_input.LT(1).getText())}? ID
      (LPAREN MINUS? INT_NUM COMMA MINUS? INT_NUM RPAREN)?
    ;

createTableTail
    : ~SEMI+
    ;

// RETURN ( SELECT ... )   inline TVF body
// RETURN expr             scalar function / procedure
returnStatement
    : RETURN parenGroup SEMI?
    | RETURN expr? SEMI?
    ;

// Catch-all: any other statement (OPEN/FETCH/CLOSE/DEALLOCATE/PRINT/THROW/
// MERGE/WITH/WAITFOR/...). Consumes up to the next statement boundary.
//
// STRUCTURED ALTERNATIVES (regex → grammar migration, batch 2): cursorStatement and
// withStatement are listed FIRST so a recognised shape produces a real sub-tree; the generic
// token-soup alternative stays LAST and is byte-for-byte the previous rule, so any statement
// these two cannot parse still parses exactly as before. Keeping them as alternatives of
// otherStatement (rather than new alternatives of `statement`) preserves the statement
// INVENTORY: the node remains an OtherStatementContext, so statementType()'s leading-keyword
// classification — and therefore every pinned statement count — is unchanged.
otherStatement
    : cursorStatement
    | withStatement
    | ~(SEMI | BEGIN | END | EXEC | EXECUTE | DECLARE | SET | INSERT | UPDATE | DELETE | SELECT | IF | WHILE | RETURN | CREATE | TRY | CATCH | COMMIT | ROLLBACK | THROW | RAISERROR)
      (~(SEMI | BEGIN | END))* SEMI?
    | SEMI
    ;

// ── T-SQL cursor lifecycle ────────────────────────────────────────────────────
// OPEN cur; FETCH [orientation] [FROM] cur [INTO vars]; CLOSE cur; DEALLOCATE cur;
// Modelled on PlSqlParser's cursor_manipulation_statements. OPEN / FETCH / CLOSE / DEALLOCATE
// and the FETCH orientation words are ordinary identifiers in this dialect, hence the predicates.
cursorStatement
    : openCursor
    | fetchCursor
    | closeCursor
    | deallocateCursor
    ;

openCursor
    : {"OPEN".equalsIgnoreCase(_input.LT(1).getText())}? ID namePart SEMI?
    ;

closeCursor
    : {"CLOSE".equalsIgnoreCase(_input.LT(1).getText())}? ID namePart SEMI?
    ;

deallocateCursor
    : {"DEALLOCATE".equalsIgnoreCase(_input.LT(1).getText())}? ID namePart SEMI?
    ;

fetchCursor
    : {"FETCH".equalsIgnoreCase(_input.LT(1).getText())}? ID
      fetchOrientation? FROM? namePart fetchInto? SEMI?
    ;

fetchOrientation
    : {"NEXT".equalsIgnoreCase(_input.LT(1).getText())}? ID
    | {"PRIOR".equalsIgnoreCase(_input.LT(1).getText())}? ID
    | {"FIRST".equalsIgnoreCase(_input.LT(1).getText())}? ID
    | {"LAST".equalsIgnoreCase(_input.LT(1).getText())}? ID
    | {"ABSOLUTE".equalsIgnoreCase(_input.LT(1).getText())}? ID MINUS? INT_NUM
    | {"RELATIVE".equalsIgnoreCase(_input.LT(1).getText())}? ID MINUS? INT_NUM
    ;

fetchInto
    : INTO expr
    ;

// ── WITH <cte>[, <cte>]… <statement> ─────────────────────────────────────────
// CTE names were previously recovered by a text regex purely so they could be EXCLUDED from table
// extraction; they are now real nodes. The trailing `statement` is what the CTE feeds, parsed by
// the ordinary statement rules (so its own tables/joins are extracted exactly as before).
withStatement
    : WITH cteDefinition (COMMA cteDefinition)* statement
    ;

cteDefinition
    : namePart (LPAREN columnNameList RPAREN)? AS LPAREN queryExpression RPAREN
    ;

// ─────────────────────────────────────────────────────────────────────────────
// Table reference  (schema.table or just table, brackets or plain ID)
// ─────────────────────────────────────────────────────────────────────────────

tableRef
    : AT_VAR
    | (BRACKET_ID | ID) (DOT (BRACKET_ID | ID))? (DOT (BRACKET_ID | ID))? (DOT (BRACKET_ID | ID))?
    ;

// ─────────────────────────────────────────────────────────────────────────────
// stmtTail — permissive rule that eats everything up to the next
// statement-starting keyword, semicolon, or unmatched END.
// Recursively handles balanced parentheses and CASE...END.
// ─────────────────────────────────────────────────────────────────────────────

stmtTail
    : stmtToken*
    ;

stmtToken
    : LPAREN stmtToken* RPAREN
    // Boundary fix (Stage 5 prerequisite): a PARENTHESISED SUBQUERY — `FROM (SELECT …) AS d`,
    // `WHERE x IN (SELECT …)`, `(SELECT MAX(x) FROM t)` in a select list.
    //
    // The first alternative cannot carry one: `stmtToken` deliberately excludes SELECT (so the
    // next real statement can start), so `LPAREN stmtToken* RPAREN` dies at the inner SELECT and
    // the alternative fails outright. LPAREN is excluded from the catch-all too, so `stmtTail`
    // simply STOPPED at the `(` — and the inner SELECT was then resynced by the top-level parser
    // as a bogus SIBLING statement, truncating the enclosing statement's text at `… FROM `.
    //
    // `parenGroup` is the pre-existing fully-balanced group rule (it consumes ANY token except
    // parens, and nests), so it swallows the whole subquery however deeply nested, WITHOUT the
    // top-level grammar needing to understand SELECT internals — it only has to get the enclosing
    // statement's BOUNDARIES right. The contents are still decoded by the nested-reparse
    // `queryUnit` mechanism / the legacy regex path, exactly as before.
    //
    // Ordering is load-bearing and is what keeps this change surgical: ANTLR takes the lowest
    // viable alternative, so an ordinary group like `(a+b)` or `ISNULL(x,0)` still matches
    // alternative 1 and produces the IDENTICAL tree it always did. Only a group that alternative 1
    // cannot parse — i.e. one containing a statement keyword, which in practice means a subquery —
    // reaches this fallback.
    | parenGroup
    // Boundary fix (Stage 7 prerequisite): a SET OPERATOR joining two SELECTs —
    // `SELECT … UNION ALL SELECT …`, `… INTERSECT SELECT …`, `… EXCEPT SELECT …`.
    //
    // Same class of truncation as the paren case above, but with no parentheses to balance:
    // `stmtToken` excludes SELECT, so `stmtTail` stopped dead at the second SELECT and the
    // top-level parser resynced it as a separate SIBLING statement. A two-branch UNION was
    // therefore reported as TWO SELECT statements rather than one, and the enclosing statement's
    // captured text ended at `… UNION ALL `.
    //
    // Consuming the operator TOGETHER WITH the SELECT that must follow it is what makes this
    // safe and narrow. A bare `SELECT` still terminates a statement exactly as before — only a
    // SELECT that is grammatically part of the current statement (because a set operator
    // immediately precedes it) is absorbed. INTERSECT/EXCEPT are matched via semantic predicate
    // rather than by promoting them to lexer tokens, for the same reason APPLY is: promoting
    // them would retokenise the entire corpus, a far larger blast radius than this is worth.
    | setOperator SELECT
    | CASE stmtToken* END
    // NEXT VALUE FOR <sequence> (batch 2): a real node so sequence USAGE is read off the tree
    // instead of being scanned for. Placed before the catch-all; the catch-all still matches the
    // bare NEXT/VALUE/FOR words if the full shape is absent, exactly as before.
    | nextValueFor
    | existsPredicate
    | ~(SEMI | BEGIN | END | CASE | EXEC | EXECUTE | DECLARE | IF | WHILE | INSERT | UPDATE | DELETE | SELECT | RETURN | CREATE | LPAREN | RPAREN)
    ;

// ═════════════════════════════════════════════════════════════════════════════
// STRUCTURED QUERY INTERNALS  (SELECT / FROM / JOIN)  —  strangler-fig migration
// ═════════════════════════════════════════════════════════════════════════════
//
// WHY THIS EXISTS, AND WHY IT IS A SEPARATE ENTRY POINT
// ----------------------------------------------------
// Historically this dialect's grammar parsed statement SKELETONS only: `selectStatement` is
// `SELECT stmtTail SEMI?`, where stmtTail is an opaque token soup. Every table, alias and join
// was therefore recovered by REGEX over raw text in SqlServerAnalysisVisitor (TABLE_REF_PATTERN,
// JOIN_PATTERN, FROM_PATTERN, …) — unlike the other four dialects, which all have real grammar
// rules for query internals. The rules below are the first stage of migrating that extraction
// onto a genuine parse tree.
//
// They are deliberately reachable ONLY through the dedicated `queryUnit` entry point, NOT by
// adding an alternative to `selectStatement`. That is the whole safety design:
//
//   * Adding an alternative to `selectStatement` would change how EVERY T-SQL routine in the
//     corpus is parsed at the statement level. ANTLR's ALL(*) prediction can prefer an
//     alternative that matches only a PREFIX of a statement (leaving the remainder to be
//     re-synced as bogus sibling statements), which would silently perturb statement counts,
//     line ranges and the catch-all recovery paths that ~250 existing tests pin down.
//   * With a separate entry point, the primary parse tree is byte-for-byte what it always was.
//     The visitor runs a SECOND, nested parse of just the SELECT's own text. If that nested
//     parse is 100% clean AND the query is within the migrated subset, the visitor walks the
//     real tree; otherwise it falls back to the pre-existing regex scan, unchanged.
//
// So the end state is an explicit, auditable HYBRID: constructs that have been migrated go
// through these rules, everything else still goes through the regex path, and the fallback is a
// hard guarantee rather than a hope.
//
// MIGRATED SO FAR:
//   Stage 1 — plain `SELECT … FROM t [alias] [<join> JOIN t [alias] ON <cond>]*`, with bare /
//             INNER / LEFT / RIGHT / FULL / CROSS joins, and chained joins.
//   Stage 2 — table hints (`WITH (NOLOCK)`, `WITH (INDEX(ix1))`, …) on any table source.
//   Stage 3 — CROSS APPLY / OUTER APPLY, including of a parameterised table-valued function
//             and of an XML data-type method with an alias column list.
//   Stage 4 — bracket-quoted identifiers (spaces, `]]` escapes) and 3-/4-part linked-server
//             names (server.database.schema.table).
//   Stage 5 — DERIVED TABLES (`FROM (SELECT …) AS d`), nested to any depth.
//   Stage 6 — SUBQUERIES in expression position: WHERE `EXISTS` / `NOT EXISTS` / `IN (SELECT …)`,
//             and scalar subqueries in the select list.
//   Stage 7 — SET OPERATORS: `UNION`, `UNION ALL`, `INTERSECT`, `EXCEPT`.
//   Stage 8 — T-SQL `UPDATE … SET … FROM … JOIN … ON …` and `DELETE … FROM … JOIN … ON …`.
//             No new rules: the statement's FROM clause is handed to the SAME `queryUnit` entry
//             point with a synthetic `SELECT * ` prefix, so every stage above applies to it
//             unchanged. See the Stage 8 block in SqlServerAnalysisVisitor.
//   Stage 9 — `OFFSET n ROWS FETCH NEXT m ROWS ONLY` pagination. Also no new rules, and it needed
//             none: OFFSET / FETCH / ROWS / NEXT / ONLY are deliberately NOT lexer tokens in this
//             dialect, so they lex as plain IDs and `queryRest`'s catch-all already consumed them.
//             Verified by observation rather than assumed — a paginated query combined with a
//             construct only the structured path handles (a comma-separated FROM) extracts both
//             tables, which is only possible if the nested parse accepted the whole thing.
//
//   Also migrated, as deliberate CORRECTNESS fixes rather than mechanism changes (each records
//   something the legacy regex path got wrong or dropped outright, and each is documented at its
//   site in the visitor):
//     * comma-separated FROM lists (`FROM a, b`) — the regex path recorded only `a`.
//     * table-valued parameters / table variables in FROM position (`FROM @tvp x JOIN t …`) —
//       the regex path dropped every join edge hanging off them, purely because FROM_PATTERN's
//       character class has no `@`, making edge capture depend on which side was written first.
//     * FROM-position table-valued / rowset function calls (`FROM dbo.GetOrders(@id) t JOIN …`) —
//       likewise dropped their join edges, and a TVF-only FROM produced a table op with a NULL
//       name. `tableSourceItem` now parses the argument list, so a call is recognised
//       structurally; it still yields NO table op, so the phantom-avoidance is preserved.
//     * ON conditions containing a string literal, and ON conditions longer than 200 characters.
//
// Stages 5-7 all depended on a STATEMENT-LEVEL fix first, not on these rules: the permissive
// `stmtTail` behind `selectStatement` used to stop at the `(` of `FROM (SELECT …` and at the
// second `SELECT` of a `UNION`, so the visitor never even received the full statement text. Both
// truncations are fixed at their source on `stmtToken` (see the two long comments there); each
// was verified against the full suite on its own before any extraction logic was widened.
//
// Still intentionally left UNPARSEABLE by these rules, so they fail fast and fall back to regex
// rather than being half-understood: a join whose ON condition itself contains a SUBQUERY, and
// `SELECT … INTO #tmp FROM …` (excluded at `selectListToken`, because the regex path is what
// currently decides how a SELECT…INTO target is recorded). Any construct the rules cannot parse
// to EOF, or that the visitor's gate does not claim, still falls back to the legacy regex scan
// exactly as before — that fallback remains a hard guarantee, not a hope.

// Entry point for the nested query parse. EOF is load-bearing: it forces the whole SELECT text to
// be consumed, so a construct these rules do not yet model can never "succeed" by matching a
// prefix and silently discarding the tables in the unmatched remainder.
queryUnit
    : queryExpression EOF
    ;

// Stage 7: UNION / UNION ALL / INTERSECT / EXCEPT.
//
// Each branch is a full querySpecification, so every branch's FROM/JOIN/derived-table/subquery
// content is extracted by exactly the same visitor code as a standalone SELECT — the set
// operator itself carries no lineage, it just says "there is another query here too".
queryExpression
    : querySpecification (setOperator querySpecification)*
    ;

// UNION and ALL are real lexer tokens. INTERSECT and EXCEPT are NOT — they lex as plain IDs, and
// deliberately stay that way: promoting them would change tokenisation for the WHOLE corpus
// (every namePart / indexModifier / paramModifier / dataType that could legitimately be the word
// "except"), which is exactly the blast radius the APPLY decision already rejected once. A
// semantic predicate keeps the recognition local to this rule, and a non-matching ID simply fails
// it, aborting the nested parse and falling back to the legacy regex path as before.
setOperator
    : UNION ALL?
    | intersectKw
    | exceptKw
    ;

intersectKw
    : {"INTERSECT".equalsIgnoreCase(_input.LT(1).getText())}? ID
    ;

exceptKw
    : {"EXCEPT".equalsIgnoreCase(_input.LT(1).getText())}? ID
    ;

querySpecification
    : SELECT selectList fromClause? queryRest
    ;

// The select list is not decomposed — no extraction value yet (column-level detail is handled
// downstream by the enricher from sqlText). It is consumed permissively but STOPS at FROM and at
// every clause keyword, so the fromClause below is actually reached.
selectList
    : selectListToken*
    ;

// INTO is excluded so `SELECT … INTO #tmp FROM t` fails this rule and falls back: the regex path
// is what currently decides how a SELECT…INTO target is (not) recorded, and silently changing
// that is not part of Stage 1.
selectListToken
    : subquery
    | LPAREN stmtToken* RPAREN
    | CASE stmtToken* END
    | ~(FROM | INTO | SEMI | BEGIN | END | CASE | LPAREN | RPAREN
        | SELECT | WHERE | GROUP | ORDER | HAVING | UNION)
    ;

// Stage 6: a parenthesised sub-SELECT appearing in an EXPRESSION position — a scalar subquery in
// the select list (`SELECT (SELECT MAX(d) FROM t) AS m, …`) or a WHERE-clause subquery
// (`WHERE EXISTS (SELECT …)`, `WHERE id IN (SELECT …)`, `WHERE NOT EXISTS (SELECT …)`).
//
// Structurally these are all the same thing — a parenthesised queryExpression — so one rule
// covers every form, and EXISTS/IN/NOT are just ordinary tokens consumed around it by the
// permissive token rules. There is no need to model the predicate itself: the only thing worth
// extracting from a subquery is the tables and joins inside it, and recursing into
// `queryExpression` gives the visitor a real subtree to walk for exactly that.
//
// Listed FIRST in both host rules and ordering is load-bearing, exactly as with `stmtToken`'s
// `parenGroup`: ANTLR takes the lowest viable alternative, and an ordinary parenthesised group
// (`(a+b)`, `ISNULL(x,0)`) can never match `subquery` because it contains no SELECT — so it still
// falls through to the pre-existing permissive alternative and parses exactly as it always did.
subquery
    : LPAREN queryExpression RPAREN
    ;

fromClause
    : FROM tableSource (COMMA tableSource)*
    ;

tableSource
    : tableSourceItem joinClause*
    ;

// Stage 5: a FROM source is either a plain (possibly qualified) table name or a DERIVED TABLE —
// a parenthesised sub-SELECT with an alias, `FROM (SELECT …) AS d`.
//
// This was previously unreachable, and the note that stood here explained why: `stmtToken`
// excluded SELECT, so the permissive `stmtTail` behind `selectStatement` STOPPED at the `(` of
// `FROM (SELECT …` and the visitor never received a SELECT whose text contained a derived table.
// That statement-level truncation has since been fixed at its source — `stmtToken` now carries a
// balanced `parenGroup` fallback (see the long comment on that rule) — so the enclosing
// statement's text now really does contain the whole derived table, and these rules can see it.
//
// `derivedTable` recurses into `queryExpression`, so the inner SELECT is a REAL subtree, not an
// opaque blob: the visitor walks into it and extracts the inner FROM's tables and joins through
// exactly the same code path as the outer query, at any nesting depth. The alias is mandatory
// here because T-SQL itself requires one on a derived table.
//
// The alternatives are deliberately UNLABELLED. Labelling them would make ANTLR split
// `TableSourceItemContext` into two subclasses and move `qualifiedTableName()`/`tableAlias()`
// onto them, breaking every existing accessor in the visitor. Unlabelled keeps one context type
// whose `qualifiedTableName()` / `derivedTable()` is null for the alternative that did not match,
// which is exactly the discriminator the visitor needs and nothing more.
// The optional `parenGroup` after the name is a FROM-POSITION CALL — a table-valued function
// (`FROM dbo.GetOrders(@id) t`) or a rowset function (`FROM OPENQUERY(srv, '…') x`,
// `FROM OPENROWSET(…) x`). It is exactly the same optional group `applySource` already carries for
// the right-hand side of a CROSS/OUTER APPLY, which is the same construct in a different position.
//
// Before this, `tableSourceItem` had no argument-list rule, so the name parsed on its own and the
// argument list was swallowed by the permissive `queryRest`. The grammar therefore could not tell
// `FROM dbo.GetOrders(@id) t` from `FROM dbo.GetOrders t`, and the visitor had to fall back to a
// TEXTUAL lookahead (`looksLikeCallAt`) to avoid fabricating a table op literally named
// GetOrders / OPENQUERY / OPENROWSET. Parsing the group here makes the distinction STRUCTURAL: a
// source with a parenGroup is a call, one without is a table, and no text scanning is involved.
//
// Note this does NOT make a TVF into a table op — the visitor still records none, so the
// phantom-avoidance the textual guard existed for is fully preserved (the call itself is already
// in the call graph via QUALIFIED_CALL_PATTERN). What it buys is that such a query is now
// PARSEABLE by these rules at all, so the joins hanging off a TVF base are no longer lost with it.
tableSourceItem
    : qualifiedTableName parenGroup? tableAlias? tableHint*
    | derivedTable tableAlias aliasColumnList?
    ;

derivedTable
    : LPAREN queryExpression RPAREN
    ;

// Stage 2: T-SQL table hints — `WITH (NOLOCK)`, `WITH (INDEX(ix1))`, `WITH (ROWLOCK, HOLDLOCK)`…
//
// Per T-SQL syntax the hint follows the (optional) alias, which is also exactly where the legacy
// JOIN_PATTERN had to special-case it: a hint sitting between a joined table and its ON clause
// used to break the whole join match and silently drop the edge. Here it is simply a node.
//
// The hint body is consumed as the existing balanced `parenGroup`, so nested forms like
// `WITH (INDEX(ix1))` are handled naturally — the legacy `\([^)]*\)` could not nest, and for that
// form it matched only as far as the first `)`, which cost it the ON condition. The structured
// path keeps the condition. Hints carry no lineage of their own, so nothing is extracted from
// them; the point is purely that they no longer break what surrounds them.
tableHint
    : WITH parenGroup
    ;

// Up to 4-part linked-server names are ACCEPTED here (server.db.schema.table) because the lexer
// and this rule cost nothing to make general. Whether the VISITOR trusts a 3-/4-part name is a
// separate, staged decision — Stage 1's gate rejects anything past 2 parts, because the legacy
// regex path resolves the base table of a multi-part name differently and that difference must be
// migrated deliberately (Stage 4), not as a side effect.
qualifiedTableName
    : AT_VAR
    | namePart (DOT namePart)*
    ;

tableAlias
    : AS? namePart
    ;

// A bare JOIN (no qualifier) is the ANSI synonym for INNER JOIN — hence the optional joinTypeSpec.
// The visitor defaults it to "INNER", matching the long-standing regex behaviour.
//
// Stage 3 adds the second alternative: T-SQL's CROSS APPLY / OUTER APPLY. There is no bare
// `APPLY` in T-SQL, so the CROSS/OUTER qualifier is mandatory here — which also keeps this
// alternative unambiguous against `CROSS JOIN` (the token after CROSS decides: JOIN vs APPLY).
joinClause
    : joinTypeSpec? JOIN tableSourceItem (ON joinCondition)?      # joinOn
    | (CROSS | OUTER) applyKw applySource                         # applyJoin
    ;

// APPLY is NOT a lexer token in this dialect — it lexes as a plain ID, and deliberately stays
// that way. Promoting it would change tokenisation for the ENTIRE corpus (every `namePart`,
// `indexModifier`, `paramModifier`, `dataType`… that could legitimately be the word "apply"),
// which is a far larger blast radius than this stage is worth. A semantic predicate keeps the
// recognition local to this one rule: `CROSS <some-other-ID>` simply fails the predicate, the
// nested parse aborts, and the query falls back to the legacy regex path exactly as before.
applyKw
    : {"APPLY".equalsIgnoreCase(_input.LT(1).getText())}? ID
    ;

// The right-hand side of an APPLY is usually a parameterised table-valued function
// (`CROSS APPLY dbo.GetOrderLines(o.id) ol`) or an XML data-type method
// (`CROSS APPLY x.nodes('/a/b') AS t(c)`), but a plain table is legal too. The optional
// parenthesised list AFTER the alias is the XML form's column alias list — the legacy
// JOIN_PATTERN had to special-case it, because an alias glued to `(c)` broke the whole match and
// silently dropped the CROSS APPLY edge.
applySource
    : qualifiedTableName parenGroup? tableAlias? aliasColumnList? tableHint*
    ;

aliasColumnList
    : LPAREN columnNameList RPAREN
    ;

joinTypeSpec
    : INNER
    | LEFT OUTER?
    | RIGHT OUTER?
    | FULL OUTER?
    | CROSS
    ;

// An ON condition runs until the next join keyword or trailing clause. COMMA is excluded at the
// top level so the `FROM a JOIN b ON x = y, c` (mixed comma/ANSI) form does not have its trailing
// comma-separated source swallowed into the condition text; a comma inside a function call is
// still fine, being nested within the LPAREN … RPAREN alternative.
joinCondition
    : joinConditionToken+
    ;

joinConditionToken
    : LPAREN stmtToken* RPAREN
    | CASE stmtToken* END
    | ~(SEMI | BEGIN | END | CASE | LPAREN | RPAREN | COMMA
        | JOIN | INNER | LEFT | RIGHT | FULL | CROSS | OUTER
        | SELECT | FROM | WHERE | GROUP | ORDER | HAVING | UNION)
    ;

// Trailing WHERE / GROUP BY / HAVING / ORDER BY.
//
// Stage 6 adds the `subquery` alternative, so a WHERE-clause subquery is now a real subtree the
// visitor recurses into instead of a parse failure. Everything else here stays deliberately
// UNpermissive — a bare SELECT, FROM, JOIN or UNION token still makes the whole nested parse
// FAIL, so the query falls back to regex rather than having a second SELECT's tables silently
// swallowed and lost. (UNION is Stage 7's to lift, and is out of scope here.)
queryRest
    : queryRestToken*
    ;

// UNION is excluded as a token, but INTERSECT/EXCEPT lex as plain IDs, so without the predicate
// the catch-all below would happily swallow them as ordinary words — queryRest would run past the
// set operator, the queryExpression loop would never see it, and an INTERSECT/EXCEPT query would
// fail the nested parse instead of being migrated. The predicate makes them terminate queryRest
// exactly the way the real UNION token does.
queryRestToken
    : subquery
    | CASE stmtToken* END
    | LPAREN stmtToken* RPAREN
    | {!"INTERSECT".equalsIgnoreCase(_input.LT(1).getText())
        && !"EXCEPT".equalsIgnoreCase(_input.LT(1).getText())}?
      ~(SEMI | BEGIN | END | CASE | LPAREN | RPAREN
        | SELECT | FROM | JOIN | UNION | INNER | LEFT | RIGHT | FULL | CROSS | OUTER)
    ;

// ─────────────────────────────────────────────────────────────────────────────
// Expression (simplified, enough to parse conditions and SET rhs)
// ─────────────────────────────────────────────────────────────────────────────

// NEXT VALUE FOR [schema.]sequence  — sequence USAGE.
// NEXT / VALUE / FOR are ordinary identifiers in this dialect, hence the predicates.
nextValueFor
    : {"NEXT".equalsIgnoreCase(_input.LT(1).getText())}? ID
      {"VALUE".equalsIgnoreCase(_input.LT(1).getText())}? ID
      {"FOR".equalsIgnoreCase(_input.LT(1).getText())}? ID
      tableRef
    ;

// [NOT] EXISTS ( <query> ) — the predicate wrapper around a subquery. EXISTS is a real token.
existsPredicate
    : EXISTS LPAREN queryExpression RPAREN
    ;

expr
    : exprAtom+
    ;

exprAtom
    : parenGroup                 // handles (a+b), function args, and (SELECT ...) subqueries
    // Batch 2: structured forms placed BEFORE the permissive alternatives so they win when they
    // match; if either fails, the fallbacks below parse the tokens exactly as before.
    | existsPredicate
    | nextValueFor
    | CASE stmtToken* END
    | literal
    | AT_VAR
    | AT_AT_VAR
    | BRACKET_ID
    | ID
    | SEQUENCE                   // "sequence" promoted to a keyword token for CREATE SEQUENCE;
                                 // still usable as an ordinary identifier inside expressions
    | INDEX                      // likewise for CREATE INDEX / the legacy FROM t INDEX(ix) hint
    | TYPE                       // likewise for CREATE TYPE; "type" is a very common ordinary
                                 // column/variable name, so it must stay usable as an identifier
    | CONSTRAINT | FOREIGN | REFERENCES   // promoted for ALTER TABLE ... FOREIGN KEY REFERENCES;
                                 // kept usable as ordinary identifiers here
    | SP_EXECUTESQL              // re-admitted as an ordinary identifier inside expressions
    | UPDATE                     // T-SQL UPDATE(column) trigger function
    | STAR
    | DOT
    | EQ | NEQ | LT | GT | LEQ | GEQ | PLUS | MINUS | SLASH | PERCENT
    | AND | OR | NOT | IN | LIKE | IS | BETWEEN | EXISTS | NULL_KW | COMMA
    | AMPERSAND | PIPE | CARET | TILDE | EXCL
    ;

literal
    : SQ_STRING
    | N_STRING
    | INT_NUM
    | DEC_NUM
    | NULL_KW
    ;
