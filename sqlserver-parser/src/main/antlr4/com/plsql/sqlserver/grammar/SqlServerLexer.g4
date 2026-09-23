// ============================================================
// SqlServerLexer.g4
// Lexer grammar for SQL Server T-SQL stored procedures / functions.
// Produces tokens consumed by SqlServerParser.g4.
// Case-insensitive via fragment A-Z helpers.
// ============================================================
lexer grammar SqlServerLexer;

// ── Keywords ─────────────────────────────────────────────────────────────────
ALL         : A L L ;
ALTER       : A L T E R ;
AND         : A N D ;
AS          : A S ;
ASC         : A S C ;
BEGIN       : B E G I N ;
BETWEEN     : B E T W E E N ;
BY          : B Y ;
CASE        : C A S E ;
CATCH       : C A T C H ;
COMMIT      : C O M M I T ;
// Top-level `ALTER TABLE` DDL support. Only the three words needed to pin down a FOREIGN KEY
// ... REFERENCES clause precisely (so BOTH the altered table and the referenced table can be
// captured as a real relationship) are promoted to tokens; TABLE / ADD / DROP / PRIMARY / KEY /
// COLUMN / UNIQUE / CHECK deliberately stay plain ID so they remain ordinary identifiers, and
// alterTable matches them positionally / eats them permissively. All three are also re-admitted
// as ordinary identifiers inside expressions via exprAtom, same precedent as SEQUENCE/INDEX/TYPE.
CONSTRAINT  : C O N S T R A I N T ;
CREATE      : C R E A T E ;
CROSS       : C R O S S ;
DECLARE     : D E C L A R E ;
DELETE      : D E L E T E ;
DESC        : D E S C ;
DISTINCT    : D I S T I N C T ;
ELSE        : E L S E ;
END         : E N D ;
EXEC        : E X E C ;
EXECUTE     : E X E C U T E ;
EXISTS      : E X I S T S ;
FOREIGN     : F O R E I G N ;
FROM        : F R O M ;
FULL        : F U L L ;
FUNCTION    : F U N C T I O N ;
GOTO        : G O T O ;
GROUP       : G R O U P ;
HAVING      : H A V I N G ;
IF          : I F ;
IN          : I N ;
// Top-level `CREATE INDEX` DDL support. As with SEQUENCE, only INDEX is promoted to a token;
// UNIQUE / CLUSTERED / NONCLUSTERED / INCLUDE stay as plain ID so they remain usable as ordinary
// identifiers, and createIndex matches them positionally via indexModifier.
INDEX       : I N D E X ;
INNER       : I N N E R ;
INSERT      : I N S E R T ;
INTO        : I N T O ;
IS          : I S ;
JOIN        : J O I N ;
LEFT        : L E F T ;
LIKE        : L I K E ;
MATCHED     : M A T C H E D ;
MERGE       : M E R G E ;
NOCOUNT     : N O C O U N T ;
NOT         : N O T ;
NULL_KW     : N U L L ;
ON          : O N ;
OR          : O R ;
ORDER       : O R D E R ;
OUT         : O U T ;
OUTER       : O U T E R ;
OUTPUT      : O U T P U T ;
OVER        : O V E R ;
PARTITION   : P A R T I T I O N ;
PRINT       : P R I N T ;
PROC        : P R O C ;
PROCEDURE   : P R O C E D U R E ;
RAISERROR   : R A I S E R R O R ;
REFERENCES  : R E F E R E N C E S ;
RETURN      : R E T U R N ;
RETURNS     : R E T U R N S ;
RIGHT       : R I G H T ;
ROLLBACK    : R O L L B A C K ;
SAVE        : S A V E ;
SELECT      : S E L E C T ;
// Top-level `CREATE SEQUENCE` DDL support. Only the SEQUENCE keyword itself is promoted to a
// token; the option words (START WITH / INCREMENT BY / MINVALUE / MAXVALUE / CYCLE / CACHE / NO)
// are deliberately left lexing as plain ID so they keep working as ordinary column/variable
// identifiers everywhere else in the dialect. createSequenceTail eats them permissively and the
// visitor recovers their values by regex over the original text.
SEQUENCE    : S E Q U E N C E ;
SET         : S E T ;
THEN        : T H E N ;
THROW       : T H R O W ;
TOP         : T O P ;
TRAN        : T R A N ;
TRANSACTION : T R A N S A C T I O N ;
TRIGGER     : T R I G G E R ;
TRY         : T R Y ;
// Top-level `CREATE TYPE` DDL support (alias types and table types). Same precedent as
// SEQUENCE / INDEX above: only the TYPE keyword itself is promoted to a token — TABLE and the
// column/constraint words stay plain ID so they remain ordinary identifiers everywhere else —
// and TYPE is re-admitted as an ordinary identifier inside expressions via exprAtom.
//
// Promoting TYPE is what actually fixes a real crash: while TYPE lexed as a plain ID,
// `CREATE TYPE ...` was greedily absorbed by createIndex (whose `indexModifier : ID` happily
// ate the literal "TYPE"), error recovery then produced a CreateIndexContext with a null
// tableRef(), and visitCreateIndex NPE'd on ctx.tableRef().getText().
TYPE        : T Y P E ;
VIEW        : V I E W ;
UNION       : U N I O N ;
UPDATE      : U P D A T E ;
USING       : U S I N G ;
VALUES      : V A L U E S ;
WHEN        : W H E N ;
WHERE       : W H E R E ;
WHILE       : W H I L E ;
WITH        : W I T H ;

// ── Variables ─────────────────────────────────────────────────────────────────
// AT_AT_VAR must come BEFORE AT_VAR so @@ prefix is matched first
AT_AT_VAR   : '@@' [a-zA-Z_$] [a-zA-Z_$0-9]* ;
AT_VAR      : '@'  [a-zA-Z_$] [a-zA-Z_$0-9]* ;

// ── Identifiers ───────────────────────────────────────────────────────────────
// Bug (escaped-']]'-in-bracket-identifier audit): the old rule `'[' ~(']')+ ']'` stops at the
// FIRST ']', so a valid T-SQL identifier using the standard `]]`-escape for a literal `]` inside
// a bracketed name — e.g. `[table]]name]`, meaning the identifier `table]name` — got mis-tokenized
// at the grammar level before any visitor ever ran (the token ended at "[table]", leaving
// "]name]" as garbage trailing text). `(~']' | ']]')*` matches any non-`]` character OR a doubled
// `]]` escape, repeated, until the real closing single `]`.
BRACKET_ID  : '[' (~']' | ']]')* ']' ;
// Real bug, found via a live audit: the continuation class below used to omit `#`, so a
// global temp table like `##g13` lexed as TWO separate ID tokens — "#" then "#g13" — instead
// of one. Since `tableRef` only consumes a single un-dotted token, every `INSERT INTO ##name`
// (and any other single-token reference to a `##`-prefixed table) resolved to the bare name
// "#", merging every distinct `##`-prefixed table in a procedure into one fake table entry.
// `#` in the continuation class lets `##name`/`#name` lex as one token, matching how SQL
// Server itself treats `#`/`##` as part of the identifier, not a separate character.
ID          : [a-zA-Z_#$] [a-zA-Z_#$0-9]* ;

// ── String literals ───────────────────────────────────────────────────────────
N_STRING    : [Nn] '\'' (~'\'' | '\'\'')* '\'' ;
SQ_STRING   : '\'' (~'\'' | '\'\'')* '\'' ;

// ── Numeric literals ─────────────────────────────────────────────────────────
DEC_NUM     : [0-9]* '.' [0-9]+ | [0-9]+ '.' [0-9]* ;
INT_NUM     : [0-9]+ ;

// ── Operators & punctuation ──────────────────────────────────────────────────
LPAREN      : '(' ;
RPAREN      : ')' ;
COMMA       : ',' ;
SEMI        : ';' ;
DOT         : '.' ;
COLON       : ':' ;
EQ          : '=' ;
NEQ         : '<>' | '!=' ;
LEQ         : '<=' ;
GEQ         : '>=' ;
LT          : '<' ;
GT          : '>' ;
PLUS        : '+' ;
MINUS       : '-' ;
STAR        : '*' ;
SLASH       : '/' ;
PERCENT     : '%' ;
AMPERSAND   : '&' ;
PIPE        : '|' ;
CARET       : '^' ;
TILDE       : '~' ;
EXCL        : '!' ;

// ── Comments & whitespace ────────────────────────────────────────────────────
LINE_COMMENT  : '--' ~[\r\n]*   -> skip ;
BLOCK_COMMENT : '/*' .*? '*/'  -> skip ;
WS            : [ \t\r\n]+     -> skip ;

// ── Fragments (case-insensitive letter pairs) ─────────────────────────────────
fragment A : [aA] ; fragment B : [bB] ; fragment C : [cC] ; fragment D : [dD] ;
fragment E : [eE] ; fragment F : [fF] ; fragment G : [gG] ; fragment H : [hH] ;
fragment I : [iI] ; fragment J : [jJ] ; fragment K : [kK] ; fragment L : [lL] ;
fragment M : [mM] ; fragment N : [nN] ; fragment O : [oO] ; fragment P : [pP] ;
fragment Q : [qQ] ; fragment R : [rR] ; fragment S : [sS] ; fragment T : [tT] ;
fragment U : [uU] ; fragment V : [vV] ; fragment W : [wW] ; fragment X : [xX] ;
fragment Y : [yY] ; fragment Z : [zZ] ;
