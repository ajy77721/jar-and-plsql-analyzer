// ============================================================
// PostgresLexer.g4
// Lexer grammar for IBM POSTGRES SQL PL stored procedures / functions.
// Produces tokens consumed by PostgresParser.g4.
// Case-insensitive via fragment A-Z helpers.
// ============================================================
lexer grammar PostgresLexer;

// ── Keywords ─────────────────────────────────────────────────────────────────
// NOTE: Longest-match rule applies; INTO before IN_KW ensures INTO is tokenized first.
// INOUT must come before IN_KW and OUT_KW so INOUT is matched as one token.
// ELSEIF must come before ELSE so ELSEIF is matched as one token.
// RETURNS must come before RETURN so RETURNS is matched as one token.
// SQLEXCEPTION, SQLSTATE, SQLWARNING must come before SQL_KW.

// ── CREATE TRIGGER keywords ──────────────────────────────────────────────────
// Added for the `createTrigger` parser rule (top-level CREATE TRIGGER support). All of these
// are ALSO listed in the parser's permissive `id` rule, so a column/table/variable that happens
// to be called "row", "statement", "each", "of", "before", "after", "instead" or "trigger"
// still parses exactly as it did before these tokens existed.
AFTER        : A F T E R ;
BEFORE       : B E F O R E ;
EACH         : E A C H ;
INSTEAD      : I N S T E A D ;
OF           : O F ;
ROW          : R O W ;
STATEMENT    : S T A T E M E N T ;
TRIGGER      : T R I G G E R ;

// ── CREATE [MATERIALIZED] VIEW keywords ──────────────────────────────────────
// Added for the `createView` parser rule. Like the trigger keywords above, both are also
// listed in the parser's `id` rule so using them as an identifier still parses. The
// WITH [CASCADED|LOCAL] CHECK OPTION / WITH [NO] DATA tails deliberately get NO tokens of
// their own — `viewTail` consumes them permissively as ordinary identifiers.
MATERIALIZED : M A T E R I A L I Z E D ;
VIEW         : V I E W ;

// ── CREATE SEQUENCE keywords ─────────────────────────────────────────────────
// Added for the `createSequence` parser rule (top-level CREATE SEQUENCE support). Same rationale
// as the trigger/view keywords above: every one of these is a perfectly legal unquoted identifier
// in real PostgreSQL (there are plenty of columns called "cache", "cycle", "start", "no"), so all
// of them are ALSO listed in the parser's permissive `id` rule. They are structural only INSIDE
// `createSequence`, where they are matched as explicit tokens.
CACHE        : C A C H E ;
CYCLE        : C Y C L E ;
INCREMENT    : I N C R E M E N T ;
MAXVALUE     : M A X V A L U E ;
MINVALUE     : M I N V A L U E ;
NO           : N O ;
SEQUENCE     : S E Q U E N C E ;
START        : S T A R T ;

// ── CREATE INDEX keywords ────────────────────────────────────────────────────
// Added for the `createIndex` parser rule. Same rationale as the sequence keywords above: all
// three are legal unquoted identifiers in PostgreSQL and are therefore also listed in the
// parser's `id` rule. (USING already existed as a token — it is deliberately NOT in `id`.)
CONCURRENTLY : C O N C U R R E N T L Y ;
INDEX        : I N D E X ;
UNIQUE       : U N I Q U E ;

// ── ALTER TABLE keywords ─────────────────────────────────────────────────────
// Added for the `alterTable` parser rule (top-level AND in-body ALTER TABLE support). Before
// these existed, ALTER/TABLE/ADD/… all lexed as plain ID, so an ALTER TABLE inside a routine
// body was swallowed whole by `otherStatement`'s catch-all with ZERO reported errors — an FK
// dependency between two tables disappeared with no signal at all. Same rationale as the
// trigger/view/sequence/index keyword blocks above: every one of these is also listed in the
// parser's permissive `id` rule, so anything that previously used one of these words as a
// column/table/variable/function name still parses exactly as before. They are structural only
// INSIDE `alterTable`, where they are matched as explicit tokens.
ALTER        : A L T E R ;
TABLE        : T A B L E ;
ADD          : A D D ;
DROP         : D R O P ;
COLUMN       : C O L U M N ;
CONSTRAINT   : C O N S T R A I N T ;
FOREIGN      : F O R E I G N ;
KEY          : K E Y ;
PRIMARY      : P R I M A R Y ;
REFERENCES   : R E F E R E N C E S ;
CHECK        : C H E C K ;

// ── CREATE TYPE / CREATE DOMAIN keywords ─────────────────────────────────────
// Added for the `createType` / `createDomain` parser rules. Same rationale/`id` treatment as
// every keyword block above.
TYPE         : T Y P E ;
ENUM         : E N U M ;
DOMAIN       : D O M A I N ;

ALL          : A L L ;
AND          : A N D ;
AS           : A S ;
ASC          : A S C ;
ATOMIC       : A T O M I C ;
BEGIN        : B E G I N ;
BETWEEN      : B E T W E E N ;
BY           : B Y ;
CALL         : C A L L ;
CASE         : C A S E ;
CAST         : C A S T ;
CLOSE        : C L O S E ;
COMMIT       : C O M M I T ;
CONDITION    : C O N D I T I O N ;
CONFLICT     : C O N F L I C T ;
CONTINUE     : C O N T I N U E ;
CREATE       : C R E A T E ;
CROSS        : C R O S S ;
CURRENT      : C U R R E N T ;
CURSOR       : C U R S O R ;
DECLARE      : D E C L A R E ;
DEFAULT      : D E F A U L T ;
DELETE       : D E L E T E ;
DESC         : D E S C ;
DISTINCT     : D I S T I N C T ;
DO           : D O ;
// ELSEIF/ELSIF must both come before ELSE so they are matched as one token each.
// ELSIF is the canonical PL/pgSQL spelling (ELSEIF is the accepted alias); without its own
// token it lexed as a bare ID and every ELSIF/ELSE branch of the IF was lost with syntax errors.
ELSEIF       : E L S E I F ;
ELSIF        : E L S I F ;
ELSE         : E L S E ;
END          : E N D ;
EXECUTE      : E X E C U T E ;
EXISTS       : E X I S T S ;
EXIT         : E X I T ;
FETCH        : F E T C H ;
FOREACH      : F O R E A C H ;
FOR          : F O R ;
FROM         : F R O M ;
FULL         : F U L L ;
FUNCTION     : F U N C T I O N ;
GROUP        : G R O U P ;
HANDLER      : H A N D L E R ;
HAVING       : H A V I N G ;
HOLD         : H O L D ;
IF           : I F ;
IMMEDIATE    : I M M E D I A T E ;
INOUT        : I N O U T ;
INNER        : I N N E R ;
INSERT       : I N S E R T ;
INTO         : I N T O ;
IN_KW        : I N ;
IS           : I S ;
JOIN         : J O I N ;
LANGUAGE     : L A N G U A G E ;
// Sep05-11. Without this token LATERAL matched ID, so `CROSS JOIN LATERAL (SELECT …)` parsed as a
// join against a table literally named "LATERAL" — 81 phantom access rows and a fake entry in the
// table list. It is a reserved word in PostgreSQL, so it is deliberately NOT added to the `id`
// rule: no real column or table can be called LATERAL unquoted, and a quoted "lateral" still
// reaches `id` via DQ_ID.
LATERAL      : L A T E R A L ;
LEFT         : L E F T ;
LIKE         : L I K E ;
LOOP         : L O O P ;
MERGE        : M E R G E ;
NEXT         : N E X T ;
NOT          : N O T ;
NULL_KW      : N U L L ;
ON           : O N ;
OPEN         : O P E N ;
OR           : O R ;
ORDER        : O R D E R ;
OUT_KW       : O U T ;
OUTER        : O U T E R ;
OVER         : O V E R ;
PARTITION    : P A R T I T I O N ;
PREPARE      : P R E P A R E ;
PROCEDURE    : P R O C E D U R E ;
RELEASE      : R E L E A S E ;
REPEAT       : R E P E A T ;
REPLACE      : R E P L A C E ;
RESIGNAL     : R E S I G N A L ;
RETURNING    : R E T U R N I N G ;
RETURNS      : R E T U R N S ;
RETURN       : R E T U R N ;
RIGHT        : R I G H T ;
ROLLBACK     : R O L L B A C K ;
EXCEPT       : E X C E P T ;
INTERSECT    : I N T E R S E C T ;
SAVEPOINT    : S A V E P O I N T ;
SELECT       : S E L E C T ;
SET          : S E T ;
SIGNAL       : S I G N A L ;
SQLEXCEPTION : S Q L E X C E P T I O N ;
EXCEPTION    : E X C E P T I O N ;
SQLSTATE     : S Q L S T A T E ;
SQLWARNING   : S Q L W A R N I N G ;
SQL_KW       : S Q L ;
THEN         : T H E N ;
TO           : T O ;
TRUNCATE     : T R U N C A T E ;
UNDO         : U N D O ;
UNION        : U N I O N ;
UNTIL        : U N T I L ;
UPDATE       : U P D A T E ;
USING        : U S I N G ;
VALUE        : V A L U E ;
VALUES       : V A L U E S ;
WHEN         : W H E N ;
WHERE        : W H E R E ;
WHILE        : W H I L E ;
WITH         : W I T H ;
RECURSIVE    : R E C U R S I V E ;
WORK         : W O R K ;

// ── Identifiers ───────────────────────────────────────────────────────────────
// Double-quoted identifiers (POSTGRES uses " not [])
DQ_ID       : '"' (~'"')+ '"' ;
ID          : [a-zA-Z_$-￿] [a-zA-Z_$0-9-￿]* ;

// ── String literals ───────────────────────────────────────────────────────────
// PostgreSQL C-style escape string: E'…' / e'…'. Distinct from SQ_STRING because a backslash
// escapes the following character (so '\'' does NOT terminate the literal). Declared before
// SQ_STRING; the leading E would otherwise lex as a 1-char ID followed by a separate SQ_STRING,
// which silently defeated every "is this whole expression one string literal?" test (e.g.
// format(E'SELECT * FROM %I', 'emp') produced no table op and no error).
ESQ_STRING  : [eE] '\'' ('\\' . | '\'\'' | ~['\\])* '\'' ;
SQ_STRING   : '\'' (~'\'' | '\'\'')* '\'' ;

// ── Numeric literals ─────────────────────────────────────────────────────────
DEC_NUM     : [0-9]* '.' [0-9]+ | [0-9]+ '.' [0-9]* ;
INT_NUM     : [0-9]+ ;

// ── Operators & punctuation ──────────────────────────────────────────────────
LPAREN      : '(' ;
RPAREN      : ')' ;
LBRACK      : '[' ;
RBRACK      : ']' ;
COMMA       : ',' ;
SEMI        : ';' ;
DOT         : '.' ;
EQ          : '=' ;
NEQ         : '<>' | '!=' ;
LEQ         : '<=' ;
GEQ         : '>=' ;
// Bug 1 (§postgres-operator-audit): `@>` (containment) and `<@` (contained-by) must be declared
// as their own 2-char tokens BEFORE the bare `<`/`>` single-char rules so ANTLR's longest-match
// picks them over a mis-split `< @` / `@ >`. Previously there was no lexer rule for `@` at all,
// so the character was silently dropped by ANTLR's default error recovery, collapsing `@>` into
// a bare `>` (GT) — a silent semantic corruption (containment test became greater-than).
CONTAINS_OP     : '@>' ;
CONTAINED_BY_OP : '<@' ;
LT          : '<' ;
GT          : '>' ;
PLUS        : '+' ;
// Bug 2 (§postgres-operator-audit): JSON extract operators `->` and `->>` must be declared as
// their own tokens BEFORE bare MINUS/GT so ANTLR's longest-match picks `->>' (3 chars) over `->'
// (2 chars) over a mis-split MINUS+GT. Previously neither existed, so `payload->'k'` silently
// decomposed into unrelated ID/MINUS/SQ_STRING tokens with zero surfaced error.
JSON_EXTRACT_TEXT_OP : '->>' ;
JSON_EXTRACT_OP       : '->' ;
MINUS       : '-' ;
STAR        : '*' ;
SLASH       : '/' ;
PERCENT     : '%' ;
AMPERSAND   : '&' ;
PIPE        : '|' ;
CARET       : '^' ;
// Bug 3 (§postgres-operator-audit): `~*` (case-insensitive regex match) must be declared as its
// own 2-char token BEFORE the bare `~' rule so ANTLR's longest-match prefers it over a mis-split
// TILDE+STAR. Previously only bare TILDE existed, so `name ~* '^abc.*'` lexed as three
// disconnected atoms (TILDE, STAR, SQ_STRING).
REGEX_IMATCH_OP : '~*' ;
// Round-3 bug (§operator-audit, same class as `@>`/`->` above): `!~` / `!~*` (regex NOT-match) had
// no rule, so the `!` was dropped by the lexer's error recovery and `a !~ 'x'` silently became the
// regex MATCH `a ~ 'x'` — an inverted predicate with no error a caller could see. Declared before
// TILDE so longest-match prefers the 3-char then the 2-char form.
REGEX_NOT_IMATCH_OP : '!~*' ;
REGEX_NOT_MATCH_OP  : '!~' ;
TILDE       : '~' ;
// Round-3 bug (§operator-audit): the JSON/JSONB path operators `#>` (get at path) and `#>>` (get
// at path as text), the jsonb path-delete `#-`, the bitwise XOR `#`, the full-text match `@@` and
// the jsonb key-existence family `?` / `?|` / `?&` had NO lexer rule at all for their leading
// character, so — exactly like `@>` before it was fixed — that character was silently discarded by
// ANTLR's default error recovery and the operator collapsed into an unrelated one (`#>` became a
// bare `>`, i.e. a JSON path lookup became a greater-than comparison). Longest-match ordering:
// the 3-char forms are declared before the 2-char forms, which precede the 1-char fallbacks.
JSON_PATH_TEXT_OP : '#>>' ;
JSON_PATH_OP      : '#>' ;
JSON_DELETE_OP    : '#-' ;
HASH              : '#' ;
TEXT_MATCH_OP     : '@@' ;
AT_SIGN           : '@' ;
JSON_EXISTS_ANY_OP : '?|' ;
JSON_EXISTS_ALL_OP : '?&' ;
QUESTION           : '?' ;
ASSIGN      : ':=' ;
COLON       : ':' ;

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
