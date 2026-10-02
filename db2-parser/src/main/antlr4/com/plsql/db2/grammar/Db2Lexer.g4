// ============================================================
// Db2Lexer.g4
// Lexer grammar for IBM DB2 SQL PL stored procedures / functions.
// Produces tokens consumed by Db2Parser.g4.
// Case-insensitive via fragment A-Z helpers.
// ============================================================
lexer grammar Db2Lexer;

// ── Keywords ─────────────────────────────────────────────────────────────────
// NOTE: Longest-match rule applies; INTO before IN_KW ensures INTO is tokenized first.
// INOUT must come before IN_KW and OUT_KW so INOUT is matched as one token.
// ELSEIF must come before ELSE so ELSEIF is matched as one token.
// RETURNS must come before RETURN so RETURNS is matched as one token.
// SQLEXCEPTION, SQLSTATE, SQLWARNING must come before SQL_KW.

// ADD / ALTER / TABLE / COLUMN / DROP / CONSTRAINT / FOREIGN / KEY / PRIMARY / CHECK /
// REFERENCES / TYPE are structural only inside alterTable / createType. Every one of them is
// also listed in the parser's `id` rule so it stays usable as an ordinary identifier
// (TABLE in DECLARE GLOBAL TEMPORARY TABLE, CHECK/KEY/TYPE as column names, …).
// REFERENCES is not a prefix of REFERENCING (they diverge at the 9th char), so their relative
// order here is irrelevant to the longest-match rule.
ADD          : A D D ;
AFTER        : A F T E R ;
ALL          : A L L ;
ALTER        : A L T E R ;
AND          : A N D ;
AS           : A S ;
ASC          : A S C ;
ATOMIC       : A T O M I C ;
BEFORE       : B E F O R E ;
BEGIN        : B E G I N ;
BETWEEN      : B E T W E E N ;
BY           : B Y ;
CACHE        : C A C H E ;
CALL         : C A L L ;
CASCADE      : C A S C A D E ;
CASE         : C A S E ;
CAST         : C A S T ;
CHECK        : C H E C K ;
CLOSE        : C L O S E ;
COLUMN       : C O L U M N ;
COMMIT       : C O M M I T ;
CONDITION    : C O N D I T I O N ;
CONSTRAINT   : C O N S T R A I N T ;
CONTINUE     : C O N T I N U E ;
CREATE       : C R E A T E ;
CROSS        : C R O S S ;
CURRENT      : C U R R E N T ;
CURSOR       : C U R S O R ;
CYCLE        : C Y C L E ;
DECLARE      : D E C L A R E ;
DEFAULT      : D E F A U L T ;
DELETE       : D E L E T E ;
DESC         : D E S C ;
DISTINCT     : D I S T I N C T ;
DO           : D O ;
DROP         : D R O P ;
EACH         : E A C H ;
ELSEIF       : E L S E I F ;
ELSE         : E L S E ;
END          : E N D ;
EXECUTE      : E X E C U T E ;
EXISTS       : E X I S T S ;
EXIT         : E X I T ;
FETCH        : F E T C H ;
FOR          : F O R ;
FOREIGN      : F O R E I G N ;
FROM         : F R O M ;
FULL         : F U L L ;
FUNCTION     : F U N C T I O N ;
GROUP        : G R O U P ;
HANDLER      : H A N D L E R ;
HAVING       : H A V I N G ;
HOLD         : H O L D ;
IF           : I F ;
IMMEDIATE    : I M M E D I A T E ;
INCREMENT    : I N C R E M E N T ;
INDEX        : I N D E X ;
INOUT        : I N O U T ;
INNER        : I N N E R ;
INSERT       : I N S E R T ;
INSTEAD      : I N S T E A D ;
INTO         : I N T O ;
IN_KW        : I N ;
IS           : I S ;
ITERATE      : I T E R A T E ;
JOIN         : J O I N ;
KEY          : K E Y ;
LANGUAGE     : L A N G U A G E ;
LEAVE        : L E A V E ;
LEFT         : L E F T ;
LIKE         : L I K E ;
LOOP         : L O O P ;
MAXVALUE     : M A X V A L U E ;
// MERGE is structural only as the first token of a mergeStatement; it stays usable as an
// ordinary identifier everywhere else via the `id` rule in Db2Parser.g4.
MERGE        : M E R G E ;
MINVALUE     : M I N V A L U E ;
// MODULE / VARIABLE / PUBLISH are structural only inside createModule / alterModule /
// createVariable. Every one of them is also listed in the parser's `id` rule so it stays
// usable as an ordinary identifier elsewhere.
MODULE       : M O D U L E ;
NEXT         : N E X T ;
NOT          : N O T ;
NO           : N O ;
NULL_KW      : N U L L ;
OF           : O F ;
ON           : O N ;
OPEN         : O P E N ;
OR           : O R ;
ORDER        : O R D E R ;
OUT_KW       : O U T ;
OUTER        : O U T E R ;
OVER         : O V E R ;
PARTITION    : P A R T I T I O N ;
PREPARE      : P R E P A R E ;
PRIMARY      : P R I M A R Y ;
PROCEDURE    : P R O C E D U R E ;
PUBLISH      : P U B L I S H ;
REFERENCES   : R E F E R E N C E S ;
REFERENCING  : R E F E R E N C I N G ;
RELEASE      : R E L E A S E ;
REPEAT       : R E P E A T ;
REPLACE      : R E P L A C E ;
RESIGNAL     : R E S I G N A L ;
RETURNS      : R E T U R N S ;
RETURN       : R E T U R N ;
RIGHT        : R I G H T ;
ROLLBACK     : R O L L B A C K ;
ROW          : R O W ;
EXCEPT       : E X C E P T ;
INTERSECT    : I N T E R S E C T ;
SAVEPOINT    : S A V E P O I N T ;
SELECT       : S E L E C T ;
SEQUENCE     : S E Q U E N C E ;
SET          : S E T ;
SIGNAL       : S I G N A L ;
SQLEXCEPTION : S Q L E X C E P T I O N ;
SQLSTATE     : S Q L S T A T E ;
SQLWARNING   : S Q L W A R N I N G ;
SQL_KW       : S Q L ;
START        : S T A R T ;
STATEMENT    : S T A T E M E N T ;
TABLE        : T A B L E ;
THEN         : T H E N ;
TO           : T O ;
TRIGGER      : T R I G G E R ;
TYPE         : T Y P E ;
UNDO         : U N D O ;
UNION        : U N I O N ;
UNIQUE       : U N I Q U E ;
UNTIL        : U N T I L ;
UPDATE       : U P D A T E ;
USING        : U S I N G ;
VALUE        : V A L U E ;
VALUES       : V A L U E S ;
VARIABLE     : V A R I A B L E ;
VIEW         : V I E W ;
WHEN         : W H E N ;
WHERE        : W H E R E ;
WHILE        : W H I L E ;
WITH         : W I T H ;
WORK         : W O R K ;

// ── Identifiers ───────────────────────────────────────────────────────────────
// Double-quoted identifiers (DB2 uses " not [])
DQ_ID       : '"' (~'"')+ '"' ;
ID          : [a-zA-Z_$] [a-zA-Z_$0-9]* ;

// ── String literals ───────────────────────────────────────────────────────────
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
COLON       : ':' ;
// JDBC/DB2 parameter marker. Appears in prepared / dynamic SQL text (e.g.
// "SELECT ... WHERE id = ?"); without a token for it the lexer raised a
// token-recognition error and such a statement could not be parsed at all.
QMARK       : '?' ;

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
