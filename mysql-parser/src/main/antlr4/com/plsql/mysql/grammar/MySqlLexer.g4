// ============================================================
// MySqlLexer.g4
// Lexer grammar for MySQL stored procedures / functions.
// Produces tokens consumed by MySqlParser.g4.
// ============================================================
lexer grammar MySqlLexer;

// ── Keywords (case-insensitive via fragment helpers) ─────────────────────────
ALL           : A L L ;
AND           : A N D ;
AS            : A S ;
ASC           : A S C ;
AUTO_INC      : A U T O '_' I N C R E M E N T ;
BEGIN         : B E G I N ;
BETWEEN       : B E T W E E N ;
BY            : B Y ;
CALL          : C A L L ;
CASE          : C A S E ;
CHAIN         : C H A I N ;
COMMIT        : C O M M I T ;
STACKED       : S T A C K E D ;
START         : S T A R T ;
TRANSACTION   : T R A N S A C T I O N ;
DUPLICATE     : D U P L I C A T E ;
CAST          : C A S T ;
CHARACTER     : C H A R A C T E R ;
CHARSET       : C H A R S E T ;
CLOSE         : C L O S E ;
COLLATE       : C O L L A T E ;
COMMENT_KW    : C O M M E N T ;
CONDITION     : C O N D I T I O N ;
CONTAINS      : C O N T A I N S ;
CONTINUE      : C O N T I N U E ;
CONVERT       : C O N V E R T ;
CREATE        : C R E A T E ;
CROSS         : C R O S S ;
CURSOR        : C U R S O R ;
DATA          : D A T A ;
DEALLOCATE    : D E A L L O C A T E ;
DECLARE       : D E C L A R E ;
DEFAULT       : D E F A U L T ;
DEFINER       : D E F I N E R ;
DELAYED       : D E L A Y E D ;
DELETE        : D E L E T E ;
DESC          : D E S C ;
DETERMINISTIC : D E T E R M I N I S T I C ;
DIAGNOSTICS   : D I A G N O S T I C S ;
DISTINCT      : D I S T I N C T ;
DO            : D O ;
DROP          : D R O P ;
ELSE          : E L S E ;
ELSEIF        : E L S E I F ;
END           : E N D ;
EXCEPT        : E X C E P T ;
EXECUTE       : E X E C U T E ;
EXISTS        : E X I S T S ;
EXIT          : E X I T ;
FALSE_LIT     : F A L S E ;
FETCH         : F E T C H ;
FOR           : F O R ;
FORCE         : F O R C E ;
FOUND         : F O U N D ;
FROM          : F R O M ;
FUNCTION      : F U N C T I O N ;
GET           : G E T ;
GROUP         : G R O U P ;
HANDLER       : H A N D L E R ;
HAVING        : H A V I N G ;
HIGH_PRI      : H I G H '_' P R I O R I T Y ;
IF            : I F ;
IGNORE        : I G N O R E ;
IN            : I N ;
INDEX         : I N D E X ;
INNER         : I N N E R ;
INOUT         : I N O U T ;
INSERT        : I N S E R T ;
INTERSECT     : I N T E R S E C T ;
INTO          : I N T O ;
IS            : I S ;
ITERATE       : I T E R A T E ;
JOIN          : J O I N ;
KEY           : K E Y ;
LANGUAGE      : L A N G U A G E ;
LEAVE         : L E A V E ;
LEFT          : L E F T ;
LIKE          : L I K E ;
LIMIT         : L I M I T ;
LOOP          : L O O P ;
LOW_PRI       : L O W '_' P R I O R I T Y ;
MODIFIES      : M O D I F I E S ;
NATURAL       : N A T U R A L ;
NEXT          : N E X T ;
NO            : N O ;
NOT           : N O T ;
NULL_LIT      : N U L L ;
OF            : O F ;
ON            : O N ;
OPEN          : O P E N ;
OR            : O R ;
ORDER         : O R D E R ;
OUT           : O U T ;
OUTER         : O U T E R ;
OUTFILE       : O U T F I L E ;
OVER          : O V E R ;
PARTITION     : P A R T I T I O N ;
PRECEDING     : P R E C E D I N G ;
FOLLOWING     : F O L L O W I N G ;
UNBOUNDED     : U N B O U N D E D ;
ROWS          : R O W S ;
RANGE         : R A N G E ;
WINDOW        : W I N D O W ;
PREPARE       : P R E P A R E ;
PRIMARY       : P R I M A R Y ;
PROCEDURE     : P R O C E D U R E ;
QUICK         : Q U I C K ;
READS         : R E A D S ;
RECURSIVE     : R E C U R S I V E ;
REGEXP        : R E G E X P ;
REPEAT        : R E P E A T ;
REPLACE       : R E P L A C E ;
RELEASE       : R E L E A S E ;
RESIGNAL      : R E S I G N A L ;
RETURN        : R E T U R N ;
ROLLBACK      : R O L L B A C K ;
SAVEPOINT     : S A V E P O I N T ;
RETURNS       : R E T U R N S ;
RIGHT         : R I G H T ;
ROW           : R O W ;
SECURITY      : S E C U R I T Y ;
SELECT        : S E L E C T ;
SET           : S E T ;
SIGNAL        : S I G N A L ;
SIGNED        : S I G N E D ;
SQL_KW        : S Q L ;
SQLEXCEPTION  : S Q L E X C E P T I O N ;
SQLSTATE      : S Q L S T A T E ;
SQLWARNING    : S Q L W A R N I N G ;
STRAIGHT_JOIN : S T R A I G H T '_' J O I N ;
TABLE         : T A B L E ;
TRUNCATE      : T R U N C A T E ;
TEMPORARY     : T E M P O R A R Y ;
THEN          : T H E N ;
TO            : T O ;
TRUE_LIT      : T R U E ;
UNDO          : U N D O ;
UNION         : U N I O N ;
UNIQUE        : U N I Q U E ;
UNKNOWN       : U N K N O W N ;
UNTIL         : U N T I L ;
UPDATE        : U P D A T E ;
USE           : U S E ;
USING         : U S I N G ;
VALUE         : V A L U E ;
VALUES        : V A L U E S ;
WHEN          : W H E N ;
WHERE         : W H E R E ;
WHILE         : W H I L E ;
WITH          : W I T H ;
WORK          : W O R K ;
// GROUP BY modifiers (MySQL 8.0+): `WITH ROLLUP` and the modern `ROLLUP(...)` /
// `CUBE(...)` / `GROUPING SETS(...)` syntaxes — recognized as dedicated tokens so the
// groupByClause grammar rule can model them instead of falling through to the unknown-
// statement catch-all (WITH ROLLUP) or being mistaken for a plain function call (ROLLUP(...)).
ROLLUP        : R O L L U P ;
CUBE          : C U B E ;
GROUPING      : G R O U P I N G ;
SETS          : S E T S ;
// ── CREATE TRIGGER keywords ──────────────────────────────────────────────────
// TRIGGER/BEFORE/AFTER/EACH/FOLLOWS/PRECEDES are only needed by the createTrigger
// rule. They are non-reserved in MySQL, so every one of them is also listed in the
// parser's reservedAsId rule — otherwise a column or variable literally named
// `after`, `each` or `trigger` (all legal MySQL identifiers) would stop lexing as ID
// and break routine bodies that previously parsed.
// FOLLOWS must not be confused with the pre-existing FOLLOWING window-frame keyword;
// ANTLR's longest-match rule keeps them distinct.
TRIGGER       : T R I G G E R ;
BEFORE        : B E F O R E ;
AFTER         : A F T E R ;
EACH          : E A C H ;
FOLLOWS       : F O L L O W S ;
PRECEDES      : P R E C E D E S ;
// ── CREATE VIEW keywords ─────────────────────────────────────────────────────
// Same non-reserved treatment as the trigger keywords above: all of these are also
// listed in the parser's reservedAsId rule so ordinary identifiers named `view`,
// `check`, `option` or `local` keep working inside routine bodies.
VIEW          : V I E W ;
ALGORITHM     : A L G O R I T H M ;
CHECK         : C H E C K ;
OPTION        : O P T I O N ;
CASCADED      : C A S C A D E D ;
LOCAL         : L O C A L ;
// ── CREATE INDEX keywords ────────────────────────────────────────────────────
// INDEX, UNIQUE, USING, ASC and DESC already exist as tokens above; only the index
// flavours and index-type names are new. Same non-reserved treatment as the trigger
// and view keywords: all four are also listed in the parser's reservedAsId rule, so
// a column or variable named `hash`, `btree`, `spatial` or `fulltext` (all legal
// MySQL identifiers) keeps lexing usable inside routine bodies.
// ── Generated / computed column keywords ─────────────────────────────────────
// `c INT GENERATED ALWAYS AS (a + b) STORED` and its short form `c INT AS (a + b) VIRTUAL`.
// All four are non-reserved in MySQL, so all four are also listed in the parser's reservedAsId
// rule — otherwise a column or variable named `stored`, `virtual`, `always` or `generated`
// (every one a legal MySQL identifier) would stop lexing as ID and break bodies that parsed.
GENERATED     : G E N E R A T E D ;
ALWAYS        : A L W A Y S ;
STORED        : S T O R E D ;
VIRTUAL       : V I R T U A L ;
FULLTEXT      : F U L L T E X T ;
SPATIAL       : S P A T I A L ;
BTREE         : B T R E E ;
HASH          : H A S H ;
// ── ALTER TABLE keywords ─────────────────────────────────────────────────────
// TABLE, DROP, PRIMARY, KEY, UNIQUE, CHECK and INDEX already exist as tokens above;
// only these six are new. ALTER TABLE previously had no grammar rule at all and fell
// into the unknownStatement catch-all, which threw away both the altered table and —
// far more costly — the FOREIGN KEY … REFERENCES dependency edge the statement exists
// to declare. Same non-reserved treatment as the trigger/view/index keywords: all six
// are also listed in the parser's reservedAsId rule, so a column or variable named
// `add`, `column`, `constraint`, `references`, `foreign` or `alter` (all legal MySQL
// identifiers in these positions) keeps working inside routine bodies.
ALTER         : A L T E R ;
ADD           : A D D ;
COLUMN        : C O L U M N ;
CONSTRAINT    : C O N S T R A I N T ;
FOREIGN       : F O R E I G N ;
REFERENCES    : R E F E R E N C E S ;
// ── CREATE EVENT keywords (MySQL's scheduled jobs) ───────────────────────────
// MySQL's own scheduler is the EVENT object (`CREATE EVENT ... ON SCHEDULE ... DO ...`),
// the MySQL analogue of Oracle DBMS_SCHEDULER / SQL Agent / pg_cron. Without these
// tokens a standalone CREATE EVENT failed at the first token after CREATE and the event
// was invisible to the analyzer. Every one is non-reserved in MySQL and is also listed in
// the parser's reservedAsId rule, so a column or variable named `event`, `schedule`,
// `day` or `enable` (all legal MySQL identifiers) keeps lexing usable.
EVENT         : E V E N T ;
SCHEDULE      : S C H E D U L E ;
EVERY         : E V E R Y ;
AT            : A T ;
STARTS        : S T A R T S ;
ENDS          : E N D S ;
COMPLETION    : C O M P L E T I O N ;
PRESERVE      : P R E S E R V E ;
ENABLE        : E N A B L E ;
DISABLE       : D I S A B L E ;
SLAVE         : S L A V E ;
RENAME        : R E N A M E ;
// Simple interval units for `ON SCHEDULE EVERY <n> DAY`. The compound YEAR_MONTH /
// DAY_HOUR spellings are rare in events and would add 14 more tokens, so they are not
// tokenized; only the simple units below are needed for real event schedules.
YEAR          : Y E A R ;
QUARTER       : Q U A R T E R ;
MONTH         : M O N T H ;
WEEK          : W E E K ;
DAY           : D A Y ;
HOUR          : H O U R ;
MINUTE        : M I N U T E ;
SECOND        : S E C O N D ;
MICROSECOND   : M I C R O S E C O N D ;
// ── JSON_TABLE keywords ──────────────────────────────────────────────────────
// `SELECT ... FROM t, JSON_TABLE(t.doc, '$.items[*]' COLUMNS (...)) AS jt` is the only
// table function MySQL has, and without a grammar rule for it the entire SELECT — every
// ordinary table in its FROM list included — was discarded by the unknown-statement
// catch-all. JSON_TABLE is RESERVED in MySQL 8.0 and is therefore deliberately NOT listed
// in reservedAsId; the five spec keywords below (COLUMNS/PATH/NESTED/ORDINALITY/EMPTY/
// ERROR) are all non-reserved and ARE listed there, so a column named `path`, `columns`
// or `error` keeps working.
JSON_TABLE    : J S O N '_' T A B L E ;
// LATERAL (MySQL 8.0.14+) — a derived table allowed to reference columns of the tables
// preceding it in the FROM list. Reserved in MySQL 8.0, so deliberately NOT in reservedAsId.
LATERAL       : L A T E R A L ;
// ── Fulltext search: MATCH (cols) AGAINST ('text' [modifier]) ────────────────
// MATCH and AGAINST are reserved in MySQL and stay out of reservedAsId. BOOLEAN, MODE,
// QUERY and EXPANSION are not reserved and ARE listed there — BOOLEAN especially, since
// `DECLARE flag BOOLEAN` resolves its type name through simpleId.
// ── SELECT modifiers ─────────────────────────────────────────────────────────
// The complete set MySQL allows between SELECT and the select list. They exist as real
// tokens because the alternative — a catch-all `ID` in the selectModifier rule — silently
// swallowed the NAME of the first function in the select list: `SELECT fa(1), fb(2) FROM t`
// parsed as modifier `fa`, item `(1)`, item `fb(2)`, with no syntax error to show for it.
// SQL_BUFFER_RESULT and SQL_NO_CACHE are the only two that are non-reserved in MySQL, so
// those two alone are also listed in the parser's reservedAsId rule.
DISTINCTROW           : D I S T I N C T R O W ;
SQL_SMALL_RESULT      : S Q L '_' S M A L L '_' R E S U L T ;
SQL_BIG_RESULT        : S Q L '_' B I G '_' R E S U L T ;
SQL_BUFFER_RESULT     : S Q L '_' B U F F E R '_' R E S U L T ;
SQL_NO_CACHE          : S Q L '_' N O '_' C A C H E ;
SQL_CALC_FOUND_ROWS   : S Q L '_' C A L C '_' F O U N D '_' R O W S ;
MATCH         : M A T C H ;
AGAINST       : A G A I N S T ;
BOOLEAN       : B O O L E A N ;
MODE          : M O D E ;
QUERY         : Q U E R Y ;
EXPANSION     : E X P A N S I O N ;
COLUMNS       : C O L U M N S ;
PATH          : P A T H ;
NESTED        : N E S T E D ;
ORDINALITY    : O R D I N A L I T Y ;
EMPTY         : E M P T Y ;
ERROR         : E R R O R ;
XOR           : X O R ;
UNSIGNED      : U N S I G N E D ;
ZEROFILL      : Z E R O F I L L ;

// CURRENT_USER — definer value
CURRENT_USER : C U R R E N T '_' U S E R ('(' WS_FRAG* ')')? ;
CURRENT      : C U R R E N T ;

// ── Charset-introducer string literals (e.g. _utf8mb4'x', N'x') ───────────────
// Must precede ID so the whole introducer+string is one token; a bare
// underscore identifier (no immediately-following quote) still falls through to ID.
CHARSET_STRING : '_' [a-zA-Z0-9]+ ( '\'' (~'\'' | '\'\'')* '\''
                                  | '"'  (~'"'  | '""'  )* '"'  ) ;
NCHAR_STRING   : N '\'' (~'\'' | '\'\'')* '\'' ;

// ── Identifiers & literals ────────────────────────────────────────────────────
ID          : [a-zA-Z_$] [a-zA-Z_$0-9]* ;
BACKTICK_ID : '`' (~'`' | '``')+ '`' ;
AT_VAR      : '@' '@'? [a-zA-Z_$] [a-zA-Z_$0-9]* ;
PARAM_MARK  : '?' ;

// ── MySQL hex / bit literal formats ─────────────────────────────────────────
// Quoted forms require the leading X/B introducer (mandatory, not optional) so
// they cannot be confused with a plain SQ_STRING like '1A' or '01'.
HEX_NUM        : '0' X [0-9a-fA-F]+ ;
HEX_NUM_QUOTED : X '\'' [0-9a-fA-F]+ '\'' ;
BIT_NUM        : '0' B [01]+ ;
BIT_NUM_QUOTED : B '\'' [01]+ '\'' ;

INT_NUM     : [0-9]+ ;
DEC_NUM     : [0-9]+ '.' [0-9]* | '.' [0-9]+ ;

SQ_STRING   : '\'' (~'\'' | '\'\'')* '\'' ;
DQ_STRING   : '"'  (~'"'  | '""'  )* '"'  ;

// ── Operators & punctuation ──────────────────────────────────────────────────
SEMI        : ';' ;
COMMA       : ',' ;
DOT         : '.' ;
COLON       : ':' ;
AT_SIGN     : '@' ;
ASSIGN_OP   : ':=' ;
EQ          : '=' ;
NULL_SAFE   : '<=>' ;
NEQ         : '<>' | '!=' ;
LT          : '<' ;
GT          : '>' ;
LE          : '<=' ;
GE          : '>=' ;
PLUS        : '+' ;
MINUS       : '-' ;
// MySQL JSON path operators (8.0+): `col->'$.a'` is shorthand for JSON_EXTRACT and `col->>'$.a'`
// for JSON_UNQUOTE(JSON_EXTRACT(...)). Without tokens for them the lexer produced MINUS GT (and
// MINUS GT GT), no expression alternative matched, and the whole SELECT was discarded as an
// unsupported statement — taking its FROM tables with it. ANTLR's longest-match rule picks
// JSON_UNQUOTE_OP over JSON_EXTRACT_OP for `->>`.
JSON_EXTRACT_OP : '->'  ;
JSON_UNQUOTE_OP : '->>' ;
STAR        : '*' ;
SLASH       : '/' ;
PERCENT     : '%' ;
AMP         : '&' ;
PIPE        : '|' ;
TILDE       : '~' ;
CARET       : '^' ;
EXCL        : '!' ;
LPAREN      : '(' ;
RPAREN      : ')' ;

// ── Whitespace and comments ──────────────────────────────────────────────────
WS            : [ \t\r\n]+                   -> skip ;
// MySQL's `--` comment is NOT the SQL-standard one: the manual (Comment Syntax) requires the
// second dash to be "followed by at least one whitespace or control character (such as a space,
// tab, newline, and so on)". Without that requirement `10--3` (ten minus negative three, a legal
// expression MySQL evaluates to 13) lexed as `10` followed by a comment, the enclosing statement
// failed to parse, and the whole statement was dropped as "unsupported". Requiring the separator
// — or end of line / end of input for a bare `--` — makes the two dashes fall back to the longest
// alternative match, two MINUS tokens, exactly as real MySQL does.
LINE_COMMENT  : '--' ( [ \t] ~[\r\n]* )? ( '\r'? '\n' | EOF )
                                             -> channel(HIDDEN) ;
BLOCK_COMMENT : '/*' .*? '*/'               -> channel(HIDDEN) ;
HASH_COMMENT  : '#' ~[\r\n]*                -> channel(HIDDEN) ;

// ── Fragments ────────────────────────────────────────────────────────────────
fragment WS_FRAG : [ \t] ;
fragment A : [aA] ; fragment B : [bB] ; fragment C : [cC] ; fragment D : [dD] ;
fragment E : [eE] ; fragment F : [fF] ; fragment G : [gG] ; fragment H : [hH] ;
fragment I : [iI] ; fragment J : [jJ] ; fragment K : [kK] ; fragment L : [lL] ;
fragment M : [mM] ; fragment N : [nN] ; fragment O : [oO] ; fragment P : [pP] ;
fragment Q : [qQ] ; fragment R : [rR] ; fragment S : [sS] ; fragment T : [tT] ;
fragment U : [uU] ; fragment V : [vV] ; fragment W : [wW] ; fragment X : [xX] ;
fragment Y : [yY] ; fragment Z : [zZ] ;
