/*
 * PlSqlAnalyzer.g4 — Custom lightweight PL/SQL grammar for call tracing & table analysis.
 * Covers: packages, procedures, functions, triggers, call statements, SQL DML, EXECUTE IMMEDIATE.
 * Does NOT cover full Oracle PL/SQL syntax — only what's needed for impact analysis.
 */
grammar PlSqlAnalyzer;

// ============ Parser Rules ============

compilationUnit
    : ( createPackageBody
      | createPackageSpec
      | createProcedure
      | createFunction
      | createTrigger
      | anyStatement
      )* EOF
    ;

// ---- Package Body ----
createPackageBody
    : CREATE (OR REPLACE)? (EDITIONABLE | NONEDITIONABLE)? PACKAGE BODY qualifiedName
      (AUTHID (identifier | DEFINER))? (IS | AS)
      packageBodyContent
      END (identifier)? SEMI
    ;

packageBodyContent
    : ( variableDeclaration
      | cursorDeclaration
      | typeDeclaration
      | exceptionDeclaration
      | pragmaDeclaration
      | procedureBody
      | functionBody
      | procedureSpec
      | functionSpec
      | packageBodyRecovery
      )*
      ( BEGIN sequenceOfStatements (exceptionSection)? )?
    ;

// Error recovery: skip unrecognized tokens between procedure/function bodies.
// If a proc/func fails to parse, this consumes tokens until the next PROCEDURE/FUNCTION/BEGIN/END
// so subsequent procs can still be parsed.
packageBodyRecovery
    : ~(PROCEDURE | FUNCTION | BEGIN | END)
    ;

// ---- Package Spec ----
createPackageSpec
    : CREATE (OR REPLACE)? (EDITIONABLE | NONEDITIONABLE)? PACKAGE qualifiedName
      (AUTHID (identifier | DEFINER))? (IS | AS)
      ( variableDeclaration | cursorDeclaration | typeDeclaration
      | exceptionDeclaration | pragmaDeclaration
      | procedureSpec | functionSpec
      )*
      END (identifier)? SEMI
    ;

// ---- Standalone Procedure/Function ----
createProcedure
    : CREATE (OR REPLACE)? (EDITIONABLE | NONEDITIONABLE)? PROCEDURE qualifiedName parameterList? (IS | AS)
      declarationSection?
      BEGIN sequenceOfStatements (exceptionSection)?
      END (identifier)? SEMI
    ;

createFunction
    : CREATE (OR REPLACE)? (EDITIONABLE | NONEDITIONABLE)? FUNCTION qualifiedName parameterList?
      RETURN dataTypeRef functionAttribute*
      (IS | AS)
      declarationSection?
      BEGIN sequenceOfStatements (exceptionSection)?
      END (identifier)? SEMI
    ;

// ---- Trigger ----
createTrigger
    : CREATE (OR REPLACE)? (EDITIONABLE | NONEDITIONABLE)? TRIGGER qualifiedName
      triggerTimingClause
      ON qualifiedName
      (FOR EACH ROW)?
      (WHEN LPAREN expression RPAREN)?
      (DECLARE declarationSection?)?
      BEGIN sequenceOfStatements (exceptionSection)?
      END (identifier)? SEMI
    ;

triggerTimingClause
    : (BEFORE | AFTER | INSTEAD OF)
      triggerEvent (OR triggerEvent)*
    ;

triggerEvent
    : (INSERT | UPDATE (OF identifierList)? | DELETE)
    ;

// ---- Procedure/Function within Package Body ----
procedureBody
    : PROCEDURE identifier parameterList? (IS | AS)
      declarationSection?
      BEGIN sequenceOfStatements (exceptionSection)?
      END (identifier)? SEMI
    ;

functionBody
    : FUNCTION identifier parameterList? RETURN dataTypeRef functionAttribute* (IS | AS)
      declarationSection?
      BEGIN sequenceOfStatements (exceptionSection)?
      END (identifier)? SEMI
    ;

// ---- Procedure/Function Spec (in package spec) ----
procedureSpec
    : PROCEDURE identifier parameterList? SEMI
    ;

functionSpec
    : FUNCTION identifier parameterList? RETURN dataTypeRef functionAttribute* SEMI
    ;

functionAttribute
    : DETERMINISTIC
    | PIPELINED
    | AUTHID (identifier | DEFINER)
    ;

// ---- Parameter List ----
parameterList
    : LPAREN parameterDecl (COMMA parameterDecl)* RPAREN
    ;

parameterDecl
    : identifier (IN? OUT? | IN)? NOCOPY? dataTypeRef (DEFAULT expression)?
      (ASSIGN expression)?
    ;

// ---- Declaration Section ----
declarationSection
    : ( variableDeclaration
      | cursorDeclaration
      | typeDeclaration
      | exceptionDeclaration
      | pragmaDeclaration
      | procedureBody
      | functionBody
      | procedureSpec
      | functionSpec
      )*
    ;

variableDeclaration
    : identifier CONSTANT? dataTypeRef (NOT NULL_)? ((ASSIGN | DEFAULT) expression)? SEMI
    ;

cursorDeclaration
    : CURSOR identifier (parameterList)? (IS selectStatement)? SEMI
    ;

typeDeclaration
    : TYPE identifier IS ~SEMI+ SEMI
    ;

exceptionDeclaration
    : identifier EXCEPTION SEMI
    ;

pragmaDeclaration
    : PRAGMA ~SEMI+ SEMI
    ;

// ---- Statements ----
sequenceOfStatements
    : statement*
    ;

statement
    : labelDeclaration? statementBody
    ;

labelDeclaration
    : LLABEL identifier RLABEL
    ;

statementBody
    : selectIntoStatement
    | insertStatement
    | updateStatement
    | deleteStatement
    | mergeStatement
    | commitStatement
    | rollbackStatement
    | savepointStatement
    | executeImmediateStatement
    | openStatement
    | fetchStatement
    | closeStatement
    | forallStatement
    | pipeRowStatement
    | ifStatement
    | caseStatement
    | basicLoopStatement
    | whileLoopStatement
    | forLoopStatement
    | cursorForLoopStatement
    | exitStatement
    | continueStatement
    | gotoStatement
    | returnStatement
    | raiseStatement
    | nullStatement
    | blockStatement
    | callOrAssignmentStatement
    ;

// ---- SQL DML Statements ----
selectIntoStatement
    : SELECT selectElements INTO identifierList FROM tableReferenceList
      (whereClause)?
      sqlRemainder SEMI
    ;

selectStatement
    : SELECT (parenExpression | ~SEMI)+
    ;

// Used inside cursor FOR loops: FOR x IN (inlineSelect) LOOP
// Must not consume the closing RPAREN
inlineSelect
    : SELECT (parenExpression | ~(SEMI | RPAREN))+
    ;

insertStatement
    : INSERT INTO tableReference insertRemainder SEMI
    ;

updateStatement
    : UPDATE tableReference SET updateRemainder SEMI
    ;

deleteStatement
    : DELETE (FROM)? tableReference deleteRemainder SEMI
    ;

mergeStatement
    : MERGE INTO tableReference mergeRemainder SEMI
    ;

// ---- Transaction ----
commitStatement    : COMMIT (WORK)? SEMI ;
rollbackStatement  : ROLLBACK (WORK)? (TO SAVEPOINT? identifier)? SEMI ;
savepointStatement : SAVEPOINT identifier SEMI ;

// ---- EXECUTE IMMEDIATE ----
executeImmediateStatement
    : EXECUTE IMMEDIATE expression
      ( INTO identifierList
      | RETURNING INTO identifierList
      | USING (IN? OUT? NOCOPY? | IN) expression (COMMA (IN? OUT? NOCOPY? | IN) expression)*
      | BULK COLLECT INTO identifierList
      )*
      SEMI
    ;

// ---- Cursor operations ----
openStatement  : OPEN identifier (LPAREN expressionList RPAREN | FOR (selectStatement | expression))? SEMI ;
fetchStatement : FETCH identifier (BULK COLLECT)? INTO identifierList (LIMIT expression)? SEMI ;
closeStatement : CLOSE identifier SEMI ;

// ---- FORALL ----
forallStatement
    : FORALL identifier IN expression DOTDOT expression (SAVE EXCEPTIONS)?
      (insertStatement | updateStatement | deleteStatement | mergeStatement | executeImmediateStatement)
    ;

// ---- PIPE ROW ----
pipeRowStatement : PIPE ROW LPAREN expression RPAREN SEMI ;

// ---- Control Flow ----
ifStatement
    : IF expression THEN sequenceOfStatements
      (ELSIF expression THEN sequenceOfStatements)*
      (ELSE sequenceOfStatements)?
      END IF SEMI
    ;

caseStatement
    : CASE expression?
      (WHEN expression THEN sequenceOfStatements)+
      (ELSE sequenceOfStatements)?
      END CASE SEMI
    ;

basicLoopStatement
    : LOOP sequenceOfStatements END LOOP (identifier)? SEMI
    ;

whileLoopStatement
    : WHILE expression LOOP sequenceOfStatements END LOOP (identifier)? SEMI
    ;

forLoopStatement
    : FOR identifier IN REVERSE? expression DOTDOT expression
      LOOP sequenceOfStatements END LOOP (identifier)? SEMI
    ;

cursorForLoopStatement
    : FOR identifier IN (qualifiedName (LPAREN expressionList RPAREN)? | LPAREN inlineSelect RPAREN)
      LOOP sequenceOfStatements END LOOP (identifier)? SEMI
    ;

exitStatement     : EXIT (identifier)? (WHEN expression)? SEMI ;
continueStatement : CONTINUE (identifier)? (WHEN expression)? SEMI ;
gotoStatement     : GOTO identifier SEMI ;
returnStatement   : RETURN expression? SEMI ;
raiseStatement    : RAISE (qualifiedName)? SEMI ;
nullStatement     : NULL_ SEMI ;

blockStatement
    : (DECLARE declarationSection?)?
      BEGIN sequenceOfStatements (exceptionSection)?
      END (identifier)? SEMI
    ;

// ---- Call or Assignment (critical for call trace) ----
// Handles: proc(args);  pkg.proc(args);  var := func(args);  var := expr;
// Also: collection(idx).field := val;  obj.method(args).prop;
callOrAssignmentStatement
    : qualifiedName ( LPAREN expressionList? RPAREN (DOT identifier (LPAREN expressionList? RPAREN)?)* )?
      ( ASSIGN expression )?
      SEMI
    ;

// ---- Exception Section ----
exceptionSection
    : EXCEPTION exceptionHandler+
    ;

exceptionHandler
    : WHEN (qualifiedName | OTHERS) (OR (qualifiedName | OTHERS))* THEN sequenceOfStatements
    ;

// ---- Table References ----
tableReferenceList
    : tableReference (COMMA tableReference)*
    ;

tableReference
    : qualifiedName (AT_SIGN identifier)?  // dblink support
      (identifier)?                        // alias
    | parenExpression (identifier)?        // subquery: (SELECT ... FROM ...) alias
    ;

// Balanced parentheses — matches (anything) with correctly nested inner parens.
// Used for subqueries in FROM, and nested function calls in expressions.
parenExpression
    : LPAREN ( ~(LPAREN | RPAREN) | parenExpression )* RPAREN
    ;

// ---- Supporting ----
qualifiedName
    : identifier (DOT identifier)*
    ;

identifierList
    : qualifiedName (COMMA qualifiedName)*
    ;

dataTypeRef
    : qualifiedName (LPAREN NUMBER_LITERAL (COMMA NUMBER_LITERAL)? RPAREN)?
      (PCT (TYPE | ROWTYPE))?
    ;

selectElements : (parenExpression | ~(INTO | SEMI))+ ;
whereClause    : WHERE (parenExpression | ~(SEMI | GROUP | ORDER | HAVING | UNION | MINUS_ | INTERSECT | FETCH | FOR | CONNECT | START))+ ;
insertRemainder : (parenExpression | ~SEMI)* ;
updateRemainder : (parenExpression | ~SEMI)* ;
deleteRemainder : (parenExpression | ~SEMI)* ;
mergeRemainder  : (parenExpression | ~SEMI)* ;
sqlRemainder    : (parenExpression | ~SEMI)* ;

expression     : expressionAtom+ ;
expressionAtom : caseExpression | parenExpression | ~(SEMI | COMMA | RPAREN | THEN | LOOP | DOTDOT)  ;
expressionList : expression (COMMA expression)* ;

// CASE expression (searched and simple forms) — must be parsed explicitly
// because THEN/END inside CASE would otherwise confuse the loose expression rule.
//   Searched: CASE WHEN cond THEN val (WHEN cond THEN val)* (ELSE val)? END
//   Simple:   CASE expr WHEN val THEN val (WHEN val THEN val)* (ELSE val)? END
caseExpression
    : CASE (WHEN caseExprAtom+ THEN caseExprAtom+)+  (ELSE caseExprAtom+)? END
    | CASE caseExprAtom+ (WHEN caseExprAtom+ THEN caseExprAtom+)+  (ELSE caseExprAtom+)? END
    ;

// Atoms inside a CASE expression — like expressionAtom but allows THEN/LOOP/DOTDOT
// since those are delimited by the CASE structure itself.
// Stops at WHEN, ELSE, END, SEMI, COMMA, RPAREN to bound each arm.
caseExprAtom
    : caseExpression
    | parenExpression
    | ~(SEMI | COMMA | RPAREN | WHEN | THEN | ELSE | END)
    ;

// Catch-all: skip any unrecognized top-level token sequence (e.g. grants, synonyms, etc.)
anyStatement   : ~(CREATE | EOF)+ ;

identifier
    : IDENTIFIER
    | QUOTED_IDENTIFIER
    | unreservedKeyword
    ;

// Keywords that can also be used as identifiers in certain contexts
unreservedKeyword
    : NAME | TYPE | BODY | REPLACE | WORK | LIMIT | SAVE | EXCEPTIONS
    | REVERSE | CONSTANT | EACH | INSTEAD | SAVEPOINT | RESULT
    | ROW | NOCOPY | OF | CONTINUE | EDITIONABLE | NONEDITIONABLE
    ;

// ============ Lexer Rules ============

// --- Symbols ---
SEMI       : ';' ;
COMMA      : ',' ;
DOT        : '.' ;
LPAREN     : '(' ;
RPAREN     : ')' ;
ASSIGN     : ':=' ;
DOTDOT     : '..' ;
PCT        : '%' ;
AT_SIGN    : '@' ;
LLABEL     : '<<' ;
RLABEL     : '>>' ;
PLUS       : '+' ;
MINUS      : '-' ;
STAR       : '*' ;
SLASH      : '/' ;
EQ         : '=' ;
NEQ        : ('!=' | '<>' | '~=' | '^=') ;
LT         : '<' ;
GT         : '>' ;
LTE        : '<=' ;
GTE        : '>=' ;
CONCAT     : '||' ;
BIND       : ':' ;
ARROW      : '=>' ;

// --- Keywords (case-insensitive) ---
CREATE     : C R E A T E ;
OR         : O R ;
REPLACE    : R E P L A C E ;
PACKAGE    : P A C K A G E ;
BODY       : B O D Y ;
PROCEDURE  : P R O C E D U R E ;
FUNCTION   : F U N C T I O N ;
TRIGGER    : T R I G G E R ;
IS         : I S ;
AS         : A S ;
BEGIN      : B E G I N ;
END        : E N D ;
DECLARE    : D E C L A R E ;
EXCEPTION  : E X C E P T I O N ;
WHEN       : W H E N ;
THEN       : T H E N ;
ELSE       : E L S E ;
ELSIF      : E L S I F ;
IF         : I F ;
CASE       : C A S E ;
LOOP       : L O O P ;
WHILE      : W H I L E ;
FOR        : F O R ;
IN         : I N ;
OUT        : O U T ;
NOCOPY     : N O C O P Y ;
NOT        : N O T ;
NULL_      : N U L L ;
RETURN     : R E T U R N ;
RETURNING  : R E T U R N I N G ;
INTO       : I N T O ;
FROM       : F R O M ;
WHERE      : W H E R E ;
SELECT     : S E L E C T ;
INSERT     : I N S E R T ;
UPDATE     : U P D A T E ;
DELETE     : D E L E T E ;
MERGE      : M E R G E ;
SET        : S E T ;
VALUES     : V A L U E S ;
DEFAULT    : D E F A U L T ;
EXECUTE    : E X E C U T E ;
IMMEDIATE  : I M M E D I A T E ;
USING      : U S I N G ;
BULK       : B U L K ;
COLLECT    : C O L L E C T ;
FORALL     : F O R A L L ;
OPEN       : O P E N ;
FETCH      : F E T C H ;
CLOSE      : C L O S E ;
CURSOR     : C U R S O R ;
TYPE       : T Y P E ;
ROWTYPE    : R O W T Y P E ;
RAISE      : R A I S E ;
GOTO       : G O T O ;
OTHERS     : O T H E R S ;
COMMIT     : C O M M I T ;
ROLLBACK   : R O L L B A C K ;
SAVEPOINT  : S A V E P O I N T ;
PIPE       : P I P E ;
ROW        : R O W ;
ON         : O N ;
BEFORE     : B E F O R E ;
AFTER      : A F T E R ;
INSTEAD    : I N S T E A D ;
EACH       : E A C H ;
REVERSE    : R E V E R S E ;
EXIT       : E X I T ;
CONTINUE   : C O N T I N U E ;
PRAGMA     : P R A G M A ;
CONSTANT   : C O N S T A N T ;
SAVE       : S A V E ;
EXCEPTIONS : E X C E P T I O N S ;
WORK       : W O R K ;
LIMIT      : L I M I T ;
NAME       : N A M E ;
OF         : O F ;
TO         : T O ;
RESULT     : R E S U L T ;
GROUP      : G R O U P ;
ORDER      : O R D E R ;
HAVING     : H A V I N G ;
UNION      : U N I O N ;
MINUS_     : M I N U S ;
INTERSECT  : I N T E R S E C T ;
CONNECT    : C O N N E C T ;
START      : S T A R T ;
AND        : A N D ;
LIKE       : L I K E ;
BETWEEN    : B E T W E E N ;
EXISTS     : E X I S T S ;
EDITIONABLE    : E D I T I O N A B L E ;
NONEDITIONABLE : N O N E D I T I O N A B L E ;
PIPELINED      : P I P E L I N E D ;
DETERMINISTIC  : D E T E R M I N I S T I C ;
AUTHID         : A U T H I D ;
DEFINER        : D E F I N E R ;

// --- Literals ---
NUMBER_LITERAL
    : DIGIT+ ('.' DIGIT+)? (('e' | 'E') ('+' | '-')? DIGIT+)?
    ;

STRING_LITERAL
    : '\'' ( ~'\'' | '\'\'' )* '\''
    ;

QUOTED_IDENTIFIER
    : '"' (~'"')* '"'
    ;

// --- Identifier ---
IDENTIFIER
    : LETTER (LETTER | DIGIT | '_' | '$' | '#')*
    ;

// --- Bind variable ---
BIND_VARIABLE
    : ':' (LETTER | DIGIT | '_')+
    ;

// --- Whitespace & comments ---
WS             : [ \t\r\n]+ -> skip ;
LINE_COMMENT   : '--' ~[\r\n]* -> channel(HIDDEN) ;
BLOCK_COMMENT  : '/*' .*? '*/' -> channel(HIDDEN) ;

// --- Catch-all for unrecognized tokens ---
ANY_CHAR : . ;

// --- Case-insensitive letter fragments ---
fragment A : [aA] ; fragment B : [bB] ; fragment C : [cC] ; fragment D : [dD] ;
fragment E : [eE] ; fragment F : [fF] ; fragment G : [gG] ; fragment H : [hH] ;
fragment I : [iI] ; fragment J : [jJ] ; fragment K : [kK] ; fragment L : [lL] ;
fragment M : [mM] ; fragment N : [nN] ; fragment O : [oO] ; fragment P : [pP] ;
fragment Q : [qQ] ; fragment R : [rR] ; fragment S : [sS] ; fragment T : [tT] ;
fragment U : [uU] ; fragment V : [vV] ; fragment W : [wW] ; fragment X : [xX] ;
fragment Y : [yY] ; fragment Z : [zZ] ;
fragment DIGIT  : [0-9] ;
fragment LETTER : [a-zA-Z_] ;
