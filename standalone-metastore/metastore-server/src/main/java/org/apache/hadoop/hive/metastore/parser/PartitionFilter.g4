// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

grammar PartitionFilter;

filter
    : orExpression
    ;

orExpression
    : andExprs+=andExpression (OR andExprs+=andExpression)*
    ;

andExpression
    : exprs+=expression (AND exprs+=expression)*
    ;

expression
    : LPAREN orExpression RPAREN
    | conditionExpression
    ;

conditionExpression
    : key=identifier comparisonOperator value=constant                      #comparison
    | value=constant comparisonOperator key=identifier                      #reverseComparison
    | key=identifier NOT? BETWEEN lower=constant AND upper=constant         #betweenCondition
    | LPAREN key=identifier RPAREN NOT? IN LPAREN values=constantSeq RPAREN #inCondition
    | LPAREN STRUCT identifierList RPAREN NOT? IN constStructList           #multiColInExpression
    ;

comparisonOperator
    : EQ | NEQ | NEQJ | LT | LTE | GT | GTE | NSEQ | LIKE
    ;

identifier
    : IDENTIFIER #unquotedIdentifer
    | QUOTEDIDENTIFIER #quotedIdentifier
    ;

identifierList
    : LPAREN ident+=identifier (COMMA ident+=identifier)* RPAREN
    ;

constant
    : STRING+        #stringLiteral
    | number         #numericLiteral
    | date           #dateLiteral
    | timestamp      #timestampLiteral
    ;

constantSeq
    : values+=constant (COMMA values+=constant)*
    ;

constStruct
    : CONST? STRUCT LPAREN constantSeq RPAREN
    ;

constStructList
    : LPAREN structs+=constStruct (COMMA structs+=constStruct) RPAREN
    ;

number
    : MINUS? INTEGER_VALUE #integerLiteral
    ;

date
    : DATE value=STRING
    | value=DATE_VALUE
    ;

timestamp
    : TIMESTAMP value=STRING
    | value=TIMESTAMP_VALUE
    ;

// Keywords
LPAREN : '(' ;
RPAREN : ')' ;
COMMA : ',' ;
AND : 'AND';
OR : 'OR';
NOT : 'NOT';
BETWEEN : 'BETWEEN';
IN : 'IN';
STRUCT : 'STRUCT';
CONST : 'CONST';
MINUS : '-';
DATE : 'DATE';
TIMESTAMP : 'TIMESTAMP';

EQ : '=' | '==';
NSEQ : '<=>';
NEQ : '<>';
NEQJ : '!=';
LT : '<';
LTE : '<=';
GT : '>';
GTE : '>=';
LIKE : 'LIKE';

STRING
    : '\'' ( ~('\''|'\\') | ('\\' .) )* '\''
    | '"' ( ~('"'|'\\') | ('\\' .) )* '"'
    ;

INTEGER_VALUE
    : DIGIT+
    ;

DATE_VALUE
    : (DIGIT)(DIGIT)(DIGIT)(DIGIT) '-' (DIGIT)(DIGIT) '-' (DIGIT)(DIGIT)
    ;

TIMESTAMP_VALUE
    : (DIGIT)(DIGIT)(DIGIT)(DIGIT) '-' (DIGIT)(DIGIT) '-' (DIGIT)(DIGIT) ' ' (DIGIT)(DIGIT) ':' (DIGIT)(DIGIT) ':' (DIGIT)(DIGIT)
    ;

IDENTIFIER
    : (LETTER | DIGIT | '_')+
    ;

QUOTEDIDENTIFIER
    :
    ('`'  ( '``' | ~('`') )* '`')
    ;

fragment LETTER
    : [A-Z]
    ;

fragment DIGIT
    : [0-9]
    ;

WS
    : [ \r\n\t]+ -> skip
    ;

// Catch-all for anything we can't recognize.
// We use this to be able to ignore and recover all the text
// when splitting statements with DelimiterLexer
UNRECOGNIZED
    : .
    ;
