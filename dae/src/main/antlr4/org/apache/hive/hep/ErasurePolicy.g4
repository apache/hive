/**
   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

// Erasure Policy Domain-Specific Language Grammar
// Used by VALIDATE / ACTIVATE / DEACTIVATE commands to parse .erp files.
grammar ErasurePolicy;

// ---------------------------------------------------------------------------
// Parser Rules
// ---------------------------------------------------------------------------

policy
    : headerDecl* identityDecl schemaTypeDecl statement+ EOF
    ;

// Optional LPL-style metadata headers carried at the top of the file. They
// record governance context (version, purpose, legal basis) that the narrow
// erasure DSL otherwise delegates to the policy tier above. The validator
// records them on the ParsedPolicy but does not interpret them as executable
// logic, so the linear-time validation property is preserved. VERSION, when
// present, becomes the policy version label at LOAD time.
headerDecl
    : versionDecl
    | purposeDecl
    | legalBasisDecl
    ;

versionDecl    : VERSION headerValue ;
purposeDecl    : PURPOSE STRING_LITERAL ;
legalBasisDecl : LEGAL BASIS headerValue ;

// A header value is an unquoted identifier (e.g. v3, consent), a quoted string
// (e.g. '1.2.0', 'legal-obligation'), or a bare number.
headerValue
    : ID | STRING_LITERAL | INT_LITERAL
    ;

identityDecl
    : IDENTITY fieldName TYPE fieldType
    ;

schemaTypeDecl
    : SCHEMA TYPE fieldType
    ;

statement
    : FOR SCHEMA schemaId eraseClause? replaceClause? inspectClause? flagClause?
    ;

eraseClause
    : ERASE fieldPath ( COMMA fieldPath )*
    ;

replaceClause
    : REPLACE replaceRule ( COMMA replaceRule )*
    ;

replaceRule
    : fieldPath ASSIGN literal
    ;

// INSPECT records a regex/literal match for review without modifying the value.
// FLAG marks the row for deferred review and emits a match row only; neither
// alters the underlying field. Both participate in the C1/C2/C3 conflict
// detection machinery introduced in §5.
inspectClause
    : INSPECT fieldPath ( COMMA fieldPath )*
    ;

flagClause
    : FLAG fieldPath ( COMMA fieldPath )*
    ;

// A colon-delimited sequence of path steps. Each step is an identifier
// optionally followed by one or more bracket-delimited predicates.
fieldPath
    : pathStep ( COLON pathStep )*
    ;

// A path step is either an identifier (optionally with bracket predicates)
// or a standalone STAR terminal. The STAR form is permitted by the grammar
// at any position, but semantic validation in ErasurePolicyValidator rejects
// it everywhere except as the final step of a fieldPath; this keeps the
// grammar simple while preserving the "wildcard only as terminal" property
// that the §5 prose relies on.
pathStep
    : ID predicate*
    | STAR
    ;

// Predicates select into a repeated (list-typed) field:
//   - INT_LITERAL  : indexed access, e.g. ctxs[3]
//   - STAR         : wildcard over every element, e.g. ctxs[*]
//   - ID op literal: filtered selection, e.g. addresses[country='DE']
predicate
    : LBRACK INT_LITERAL RBRACK
    | LBRACK STAR RBRACK
    | LBRACK ID compareOp literal RBRACK
    ;

compareOp
    : ASSIGN | NEQ | GT | LT | GTE | LTE
    ;

literal
    : INT_LITERAL | LONG_LITERAL | STRING_LITERAL
    ;

schemaId  : literal ;
fieldName : ID ;
fieldType : INT | LONG | STRING ;

// ---------------------------------------------------------------------------
// Lexer Rules
// ---------------------------------------------------------------------------

// Keyword tokens must precede ID so the lexer prefers them on equal-length matches.
IDENTITY : 'IDENTITY' ;
TYPE     : 'TYPE'     ;
SCHEMA   : 'SCHEMA'   ;
FOR      : 'FOR'      ;
ERASE    : 'ERASE'    ;
REPLACE  : 'REPLACE'  ;
INSPECT  : 'INSPECT'  ;
FLAG     : 'FLAG'     ;
INT      : 'INT'      ;
LONG     : 'LONG'     ;
STRING   : 'STRING'   ;

// Optional-header keywords.
VERSION  : 'VERSION'  ;
PURPOSE  : 'PURPOSE'  ;
LEGAL    : 'LEGAL'    ;
BASIS    : 'BASIS'    ;

// Structural punctuation
COLON  : ':' ;
COMMA  : ',' ;
ASSIGN : '=' ;
LBRACK : '[' ;
RBRACK : ']' ;
STAR   : '*' ;

// Comparison operators for filter predicates
NEQ : '!=' ;
GTE : '>=' ;
LTE : '<=' ;
GT  : '>'  ;
LT  : '<'  ;

// Literals
INT_LITERAL    : [0-9]+ ;
LONG_LITERAL   : [0-9]+ [lL] ;
STRING_LITERAL : '\'' ( ~['\\\r\n] | '\\' . )* '\'' ;

// Identifiers (must follow keyword tokens above)
ID : [a-zA-Z_] [a-zA-Z_0-9]* ;

// Whitespace and comments (skipped by the parser).
// Only '#' line comments are supported; the SQL-style '--' marker is reserved
// for the surrounding HiveQL command grammar to avoid confusion when policy
// bodies appear inline inside SQL statements.
WS           : [ \t\r\n]+    -> skip ;
LINE_COMMENT : '#' ~[\r\n]*  -> skip ;
