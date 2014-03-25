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
parser grammar IdentifiersParser;

options
{
output=AST;
ASTLabelType=CommonTree;
backtrack=false;
k=3;
}

@members {
  @Override
  public Object recoverFromMismatchedSet(IntStream input,
      RecognitionException re, BitSet follow) throws RecognitionException {
    throw re;
  }
  @Override
  public void displayRecognitionError(String[] tokenNames,
      RecognitionException e) {
    gParent.errors.add(new ParseError(gParent, e, tokenNames));
  }
}

@rulecatch {
catch (RecognitionException e) {
  throw e;
}
}

//-----------------------------------------------------------------------------------

// group by a,b
groupByClause
@init { gParent.pushMsg("group by clause", state); }
@after { gParent.popMsg(state); }
    :
    KW_GROUP KW_BY
    groupByExpression
    ( COMMA groupByExpression )*
    ((rollup=KW_WITH KW_ROLLUP) | (cube=KW_WITH KW_CUBE)) ?
    (sets=KW_GROUPING KW_SETS 
    LPAREN groupingSetExpression ( COMMA groupingSetExpression)*  RPAREN ) ?
    -> {rollup != null}? ^(TOK_ROLLUP_GROUPBY groupByExpression+)
    -> {cube != null}? ^(TOK_CUBE_GROUPBY groupByExpression+)
    -> {sets != null}? ^(TOK_GROUPING_SETS groupByExpression+ groupingSetExpression+)
    -> ^(TOK_GROUPBY groupByExpression+)
    ;

groupingSetExpression
@init {gParent.pushMsg("grouping set expression", state); }
@after {gParent.popMsg(state); }
   :
   groupByExpression
   -> ^(TOK_GROUPING_SETS_EXPRESSION groupByExpression)
   |
   LPAREN 
   groupByExpression (COMMA groupByExpression)*
   RPAREN
   -> ^(TOK_GROUPING_SETS_EXPRESSION groupByExpression+)
   |
   LPAREN
   RPAREN
   -> ^(TOK_GROUPING_SETS_EXPRESSION)
   ;


groupByExpression
@init { gParent.pushMsg("group by expression", state); }
@after { gParent.popMsg(state); }
    :
    expression
    ;

havingClause
@init { gParent.pushMsg("having clause", state); }
@after { gParent.popMsg(state); }
    :
    KW_HAVING havingCondition -> ^(TOK_HAVING havingCondition)
    ;

havingCondition
@init { gParent.pushMsg("having condition", state); }
@after { gParent.popMsg(state); }
    :
    expression
    ;

// order by a,b
orderByClause
@init { gParent.pushMsg("order by clause", state); }
@after { gParent.popMsg(state); }
    :
    KW_ORDER KW_BY columnRefOrder ( COMMA columnRefOrder)* -> ^(TOK_ORDERBY columnRefOrder+)
    ;

clusterByClause
@init { gParent.pushMsg("cluster by clause", state); }
@after { gParent.popMsg(state); }
    :
    KW_CLUSTER KW_BY
    LPAREN expression (COMMA expression)* RPAREN -> ^(TOK_CLUSTERBY expression+)
    |
    KW_CLUSTER KW_BY
    expression
    ( (COMMA)=>COMMA expression )* -> ^(TOK_CLUSTERBY expression+)
    ;

partitionByClause
@init  { gParent.pushMsg("partition by clause", state); }
@after { gParent.popMsg(state); }
    :
    KW_PARTITION KW_BY
    LPAREN expression (COMMA expression)* RPAREN -> ^(TOK_DISTRIBUTEBY expression+)
    |
    KW_PARTITION KW_BY
    expression ((COMMA)=> COMMA expression)* -> ^(TOK_DISTRIBUTEBY expression+)
    ;

distributeByClause
@init { gParent.pushMsg("distribute by clause", state); }
@after { gParent.popMsg(state); }
    :
    KW_DISTRIBUTE KW_BY
    LPAREN expression (COMMA expression)* RPAREN -> ^(TOK_DISTRIBUTEBY expression+)
    |
    KW_DISTRIBUTE KW_BY
    expression ((COMMA)=> COMMA expression)* -> ^(TOK_DISTRIBUTEBY expression+)
    ;

sortByClause
@init { gParent.pushMsg("sort by clause", state); }
@after { gParent.popMsg(state); }
    :
    KW_SORT KW_BY
    LPAREN columnRefOrder
    ( COMMA columnRefOrder)* RPAREN -> ^(TOK_SORTBY columnRefOrder+)
    |
    KW_SORT KW_BY
    columnRefOrder
    ( (COMMA)=> COMMA columnRefOrder)* -> ^(TOK_SORTBY columnRefOrder+)
    ;

// fun(par1, par2, par3)
function
@init { gParent.pushMsg("function specification", state); }
@after { gParent.popMsg(state); }
    :
    functionName
    LPAREN
      (
        (star=STAR)
        | (dist=KW_DISTINCT)? (selectExpression (COMMA selectExpression)*)?
      )
    RPAREN (KW_OVER ws=window_specification)?
           -> {$star != null}? ^(TOK_FUNCTIONSTAR functionName $ws?)
           -> {$dist == null}? ^(TOK_FUNCTION functionName (selectExpression+)? $ws?)
                            -> ^(TOK_FUNCTIONDI functionName (selectExpression+)?)
    ;

functionName
@init { gParent.pushMsg("function name", state); }
@after { gParent.popMsg(state); }
    : // Keyword IF is also a function name
    KW_IF | KW_ARRAY | KW_MAP | KW_STRUCT | KW_UNIONTYPE | functionIdentifier
    ;

castExpression
@init { gParent.pushMsg("cast expression", state); }
@after { gParent.popMsg(state); }
    :
    KW_CAST
    LPAREN
          expression
          KW_AS
          primitiveType
    RPAREN -> ^(TOK_FUNCTION primitiveType expression)
    ;

caseExpression
@init { gParent.pushMsg("case expression", state); }
@after { gParent.popMsg(state); }
    :
    KW_CASE expression
    (KW_WHEN expression KW_THEN expression)+
    (KW_ELSE expression)?
    KW_END -> ^(TOK_FUNCTION KW_CASE expression*)
    ;

whenExpression
@init { gParent.pushMsg("case expression", state); }
@after { gParent.popMsg(state); }
    :
    KW_CASE
     ( KW_WHEN expression KW_THEN expression)+
    (KW_ELSE expression)?
    KW_END -> ^(TOK_FUNCTION KW_WHEN expression*)
    ;

constant
@init { gParent.pushMsg("constant", state); }
@after { gParent.popMsg(state); }
    :
    Number
    | dateLiteral
    | StringLiteral
    | stringLiteralSequence
    | BigintLiteral
    | SmallintLiteral
    | TinyintLiteral
    | DecimalLiteral
    | charSetStringLiteral
    | booleanValue
    ;

stringLiteralSequence
    :
    StringLiteral StringLiteral+ -> ^(TOK_STRINGLITERALSEQUENCE StringLiteral StringLiteral+)
    ;

charSetStringLiteral
@init { gParent.pushMsg("character string literal", state); }
@after { gParent.popMsg(state); }
    :
    csName=CharSetName csLiteral=CharSetLiteral -> ^(TOK_CHARSETLITERAL $csName $csLiteral)
    ;

dateLiteral
    :
    KW_DATE StringLiteral ->
    {
      // Create DateLiteral token, but with the text of the string value
      // This makes the dateLiteral more consistent with the other type literals.
      adaptor.create(TOK_DATELITERAL, $StringLiteral.text)
    }
    ;

expression
@init { gParent.pushMsg("expression specification", state); }
@after { gParent.popMsg(state); }
    :
    precedenceOrExpression
    ;

atomExpression
    :
    KW_NULL -> TOK_NULL
    | dateLiteral
    | constant
    | castExpression
    | caseExpression
    | whenExpression
    | (functionName LPAREN) => function
    | tableOrColumn
    | LPAREN! expression RPAREN!
    ;


precedenceFieldExpression
    :
    atomExpression ((LSQUARE^ expression RSQUARE!) | (DOT^ identifier))*
    ;

precedenceUnaryOperator
    :
    PLUS | MINUS | TILDE
    ;

nullCondition
    :
    KW_NULL -> ^(TOK_ISNULL)
    | KW_NOT KW_NULL -> ^(TOK_ISNOTNULL)
    ;

precedenceUnaryPrefixExpression
    :
    (precedenceUnaryOperator^)* precedenceFieldExpression
    ;

precedenceUnarySuffixExpression
    : precedenceUnaryPrefixExpression (a=KW_IS nullCondition)?
    -> {$a != null}? ^(TOK_FUNCTION nullCondition precedenceUnaryPrefixExpression)
    -> precedenceUnaryPrefixExpression
    ;


precedenceBitwiseXorOperator
    :
    BITWISEXOR
    ;

precedenceBitwiseXorExpression
    :
    precedenceUnarySuffixExpression (precedenceBitwiseXorOperator^ precedenceUnarySuffixExpression)*
    ;


precedenceStarOperator
    :
    STAR | DIVIDE | MOD | DIV
    ;

precedenceStarExpression
    :
    precedenceBitwiseXorExpression (precedenceStarOperator^ precedenceBitwiseXorExpression)*
    ;


precedencePlusOperator
    :
    PLUS | MINUS
    ;

precedencePlusExpression
    :
    precedenceStarExpression (precedencePlusOperator^ precedenceStarExpression)*
    ;


precedenceAmpersandOperator
    :
    AMPERSAND
    ;

precedenceAmpersandExpression
    :
    precedencePlusExpression (precedenceAmpersandOperator^ precedencePlusExpression)*
    ;


precedenceBitwiseOrOperator
    :
    BITWISEOR
    ;

precedenceBitwiseOrExpression
    :
    precedenceAmpersandExpression (precedenceBitwiseOrOperator^ precedenceAmpersandExpression)*
    ;


// Equal operators supporting NOT prefix
precedenceEqualNegatableOperator
    :
    KW_LIKE | KW_RLIKE | KW_REGEXP
    ;

precedenceEqualOperator
    :
    precedenceEqualNegatableOperator | EQUAL | EQUAL_NS | NOTEQUAL | LESSTHANOREQUALTO | LESSTHAN | GREATERTHANOREQUALTO | GREATERTHAN
    ;

subQueryExpression 
    : 
    LPAREN! selectStatement[true] RPAREN!     
 ;
 
precedenceEqualExpression
    :
    (left=precedenceBitwiseOrExpression -> $left)
    (
       (KW_NOT precedenceEqualNegatableOperator notExpr=precedenceBitwiseOrExpression)
       -> ^(KW_NOT ^(precedenceEqualNegatableOperator $precedenceEqualExpression $notExpr))
    | (precedenceEqualOperator equalExpr=precedenceBitwiseOrExpression)
       -> ^(precedenceEqualOperator $precedenceEqualExpression $equalExpr)
    | (KW_NOT KW_IN LPAREN KW_SELECT)=>  (KW_NOT KW_IN subQueryExpression) 
       -> ^(KW_NOT ^(TOK_SUBQUERY_EXPR ^(TOK_SUBQUERY_OP KW_IN) subQueryExpression $precedenceEqualExpression))
    | (KW_NOT KW_IN expressions)
       -> ^(KW_NOT ^(TOK_FUNCTION KW_IN $precedenceEqualExpression expressions))
    | (KW_IN LPAREN KW_SELECT)=>  (KW_IN subQueryExpression) 
       -> ^(TOK_SUBQUERY_EXPR ^(TOK_SUBQUERY_OP KW_IN) subQueryExpression $precedenceEqualExpression)
    | (KW_IN expressions)
       -> ^(TOK_FUNCTION KW_IN $precedenceEqualExpression expressions)
    | ( KW_NOT KW_BETWEEN (min=precedenceBitwiseOrExpression) KW_AND (max=precedenceBitwiseOrExpression) )
       -> ^(TOK_FUNCTION Identifier["between"] KW_TRUE $left $min $max)
    | ( KW_BETWEEN (min=precedenceBitwiseOrExpression) KW_AND (max=precedenceBitwiseOrExpression) )
       -> ^(TOK_FUNCTION Identifier["between"] KW_FALSE $left $min $max)
    )*
    | (KW_EXISTS LPAREN KW_SELECT)=> (KW_EXISTS subQueryExpression) -> ^(TOK_SUBQUERY_EXPR ^(TOK_SUBQUERY_OP KW_EXISTS) subQueryExpression)
    ;

expressions
    :
    LPAREN expression (COMMA expression)* RPAREN -> expression*
    ;

precedenceNotOperator
    :
    KW_NOT
    ;

precedenceNotExpression
    :
    (precedenceNotOperator^)* precedenceEqualExpression
    ;


precedenceAndOperator
    :
    KW_AND
    ;

precedenceAndExpression
    :
    precedenceNotExpression (precedenceAndOperator^ precedenceNotExpression)*
    ;


precedenceOrOperator
    :
    KW_OR
    ;

precedenceOrExpression
    :
    precedenceAndExpression (precedenceOrOperator^ precedenceAndExpression)*
    ;


booleanValue
    :
    KW_TRUE^ | KW_FALSE^
    ;

tableOrPartition
   :
   tableName partitionSpec? -> ^(TOK_TAB tableName partitionSpec?)
   ;

partitionSpec
    :
    KW_PARTITION
     LPAREN partitionVal (COMMA  partitionVal )* RPAREN -> ^(TOK_PARTSPEC partitionVal +)
    ;

partitionVal
    :
    identifier (EQUAL constant)? -> ^(TOK_PARTVAL identifier constant?)
    ;

dropPartitionSpec
    :
    KW_PARTITION
     LPAREN dropPartitionVal (COMMA  dropPartitionVal )* RPAREN -> ^(TOK_PARTSPEC dropPartitionVal +)
    ;

dropPartitionVal
    :
    identifier dropPartitionOperator constant -> ^(TOK_PARTVAL identifier dropPartitionOperator constant)
    ;

dropPartitionOperator
    :
    EQUAL | NOTEQUAL | LESSTHANOREQUALTO | LESSTHAN | GREATERTHANOREQUALTO | GREATERTHAN
    ;

sysFuncNames
    :
      KW_AND
    | KW_OR
    | KW_NOT
    | KW_LIKE
    | KW_IF
    | KW_CASE
    | KW_WHEN
    | KW_TINYINT
    | KW_SMALLINT
    | KW_INT
    | KW_BIGINT
    | KW_FLOAT
    | KW_DOUBLE
    | KW_BOOLEAN
    | KW_STRING
    | KW_BINARY
    | KW_ARRAY
    | KW_MAP
    | KW_STRUCT
    | KW_UNIONTYPE
    | EQUAL
    | EQUAL_NS
    | NOTEQUAL
    | LESSTHANOREQUALTO
    | LESSTHAN
    | GREATERTHANOREQUALTO
    | GREATERTHAN
    | DIVIDE
    | PLUS
    | MINUS
    | STAR
    | MOD
    | DIV
    | AMPERSAND
    | TILDE
    | BITWISEOR
    | BITWISEXOR
    | KW_RLIKE
    | KW_REGEXP
    | KW_IN
    | KW_BETWEEN
    ;

descFuncNames
    :
      sysFuncNames
    | StringLiteral
    | functionIdentifier
    ;

identifier
    :
    Identifier
    | nonReserved -> Identifier[$nonReserved.text]
    ;

functionIdentifier
@init { gParent.pushMsg("function identifier", state); }
@after { gParent.popMsg(state); }
    : db=identifier DOT fn=identifier
    -> Identifier[$db.text + "." + $fn.text]
    |
    identifier
    ;

nonReserved
    :
    KW_TRUE | KW_FALSE | KW_LIKE | KW_EXISTS | KW_ASC | KW_DESC | KW_ORDER | KW_GROUP | KW_BY | KW_AS | KW_INSERT | KW_OVERWRITE | KW_OUTER | KW_LEFT | KW_RIGHT | KW_FULL | KW_PARTITION | KW_PARTITIONS | KW_TABLE | KW_TABLES | KW_COLUMNS | KW_INDEX | KW_INDEXES | KW_REBUILD | KW_FUNCTIONS | KW_SHOW | KW_MSCK | KW_REPAIR | KW_DIRECTORY | KW_LOCAL | KW_USING | KW_CLUSTER | KW_DISTRIBUTE | KW_SORT | KW_UNION | KW_LOAD | KW_EXPORT | KW_IMPORT | KW_DATA | KW_INPATH | KW_IS | KW_NULL | KW_CREATE | KW_EXTERNAL | KW_ALTER | KW_CHANGE | KW_FIRST | KW_AFTER | KW_DESCRIBE | KW_DROP | KW_RENAME | KW_IGNORE | KW_PROTECTION | KW_TO | KW_COMMENT | KW_BOOLEAN | KW_TINYINT | KW_SMALLINT | KW_INT | KW_BIGINT | KW_FLOAT | KW_DOUBLE | KW_DATE | KW_DATETIME | KW_TIMESTAMP | KW_DECIMAL | KW_STRING | KW_ARRAY | KW_STRUCT | KW_UNIONTYPE | KW_PARTITIONED | KW_CLUSTERED | KW_SORTED | KW_INTO | KW_BUCKETS | KW_ROW | KW_ROWS | KW_FORMAT | KW_DELIMITED | KW_FIELDS | KW_TERMINATED | KW_ESCAPED | KW_COLLECTION | KW_ITEMS | KW_KEYS | KW_KEY_TYPE | KW_LINES | KW_STORED | KW_FILEFORMAT | KW_SEQUENCEFILE | KW_TEXTFILE | KW_RCFILE | KW_ORCFILE | KW_PARQUETFILE | KW_INPUTFORMAT | KW_OUTPUTFORMAT | KW_INPUTDRIVER | KW_OUTPUTDRIVER | KW_OFFLINE | KW_ENABLE | KW_DISABLE | KW_READONLY | KW_NO_DROP | KW_LOCATION | KW_BUCKET | KW_OUT | KW_OF | KW_PERCENT | KW_ADD | KW_REPLACE | KW_RLIKE | KW_REGEXP | KW_TEMPORARY | KW_EXPLAIN | KW_FORMATTED | KW_PRETTY | KW_DEPENDENCY | KW_LOGICAL | KW_SERDE | KW_WITH | KW_DEFERRED | KW_SERDEPROPERTIES | KW_DBPROPERTIES | KW_LIMIT | KW_SET | KW_UNSET | KW_TBLPROPERTIES | KW_IDXPROPERTIES | KW_VALUE_TYPE | KW_ELEM_TYPE | KW_MAPJOIN | KW_STREAMTABLE | KW_HOLD_DDLTIME | KW_CLUSTERSTATUS | KW_UTC | KW_UTCTIMESTAMP | KW_LONG | KW_DELETE | KW_PLUS | KW_MINUS | KW_FETCH | KW_INTERSECT | KW_VIEW | KW_IN | KW_DATABASES | KW_MATERIALIZED | KW_SCHEMA | KW_SCHEMAS | KW_GRANT | KW_REVOKE | KW_SSL | KW_UNDO | KW_LOCK | KW_LOCKS | KW_UNLOCK | KW_SHARED | KW_EXCLUSIVE | KW_PROCEDURE | KW_UNSIGNED | KW_WHILE | KW_READ | KW_READS | KW_PURGE | KW_RANGE | KW_ANALYZE | KW_BEFORE | KW_BETWEEN | KW_BOTH | KW_BINARY | KW_CONTINUE | KW_CURSOR | KW_TRIGGER | KW_RECORDREADER | KW_RECORDWRITER | KW_SEMI | KW_LATERAL | KW_TOUCH | KW_ARCHIVE | KW_UNARCHIVE | KW_COMPUTE | KW_STATISTICS | KW_USE | KW_OPTION | KW_CONCATENATE | KW_SHOW_DATABASE | KW_UPDATE | KW_RESTRICT | KW_CASCADE | KW_SKEWED | KW_ROLLUP | KW_CUBE | KW_DIRECTORIES | KW_FOR | KW_GROUPING | KW_SETS | KW_TRUNCATE | KW_NOSCAN | KW_USER | KW_ROLE | KW_ROLES | KW_INNER | KW_DEFINED | KW_ADMIN | KW_JAR | KW_FILE | KW_OWNER | KW_PRINCIPALS | KW_ALL | KW_DEFAULT | KW_NONE | KW_COMPACT | KW_COMPACTIONS | KW_TRANSACTIONS
    ;
