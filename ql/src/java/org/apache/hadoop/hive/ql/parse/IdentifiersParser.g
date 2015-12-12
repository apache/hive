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
  protected boolean useSQL11ReservedKeywordsForIdentifier() {
    return gParent.useSQL11ReservedKeywordsForIdentifier();
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
    expression
    ( COMMA expression)*
    ((rollup=KW_WITH KW_ROLLUP) | (cube=KW_WITH KW_CUBE)) ?
    (sets=KW_GROUPING KW_SETS 
    LPAREN groupingSetExpression ( COMMA groupingSetExpression)*  RPAREN ) ?
    -> {rollup != null}? ^(TOK_ROLLUP_GROUPBY expression+)
    -> {cube != null}? ^(TOK_CUBE_GROUPBY expression+)
    -> {sets != null}? ^(TOK_GROUPING_SETS expression+ groupingSetExpression+)
    -> ^(TOK_GROUPBY expression+)
    ;

groupingSetExpression
@init {gParent.pushMsg("grouping set expression", state); }
@after {gParent.popMsg(state); }
   :
   (LPAREN) => groupingSetExpressionMultiple 
   |
   groupingExpressionSingle
   ;

groupingSetExpressionMultiple
@init {gParent.pushMsg("grouping set part expression", state); }
@after {gParent.popMsg(state); }
   :
   LPAREN 
   expression? (COMMA expression)*
   RPAREN
   -> ^(TOK_GROUPING_SETS_EXPRESSION expression*)
   ;

groupingExpressionSingle
@init { gParent.pushMsg("groupingExpression expression", state); }
@after { gParent.popMsg(state); }
    :
    expression -> ^(TOK_GROUPING_SETS_EXPRESSION expression)
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

expressionsInParenthese
    :
    LPAREN expression (COMMA expression)* RPAREN -> expression+
    ;

expressionsNotInParenthese
    :
    expression (COMMA expression)* -> expression+
    ;

columnRefOrderInParenthese
    :
    LPAREN columnRefOrder (COMMA columnRefOrder)* RPAREN -> columnRefOrder+
    ;

columnRefOrderNotInParenthese
    :
    columnRefOrder (COMMA columnRefOrder)* -> columnRefOrder+
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
    (
    (LPAREN) => expressionsInParenthese -> ^(TOK_CLUSTERBY expressionsInParenthese)
    |
    expressionsNotInParenthese -> ^(TOK_CLUSTERBY expressionsNotInParenthese)
    )
    ;

partitionByClause
@init  { gParent.pushMsg("partition by clause", state); }
@after { gParent.popMsg(state); }
    :
    KW_PARTITION KW_BY
    (
    (LPAREN) => expressionsInParenthese -> ^(TOK_DISTRIBUTEBY expressionsInParenthese)
    |
    expressionsNotInParenthese -> ^(TOK_DISTRIBUTEBY expressionsNotInParenthese)
    )
    ;

distributeByClause
@init { gParent.pushMsg("distribute by clause", state); }
@after { gParent.popMsg(state); }
    :
    KW_DISTRIBUTE KW_BY
    (
    (LPAREN) => expressionsInParenthese -> ^(TOK_DISTRIBUTEBY expressionsInParenthese)
    |
    expressionsNotInParenthese -> ^(TOK_DISTRIBUTEBY expressionsNotInParenthese)
    )
    ;

sortByClause
@init { gParent.pushMsg("sort by clause", state); }
@after { gParent.popMsg(state); }
    :
    KW_SORT KW_BY
    (
    (LPAREN) => columnRefOrderInParenthese -> ^(TOK_SORTBY columnRefOrderInParenthese)
    |
    columnRefOrderNotInParenthese -> ^(TOK_SORTBY columnRefOrderNotInParenthese)
    )
    ;

// fun(par1, par2, par3)
function
@init { gParent.pushMsg("function specification", state); }
@after { gParent.popMsg(state); }
    :
    functionName
    LPAREN
      (
        (STAR) => (star=STAR)
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
    (KW_IF | KW_ARRAY | KW_MAP | KW_STRUCT | KW_UNIONTYPE) => (KW_IF | KW_ARRAY | KW_MAP | KW_STRUCT | KW_UNIONTYPE)
    | 
    (functionIdentifier) => functionIdentifier
    |
    {!useSQL11ReservedKeywordsForIdentifier()}? sql11ReservedKeywordsUsedAsCastFunctionName -> Identifier[$sql11ReservedKeywordsUsedAsCastFunctionName.text]
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
    | timestampLiteral
    | intervalLiteral
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
    |
    KW_CURRENT_DATE -> ^(TOK_FUNCTION KW_CURRENT_DATE)
    ;

timestampLiteral
    :
    KW_TIMESTAMP StringLiteral ->
    {
      adaptor.create(TOK_TIMESTAMPLITERAL, $StringLiteral.text)
    }
    |
    KW_CURRENT_TIMESTAMP -> ^(TOK_FUNCTION KW_CURRENT_TIMESTAMP)
    ;

intervalLiteral
    :
    KW_INTERVAL StringLiteral qualifiers=intervalQualifiers ->
    {
      adaptor.create(qualifiers.tree.token.getType(), $StringLiteral.text)
    }
    ;

intervalQualifiers
    :
    KW_YEAR KW_TO KW_MONTH -> TOK_INTERVAL_YEAR_MONTH_LITERAL
    | KW_DAY KW_TO KW_SECOND -> TOK_INTERVAL_DAY_TIME_LITERAL
    | KW_YEAR -> TOK_INTERVAL_YEAR_LITERAL
    | KW_MONTH -> TOK_INTERVAL_MONTH_LITERAL
    | KW_DAY -> TOK_INTERVAL_DAY_LITERAL
    | KW_HOUR -> TOK_INTERVAL_HOUR_LITERAL
    | KW_MINUTE -> TOK_INTERVAL_MINUTE_LITERAL
    | KW_SECOND -> TOK_INTERVAL_SECOND_LITERAL
    ;

expression
@init { gParent.pushMsg("expression specification", state); }
@after { gParent.popMsg(state); }
    :
    precedenceOrExpression
    ;

atomExpression
    :
    (KW_NULL) => KW_NULL -> TOK_NULL
    | (constant) => constant
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
    (LPAREN precedenceBitwiseOrExpression COMMA) => precedenceEqualExpressionMutiple
    |
    precedenceEqualExpressionSingle
    ;

precedenceEqualExpressionSingle
    :
    (left=precedenceBitwiseOrExpression -> $left)
    (
       (KW_NOT precedenceEqualNegatableOperator notExpr=precedenceBitwiseOrExpression)
       -> ^(KW_NOT ^(precedenceEqualNegatableOperator $precedenceEqualExpressionSingle $notExpr))
    | (precedenceEqualOperator equalExpr=precedenceBitwiseOrExpression)
       -> ^(precedenceEqualOperator $precedenceEqualExpressionSingle $equalExpr)
    | (KW_NOT KW_IN LPAREN KW_SELECT)=>  (KW_NOT KW_IN subQueryExpression) 
       -> ^(KW_NOT ^(TOK_SUBQUERY_EXPR ^(TOK_SUBQUERY_OP KW_IN) subQueryExpression $precedenceEqualExpressionSingle))
    | (KW_NOT KW_IN expressions)
       -> ^(KW_NOT ^(TOK_FUNCTION KW_IN $precedenceEqualExpressionSingle expressions))
    | (KW_IN LPAREN KW_SELECT)=>  (KW_IN subQueryExpression) 
       -> ^(TOK_SUBQUERY_EXPR ^(TOK_SUBQUERY_OP KW_IN) subQueryExpression $precedenceEqualExpressionSingle)
    | (KW_IN expressions)
       -> ^(TOK_FUNCTION KW_IN $precedenceEqualExpressionSingle expressions)
    | ( KW_NOT KW_BETWEEN (min=precedenceBitwiseOrExpression) KW_AND (max=precedenceBitwiseOrExpression) )
       -> ^(TOK_FUNCTION Identifier["between"] KW_TRUE $left $min $max)
    | ( KW_BETWEEN (min=precedenceBitwiseOrExpression) KW_AND (max=precedenceBitwiseOrExpression) )
       -> ^(TOK_FUNCTION Identifier["between"] KW_FALSE $left $min $max)
    )*
    | (KW_EXISTS LPAREN KW_SELECT)=> (KW_EXISTS subQueryExpression) -> ^(TOK_SUBQUERY_EXPR ^(TOK_SUBQUERY_OP KW_EXISTS) subQueryExpression)
    ;

expressions
    :
    LPAREN expression (COMMA expression)* RPAREN -> expression+
    ;

//we transform the (col0, col1) in ((v00,v01),(v10,v11)) into struct(col0, col1) in (struct(v00,v01),struct(v10,v11))
precedenceEqualExpressionMutiple
    :
    (LPAREN precedenceBitwiseOrExpression (COMMA precedenceBitwiseOrExpression)+ RPAREN -> ^(TOK_FUNCTION Identifier["struct"] precedenceBitwiseOrExpression+))
    ( (KW_IN LPAREN expressionsToStruct (COMMA expressionsToStruct)+ RPAREN)
       -> ^(TOK_FUNCTION KW_IN $precedenceEqualExpressionMutiple expressionsToStruct+)
    | (KW_NOT KW_IN LPAREN expressionsToStruct (COMMA expressionsToStruct)+ RPAREN)
       -> ^(KW_NOT ^(TOK_FUNCTION KW_IN $precedenceEqualExpressionMutiple expressionsToStruct+)))
    ;

expressionsToStruct
    :
    LPAREN expression (COMMA expression)* RPAREN -> ^(TOK_FUNCTION Identifier["struct"] expression+)
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

booleanValueTok
   :
   KW_TRUE -> TOK_TRUE
   | KW_FALSE -> TOK_FALSE
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
      (sysFuncNames) => sysFuncNames
    | StringLiteral
    | functionIdentifier
    ;

identifier
    :
    Identifier
    | nonReserved -> Identifier[$nonReserved.text]
    // If it decides to support SQL11 reserved keywords, i.e., useSQL11ReservedKeywordsForIdentifier()=false, 
    // the sql11keywords in existing q tests will NOT be added back.
    | {useSQL11ReservedKeywordsForIdentifier()}? sql11ReservedKeywordsUsedAsIdentifier -> Identifier[$sql11ReservedKeywordsUsedAsIdentifier.text]
    ;

functionIdentifier
@init { gParent.pushMsg("function identifier", state); }
@after { gParent.popMsg(state); }
    : db=identifier DOT fn=identifier
    -> Identifier[$db.text + "." + $fn.text]
    |
    identifier
    ;

principalIdentifier
@init { gParent.pushMsg("identifier for principal spec", state); }
@after { gParent.popMsg(state); }
    : identifier
    | QuotedIdentifier
    ;

//The new version of nonReserved + sql11ReservedKeywordsUsedAsIdentifier = old version of nonReserved
//Non reserved keywords are basically the keywords that can be used as identifiers.
//All the KW_* are automatically not only keywords, but also reserved keywords.
//That means, they can NOT be used as identifiers.
//If you would like to use them as identifiers, put them in the nonReserved list below.
//If you are not sure, please refer to the SQL2011 column in
//http://www.postgresql.org/docs/9.5/static/sql-keywords-appendix.html
nonReserved
    :
    KW_ADD | KW_ADMIN | KW_AFTER | KW_ANALYZE | KW_ARCHIVE | KW_ASC | KW_BEFORE | KW_BUCKET | KW_BUCKETS
    | KW_CASCADE | KW_CHANGE | KW_CLUSTER | KW_CLUSTERED | KW_CLUSTERSTATUS | KW_COLLECTION | KW_COLUMNS
    | KW_COMMENT | KW_COMPACT | KW_COMPACTIONS | KW_COMPUTE | KW_CONCATENATE | KW_CONTINUE | KW_DATA | KW_DAY
    | KW_DATABASES | KW_DATETIME | KW_DBPROPERTIES | KW_DEFERRED | KW_DEFINED | KW_DELIMITED | KW_DEPENDENCY 
    | KW_DESC | KW_DIRECTORIES | KW_DIRECTORY | KW_DISABLE | KW_DISTRIBUTE | KW_ELEM_TYPE 
    | KW_ENABLE | KW_ESCAPED | KW_EXCLUSIVE | KW_EXPLAIN | KW_EXPORT | KW_FIELDS | KW_FILE | KW_FILEFORMAT
    | KW_FIRST | KW_FORMAT | KW_FORMATTED | KW_FUNCTIONS | KW_HOLD_DDLTIME | KW_HOUR | KW_IDXPROPERTIES | KW_IGNORE
    | KW_INDEX | KW_INDEXES | KW_INPATH | KW_INPUTDRIVER | KW_INPUTFORMAT | KW_ITEMS | KW_JAR
    | KW_KEYS | KW_KEY_TYPE | KW_LIMIT | KW_OFFSET | KW_LINES | KW_LOAD | KW_LOCATION | KW_LOCK | KW_LOCKS | KW_LOGICAL | KW_LONG
    | KW_MAPJOIN | KW_MATERIALIZED | KW_METADATA | KW_MINUS | KW_MINUTE | KW_MONTH | KW_MSCK | KW_NOSCAN | KW_NO_DROP | KW_OFFLINE
    | KW_OPTION | KW_OUTPUTDRIVER | KW_OUTPUTFORMAT | KW_OVERWRITE | KW_OWNER | KW_PARTITIONED | KW_PARTITIONS | KW_PLUS | KW_PRETTY
    | KW_PRINCIPALS | KW_PROTECTION | KW_PURGE | KW_READ | KW_READONLY | KW_REBUILD | KW_RECORDREADER | KW_RECORDWRITER
    | KW_RELOAD | KW_RENAME | KW_REPAIR | KW_REPLACE | KW_REPLICATION | KW_RESTRICT | KW_REWRITE
    | KW_ROLE | KW_ROLES | KW_SCHEMA | KW_SCHEMAS | KW_SECOND | KW_SEMI | KW_SERDE | KW_SERDEPROPERTIES | KW_SERVER | KW_SETS | KW_SHARED
    | KW_SHOW | KW_SHOW_DATABASE | KW_SKEWED | KW_SORT | KW_SORTED | KW_SSL | KW_STATISTICS | KW_STORED
    | KW_STREAMTABLE | KW_STRING | KW_STRUCT | KW_TABLES | KW_TBLPROPERTIES | KW_TEMPORARY | KW_TERMINATED
    | KW_TINYINT | KW_TOUCH | KW_TRANSACTIONS | KW_UNARCHIVE | KW_UNDO | KW_UNIONTYPE | KW_UNLOCK | KW_UNSET
    | KW_UNSIGNED | KW_URI | KW_USE | KW_UTC | KW_UTCTIMESTAMP | KW_VALUE_TYPE | KW_VIEW | KW_WHILE | KW_YEAR
    | KW_WORK
    | KW_TRANSACTION
    | KW_WRITE
    | KW_ISOLATION
    | KW_LEVEL
    | KW_SNAPSHOT
    | KW_AUTOCOMMIT
;

//The following SQL2011 reserved keywords are used as cast function name only, but not as identifiers.
sql11ReservedKeywordsUsedAsCastFunctionName
    :
    KW_BIGINT | KW_BINARY | KW_BOOLEAN | KW_CURRENT_DATE | KW_CURRENT_TIMESTAMP | KW_DATE | KW_DOUBLE | KW_FLOAT | KW_INT | KW_SMALLINT | KW_TIMESTAMP
    ;

//The following SQL2011 reserved keywords are used as identifiers in many q tests, they may be added back due to backward compatibility.
//We are planning to remove the following whole list after several releases.
//Thus, please do not change the following list unless you know what to do.
sql11ReservedKeywordsUsedAsIdentifier
    :
    KW_ALL | KW_ALTER | KW_ARRAY | KW_AS | KW_AUTHORIZATION | KW_BETWEEN | KW_BIGINT | KW_BINARY | KW_BOOLEAN 
    | KW_BOTH | KW_BY | KW_CREATE | KW_CUBE | KW_CURRENT_DATE | KW_CURRENT_TIMESTAMP | KW_CURSOR | KW_DATE | KW_DECIMAL | KW_DELETE | KW_DESCRIBE 
    | KW_DOUBLE | KW_DROP | KW_EXISTS | KW_EXTERNAL | KW_FALSE | KW_FETCH | KW_FLOAT | KW_FOR | KW_FULL | KW_GRANT 
    | KW_GROUP | KW_GROUPING | KW_IMPORT | KW_IN | KW_INNER | KW_INSERT | KW_INT | KW_INTERSECT | KW_INTO | KW_IS | KW_LATERAL 
    | KW_LEFT | KW_LIKE | KW_LOCAL | KW_NONE | KW_NULL | KW_OF | KW_ORDER | KW_OUT | KW_OUTER | KW_PARTITION 
    | KW_PERCENT | KW_PROCEDURE | KW_RANGE | KW_READS | KW_REVOKE | KW_RIGHT 
    | KW_ROLLUP | KW_ROW | KW_ROWS | KW_SET | KW_SMALLINT | KW_TABLE | KW_TIMESTAMP | KW_TO | KW_TRIGGER | KW_TRUE 
    | KW_TRUNCATE | KW_UNION | KW_UPDATE | KW_USER | KW_USING | KW_VALUES | KW_WITH 
//The following two keywords come from MySQL. Although they are not keywords in SQL2011, they are reserved keywords in MySQL.    
    | KW_REGEXP | KW_RLIKE
    ;
