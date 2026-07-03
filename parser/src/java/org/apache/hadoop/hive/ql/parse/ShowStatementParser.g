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
parser grammar ShowStatementParser;

options
{
output=AST;
ASTLabelType=ASTNode;
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

// SHOW statement family (moved out of HiveParser.g so its decision DFA
// lives in its own generated subfile, freeing space in HiveParser.java's
// 64KB-per-method static initializer budget).

showStatement
@init { gParent.pushMsg("show statement", state); }
@after { gParent.popMsg(state); }
    : KW_SHOW KW_CATALOGS (KW_LIKE showStmtIdentifier)? -> ^(TOK_SHOWCATALOGS showStmtIdentifier?)
    | KW_SHOW (KW_DATABASES|KW_SCHEMAS) (KW_LIKE showStmtIdentifier)? -> ^(TOK_SHOWDATABASES showStmtIdentifier?)
    | KW_SHOW (isExtended=KW_EXTENDED)? KW_TABLES ((KW_FROM|KW_IN) db_name=identifier)? (filter=showTablesFilterExpr)?
    -> ^(TOK_SHOWTABLES (TOK_FROM $db_name)? $filter? $isExtended?)
    | KW_SHOW KW_VIEWS ((KW_FROM|KW_IN) db_name=identifier)? (KW_LIKE showStmtIdentifier|showStmtIdentifier)?  -> ^(TOK_SHOWVIEWS (TOK_FROM $db_name)? showStmtIdentifier?)
    | KW_SHOW KW_MATERIALIZED KW_VIEWS ((KW_FROM|KW_IN) db_name=identifier)? (KW_LIKE showStmtIdentifier|showStmtIdentifier)?  -> ^(TOK_SHOWMATERIALIZEDVIEWS (TOK_FROM $db_name)? showStmtIdentifier?)
    | KW_SHOW KW_SORTED? KW_COLUMNS (KW_FROM|KW_IN) tableName ((KW_FROM|KW_IN) db_name=identifier)? (KW_LIKE showStmtIdentifier|showStmtIdentifier)?
    -> ^(TOK_SHOWCOLUMNS tableName (TOK_FROM $db_name)? showStmtIdentifier? KW_SORTED?)
    | KW_SHOW KW_FUNCTIONS (KW_LIKE showFunctionIdentifier)?  -> ^(TOK_SHOWFUNCTIONS KW_LIKE? showFunctionIdentifier?)
    | KW_SHOW KW_PARTITIONS tabName=tableName partitionSpec? whereClause? orderByClause? limitClause? -> ^(TOK_SHOWPARTITIONS $tabName partitionSpec? whereClause? orderByClause? limitClause?)
    | KW_SHOW KW_CREATE (
        (KW_DATABASE|KW_SCHEMA) => (KW_DATABASE|KW_SCHEMA) db_name=databaseName -> ^(TOK_SHOW_CREATEDATABASE $db_name)
        |
        KW_TABLE tabName=tableName -> ^(TOK_SHOW_CREATETABLE $tabName)
      )
    | KW_SHOW KW_TABLE KW_EXTENDED ((KW_FROM|KW_IN) db_name=identifier)? KW_LIKE showStmtIdentifier partitionSpec?
    -> ^(TOK_SHOW_TABLESTATUS showStmtIdentifier $db_name? partitionSpec?)
    | KW_SHOW KW_TBLPROPERTIES tableName (LPAREN prptyName=StringLiteral RPAREN)? -> ^(TOK_SHOW_TBLPROPERTIES tableName $prptyName?)
    | KW_SHOW KW_LOCKS
      (
      (KW_DATABASE|KW_SCHEMA) => (KW_DATABASE|KW_SCHEMA) (dbName=identifier) (isExtended=KW_EXTENDED)? -> ^(TOK_SHOWDBLOCKS $dbName $isExtended?)
      |
      (parttype=partTypeExpr)? (isExtended=KW_EXTENDED)? -> ^(TOK_SHOWLOCKS $parttype? $isExtended?)
      )
    | KW_SHOW KW_ERASURE KW_LOCKS -> ^(TOK_SHOW_ERASURE_LOCKS)
    | KW_SHOW KW_COMPACTIONS
      (
      (KW_COMPACT_ID) => compactionId -> ^(TOK_SHOW_COMPACTIONS compactionId)
      |
      (KW_DATABASE|KW_SCHEMA) => (KW_DATABASE|KW_SCHEMA) (dbName=identifier) compactionPool? compactionType? compactionStatus? orderByClause? limitClause? -> ^(TOK_SHOW_COMPACTIONS $dbName compactionPool? compactionType? compactionStatus? orderByClause? limitClause?)
      |
      (parttype=partTypeExpr)? compactionPool? compactionType? compactionStatus? orderByClause? limitClause? -> ^(TOK_SHOW_COMPACTIONS $parttype? compactionPool? compactionType? compactionStatus? orderByClause? limitClause?)
      )
    | KW_SHOW KW_TRANSACTIONS -> ^(TOK_SHOW_TRANSACTIONS)
    | KW_SHOW KW_CONF StringLiteral -> ^(TOK_SHOWCONF StringLiteral)
    | KW_SHOW KW_RESOURCE
      (
        (KW_PLAN rp_name=identifier -> ^(TOK_SHOW_RP $rp_name))
        | (KW_PLANS -> ^(TOK_SHOW_RP))
      )
    | KW_SHOW (KW_DATACONNECTORS) -> ^(TOK_SHOWDATACONNECTORS)
    | KW_SHOW KW_DATA? KW_ERASURE KW_POLICIES ->  ^(TOK_SHOW_ERASURE_POLICIES)
    | KW_SHOW KW_DATA? KW_ERASURE KW_POLICY pn=Identifier KW_HISTORY
      -> ^(TOK_SHOW_ERASURE_POLICY_HISTORY $pn)
    | KW_SHOW KW_DATA? KW_ERASURE KW_GRANT
        (KW_FOR fp=Identifier)?
        (KW_ON KW_POLICY pp=Identifier)?
      -> ^(TOK_SHOW_ERASURE_GRANTS ^(TOK_GRANTS_FOR_PRINCIPAL $fp)? ^(TOK_GRANTS_FOR_POLICY $pp)?)
    ;

showTablesFilterExpr
@init { gParent.pushMsg("show tables filter expr", state); }
@after { gParent.popMsg(state); }
    : KW_WHERE identifier EQUAL StringLiteral
    -> ^(TOK_TABLE_TYPE identifier StringLiteral)
    | KW_LIKE showStmtIdentifier|showStmtIdentifier
    -> showStmtIdentifier
    ;
