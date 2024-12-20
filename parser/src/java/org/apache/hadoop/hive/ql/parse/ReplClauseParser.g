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
parser grammar ReplClauseParser;

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

replicationClause
@init { gParent.pushMsg("replication clause", state); }
@after { gParent.popMsg(state); }
    : KW_FOR (isMetadataOnly=KW_METADATA)? KW_REPLICATION LPAREN (replId=StringLiteral) RPAREN
    -> ^(TOK_REPLICATION $replId $isMetadataOnly?)
    ;

replDumpStatement
@init { gParent.pushMsg("Replication dump statement", state); }
@after { gParent.popMsg(state); }
      : KW_REPL KW_DUMP
        (dbPolicy=replDbPolicy)
        (KW_REPLACE oldDbPolicy=replDbPolicy)?
        (KW_WITH replConf=replConfigs)?
    -> ^(TOK_REPL_DUMP $dbPolicy ^(TOK_REPLACE $oldDbPolicy)? $replConf?)
    ;

replDbPolicy
@init { gParent.pushMsg("Repl dump DB replication policy", state); }
@after { gParent.popMsg(state); }
    :
      (dbName=identifier) (DOT tablePolicy=replTableLevelPolicy)? -> $dbName $tablePolicy?
    ;

replLoadStatement
@init { gParent.pushMsg("Replication load statement", state); }
@after { gParent.popMsg(state); }
      : KW_REPL KW_LOAD
      (sourceDbPolicy=replDbPolicy)
      (KW_INTO dbName=identifier)?
      (KW_WITH replConf=replConfigs)?
      -> ^(TOK_REPL_LOAD $sourceDbPolicy ^(TOK_DBNAME $dbName)? $replConf?)
      ;

replConfigs
@init { gParent.pushMsg("Repl configurations", state); }
@after { gParent.popMsg(state); }
    :
      LPAREN replConfigsList RPAREN -> ^(TOK_REPL_CONFIG replConfigsList)
    ;

replConfigsList
@init { gParent.pushMsg("Repl configurations list", state); }
@after { gParent.popMsg(state); }
    :
      keyValueProperty (COMMA keyValueProperty)* -> ^(TOK_REPL_CONFIG_LIST keyValueProperty+)
    ;

replTableLevelPolicy
@init { gParent.pushMsg("Replication table level policy definition", state); }
@after { gParent.popMsg(state); }
    :
      ((replTablesIncludeList=StringLiteral) (DOT replTablesExcludeList=StringLiteral)?)
      -> ^(TOK_REPL_TABLES $replTablesIncludeList $replTablesExcludeList?)
    ;

replStatusStatement
@init { gParent.pushMsg("replication status statement", state); }
@after { gParent.popMsg(state); }
      : KW_REPL KW_STATUS
        (dbName=identifier)
        (KW_WITH replConf=replConfigs)?
      -> ^(TOK_REPL_STATUS $dbName $replConf?)
      ;

