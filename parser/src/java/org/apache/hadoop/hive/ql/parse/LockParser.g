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
parser grammar LockParser;

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

lockStatements
@init { gParent.pushMsg("lock statements", state); }
@after { gParent.popMsg(state); }
    : lockStatement
    | unlockStatement
    | lockDatabase
    | unlockDatabase
    ;

lockStatement
@init { gParent.pushMsg("lock statement", state); }
@after { gParent.popMsg(state); }
    : KW_LOCK KW_TABLE tableName partitionSpec? lockMode -> ^(TOK_LOCKTABLE tableName lockMode partitionSpec?)
    ;

lockDatabase
@init { gParent.pushMsg("lock database statement", state); }
@after { gParent.popMsg(state); }
    : KW_LOCK (KW_DATABASE|KW_SCHEMA) (dbName=databaseName) lockMode -> ^(TOK_LOCKDB $dbName lockMode)
    ;

lockMode
@init { gParent.pushMsg("lock mode", state); }
@after { gParent.popMsg(state); }
    : KW_SHARED | KW_EXCLUSIVE
    ;

unlockStatement
@init { gParent.pushMsg("unlock statement", state); }
@after { gParent.popMsg(state); }
    : KW_UNLOCK KW_TABLE tableName partitionSpec?  -> ^(TOK_UNLOCKTABLE tableName partitionSpec?)
    ;

unlockDatabase
@init { gParent.pushMsg("unlock database statement", state); }
@after { gParent.popMsg(state); }
    : KW_UNLOCK (KW_DATABASE|KW_SCHEMA) (dbName=databaseName) -> ^(TOK_UNLOCKDB $dbName)
    ;
