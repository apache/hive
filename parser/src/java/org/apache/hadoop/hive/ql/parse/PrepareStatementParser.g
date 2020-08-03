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
parser grammar PrepareStatementParser;

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

//----------------------- Rules for parsing Prepare statement-----------------------------
prepareStatement
@init { gParent.pushMsg("prepare statement ", state); }
@after { gParent.popMsg(state); }
    : KW_PREPARE identifier KW_FROM queryStatementExpression
    -> ^(TOK_PREPARE queryStatementExpression identifier)
    ;

executeStatement
@init { gParent.pushMsg("execute statement ", state); }
@after { gParent.popMsg(state); }
    : KW_EXECUTE identifier KW_USING executeParamList
    -> ^(TOK_EXECUTE executeParamList identifier)
    ;

//TODO: instead of constant using expression will provide richer and broader parameters
executeParamList
@init { gParent.pushMsg("execute param list", state); }
@after { gParent.popMsg(state); }
    : constant (COMMA constant)*
    -> ^(TOK_EXECUTE_PARAM_LIST constant+)
    ;
