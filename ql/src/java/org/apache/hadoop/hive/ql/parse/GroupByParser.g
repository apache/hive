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
parser grammar GroupByParser;

options
{
output=AST;
ASTLabelType=CommonTree;
backtrack=false;
k=3;
}

//import IdentifiersParser;

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
