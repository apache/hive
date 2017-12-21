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
parser grammar ResourcePlanParser;

options
{
  output=AST;
  ASTLabelType=ASTNode;
  backtrack=false;
  k=3;
}

resourcePlanDdlStatements
    : createResourcePlanStatement
    | alterResourcePlanStatement
    | dropResourcePlanStatement
    | globalWmStatement
    | createTriggerStatement
    | alterTriggerStatement
    | dropTriggerStatement
    | createPoolStatement
    | alterPoolStatement
    | dropPoolStatement
    | createMappingStatement
    | alterMappingStatement
    | dropMappingStatement
    ;

rpAssign
@init { gParent.pushMsg("rpAssign", state); }
@after { gParent.popMsg(state); }
  : (
      (KW_QUERY_PARALLELISM EQUAL parallelism=Number) -> ^(TOK_QUERY_PARALLELISM $parallelism)
    | (KW_DEFAULT KW_POOL EQUAL poolPath) -> ^(TOK_DEFAULT_POOL poolPath)
    )
  ;

rpAssignList
@init { gParent.pushMsg("rpAssignList", state); }
@after { gParent.popMsg(state); }
  : rpAssign (COMMA rpAssign)* -> rpAssign+
  ;

createResourcePlanStatement
@init { gParent.pushMsg("create resource plan statement", state); }
@after { gParent.popMsg(state); }
    : KW_CREATE KW_RESOURCE KW_PLAN name=identifier (KW_WITH rpAssignList)?
    -> ^(TOK_CREATE_RP $name rpAssignList?)
    ;

activate : KW_ACTIVATE -> ^(TOK_ACTIVATE);
enable : KW_ENABLE -> ^(TOK_ENABLE);
disable : KW_DISABLE -> ^(TOK_DISABLE);

alterResourcePlanStatement
@init { gParent.pushMsg("alter resource plan statement", state); }
@after { gParent.popMsg(state); }
    : KW_ALTER KW_RESOURCE KW_PLAN name=identifier (
          (KW_VALIDATE -> ^(TOK_ALTER_RP $name TOK_VALIDATE))
        | (KW_DISABLE -> ^(TOK_ALTER_RP $name TOK_DISABLE))
        | (KW_SET rpAssignList -> ^(TOK_ALTER_RP $name rpAssignList))
        | (KW_RENAME KW_TO newName=identifier -> ^(TOK_ALTER_RP $name TOK_RENAME $newName))
        | ((activate enable? | enable activate?) -> ^(TOK_ALTER_RP $name activate? enable?))
      )
    ;

/** It might make sense to make this more generic, if something else could be enabled/disabled.
    For now, it's only used for WM. Translate into another form of an alter statement. */
globalWmStatement
@init { gParent.pushMsg("global WM statement", state); }
@after { gParent.popMsg(state); }
    : (enable | disable) KW_WORKLOAD KW_MANAGEMENT -> ^(TOK_ALTER_RP enable? disable?)
    ;

dropResourcePlanStatement
@init { gParent.pushMsg("drop resource plan statement", state); }
@after { gParent.popMsg(state); }
    : KW_DROP KW_RESOURCE KW_PLAN name=identifier -> ^(TOK_DROP_RP $name)
    ;

poolPath
@init { gParent.pushMsg("poolPath", state); }
@after { gParent.popMsg(state); }
    : identifier^ (DOT identifier)*
    ;

triggerExpression
@init { gParent.pushMsg("triggerExpression", state); }
@after { gParent.popMsg(state); }
    : triggerOrExpression -> ^(TOK_TRIGGER_EXPRESSION triggerOrExpression)
    ;

triggerOrExpression
@init { gParent.pushMsg("triggerOrExpression", state); }
@after { gParent.popMsg(state); }
    : triggerAndExpression (KW_OR triggerAndExpression)*
    ;

triggerAndExpression
@init { gParent.pushMsg("triggerAndExpression", state); }
@after { gParent.popMsg(state); }
    : triggerAtomExpression (KW_AND triggerAtomExpression)*
    ;

triggerAtomExpression
@init { gParent.pushMsg("triggerAtomExpression", state); }
@after { gParent.popMsg(state); }
    : (identifier comparisionOperator triggerLiteral)
    | (LPAREN triggerOrExpression RPAREN)
    ;

triggerLiteral
@init { gParent.pushMsg("triggerLiteral", state); }
@after { gParent.popMsg(state); }
    : (Number (KW_HOUR|KW_MINUTE|KW_SECOND)?)
    | ByteLengthLiteral
    | StringLiteral
    ;

comparisionOperator
@init { gParent.pushMsg("comparisionOperator", state); }
@after { gParent.popMsg(state); }
    : EQUAL | LESSTHAN | LESSTHANOREQUALTO | GREATERTHAN | GREATERTHANOREQUALTO
    ;

triggerActionExpression
@init { gParent.pushMsg("triggerActionExpression", state); }
@after { gParent.popMsg(state); }
    : KW_KILL
    | (KW_MOVE^ KW_TO! poolPath)
    ;

createTriggerStatement
@init { gParent.pushMsg("create trigger statement", state); }
@after { gParent.popMsg(state); }
    : KW_CREATE KW_TRIGGER rpName=identifier DOT triggerName=identifier
      KW_WHEN triggerExpression KW_DO triggerActionExpression
    -> ^(TOK_CREATE_TRIGGER $rpName $triggerName triggerExpression triggerActionExpression)
    ;

alterTriggerStatement
@init { gParent.pushMsg("alter trigger statement", state); }
@after { gParent.popMsg(state); }
    : KW_ALTER KW_TRIGGER rpName=identifier DOT triggerName=identifier
      KW_WHEN triggerExpression KW_DO triggerActionExpression
    -> ^(TOK_ALTER_TRIGGER $rpName $triggerName triggerExpression triggerActionExpression)
    ;

dropTriggerStatement
@init { gParent.pushMsg("drop trigger statement", state); }
@after { gParent.popMsg(state); }
    : KW_DROP KW_TRIGGER rpName=identifier DOT triggerName=identifier
    -> ^(TOK_DROP_TRIGGER $rpName $triggerName)
    ;

poolAssign
@init { gParent.pushMsg("poolAssign", state); }
@after { gParent.popMsg(state); }
    : (
        (KW_ALLOC_FRACTION EQUAL allocFraction=Number) -> ^(TOK_ALLOC_FRACTION $allocFraction)
      | (KW_QUERY_PARALLELISM EQUAL parallelism=Number) -> ^(TOK_QUERY_PARALLELISM $parallelism)
      | (KW_SCHEDULING_POLICY EQUAL policy=StringLiteral) -> ^(TOK_SCHEDULING_POLICY $policy)
      | (KW_PATH EQUAL path=poolPath) -> ^(TOK_PATH $path)
      )
    ;

poolAssignList
@init { gParent.pushMsg("poolAssignList", state); }
@after { gParent.popMsg(state); }
    : poolAssign (COMMA poolAssign)* -> poolAssign+
    ;

createPoolStatement
@init { gParent.pushMsg("create pool statement", state); }
@after { gParent.popMsg(state); }
    : KW_CREATE KW_POOL rpName=identifier DOT poolPath
      KW_WITH poolAssignList
    -> ^(TOK_CREATE_POOL $rpName poolPath poolAssignList)
    ;

alterPoolStatement
@init { gParent.pushMsg("alter pool statement", state); }
@after { gParent.popMsg(state); }
    : KW_ALTER KW_POOL rpName=identifier DOT poolPath (
        (KW_SET poolAssignList -> ^(TOK_ALTER_POOL $rpName poolPath poolAssignList))
        | (KW_ADD KW_TRIGGER triggerName=identifier
            -> ^(TOK_ALTER_POOL $rpName poolPath ^(TOK_ADD_TRIGGER $triggerName)))
        | (KW_DROP KW_TRIGGER triggerName=identifier
            -> ^(TOK_ALTER_POOL $rpName poolPath ^(TOK_DROP_TRIGGER $triggerName)))
      )
    ;

dropPoolStatement
@init { gParent.pushMsg("drop pool statement", state); }
@after { gParent.popMsg(state); }
    : KW_DROP KW_POOL rpName=identifier DOT poolPath
    -> ^(TOK_DROP_POOL $rpName poolPath)
    ;

createMappingStatement
@init { gParent.pushMsg("create mapping statement", state); }
@after { gParent.popMsg(state); }
    : (KW_CREATE mappingType=(KW_USER | KW_GROUP)
         KW_MAPPING name=StringLiteral
         KW_IN rpName=identifier KW_TO poolPath
         (KW_WITH KW_ORDER order=Number)?)
    -> ^(TOK_CREATE_MAPPING $rpName $mappingType $name poolPath $order?)
    ;

alterMappingStatement
@init { gParent.pushMsg("alter mapping statement", state); }
@after { gParent.popMsg(state); }
    : (KW_ALTER mappingType=(KW_USER | KW_GROUP) KW_MAPPING
         KW_MAPPING name=StringLiteral
         KW_IN rpName=identifier KW_TO poolPath
         (KW_WITH KW_ORDER order=Number)?)
    -> ^(TOK_ALTER_MAPPING $rpName $mappingType $name poolPath $order?)
    ;

dropMappingStatement
@init { gParent.pushMsg("drop mapping statement", state); }
@after { gParent.popMsg(state); }
    : KW_DROP mappingType=(KW_USER | KW_GROUP) KW_MAPPING
         name=StringLiteral KW_IN rpName=identifier
    -> ^(TOK_DROP_MAPPING $rpName $mappingType $name)
    ;
