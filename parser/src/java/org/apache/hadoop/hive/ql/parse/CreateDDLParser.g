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
parser grammar CreateDDLParser;

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

likeTableOrFile
    : (KW_LIKE KW_FILE) => KW_LIKE KW_FILE
    | (KW_LIKE KW_FILE format=identifier uri=StringLiteral) -> ^(TOK_LIKEFILE $format $uri)
    | (KW_LIKE likeName=tableName) -> ^(TOK_LIKETABLE $likeName)
    ;

//----------------------- Rules for parsing createtable -----------------------------
createTableStatement
@init { gParent.pushMsg("create table statement", state); }
@after { gParent.popMsg(state); }
    : KW_CREATE (temp=KW_TEMPORARY)? (trans=KW_TRANSACTIONAL)? (ext=KW_EXTERNAL)? KW_TABLE ifNotExists? name=tableName
      (  likeTableOrFile
         createTablePartitionSpec?
         tableRowFormat?
         tableFileFormat?
         tableLocation?
         tablePropertiesPrefixed?
       | (LPAREN columnNameTypeOrConstraintList RPAREN)?
         tableComment?
         createTablePartitionSpec?
         tableBuckets?
         tableSkewed?
         tableRowFormat?
         tableFileFormat?
         tableLocation?
         tablePropertiesPrefixed?
         (KW_AS selectStatementWithCTE)?
      )
    -> ^(TOK_CREATETABLE $name $temp? $trans? $ext? ifNotExists?
         likeTableOrFile?
         columnNameTypeOrConstraintList?
         tableComment?
         createTablePartitionSpec?
         tableBuckets?
         tableSkewed?
         tableRowFormat?
         tableFileFormat?
         tableLocation?
         tablePropertiesPrefixed?
         selectStatementWithCTE?
        )
    | KW_CREATE mgd=KW_MANAGED KW_TABLE ifNotExists? name=tableName
      (  likeTableOrFile
         tableRowFormat?
         tableFileFormat?
         tableLocation?
         tablePropertiesPrefixed?
       | (LPAREN columnNameTypeOrConstraintList RPAREN)?
         tableComment?
         createTablePartitionSpec?
         tableBuckets?
         tableSkewed?
         tableRowFormat?
         tableFileFormat?
         tableLocation?
         tablePropertiesPrefixed?
         (KW_AS selectStatementWithCTE)?
      )
    -> ^(TOK_CREATETABLE $name $mgd ifNotExists?
         likeTableOrFile?
         columnNameTypeOrConstraintList?
         tableComment?
         createTablePartitionSpec?
         tableBuckets?
         tableSkewed?
         tableRowFormat?
         tableFileFormat?
         tableLocation?
         tablePropertiesPrefixed?
         selectStatementWithCTE?
        )
    ;

createDataConnectorStatement
@init { gParent.pushMsg("create connector statement", state); }
@after { gParent.popMsg(state); }
    : KW_CREATE KW_DATACONNECTOR ifNotExists? name=identifier dataConnectorType dataConnectorUrl dataConnectorComment? ( KW_WITH KW_DCPROPERTIES dcprops=dcProperties)?
    -> ^(TOK_CREATEDATACONNECTOR $name ifNotExists? dataConnectorType dataConnectorUrl dataConnectorComment? $dcprops?)
    ;

dataConnectorComment
@init { gParent.pushMsg("dataconnector comment", state); }
@after { gParent.popMsg(state); }
    : KW_COMMENT comment=StringLiteral
    -> ^(TOK_DATACONNECTORCOMMENT $comment)
    ;

dataConnectorUrl
@init { gParent.pushMsg("dataconnector URL", state); }
@after { gParent.popMsg(state); }
    : KW_URL url=StringLiteral
    -> ^(TOK_DATACONNECTORURL $url)
    ;

dataConnectorType
@init { gParent.pushMsg("dataconnector type", state); }
@after { gParent.popMsg(state); }
    : KW_TYPE dcType=StringLiteral
    -> ^(TOK_DATACONNECTORTYPE $dcType)
    ;

dcProperties
@init { gParent.pushMsg("dcproperties", state); }
@after { gParent.popMsg(state); }
    :
      LPAREN dbPropertiesList RPAREN -> ^(TOK_DATACONNECTORPROPERTIES dbPropertiesList)
    ;

dropDataConnectorStatement
@init { gParent.pushMsg("drop connector statement", state); }
@after { gParent.popMsg(state); }
    : KW_DROP (KW_DATACONNECTOR) ifExists? identifier
    -> ^(TOK_DROPDATACONNECTOR identifier ifExists?)
    ;
