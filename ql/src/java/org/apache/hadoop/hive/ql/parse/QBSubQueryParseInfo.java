/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.parse;

import org.apache.calcite.rel.RelNode;

import static org.apache.hadoop.hive.ql.parse.SubQueryUtils.buildSQOperator;

public class QBSubQueryParseInfo {
  public static QBSubQueryParseInfo parse(ASTNode subQueryExprNode) throws SemanticException {
    ASTNode subQueryOperatorAST = (ASTNode) subQueryExprNode.getChild(0);
    ASTNode subQueryAST = (ASTNode) subQueryExprNode.getChild(1);
    ASTNode insertClause = (ASTNode) subQueryAST.getFirstChildWithType(HiveParser.TOK_INSERT);
    ASTNode selectClause = (ASTNode) insertClause.getChild(1);

    int selectExprStart = 0;
    if ( selectClause.getChild(0).getType() == HiveParser.QUERY_HINT ) {
      selectExprStart = 1;
    }

    boolean hasAggregateExprs = false;
    for(int i= selectExprStart; i < selectClause.getChildCount(); i++ ) {

      ASTNode selectItem = (ASTNode) selectClause.getChild(i);
      int r = SubQueryUtils.checkAggOrWindowing(selectItem);

      hasAggregateExprs = hasAggregateExprs | ( r == 1 | r== 2 );
    }

    boolean hasExplicitGby = false;
    for(int i=0; i<insertClause.getChildCount(); i++) {
      if(insertClause.getChild(i).getType() == HiveParser.TOK_GROUPBY) {
        hasExplicitGby = true;
        break;
      }
    }

    return new QBSubQueryParseInfo(hasAggregateExprs, hasExplicitGby, buildSQOperator(subQueryOperatorAST));
  }

  public QBSubQueryParseInfo(boolean hasAggregateExprs, boolean hasExplicitGby, QBSubQuery.SubQueryTypeDef operator) {
    this.hasAggregateExprs = hasAggregateExprs;
    this.hasExplicitGby = hasExplicitGby;
    this.operator = operator;
  }

  private final boolean hasAggregateExprs;
  private final boolean hasExplicitGby;
  private final QBSubQuery.SubQueryTypeDef operator;
  private RelNode subQueryRelNode;

  public boolean hasFullAggregate() {
    return hasAggregateExprs && !hasExplicitGby;
  }

  public QBSubQuery.SubQueryTypeDef getOperator() {
    return operator;
  }

  public QBSubQueryParseInfo setSubQueryRelNode(RelNode subQueryRelNode) {
    this.subQueryRelNode = subQueryRelNode;
    return this;
  }

  public RelNode getSubQueryRelNode() {
    return subQueryRelNode;
  }
}


