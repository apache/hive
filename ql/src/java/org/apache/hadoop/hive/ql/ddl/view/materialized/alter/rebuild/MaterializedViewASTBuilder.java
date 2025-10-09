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

package org.apache.hadoop.hive.ql.ddl.view.materialized.alter.rebuild;

import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseDriver;

import java.util.List;
import java.util.stream.Collectors;

abstract class MaterializedViewASTBuilder {
  public abstract List<ASTNode> createDeleteSelectNodes(String tableName);

  public List<ASTNode> createAcidSortNodes(ASTNode inputNode) {
    return createAcidSortNodesInternal(inputNode.getText());
  }

  public abstract void appendDeleteSelectNodes(ASTNode selectNode, String tableName);

  protected abstract List<ASTNode> createAcidSortNodesInternal(String tableName);

  // .
  //    TOK_TABLE_OR_COL
  //          <tableName>
  //    <columnName>
  public ASTNode createQualifiedColumnNode(String tableName, String columnName) {
    ASTNode dotNode = (ASTNode) ParseDriver.adaptor.create(HiveParser.DOT, ".");
    ASTNode columnTokNode = (ASTNode) ParseDriver.adaptor.create(HiveParser.TOK_TABLE_OR_COL, "TOK_TABLE_OR_COL");
    ASTNode rowIdNode = (ASTNode) ParseDriver.adaptor.create(HiveParser.Identifier, columnName);
    ASTNode tableNameNode = (ASTNode) ParseDriver.adaptor.create(HiveParser.Identifier, tableName);

    ParseDriver.adaptor.addChild(dotNode, columnTokNode);
    ParseDriver.adaptor.addChild(dotNode, rowIdNode);
    ParseDriver.adaptor.addChild(columnTokNode, tableNameNode);
    return dotNode;
  }

  public List<ASTNode> wrapIntoSelExpr(List<ASTNode> expressionNodes) {
    return expressionNodes.stream().map(this::wrapIntoSelExpr).collect(Collectors.toList());
  }

  public ASTNode wrapIntoSelExpr(ASTNode expressionNode) {
    ASTNode selectExpr = (ASTNode) ParseDriver.adaptor.create(HiveParser.TOK_SELEXPR, "TOK_SELEXPR");
    ParseDriver.adaptor.addChild(selectExpr, expressionNode);
    return selectExpr;
  }

  public ASTNode createSortNodes(List<ASTNode> sortKeyNodes) {
    ASTNode sortExprNode = (ASTNode) ParseDriver.adaptor.create(HiveParser.TOK_SORTBY, "TOK_SORTBY");
    sortKeyNodes.forEach(sortKeyNode -> ParseDriver.adaptor.addChild(sortExprNode, createSortNode(sortKeyNode)));
    return sortExprNode;
  }

  //       TOK_SORTBY
  //         TOK_TABSORTCOLNAMEASC
  //            TOK_NULLS_FIRST
  //               <sortKeyNode>
  public ASTNode createSortNode(ASTNode sortKeyNodes) {
    ASTNode orderExprNode = (ASTNode) ParseDriver.adaptor.create(
            HiveParser.TOK_TABSORTCOLNAMEASC, "TOK_TABSORTCOLNAMEASC");
    ASTNode nullsOrderExprNode = (ASTNode) ParseDriver.adaptor.create(
            HiveParser.TOK_NULLS_FIRST, "TOK_NULLS_FIRST");
    ParseDriver.adaptor.addChild(orderExprNode, nullsOrderExprNode);
    ParseDriver.adaptor.addChild(nullsOrderExprNode, sortKeyNodes);
    return orderExprNode;
  }
}
