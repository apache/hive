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

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseDriver;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class NonNativeAcidMaterializedViewASTBuilder extends MaterializedViewASTBuilder {
  private final Table mvTable;

  public NonNativeAcidMaterializedViewASTBuilder(Table mvTable) {
    this.mvTable = mvTable;
  }

  @Override
  public List<ASTNode> createDeleteSelectNodes(String tableName) {
    return wrapIntoSelExpr(mvTable.getStorageHandler().acidSelectColumns(mvTable, Context.Operation.DELETE)
            .stream().map(fieldSchema -> createQualifiedColumnNode(tableName, fieldSchema.getName()))
            .collect(Collectors.toList()));
  }

  @Override
  public void appendDeleteSelectNodes(ASTNode selectNode, String tableName) {
    Set<String> selectedColumns = new HashSet<>(selectNode.getChildCount());

    for (int i = 0; i < selectNode.getChildCount(); ++i) {
      ASTNode selectExpr = (ASTNode) selectNode.getChild(i);
      ASTNode expression = (ASTNode) selectExpr.getChild(0);
      if (expression.getType() == HiveParser.DOT &&
          expression.getChildCount() == 2 &&
          expression.getChild(0).getType() == HiveParser.TOK_TABLE_OR_COL) {
        selectedColumns.add(expression.getChild(1).getText());
      }
    }

    for (FieldSchema fieldSchema : mvTable.getStorageHandler().acidSelectColumns(mvTable, Context.Operation.DELETE)) {
      if (!selectedColumns.contains(fieldSchema.getName())) {
        ParseDriver.adaptor.addChild(selectNode, wrapIntoSelExpr(
            createQualifiedColumnNode(tableName, fieldSchema.getName())));
      }
    }
  }

  @Override
  protected List<ASTNode> createAcidSortNodesInternal(String tableName) {
    return mvTable.getStorageHandler().acidSortColumns(mvTable, Context.Operation.DELETE).stream()
            .map(fieldSchema -> createQualifiedColumnNode(tableName, fieldSchema.getName()))
            .collect(Collectors.toList());
  }
}
