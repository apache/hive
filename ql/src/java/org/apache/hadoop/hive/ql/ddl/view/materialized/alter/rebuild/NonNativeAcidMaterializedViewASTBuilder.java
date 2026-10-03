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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.ql.ddl.view.materialized.alter.rebuild;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.parse.ASTNode;
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
    return wrapIntoSelExpr(deleteSelectColumns()
            .stream().map(column -> createQualifiedColumnNode(tableName, column))
            .collect(Collectors.toList()));
  }

  @Override
  public void appendDeleteSelectNodes(ASTNode selectNode, String tableName) {
    Set<String> selectedColumns = new HashSet<>(selectNode.getChildCount());

    for (int i = 0; i < selectNode.getChildCount(); ++i) {
      ASTNode selectExpr = (ASTNode) selectNode.getChild(i);
      selectedColumns.add(selectExpr.getChild(selectExpr.getChildCount() - 1).getText());
    }

    for (String column : deleteSelectColumns()) {
      if (!selectedColumns.contains(column)) {
        ParseDriver.adaptor.addChild(selectNode, wrapIntoSelExpr(
            createQualifiedColumnNode(tableName, column)));
      }
    }
  }

  // the incremental rebuild is a MERGE: a merge-on-read MERGE deletes the virtual columns of a record
  private List<String> deleteSelectColumns() {
    HiveStorageHandler storageHandler = mvTable.getStorageHandler();
    if (storageHandler.shouldOverwrite(mvTable, Context.Operation.MERGE)) {
      return storageHandler.acidSelectColumns(mvTable, Context.Operation.DELETE).stream()
          .map(FieldSchema::getName).toList();
    }
    return storageHandler.acidVirtualColumns().stream().map(VirtualColumn::getName).toList();
  }

  @Override
  protected List<ASTNode> createAcidSortNodesInternal(String tableName) {
    return mvTable.getStorageHandler().acidSortColumns(mvTable, Context.Operation.DELETE).stream()
            .map(fieldSchema -> createQualifiedColumnNode(tableName, fieldSchema.getName()))
            .collect(Collectors.toList());
  }
}
