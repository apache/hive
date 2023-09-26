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

import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;

import java.util.List;
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
  protected List<ASTNode> createAcidSortNodesInternal(String tableName) {
    return mvTable.getStorageHandler().acidSortColumns(mvTable, Context.Operation.DELETE).stream()
            .map(fieldSchema -> createQualifiedColumnNode(tableName, fieldSchema.getName()))
            .collect(Collectors.toList());
  }
}
