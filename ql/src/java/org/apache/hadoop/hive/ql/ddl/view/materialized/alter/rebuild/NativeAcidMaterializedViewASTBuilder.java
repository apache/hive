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

import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.ParseDriver;

import java.util.List;

import static java.util.Collections.singletonList;

public class NativeAcidMaterializedViewASTBuilder extends MaterializedViewASTBuilder {
  @Override
  public List<ASTNode> createDeleteSelectNodes(String tableName) {
    return wrapIntoSelExpr(singletonList(createQualifiedColumnNode(tableName, VirtualColumn.ROWID.getName())));
  }

  @Override
  public void appendDeleteSelectNodes(ASTNode selectNode, String tableName) {
    wrapIntoSelExpr(createAcidSortNodesInternal(tableName))
        .forEach(astNode -> ParseDriver.adaptor.addChild(selectNode, astNode));
  }

  @Override
  protected List<ASTNode> createAcidSortNodesInternal(String tableName) {
    return singletonList(createQualifiedColumnNode(tableName, VirtualColumn.ROWID.getName()));
  }
}
