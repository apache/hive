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

import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.rewrite.RewriterFactory;
import org.apache.hadoop.hive.ql.parse.rewrite.UpdateStatement;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class UpdateSemanticAnalyzer extends RewriteSemanticAnalyzer<UpdateStatement> {

  public UpdateSemanticAnalyzer(QueryState queryState, RewriterFactory<UpdateStatement> rewriterFactory)
      throws SemanticException {
    super(queryState, rewriterFactory);
  }

  @Override
  protected ASTNode getTargetTableNode(ASTNode tree) {
    // The first child should be the table we are updating / deleting from
    ASTNode tabName = (ASTNode) tree.getChild(0);
    assert tabName.getToken().getType() == HiveParser.TOK_TABNAME :
        "Expected tablename as first child of " + Context.Operation.UPDATE + " but found " + tabName.getName();
    return tabName;
  }

  @Override
  protected void analyze(ASTNode tree, Table table, ASTNode tableName) throws SemanticException {
    List<? extends Node> children = tree.getChildren();

    ASTNode where = null;
    int whereIndex = 2;
    if (children.size() > whereIndex) {
      where = (ASTNode) children.get(whereIndex);
      assert where.getToken().getType() == HiveParser.TOK_WHERE :
          "Expected where clause, but found " + where.getName();
    }

//    TOK_UPDATE_TABLE
//            TOK_TABNAME
//               ...
//            TOK_SET_COLUMNS_CLAUSE <- The set list from update should be the second child (index 1)
    assert children.size() >= 2 : "Expected update token to have at least two children";
    ASTNode setClause = (ASTNode) children.get(1);
    // Must be deterministic order set for consistent q-test output across Java versions (HIVE-9239)
    Set<String> setRCols = new LinkedHashSet<>();
    Map<String, ASTNode> setCols = collectSetColumnsAndExpressions(setClause, setRCols, table);
    Map<String, String> colNameToDefaultConstraint = getColNameToDefaultValueMap(table);

    rewriteAndAnalyze(new UpdateStatement(table, where, setClause, setCols, colNameToDefaultConstraint), null);

    updateOutputs(table);
    setUpAccessControlInfoForUpdate(table, setCols);

    // Add the setRCols to the input list
    if (columnAccessInfo == null) { //assuming this means we are not doing Auth
      return;
    }

    for (String colName : setRCols) {
      columnAccessInfo.add(Table.getCompleteName(table.getDbName(), table.getTableName()), colName);
    }
  }

  @Override
  protected boolean enableColumnStatsCollecting() {
    return false;
  }
}
