package org.apache.hadoop.hive.ql.optimizer.calcite;/*
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

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.metadata.HiveMaterializedViewsRegistry;
import org.apache.hadoop.hive.ql.metadata.HiveRelOptMaterialization;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.views.HiveMaterializedViewUtils;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.CalcitePlanner;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.ParseUtils;

import java.util.Map;
import java.util.Stack;

public class HiveMaterializedViewTextSubqueryRewriteShuttle extends HiveRelShuttleImpl {

  private final Map<RelNode, ASTNode> map;
  private final ASTNode ast;
  private final ASTNode expandedAST;
  private final RelBuilder relBuilder;

  public HiveMaterializedViewTextSubqueryRewriteShuttle(Map<RelNode, ASTNode> map, ASTNode ast, ASTNode expandedAST, RelBuilder relBuilder) {
    this.map = map;
    this.ast = ast;
    this.expandedAST = expandedAST;
    this.relBuilder = relBuilder;
  }

  public RelNode validate(RelNode relNode) {
    return relNode.accept(this);
  }

  @Override
  public RelNode visit(HiveProject project) {
    if (!map.containsKey(project)) {
      return super.visit(project);
    }

    HiveRelOptMaterialization match = null;
    for (HiveRelOptMaterialization materialization : HiveMaterializedViewsRegistry.get().getAllRewritingMaterializedViews()) {
      Table mvTable = HiveMaterializedViewUtils.extractTable(materialization);
      if (mvTable == null) {
        continue;
      }

      String expandedText = mvTable.getViewExpandedText();

      ASTNode mvAST;
      try {
        mvAST = ParseUtils.parse(expandedText, new Context(new HiveConf()));
      } catch (ParseException e) {
        throw new RuntimeException(e);
      }

      if (mvAST == null) {
        continue;
      }

      Stack<Integer> path = new Stack<>();
      ASTNode curr = map.get(project);
      while (curr != null && curr != ast) {
        path.push(curr.getType());
        curr = (ASTNode) curr.getParent();
      }

      int[] pathInt = new int[path.size()];
      int idx = 0;
      while (!path.isEmpty()) {
        pathInt[idx] = path.pop();
        ++idx;
      }

      ASTNode expandedSubqAST = new CalcitePlanner.ASTSearcher().simpleBreadthFirstSearch(expandedAST, pathInt);
      if (expandedSubqAST == null) {
        continue;
      }

      if (astTreeEquals(mvAST, expandedSubqAST)) {
        match = materialization;
        break;
      }

    }

    if (match != null) {
      return match.tableRel;
    }

    return super.visit(project);
  }

  private boolean astTreeEquals(ASTNode mvAST, ASTNode astNode) {
    if (!(mvAST.getName().equals(astNode.getName()) &&
            mvAST.getType() == astNode.getType() &&
            mvAST.getText().equals(astNode.getText()) &&
            mvAST.getChildCount() == astNode.getChildCount())) {
      return false;
    }

    for (int i = 0; i < mvAST.getChildCount(); ++i) {
      if (!astTreeEquals((ASTNode) mvAST.getChild(i), (ASTNode) astNode.getChild(i))) {
        return false;
      }
    }

    return true;
  }
}
