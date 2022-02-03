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
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.hadoop.hive.ql.metadata.HiveMaterializedViewsRegistry;
import org.apache.hadoop.hive.ql.metadata.HiveRelOptMaterialization;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.CalcitePlanner;

import java.util.List;
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
      return super.visit(project);
    }

    List<HiveRelOptMaterialization> matches = HiveMaterializedViewsRegistry.get().getRewritingMaterializedViews(expandedSubqAST);
    if (matches.isEmpty()) {
      return super.visit(project);
    }

    return matches.get(0).tableRel;
  }

    @Override
  public RelNode visit(HiveFilter filter) {

    RexNode newCond = filter.getCondition().accept(new HiveMaterializedViewTextSubqueryRewriteRexShuttle(map, ast, expandedAST, relBuilder));
    return relBuilder
            .push(filter.getInput().accept(this))
            .filter(newCond)
            .build();
  }
}
