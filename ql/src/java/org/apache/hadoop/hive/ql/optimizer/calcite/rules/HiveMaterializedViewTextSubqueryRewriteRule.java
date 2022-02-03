/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.optimizer.calcite.rules;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationImpl;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.metadata.HiveMaterializedViewsRegistry;
import org.apache.hadoop.hive.ql.metadata.HiveRelOptMaterialization;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveRelNode;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSortLimit;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.views.HiveMaterializedViewUtils;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.ParseUtils;

import java.util.List;
import java.util.Map;

import static org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelOptUtil.getNewRelFieldCollations;

public class HiveMaterializedViewTextSubqueryRewriteRule extends RelOptRule {

  private final Map<RelNode, ASTNode> map;

  /**
   * Creates a HiveProjectSortTransposeRule.
   * @param map
   */
  public HiveMaterializedViewTextSubqueryRewriteRule(Map<RelNode, ASTNode> map) {
    this(map, operand(RelNode.class, any()));
  }

  protected HiveMaterializedViewTextSubqueryRewriteRule(Map<RelNode, ASTNode> map, RelOptRuleOperand operand) {
    super(operand);
    this.map = map;
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    return map.containsKey(call.rel(0));
  }

  //~ Methods ----------------------------------------------------------------

  // implement RelOptRule
  public void onMatch(RelOptRuleCall call) {
    RelNode subQueryRoot = call.rel(0);

    HiveRelOptMaterialization match = null;
    for (HiveRelOptMaterialization materialization : HiveMaterializedViewsRegistry.get().getRewritingMaterializedViews()) {
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

      if (astTreeEquals(mvAST, map.get(subQueryRoot))) {
        match = materialization;
        break;
      }

    }

    if (match != null) {
      call.transformTo(match.tableRel);
    }
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
