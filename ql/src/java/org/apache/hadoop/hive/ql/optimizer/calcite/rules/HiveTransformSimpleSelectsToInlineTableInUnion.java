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

package org.apache.hadoop.hive.ql.optimizer.calcite.rules;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.hadoop.hive.ql.optimizer.calcite.CalciteSemanticException;
import org.apache.hadoop.hive.ql.optimizer.calcite.TraitsUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableFunctionScan;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveUnion;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.SqlFunctionConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;

/**
 * Transforms SELECTS of literals under UNION ALL into inline table scans.
 */
public class HiveTransformSimpleSelectsToInlineTableInUnion extends RelOptRule {

  protected static final Logger LOG = LoggerFactory.getLogger(HiveTransformSimpleSelectsToInlineTableInUnion.class);

  private RelNode dummyTable;

  public HiveTransformSimpleSelectsToInlineTableInUnion(RelNode dummyTable) {
    super(operand(HiveUnion.class, any()));
    this.dummyTable = dummyTable;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    RexBuilder rexBuilder = call.builder().getRexBuilder();
    final HiveUnion union = call.rel(0);
    if (!union.all) {
      return;
    }

    List<RelNode> inputs = new ArrayList<RelNode>();
    List<Project> projects = new ArrayList<>();
    List<HiveTableFunctionScan> inlineTables = new ArrayList<>();
    RelOptRuleOperand plainProjectOperand = operand(Project.class, none());
    RelOptRuleOperand inlineTableOperand = operand(HiveTableFunctionScan.class, none());

    for (RelNode input : union.getInputs()) {
      input = HiveRelDecorrelator.stripHep(input);

      if (plainProjectOperand.matches(input)) {
        projects.add((Project) input);
        continue;
      }
      if (inlineTableOperand.matches(input)) {
        inlineTables.add((HiveTableFunctionScan) input);
        continue;
      }
      inputs.add(input);
    }

    if (projects.size() + inlineTables.size() <= 1) {
      // nothing to do
      return;
    }

    ArrayListMultimap<RelRecordType, RexNode> newRows = ArrayListMultimap.create();
    for (HiveTableFunctionScan rel : inlineTables) {
      // inline(array(row1,row2,...))
      RexCall rex = (RexCall) ((RexCall) rel.getCall()).operands.get(0);
      for (RexNode row : rex.operands) {
        if (!(row.getType() instanceof RelRecordType)) {
          return;
        }
        newRows.put((RelRecordType) row.getType(), row);
      }
    }

    for (Project proj : projects) {
      RexNode row = rexBuilder.makeCall(SqlStdOperatorTable.ROW, proj.getProjects());
      if (!(row.getType() instanceof RelRecordType)) {
        return;
      }
      newRows.put((RelRecordType) row.getType(), row);
    }

    if (newRows.keySet().size() + inputs.size() == union.getInputs().size()) {
      // nothing to do
      return;
    }

    for (RelRecordType type : newRows.keySet()) {
      List<RexNode> rows = newRows.get(type);

      RelDataType arrayType = rexBuilder.getTypeFactory().createArrayType(type, -1);
      try {
        SqlOperator inlineFn =
            SqlFunctionConverter.getCalciteFn("inline", ImmutableList.of(arrayType), type, true, false);
        SqlOperator arrayFn =
            SqlFunctionConverter.getCalciteFn("array", Collections.nCopies(rows.size(), type), arrayType, true, false);

        RexNode expr = rexBuilder.makeCall(arrayFn, rows);
        expr = rexBuilder.makeCall(inlineFn, expr);

        RelNode newInlineTable = buildTableFunctionScan(expr, union.getCluster());

        inputs.add(newInlineTable);

      } catch (CalciteSemanticException e) {
        LOG.debug("Conversion failed with exception", e);
        return;
      }
    }

    if (inputs.size() > 1) {
      HiveUnion newUnion = (HiveUnion) union.copy(union.getTraitSet(), inputs, true);
      call.transformTo(newUnion);
    } else {
      call.transformTo(inputs.get(0));
    }
  }

  private RelNode buildTableFunctionScan(RexNode expr, RelOptCluster cluster)
      throws CalciteSemanticException {

    return HiveTableFunctionScan.create(cluster, TraitsUtil.getDefaultTraitSet(cluster),
        ImmutableList.of(dummyTable), expr, null, expr.getType(), null);

  }
}