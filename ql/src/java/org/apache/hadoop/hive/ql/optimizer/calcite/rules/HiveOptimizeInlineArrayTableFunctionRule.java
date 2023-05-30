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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.rules.FilterAggregateTransposeRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableFunctionScan;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * XXX:
 */
public class HiveOptimizeInlineArrayTableFunctionRule extends RelOptRule {

  public static final HiveOptimizeInlineArrayTableFunctionRule INSTANCE =
          new HiveOptimizeInlineArrayTableFunctionRule(HiveRelFactories.HIVE_BUILDER);

  public HiveOptimizeInlineArrayTableFunctionRule(RelBuilderFactory relBuilderFactory) {
    super(operand(HiveTableFunctionScan.class, any()), relBuilderFactory, null);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    final HiveTableFunctionScan tableFunctionScanRel = call.rel(0);

    Preconditions.checkState(tableFunctionScanRel.getCall() instanceof RexCall);
    RexCall udtfCall = (RexCall) tableFunctionScanRel.getCall();
    if (!udtfCall.getOperator().getName().toLowerCase().equals("inline")) {
      return false;
    }

    Preconditions.checkState(udtfCall.getOperands().size() > 0);
    RexNode operand = udtfCall.getOperands().get(0);
    if (!(operand instanceof RexCall)) {
      return false;
    }
    RexCall firstOperand = (RexCall) operand;
    if (!firstOperand.getOperator().getName().toLowerCase().equals("array")) {
      return false;
    }
    Preconditions.checkState(firstOperand.getOperands().size() > 0);
    int numStructParams = firstOperand.getOperands().get(0).getType().getFieldCount();

    if (tableFunctionScanRel.getRowType().getFieldCount() == numStructParams) {
      return false;
    }

    return true;
  }

  public void onMatch(RelOptRuleCall call) {
    final HiveTableFunctionScan tfs = call.rel(0);
    RelNode inputRel = tfs.getInput(0);
    RexCall inlineCall = (RexCall) tfs.getCall();
    RexCall arrayCall = (RexCall) inlineCall.getOperands().get(0);
    RelOptCluster cluster = tfs.getCluster();

    List<RexNode> inputRefs = Lists.transform(inputRel.getRowType().getFieldList(),
        input -> new RexInputRef(input.getIndex(), input.getType()));
    List<RexNode> newStructExprs = new ArrayList<>();
    for (RexNode currentStructOperand : arrayCall.getOperands()) {
      List<RexNode> allOperands = new ArrayList<>(inputRefs);
      RexCall structCall = (RexCall) currentStructOperand;
      allOperands.addAll(structCall.getOperands());
      newStructExprs.add(cluster.getRexBuilder().makeCall(structCall.op, allOperands));
    }

    List<RelDataType> returnTypes = new ArrayList<>(
        Lists.transform(inputRel.getRowType().getFieldList(), input -> input.getType()));
    RexCall firstStructCall = (RexCall) arrayCall.getOperands().get(0);
    returnTypes.addAll(Lists.transform(firstStructCall.getOperands(), input -> input.getType()));

    List<RexNode> newArrayCall =
        Lists.newArrayList(cluster.getRexBuilder().makeCall(arrayCall.op, newStructExprs));
    RexNode newInlineCall =
        cluster.getRexBuilder().makeCall(tfs.getRowType(), inlineCall.op, newArrayCall);

    final RelNode newTableFunctionScanNode = tfs.copy(tfs.getTraitSet(),
        tfs.getInputs(), newInlineCall, tfs.getElementType(), tfs.getRowType(),
        tfs.getColumnMappings());

    call.transformTo(newTableFunctionScanNode);
  }
}
