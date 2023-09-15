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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableFunctionScan;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

/**
 * This rule optimizes the inline udtf in a HiveTableFunctionScan when it
 * has an array of structures. The RelNode for a HiveTableFunctionScan places
 * the input references as the first elements in the return type followed by
 * the udtf return value which represents the items in the generated table. Take the
 * case where the base (input) table has col1, and the inline function is represented by:
 * inline(array( struct1(col2, col3), struct2(col2, col3), struct3(col2, col3), etc...)),
 * ...and the return value for the table scan node is (col1, col2, col3). In this case,
 * the same col1 value is joined with the structures within the inline array for the
 * col2 and col3 values.
 *
 * The optimization is to put the "col1" value within the inline array, resulting in
 * in the new structure:
 * inline(array(struct1(col1, col2, col3), struct2(col1, col2, col3), ...)
 * By doing this, we avoid creating a lateral view join operator and a lateral view forward
 * operator at runtime.
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

    if (!(tableFunctionScanRel.getCall() instanceof RexCall)) {
      return false;
    }

    RexCall udtfCall = (RexCall) tableFunctionScanRel.getCall();
    if (udtfCall.getOperator() != SqlStdOperatorTable.LATERAL) {
      return false;
    }

    RexCall inlineCall = (RexCall) udtfCall.getOperands().get(0);
    if (!FunctionRegistry.INLINE_FUNC_NAME.equalsIgnoreCase(inlineCall.getOperator().getName())) {
      return false;
    }

    Preconditions.checkState(!inlineCall.getOperands().isEmpty());
    RexNode operand = inlineCall.getOperands().get(0);
    if (!(operand instanceof RexCall)) {
      return false;
    }
    RexCall firstOperand = (RexCall) operand;
    if (!FunctionRegistry.ARRAY_FUNC_NAME.equalsIgnoreCase(firstOperand.getOperator().getName())) {
      return false;
    }
    Preconditions.checkState(!firstOperand.getOperands().isEmpty());
    int numStructParams = firstOperand.getOperands().get(0).getType().getFieldCount();

    if (tableFunctionScanRel.getRowType().getFieldCount() == numStructParams) {
      return false;
    }

    return true;
  }

  public void onMatch(RelOptRuleCall call) {
    final HiveTableFunctionScan tfs = call.rel(0);
    RelNode inputRel = tfs.getInput(0);
    RexCall lateralCall = (RexCall) tfs.getCall();
    RexCall inlineCall = (RexCall) lateralCall.getOperands().get(0);
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
        Lists.transform(inputRel.getRowType().getFieldList(), RelDataTypeField::getType));
    RexCall firstStructCall = (RexCall) arrayCall.getOperands().get(0);
    returnTypes.addAll(Lists.transform(firstStructCall.getOperands(), RexNode::getType));

    List<RexNode> newArrayCall = Collections.singletonList(
        cluster.getRexBuilder().makeCall(arrayCall.op, newStructExprs));
    RexNode newInlineCall =
        cluster.getRexBuilder().makeCall(tfs.getRowType(), inlineCall.op, newArrayCall);

    // Use empty listfor columnMappings. The return row type of the RelNode now comprises of
    // all the fields within the UDTF, so there is no mapping from the output fields
    // directly to the input fields anymore.
    final RelNode newTableFunctionScanNode = tfs.copy(tfs.getTraitSet(),
        tfs.getInputs(), newInlineCall, tfs.getElementType(), tfs.getRowType(),
        Collections.emptySet());

    call.transformTo(newTableFunctionScanNode);
  }
}
