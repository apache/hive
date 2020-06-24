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

package org.apache.hadoop.hive.ql.plan.impala.node;

import com.google.common.base.Preconditions;

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableFunctionScan;
import org.apache.hadoop.hive.ql.plan.impala.node.ImpalaUnionRel;
import org.apache.hadoop.hive.ql.plan.impala.ImpalaPlannerContext;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.planner.PlanNode;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Impala table function scan relnode. An Impala RelNode representation of a
 * Calcite Table Function Scan Node.
 *
 */
public class ImpalaTableFunctionScanRel extends ImpalaPlanRel {

  PlanNode planNode;
  HiveTableFunctionScan hiveUDTFS;
  public ImpalaTableFunctionScanRel(HiveTableFunctionScan udtfs) {
    super(udtfs.getCluster(), udtfs.getTraitSet(), udtfs.getInputs(), udtfs.getRowType());
    hiveUDTFS = udtfs;
    if (getDataRow(hiveUDTFS, 0) == null) {
      throw new RuntimeException("Unsuported UDTFS "+hiveUDTFS.toString());
    }
  }

  // Get the nth row from the inline(array(row(expr,...), ...)) UDTFS
  // Return null for any other function structure or rowNum out of bound
  private static RexCall getDataRow(HiveTableFunctionScan scan, int rowNum) {
    for(RexNode expr: scan.getChildExps()) {
      if (expr instanceof RexCall) {
        RexCall inlineCall = (RexCall)expr;
        if (inlineCall.getOperator().getName().equalsIgnoreCase("inline") &&
           inlineCall.getOperands().size() == 1) {
          if (inlineCall.getOperands().get(0) instanceof RexCall) {
            RexCall arrayCall = (RexCall)(inlineCall.getOperands().get(0));
            if (arrayCall.getOperator().getName().equalsIgnoreCase("ARRAY") &&
                rowNum < arrayCall.getOperands().size()) {
              if (arrayCall.getOperands().get(rowNum) instanceof RexCall) {

                RexCall rowCall = (RexCall)(arrayCall.getOperands().get(rowNum));
                if (rowCall.getOperator().getName().equalsIgnoreCase("ROW")) {
                  return rowCall;
                }
              }
            }
          }
        }
      }
    }
    return null;
  }

  @Override
  public PlanNode getPlanNode(ImpalaPlannerContext ctx) throws ImpalaException, HiveException, MetaException {
    if (planNode != null) {
      return planNode;
    }
    // Similar to ImpalaProjectPassthroughRel, bypass this UDTFS node and return
    // the union from below.
    ImpalaUnionRel inputRel = (ImpalaUnionRel)getImpalaRelInput(0);
    int rowNum = 0;
    RexCall dataRow;
    // Extract data rows from the UDTF and push them into the union which
    // has special handling for constant rows.
    while((dataRow = getDataRow(hiveUDTFS, rowNum++)) != null) {
      inputRel.addConstExprList(dataRow.getOperands());
      if (rowNum == 1) {
        inputRel.setConstRowType(dataRow.getType());
      }
    }
    // bypass this node, just get from the input.
    planNode = inputRel.getPlanNode(ctx);

    // get the output exprs for this node that are needed by the parent node.
    Preconditions.checkState(this.outputExprs == null);
    this.outputExprs = ImmutableMap.copyOf(inputRel.getOutputExprsMap());
    return planNode;
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    return mq.getNonCumulativeCost(hiveUDTFS);
  }

  @Override
  public double estimateRowCount(RelMetadataQuery mq) {
    return mq.getRowCount(hiveUDTFS);
  }

}
