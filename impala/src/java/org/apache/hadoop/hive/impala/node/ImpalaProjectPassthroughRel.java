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

package org.apache.hadoop.hive.impala.node;

import com.google.common.base.Preconditions;

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;
import org.apache.hadoop.hive.impala.plan.ImpalaPlannerContext;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.planner.PlanNode;

/**
 * Impala project passthrough relnode. An Impala RelNode representation of a
 * Calcite Project Node that is not mapped to a physical Impala Node for performance
 * reasons.
 *
 * For an expression such as SUM(c1 + c2), the Calcite plan will have a Project that
 * outputs the result of c1+c2 and the Aggregate above Project references it as SUM($0).
 *
 * When the actual tuple information is needed, the base "PlanNode" knows
 * to bypass this object and go to the Rel input of the project node.
 */
public class ImpalaProjectPassthroughRel extends ImpalaProjectRelBase {

  public ImpalaProjectPassthroughRel(HiveProject project) {
    super(project);
  }

  @Override
  public PlanNode getPlanNode(ImpalaPlannerContext ctx) throws ImpalaException, HiveException, MetaException {
    if (planNode != null) {
      return planNode;
    }
    // bypass this node, just get from the input.
    ImpalaPlanRel inputRel = getImpalaRelInput(0);
    planNode = inputRel.getPlanNode(ctx);

    // get the output exprs for this node that are needed by the parent node.
    Preconditions.checkState(this.outputExprs == null);
    this.outputExprs = createProjectExprs(ctx);
    return planNode;
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    return mq.getNonCumulativeCost(project);
  }

  @Override
  public double estimateRowCount(RelMetadataQuery mq) {
    return mq.getRowCount(project);
  }

}
