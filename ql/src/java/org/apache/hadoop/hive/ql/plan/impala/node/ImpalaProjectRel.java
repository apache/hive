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

import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;

import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;
import org.apache.hadoop.hive.ql.plan.impala.ImpalaPlannerContext;
import org.apache.hadoop.hive.ql.plan.impala.rex.ImpalaRexVisitor;
import org.apache.hadoop.hive.ql.plan.impala.rex.ReferrableNode;
import org.apache.impala.analysis.Expr;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.planner.PlanNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Impala project rel.  There is no actual physical project node on
 * the Impala side.  However, this class is useful because it keeps
 * the abstraction layer and it allows us to attach these projections to
 * the RelNode below as an output expression. For instance, if we had an
 * aggregation in our query like select sum(c1 + c2) from tbl, the projection
 * will handle the "c1 + c2" part. This will be part of the output expression
 * of the table scan node.
 */

public class ImpalaProjectRel extends ImpalaPlanRel {
  private final static Logger LOG = LoggerFactory.getLogger(ImpalaProjectRel.class);

  private final HiveProject hiveProject;

  public ImpalaProjectRel(HiveProject project) {
    super(project.getCluster(), project.getTraitSet(), project.getInputs(), project.getRowType());
    this.hiveProject = project;
  }

  @Override
  public PlanNode getPlanNode(ImpalaPlannerContext ctx) throws ImpalaException, HiveException, MetaException {
    PlanNode retNode = getImpalaRelInput(0).getPlanNode(ctx);
    Preconditions.checkState(this.outputExprs == null);
    this.outputExprs = createProjectExprs(ctx);
    return retNode;
  }

  private ImmutableMap<Integer, Expr> createProjectExprs(ImpalaPlannerContext ctx) {
    Map<Integer, Expr> projectExprs = Maps.newLinkedHashMap();
    ImpalaRexVisitor visitor = new ImpalaRexVisitor(ctx.getRootAnalyzer(), getImpalaRelInput(0));
    int index = 0;
    for (RexNode rexNode : hiveProject.getProjects()) {
      projectExprs.put(index++, rexNode.accept(visitor));
    }
    return ImmutableMap.copyOf(projectExprs);
  }
}
