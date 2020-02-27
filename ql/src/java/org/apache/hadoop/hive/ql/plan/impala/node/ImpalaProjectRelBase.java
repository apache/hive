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
import com.google.common.collect.ImmutableList;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rex.RexNode;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;
import org.apache.hadoop.hive.ql.plan.impala.ImpalaPlannerContext;
import org.apache.hadoop.hive.ql.plan.impala.rex.ImpalaRexVisitor;
import org.apache.impala.analysis.Expr;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.planner.PlanNode;

import java.util.Map;

/**
 * Impala Project relnode base. This serves as a shared base class for
 * Impala Project relnodes.
 */

abstract public class ImpalaProjectRelBase extends ImpalaPlanRel {

  protected final HiveProject hiveProject;

  public ImpalaProjectRelBase(HiveProject project) {
    super(project.getCluster(), project.getTraitSet(), project.getInputs(), project.getRowType());
    this.hiveProject = project;
  }

  /**
   * Translate the RexNode expressions in the Project to Impala Exprs.
   */
  protected ImmutableMap<Integer, Expr> createProjectExprs(ImpalaPlannerContext ctx) {
    Map<Integer, Expr> projectExprs = Maps.newLinkedHashMap();
    ImpalaRexVisitor visitor = new ImpalaRexVisitor(ctx.getRootAnalyzer(), ImmutableList.of(getImpalaRelInput(0)));
    int index = 0;
    for (RexNode rexNode : hiveProject.getProjects()) {
      projectExprs.put(index++, rexNode.accept(visitor));
    }
    return ImmutableMap.copyOf(projectExprs);
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return hiveProject.explainTerms(pw);
  }
}
