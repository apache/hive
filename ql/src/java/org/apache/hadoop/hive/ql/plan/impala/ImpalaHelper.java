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

package org.apache.hadoop.hive.ql.plan.impala;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveImpalaRules;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.ImpalaAggCastRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.ImpalaRexCastRule;
import org.apache.hadoop.hive.ql.plan.impala.ImpalaCompiledPlan;
import org.apache.hadoop.hive.ql.plan.impala.node.ImpalaPlanRel;
import org.apache.impala.catalog.BuiltinsDb;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.planner.PlanNode;
import org.apache.impala.thrift.TExecRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ImpalaHelper {

  private static final Logger LOG = LoggerFactory.getLogger(ImpalaHelper.class);

  static {
    // ensure that the instance is created with the "true" parameter.
    // If we don't call it here, it could be called from within impala-frontend with
    // the "false" parameter.
    BuiltinsDb.getInstance(true);
  }

  public HepProgram getHepProgram(Hive db) {
    List<RelOptRule> impalaRules = Lists.newArrayList();
    HepProgramBuilder programBuilder = new HepProgramBuilder();
    programBuilder.addMatchOrder(HepMatchOrder.DEPTH_FIRST);
    programBuilder.addRuleInstance(ImpalaAggCastRule.INSTANCE);
    programBuilder.addRuleInstance(ImpalaRexCastRule.FILTER_INSTANCE);
    programBuilder.addRuleInstance(ImpalaRexCastRule.JOIN_INSTANCE);
    programBuilder.addRuleInstance(ImpalaRexCastRule.PROJECT_INSTANCE);
    programBuilder.addRuleInstance(
        new HiveImpalaRules.ImpalaFilterScanRule(HiveRelFactories.HIVE_BUILDER, db));
    programBuilder.addRuleInstance(
        new HiveImpalaRules.ImpalaFilterAggRule(HiveRelFactories.HIVE_BUILDER));
    programBuilder.addRuleInstance(
        new HiveImpalaRules.ImpalaScanRule(HiveRelFactories.HIVE_BUILDER, db));
    programBuilder.addRuleInstance(
        new HiveImpalaRules.ImpalaProjectRule(HiveRelFactories.HIVE_BUILDER));
    programBuilder.addRuleInstance(
        new HiveImpalaRules.ImpalaSortLimitRule(HiveRelFactories.HIVE_BUILDER));
    programBuilder.addRuleInstance(
        new HiveImpalaRules.ImpalaAggRule(HiveRelFactories.HIVE_BUILDER));
    programBuilder.addRuleInstance(
        new HiveImpalaRules.ImpalaJoinRule(HiveRelFactories.HIVE_BUILDER));
    programBuilder.addRuleInstance(
        new HiveImpalaRules.ImpalaSemiJoinRule(HiveRelFactories.HIVE_BUILDER));
    return programBuilder.build();
  }

  public ImpalaCompiledPlan compilePlan(RelNode rootRelNode, String db, String userName)
      throws HiveException {
    try {
      Preconditions.checkState(rootRelNode instanceof ImpalaPlanRel);
      ImpalaPlanRel impalaRelNode = (ImpalaPlanRel) rootRelNode;
      ImpalaPlanner impalaPlanner = new ImpalaPlanner(db, userName);
      ImpalaPlannerContext planCtx = impalaPlanner.getPlannerContext();
      PlanNode rootImpalaNode = impalaRelNode.getRootPlanNode(planCtx);
      TExecRequest execRequest = impalaPlanner.createExecRequest(rootImpalaNode);
      LOG.debug("Impala request is {}", execRequest);
      return new ImpalaCompiledPlan(execRequest);
    } catch (ImpalaException e) {
      throw new HiveException("Encountered Impala exception generating an Impala plan: ", e);
    } catch (MetaException e) {
      throw new HiveException("Encountered Meta exception generating an Impala plan: ", e);
    }
  }
}
