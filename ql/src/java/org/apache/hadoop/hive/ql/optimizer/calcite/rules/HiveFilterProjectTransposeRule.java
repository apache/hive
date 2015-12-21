/**
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

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories.FilterFactory;
import org.apache.calcite.rel.core.RelFactories.ProjectFactory;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rex.RexNode;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;

public class HiveFilterProjectTransposeRule extends FilterProjectTransposeRule {

  public static final HiveFilterProjectTransposeRule INSTANCE_DETERMINISTIC =
          new HiveFilterProjectTransposeRule(Filter.class, HiveRelFactories.HIVE_FILTER_FACTORY,
          HiveProject.class, HiveRelFactories.HIVE_PROJECT_FACTORY, true);

  public static final HiveFilterProjectTransposeRule INSTANCE =
          new HiveFilterProjectTransposeRule(Filter.class, HiveRelFactories.HIVE_FILTER_FACTORY,
          HiveProject.class, HiveRelFactories.HIVE_PROJECT_FACTORY, false);

  private final boolean onlyDeterministic;

  public HiveFilterProjectTransposeRule(Class<? extends Filter> filterClass,
      FilterFactory filterFactory, Class<? extends Project> projectClass,
      ProjectFactory projectFactory, boolean onlyDeterministic) {
    super(filterClass, filterFactory, projectClass, projectFactory);
    this.onlyDeterministic = onlyDeterministic;
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    final Filter filterRel = call.rel(0);
    RexNode condition = filterRel.getCondition();
    if (this.onlyDeterministic && !HiveCalciteUtil.isDeterministic(condition)) {
      return false;
    }

    return super.matches(call);
  }

}
