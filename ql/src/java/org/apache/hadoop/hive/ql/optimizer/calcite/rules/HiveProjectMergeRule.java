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

import java.util.Set;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;

/**
 * ProjectMergeRule merges a {@link org.apache.calcite.rel.core.Project} into
 * another {@link org.apache.calcite.rel.core.Project},
 * provided the projects aren't projecting identical sets of input references.
 */
public class HiveProjectMergeRule extends ProjectMergeRule {

  public static final HiveProjectMergeRule INSTANCE =
          new HiveProjectMergeRule(true, HiveRelFactories.HIVE_BUILDER);

  public static final HiveProjectMergeRule INSTANCE_NO_FORCE =
          new HiveProjectMergeRule(false, HiveRelFactories.HIVE_BUILDER);


  private HiveProjectMergeRule(boolean force, RelBuilderFactory relBuilderFactory) {
    super(force, relBuilderFactory);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    // Currently we do not support merging windowing functions with other
    // windowing functions i.e. embedding windowing functions within each
    // other
    final Project topProject = call.rel(0);
    final Project bottomProject = call.rel(1);
    for (RexNode expr : topProject.getChildExps()) {
      if (expr instanceof RexOver) {
        Set<Integer> positions = HiveCalciteUtil.getInputRefs(expr);
        for (int pos : positions) {
          if (bottomProject.getChildExps().get(pos) instanceof RexOver) {
            return false;
          }
        }
      }
    }
    return super.matches(call);
  }

  public void onMatch(RelOptRuleCall call) {
    final Project topProject = call.rel(0);
    final Project bottomProject = call.rel(1);

    // If top project does not reference any column at the bottom project,
    // we can just remove botton project
    final ImmutableBitSet topRefs =
        RelOptUtil.InputFinder.bits(topProject.getChildExps(), null);
    if (topRefs.isEmpty()) {
      RelBuilder relBuilder = call.builder();
      relBuilder.push(bottomProject.getInput());
      relBuilder.project(topProject.getChildExps());
      call.transformTo(relBuilder.build());
      return;
    }
    super.onMatch(call);
  }

}
