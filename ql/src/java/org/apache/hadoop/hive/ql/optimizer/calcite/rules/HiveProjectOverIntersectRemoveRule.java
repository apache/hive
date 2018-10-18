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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Intersect;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveIntersect;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;

import com.google.common.base.Predicate;

/**
 * HiveProjectOverIntersectRemoveRule removes a HiveProject over another
 * HiveIntersect, provided the projects aren't projecting identical sets of
 * input references.
 */
public class HiveProjectOverIntersectRemoveRule extends RelOptRule {

  public static final HiveProjectOverIntersectRemoveRule INSTANCE = new HiveProjectOverIntersectRemoveRule();

  // ~ Constructors -----------------------------------------------------------

  /** Creates a HiveProjectOverIntersectRemoveRule. */
  private HiveProjectOverIntersectRemoveRule() {
    super(operand(HiveProject.class, operand(HiveIntersect.class, any())));
  }

  // ~ Methods ----------------------------------------------------------------

  @Override
  public boolean matches(RelOptRuleCall call) {
    Project project = call.rel(0);
    Intersect intersect = call.rel(1);
    return isTrivial(project, intersect);
  }

  public void onMatch(RelOptRuleCall call) {
    call.transformTo(call.rel(1));
  }

  private static boolean isTrivial(Project project, Intersect intersect) {
    return RexUtil.isIdentity(project.getProjects(), intersect.getRowType());
  }

}

// End HiveProjectOverIntersectRemoveRule.java
