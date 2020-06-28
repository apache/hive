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

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveBetween;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveIn;
import org.apache.hadoop.hive.ql.parse.SemanticException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This rule attempts to identify and pull out invert from BETWEENs.
 * Since, Druid Rules have no way to handle invert due to mismatch in invert representation in
 * HiveBetween and calcite SqlBetweenOperator.
 */
public class HiveDruidPullInvertFromBetweenRule extends RelOptRule {

  protected static final Log LOG = LogFactory.getLog(HiveDruidPullInvertFromBetweenRule.class);

  public static final HiveDruidPullInvertFromBetweenRule INSTANCE =
      new HiveDruidPullInvertFromBetweenRule();

  private HiveDruidPullInvertFromBetweenRule() {
    super(operand(Filter.class, any()));
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final Filter filter = call.rel(0);
    final RexBuilder rexBuilder = filter.getCluster().getRexBuilder();
    final RexNode condition = RexUtil.pullFactors(rexBuilder, filter.getCondition());

    RexPullInvertFromBetween t = new RexPullInvertFromBetween(rexBuilder);
    RexNode newCondition = t.apply(condition);

    // If we could not transform anything, we bail out
    if (newCondition.toString().equals(condition.toString())) {
      return;
    }
    RelNode newNode = filter.copy(filter.getTraitSet(), filter.getInput(), newCondition);

    call.transformTo(newNode);
  }

  protected static class RexPullInvertFromBetween extends RexShuttle {
    private final RexBuilder rexBuilder;

    RexPullInvertFromBetween(RexBuilder rexBuilder) {
      this.rexBuilder = rexBuilder;
    }

    @Override
    public RexNode visitCall(RexCall inputCall) {
      RexNode node = super.visitCall(inputCall);
      if (node instanceof RexCall && node.getKind() == SqlKind.BETWEEN) {
        RexCall call = (RexCall) node;
        Boolean isInverted = call.getOperands().get(0).isAlwaysTrue();
        if (isInverted) {
          return rexBuilder.makeCall(SqlStdOperatorTable.NOT, rexBuilder.makeCall(
              HiveBetween.INSTANCE, rexBuilder.makeLiteral(false),
              call.getOperands().get(1), call.getOperands().get(2), call.getOperands().get(3)));
        }
      }
      return node;
    }
  }

}
