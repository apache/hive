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
package org.apache.hadoop.hive.ql.optimizer.calcite.rules.views;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import java.util.Collection;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.calcite.plan.RelOptAbstractTable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelShuttleImpl;
import org.apache.hadoop.hive.ql.optimizer.calcite.TraitsUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveRelNode;

/**
 * This class contains logic that is useful to trigger additional materialized
 * view rewritings.
 *
 * In particular, in Hive we do not combine the MV rewriting rules with other
 * rules that may generate additional rewritings in the planner, e.g., join
 * reordering rules. In fact, the rewriting has some built-in logic to add
 * compensation joins on top of the MV scan which make adding these rules
 * unnecessary. This leads to faster planning, however it can also lead to some
 * missing rewriting opportunities if we are not careful. For instance, the
 * rewriting algorithm will bail out if an input of a join contains operators
 * that are not supported. Consider the following example where Union is not
 * supported by the rewriting algorithm:
 * MV:
 *      Join
 *     /    \
 *    A      B
 *
 * Query:
 *         Join
 *        /    \
 *     Join     B
 *    /    \
 * Union    A
 *   |
 *   S
 *
 * The rewriting can only be triggered at the root of the plan since that is
 * the operator where A and B are visible. However, after checking the
 * operators in the plan, the rewriting bails out since Union is not supported.
 *
 * This class contains boxing/unboxing logic that aims at fixing this. The
 * boxing logic will do a traversal of the query plan and introduce Box
 * operators when it detects an unsupported operator. For the former query,
 * this will be the rewritten plan:
 * Query:
 *         Join
 *        /    \
 *     Join     B
 *    /    \
 * Box$0    A
 *
 * Box extends TableScan, and thus, MV rewriting will proceed as expected. In
 * addition, the Box node keeps a internal pointer to the former subplan that
 * it replaced. During MV rewriting, we include the unboxing rule in the
 * planner, which will transform the Box node into the original subplan. The
 * Box node has an infinite cost: Though it helps the rewriting to be
 * triggered, it will never be part of the final plan, i.e., the original
 * subplan (possibly with other MV rewritings) will be chosen.
 */
public class HiveMaterializedViewBoxing {

  /**
   * Create Box operators in plan where necessary.
   */
  public static RelNode boxPlan(RelNode plan) {
    return plan.accept(new BoxingRelShuttle());
  }

  /**
   * Rule that replaces Box operators by the plan they hold.
   */
  public static final HiveMaterializedViewUnboxingRule INSTANCE_UNBOXING =
      new HiveMaterializedViewUnboxingRule();

  /* Counter for unique identifiers for Box operator. */
  private static final AtomicInteger UNIQUE_IDENTIFIER = new AtomicInteger(0);


  private static final class BoxingRelShuttle extends HiveRelShuttleImpl {

    @Override
    public RelNode visit(HiveJoin join) {
      HiveJoin newJoin = (HiveJoin) visitChildren(join);
      if (newJoin.getJoinType() != JoinRelType.INNER && !newJoin.isSemiJoin()) {
        // Nothing to do
        return newJoin;
      }
      RelMetadataQuery mq = newJoin.getCluster().getMetadataQuery();
      boolean leftValid = isValidRelNodePlan(newJoin.getLeft(), mq);
      boolean rightValid = isValidRelNodePlan(newJoin.getRight(), mq);
      if (leftValid == rightValid) {
        // Both are valid or invalid, nothing to do here
        return newJoin;
      }
      RelNode leftInput = newJoin.getLeft();
      RelNode rightInput = newJoin.getRight();
      if (!leftValid) {
        leftInput = createBoxOperator(newJoin.getCluster(), leftInput);
      } else {
        rightInput = createBoxOperator(newJoin.getCluster(), rightInput);
      }
      return newJoin.copy(
          newJoin.getTraitSet(), ImmutableList.of(leftInput, rightInput));
    }

    private static RelNode createBoxOperator(RelOptCluster cluster, RelNode eqRelNode) {
      String name = ".$box" + UNIQUE_IDENTIFIER.getAndIncrement();
      return new Box(cluster, name, eqRelNode);
    }

    private static boolean isValidRelNodePlan(RelNode rel, RelMetadataQuery mq) {
      final Multimap<Class<? extends RelNode>, RelNode> m =
          mq.getNodeTypes(rel);
      for (Entry<Class<? extends RelNode>, Collection<RelNode>> e : m.asMap().entrySet()) {
        Class<? extends RelNode> c = e.getKey();
        if (!TableScan.class.isAssignableFrom(c)
            && !Project.class.isAssignableFrom(c)
            && !Filter.class.isAssignableFrom(c)
            && !Join.class.isAssignableFrom(c)) {
          // Skip it
          return false;
        }
        if (Join.class.isAssignableFrom(c)) {
          for (RelNode n : e.getValue()) {
            final Join join = (Join) n;
            if (join.getJoinType() != JoinRelType.INNER && !join.isSemiJoin()) {
              // Skip it
              return false;
            }
          }
        }
      }
      return true;
    }

  }

  protected static final class Box extends TableScan implements HiveRelNode {
    private final RelNode eqRelNode;

    private Box(RelOptCluster cluster, String name, RelNode eqRelNode) {
      super(cluster, TraitsUtil.getDefaultTraitSet(cluster),
          new BoxRelOptTable(null, name, eqRelNode.getRowType()));
      this.eqRelNode = eqRelNode;
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
      return planner.getCostFactory().makeInfiniteCost();
    }

    private static final class BoxRelOptTable extends RelOptAbstractTable {
      private BoxRelOptTable(RelOptSchema schema, String name, RelDataType rowType) {
        super(schema, name, rowType);
      }
    }
  }

  private static final class HiveMaterializedViewUnboxingRule extends RelOptRule {

    private HiveMaterializedViewUnboxingRule() {
      super(operand(Box.class, any()),
          HiveRelFactories.HIVE_BUILDER, "HiveMaterializedViewUnboxingRule");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      Box box = call.rel(0);
      call.transformTo(box.eqRelNode);
    }
  }
}
