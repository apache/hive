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
package org.apache.hadoop.hive.ql.optimizer.calcite;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories.AggregateFactory;
import org.apache.calcite.rel.core.RelFactories.FilterFactory;
import org.apache.calcite.rel.core.RelFactories.JoinFactory;
import org.apache.calcite.rel.core.RelFactories.ProjectFactory;
import org.apache.calcite.rel.core.RelFactories.SemiJoinFactory;
import org.apache.calcite.rel.core.RelFactories.SetOpFactory;
import org.apache.calcite.rel.core.RelFactories.SortFactory;
import org.apache.calcite.rel.rel2sql.SqlImplementor;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveAggregate;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveExcept;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveIntersect;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSemiJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSortLimit;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveUnion;

import com.google.common.collect.ImmutableList;

public class HiveRelFactories {

  public static final ProjectFactory HIVE_PROJECT_FACTORY =
          new HiveProjectFactoryImpl();

  public static final FilterFactory HIVE_FILTER_FACTORY =
          new HiveFilterFactoryImpl();

  public static final JoinFactory HIVE_JOIN_FACTORY =
          new HiveJoinFactoryImpl();

  public static final SemiJoinFactory HIVE_SEMI_JOIN_FACTORY =
          new HiveSemiJoinFactoryImpl();

  public static final SortFactory HIVE_SORT_FACTORY =
          new HiveSortFactoryImpl();

  public static final AggregateFactory HIVE_AGGREGATE_FACTORY =
          new HiveAggregateFactoryImpl();

  public static final SetOpFactory HIVE_SET_OP_FACTORY =
          new HiveSetOpFactoryImpl();

  public static final RelBuilderFactory HIVE_BUILDER =
      HiveRelBuilder.proto(
          Contexts.of(HIVE_PROJECT_FACTORY,
              HIVE_FILTER_FACTORY,
              HIVE_JOIN_FACTORY,
              HIVE_SEMI_JOIN_FACTORY,
              HIVE_SORT_FACTORY,
              HIVE_AGGREGATE_FACTORY,
              HIVE_SET_OP_FACTORY));

  private HiveRelFactories() {
  }

  /**
   * Implementation of {@link ProjectFactory} that returns
   * {@link org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject}
   * .
   */
  private static class HiveProjectFactoryImpl implements ProjectFactory {
    @Override
    public RelNode createProject(RelNode child,
        List<? extends RexNode> childExprs, List<String> fieldNames) {
      RelOptCluster cluster = child.getCluster();
      RelDataType rowType = RexUtil.createStructType(
          cluster.getTypeFactory(), childExprs, fieldNames, SqlValidatorUtil.EXPR_SUGGESTER);
      RelTraitSet trait = TraitsUtil.getDefaultTraitSet(cluster, child.getTraitSet());
      RelNode project = HiveProject.create(cluster, child,
          childExprs, rowType, trait, Collections.<RelCollation> emptyList());

      return project;
    }
  }

  /**
   * Implementation of {@link FilterFactory} that returns
   * {@link org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter}
   * .
   */
  private static class HiveFilterFactoryImpl implements FilterFactory {
    @Override
    public RelNode createFilter(RelNode child, RexNode condition) {
      RelOptCluster cluster = child.getCluster();
      HiveFilter filter = new HiveFilter(cluster, TraitsUtil.getDefaultTraitSet(cluster), child, condition);
      return filter;
    }
  }

  private static class HiveJoinFactoryImpl implements JoinFactory {
    /**
     * Creates a join.
     *
     * @param left
     *          Left input
     * @param right
     *          Right input
     * @param condition
     *          Join condition
     * @param joinType
     *          Join type
     * @param variablesStopped
     *          Set of names of variables which are set by the LHS and used by
     *          the RHS and are not available to nodes above this JoinRel in the
     *          tree
     * @param semiJoinDone
     *          Whether this join has been translated to a semi-join
     */
    @Override
    public RelNode createJoin(RelNode left, RelNode right, RexNode condition, JoinRelType joinType,
        Set<String> variablesStopped, boolean semiJoinDone) {
      return HiveJoin.getJoin(left.getCluster(), left, right, condition, joinType);
    }

    @Override
    public RelNode createJoin(RelNode left, RelNode right, RexNode condition,
        Set<CorrelationId> variablesSet, JoinRelType joinType, boolean semiJoinDone) {
      // According to calcite, it is going to be removed before Calcite-2.0
      // TODO: to handle CorrelationId
      return HiveJoin.getJoin(left.getCluster(), left, right, condition, joinType);
    }
  }

  /**
   * Implementation of {@link SemiJoinFactory} that returns
   * {@link org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSemiJoin}
   * .
   */
  private static class HiveSemiJoinFactoryImpl implements SemiJoinFactory {
    @Override
    public RelNode createSemiJoin(RelNode left, RelNode right,
            RexNode condition) {
      final JoinInfo joinInfo = JoinInfo.of(left, right, condition);
      final RelOptCluster cluster = left.getCluster();
      return HiveSemiJoin.getSemiJoin(cluster, left.getTraitSet(), left, right, condition,
          joinInfo.leftKeys, joinInfo.rightKeys);
    }
  }

  private static class HiveSortFactoryImpl implements SortFactory {
    @Override
    public RelNode createSort(RelTraitSet traits, RelNode input, RelCollation collation,
        RexNode offset, RexNode fetch) {
      return createSort(input, collation, offset, fetch);
    }

    @Override
    public RelNode createSort(RelNode input, RelCollation collation, RexNode offset,
        RexNode fetch) {
      return HiveSortLimit.create(input, collation, offset, fetch);
    }
  }

  private static class HiveAggregateFactoryImpl implements AggregateFactory {
    @Override
    public RelNode createAggregate(RelNode child, boolean indicator,
            ImmutableBitSet groupSet, ImmutableList<ImmutableBitSet> groupSets,
            List<AggregateCall> aggCalls) {
        if (indicator) {
          throw new IllegalStateException("Hive does not support indicator columns but Calcite "
                  + "created an Aggregate operator containing them");
        }
        return new HiveAggregate(child.getCluster(), child.getTraitSet(), child,
                groupSet, groupSets, aggCalls);
    }
  }

  private static class HiveSetOpFactoryImpl implements SetOpFactory {
    @Override
    public RelNode createSetOp(SqlKind kind, List<RelNode> inputs, boolean all) {
      if (kind == SqlKind.UNION) {
        return new HiveUnion(inputs.get(0).getCluster(), inputs.get(0).getTraitSet(), inputs);
      } else if (kind == SqlKind.INTERSECT) {
        return new HiveIntersect(inputs.get(0).getCluster(), inputs.get(0).getTraitSet(), inputs,
            all);
      } else if (kind == SqlKind.EXCEPT) {
        return new HiveExcept(inputs.get(0).getCluster(), inputs.get(0).getTraitSet(), inputs,
            all);
      } else {
        throw new IllegalStateException("Expected to get set operator of type Union, Intersect or Except(Minus). Found : "
            + kind);
      }
    }
  }

}
