/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.rules.PruneEmptyRules;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.hadoop.hive.ql.optimizer.calcite.Bug;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveAggregate;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveAntiJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSemiJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSortLimit;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveUnion;

import java.util.Collections;
import java.util.List;

import static com.google.common.collect.Iterables.concat;

/**
 * This class provides access to Calcite's {@link PruneEmptyRules}.
 * The instances of the rules use {@link org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelBuilder}.
 */
public class HiveRemoveEmptySingleRules extends PruneEmptyRules {

  public static final RelOptRule PROJECT_INSTANCE =
          RelRule.Config.EMPTY
                  .withDescription("HivePruneEmptyProject")
                  .as(PruneEmptyRules.RemoveEmptySingleRule.Config.class)
                  .withOperandFor(HiveProject.class, project -> true)
                  .withRelBuilderFactory(HiveRelFactories.HIVE_BUILDER)
                  .toRule();

  public static final RelOptRule FILTER_INSTANCE =
          RelRule.Config.EMPTY
                  .withDescription("HivePruneEmptyFilter")
                  .as(PruneEmptyRules.RemoveEmptySingleRule.Config.class)
                  .withOperandFor(HiveFilter.class, singleRel -> true)
                  .withRelBuilderFactory(HiveRelFactories.HIVE_BUILDER)
                  .toRule();

  public static final RelOptRule JOIN_LEFT_INSTANCE = getJoinLeftInstance(HiveJoin.class);
  public static final RelOptRule SEMI_JOIN_LEFT_INSTANCE = getJoinLeftInstance(HiveSemiJoin.class);

  private static <R extends RelNode> RelOptRule getJoinLeftInstance(Class<R> clazz) {
    return RelRule.Config.EMPTY
            .withOperandSupplier(b0 ->
                    b0.operand(clazz).inputs(
                            b1 -> b1.operand(Values.class)
                                    .predicate(Values::isEmpty).noInputs(),
                            b2 -> b2.operand(RelNode.class).anyInputs()))
            .withDescription("HivePruneEmptyJoin(left)")
            .as(JoinLeftEmptyRuleConfig.class)
            .withRelBuilderFactory(HiveRelFactories.HIVE_BUILDER)
            .toRule();
  }

  /**
   * Improved version of Calcite's {@link PruneEmptyRules.JoinLeftEmptyRuleConfig}.
   * In case of right outer join if the left branch is empty the join operator can be removed
   * and take the right branch only.
   *
   * select * from (select * from emp where 1=0) right join dept
   * to
   * select null as emp.col0 ... null as emp.coln, dept.* from dept
   */
  public interface JoinLeftEmptyRuleConfig extends PruneEmptyRule.Config {
    @Override default PruneEmptyRule toRule() {
      return new PruneEmptyRule(this) {
        @Override public void onMatch(RelOptRuleCall call) {
          if (Bug.CALCITE_5294_FIXED) {
            throw new IllegalStateException(
                    "Class JoinLeftEmptyRuleConfig is redundant after fix is merged into Calcite");
          }

          final Join join = call.rel(0);
          final RelNode right = call.rel(2);
          final RelBuilder relBuilder = call.builder();
          if (join.getJoinType().generatesNullsOnLeft()) {
            // If "emp" is empty, "select * from emp right join dept" will have
            // the same number of rows as "dept", and null values for the
            // columns from "emp". The left side of the join can be removed.
            call.transformTo(padWithNulls(relBuilder, right, join.getRowType(), true));
            return;
          }
          call.transformTo(relBuilder.push(join).empty().build());
        }
      };
    }
  }

  public static final RelOptRule JOIN_RIGHT_INSTANCE = getJoinRightInstance(HiveJoin.class);
  public static final RelOptRule ANTI_JOIN_RIGHT_INSTANCE = getJoinRightInstance(HiveAntiJoin.class);
  public static final RelOptRule SEMI_JOIN_RIGHT_INSTANCE = getJoinRightInstance(HiveSemiJoin.class);

  private static <R extends RelNode> RelOptRule getJoinRightInstance(Class<R> clazz) {
    return RelRule.Config.EMPTY
            .withOperandSupplier(b0 ->
                    b0.operand(clazz).inputs(
                            b1 -> b1.operand(RelNode.class).anyInputs(),
                            b2 -> b2.operand(Values.class).predicate(Values::isEmpty)
                                    .noInputs()))
            .withDescription("HivePruneEmptyJoin(right)")
            .as(JoinRightEmptyRuleConfig.class)
            .withRelBuilderFactory(HiveRelFactories.HIVE_BUILDER)
            .toRule();
  }

  /**
   * Improved version of Calcite's {@link PruneEmptyRules.JoinRightEmptyRuleConfig}.
   * In case of left outer join if the right branch is empty the join operator can be removed
   * and take the left branch only.
   *
   * select * from emp right join (select * from dept where 1=0)
   * to
   * select emp.*, null as dept.col0 ... null as dept.coln from emp
   */
  public interface JoinRightEmptyRuleConfig extends PruneEmptyRule.Config {
    @Override default PruneEmptyRule toRule() {
      return new PruneEmptyRule(this) {
        @Override public void onMatch(RelOptRuleCall call) {
          if (Bug.CALCITE_5294_FIXED) {
            throw new IllegalStateException(
                    "Class JoinRightEmptyRuleConfig is redundant after fix is merged into Calcite");
          }

          final Join join = call.rel(0);
          final RelNode left = call.rel(1);
          final RelBuilder relBuilder = call.builder();
          if (join.getJoinType().generatesNullsOnRight()) {
            // If "dept" is empty, "select * from emp left join dept" will have
            // the same number of rows as "emp", and null values for the
            // columns from "dept". The right side of the join can be removed.
            call.transformTo(padWithNulls(relBuilder, left, join.getRowType(), false));
            return;
          }
          if (join.getJoinType() == JoinRelType.ANTI) {
            // In case of anti join: Join(X, Empty, ANTI) becomes X
            call.transformTo(join.getLeft());
            return;
          }
          call.transformTo(relBuilder.push(join).empty().build());
        }
      };
    }
  }

  private static RelNode padWithNulls(RelBuilder builder, RelNode input, RelDataType resultType,
      boolean leftPadding) {
    int padding = resultType.getFieldCount() - input.getRowType().getFieldCount();
    List<RexNode> nullLiterals = Collections.nCopies(padding, builder.literal(null));
    builder.push(input);
    if (leftPadding) {
      builder.project(concat(nullLiterals, builder.fields()));
    } else {
      builder.project(concat(builder.fields(), nullLiterals));
    }
    return builder.convert(resultType, true).build();
  }

  public static final RelOptRule CORRELATE_RIGHT_INSTANCE = RelRule.Config.EMPTY
      .withOperandSupplier(b0 ->
          b0.operand(Correlate.class).inputs(
              b1 -> b1.operand(RelNode.class).anyInputs(),
              b2 -> b2.operand(Values.class).predicate(Values::isEmpty).noInputs()))
      .withDescription("PruneEmptyCorrelate(right)")
      .withRelBuilderFactory(HiveRelFactories.HIVE_BUILDER)
      .as(CorrelateRightEmptyRuleConfig.class)
      .toRule();
  public static final RelOptRule CORRELATE_LEFT_INSTANCE = RelRule.Config.EMPTY
      .withOperandSupplier(b0 ->
          b0.operand(Correlate.class).inputs(
              b1 -> b1.operand(Values.class).predicate(Values::isEmpty).noInputs(),
              b2 -> b2.operand(RelNode.class).anyInputs()))
      .withDescription("PruneEmptyCorrelate(left)")
      .withRelBuilderFactory(HiveRelFactories.HIVE_BUILDER)
      .as(CorrelateLeftEmptyRuleConfig.class)
      .toRule();

  /** Configuration for rule that prunes a correlate if left input is empty. */
  public interface CorrelateLeftEmptyRuleConfig extends PruneEmptyRule.Config {
    @Override
    default PruneEmptyRule toRule() {
      return new PruneEmptyRule(this) {
        @Override
        public void onMatch(RelOptRuleCall call) {
          if (Bug.CALCITE_5669_FIXED) {
            throw new IllegalStateException("Class is redundant after fix is merged into Calcite");
          }
          final Correlate corr = call.rel(0);
          call.transformTo(call.builder().push(corr).empty().build());
        }
      };
    }
  }

  /** Configuration for rule that prunes a correlate if right input is empty. */
  public interface CorrelateRightEmptyRuleConfig extends PruneEmptyRule.Config {
    @Override
    default PruneEmptyRule toRule() {
      return new PruneEmptyRule(this) {
        @Override
        public void onMatch(RelOptRuleCall call) {
          if (Bug.CALCITE_5669_FIXED) {
            throw new IllegalStateException("Class is redundant after fix is merged into Calcite");
          }
          final Correlate corr = call.rel(0);
          final RelNode left = call.rel(1);
          final RelBuilder b = call.builder();
          final RelNode newRel;
          switch (corr.getJoinType()) {
          case LEFT:
            newRel = padWithNulls(b, left, corr.getRowType(), false);
            break;
          case INNER:
          case SEMI:
            newRel = b.push(corr).empty().build();
            break;
          case ANTI:
            newRel = left;
            break;
          default:
            throw new IllegalStateException("Correlate does not support " + corr.getJoinType());
          }
          call.transformTo(newRel);
        }
      };
    }
  }

  public static final RelOptRule SORT_INSTANCE =
          RelRule.Config.EMPTY
                  .withDescription("HivePruneEmptySort")
                  .as(PruneEmptyRules.RemoveEmptySingleRule.Config.class)
                  .withOperandFor(HiveSortLimit.class, singleRel -> true)
                  .withRelBuilderFactory(HiveRelFactories.HIVE_BUILDER)
                  .toRule();

  public static final RelOptRule SORT_FETCH_ZERO_INSTANCE =
          RelRule.Config.EMPTY
                  .withOperandSupplier(b ->
                          b.operand(HiveSortLimit.class).anyInputs())
                  .withDescription("HivePruneSortLimit0")
                  .as(PruneEmptyRules.SortFetchZeroRuleConfig.class)
                  .withRelBuilderFactory(HiveRelFactories.HIVE_BUILDER)
                  .toRule();

  public static final RelOptRule AGGREGATE_INSTANCE =
          RelRule.Config.EMPTY
                  .withDescription("HivePruneEmptyAggregate")
                  .as(PruneEmptyRules.RemoveEmptySingleRule.Config.class)
                  .withOperandFor(HiveAggregate.class, Aggregate::isNotGrandTotal)
                  .withRelBuilderFactory(HiveRelFactories.HIVE_BUILDER)
                  .toRule();

  public static final RelOptRule UNION_INSTANCE =
          RelRule.Config.EMPTY
                  .withOperandSupplier(b0 ->
                          b0.operand(HiveUnion.class).unorderedInputs(b1 ->
                                  b1.operand(Values.class)
                                          .predicate(Values::isEmpty).noInputs()))
                  .withDescription("HivePruneEmptyUnionBranch")
                  .as(HiveUnionEmptyPruneRuleConfig.class)
                  .withRelBuilderFactory(HiveRelFactories.HIVE_BUILDER)
                  .toRule();

  /**
   * Copy of {@link PruneEmptyRules.UnionEmptyPruneRuleConfig} but this version expects {@link Union}.
   */
  public interface HiveUnionEmptyPruneRuleConfig extends PruneEmptyRules.PruneEmptyRule.Config {
    @Override default PruneEmptyRules.PruneEmptyRule toRule() {
      return new PruneEmptyRules.PruneEmptyRule(this) {
        @Override public void onMatch(RelOptRuleCall call) {
          if (Bug.CALCITE_5293_FIXED) {
            throw new IllegalStateException(
                    "Class HiveUnionEmptyPruneRuleConfig is redundant after fix is merged into Calcite");
          }

          final Union union = call.rel(0);
          final List<RelNode> inputs = union.getInputs();
          assert inputs != null;
          final RelBuilder builder = call.builder();
          int nonEmptyInputs = 0;
          for (RelNode input : inputs) {
            if (!isEmpty(input)) {
              builder.push(input);
              nonEmptyInputs++;
            }
          }
          assert nonEmptyInputs < inputs.size()
                  : "planner promised us at least one Empty child: "
                  + RelOptUtil.toString(union);
          if (nonEmptyInputs == 0) {
            builder.push(union).empty();
          } else {
            builder.union(union.all, nonEmptyInputs);
            builder.convert(union.getRowType(), true);
          }
          call.transformTo(builder.build());
        }
      };
    }
  }

  private static boolean isEmpty(RelNode node) {
    if (Bug.CALCITE_5293_FIXED) {
      throw new IllegalStateException(
              "Method HiveRemoveEmptySingleRules.isEmpty is redundant after fix is merged into Calcite");
    }

    if (node instanceof Values) {
      return ((Values) node).getTuples().isEmpty();
    }
    if (node instanceof HepRelVertex) {
      return isEmpty(((HepRelVertex) node).getCurrentRel());
    }
    // Note: relation input might be a RelSubset, so we just iterate over the relations
    // in order to check if the subset is equivalent to an empty relation.
    if (!(node instanceof RelSubset)) {
      return false;
    }
    RelSubset subset = (RelSubset) node;
    for (RelNode rel : subset.getRels()) {
      if (isEmpty(rel)) {
        return true;
      }
    }
    return false;
  }
}
