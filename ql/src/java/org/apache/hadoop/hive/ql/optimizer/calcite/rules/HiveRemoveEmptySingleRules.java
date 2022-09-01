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
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.rules.PruneEmptyRules;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveValues;

import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil.populateProjects;

/**
 * This class provides access to Calcite's {@link PruneEmptyRules}.
 * The instances of the rules use {@link org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelBuilder}.
 */
public class HiveRemoveEmptySingleRules extends PruneEmptyRules {

  public static final RelOptRule PROJECT_INSTANCE =
          PruneEmptyRules.RemoveEmptySingleRule.Config.EMPTY
                  .withDescription("HivePruneEmptyProject")
                  .as(PruneEmptyRules.RemoveEmptySingleRule.Config.class)
                  .withOperandFor(Project.class, project -> true)
                  .withRelBuilderFactory(HiveRelFactories.HIVE_BUILDER)
                  .toRule();

  public static final RelOptRule FILTER_INSTANCE =
          PruneEmptyRules.RemoveEmptySingleRule.Config.EMPTY
                  .withDescription("HivePruneEmptyFilter")
                  .as(PruneEmptyRules.RemoveEmptySingleRule.Config.class)
                  .withOperandFor(Filter.class, singleRel -> true)
                  .withRelBuilderFactory(HiveRelFactories.HIVE_BUILDER)
                  .toRule();

  public static final RelOptRule JOIN_LEFT_INSTANCE =
          JoinLeftEmptyRuleConfig.EMPTY
                  .withOperandSupplier(b0 ->
                          b0.operand(Join.class).inputs(
                                  b1 -> b1.operand(Values.class)
                                          .predicate(Values::isEmpty).noInputs(),
                                  b2 -> b2.operand(RelNode.class).anyInputs()))
                  .withDescription("HivePruneEmptyJoin(left)")
                  .as(JoinLeftEmptyRuleConfig.class)
                  .withRelBuilderFactory(HiveRelFactories.HIVE_BUILDER)
                  .toRule();

  /**
   * Improved version of Calcite's {@link PruneEmptyRules.JoinLeftEmptyRuleConfig}.
   * In case of right outer join if the left branch is empty the join operator can be removed
   * and take the right branch only.
   *
   * select * from (select * from emp where 1=0) right join dept
   * ->
   * select null as emp.col0 ... null as emp.coln, dept.* from dept
   */
  public interface JoinLeftEmptyRuleConfig extends PruneEmptyRule.Config {
    @Override default PruneEmptyRule toRule() {
      return new PruneEmptyRule(this) {
        @Override public void onMatch(RelOptRuleCall call) {
          Join join = call.rel(0);
          HiveValues empty = call.rel(1);
          RelNode right = call.rel(2);
          RexBuilder rexBuilder = call.builder().getRexBuilder();
          if (join.getJoinType().generatesNullsOnLeft()) {
            // "select * from emp right join dept" is not necessarily empty if
            // emp is empty
            List<RexNode> projects = new ArrayList<>(
                    empty.getRowType().getFieldCount() + right.getRowType().getFieldCount());
            List<String> columnNames = new ArrayList<>(
                    empty.getRowType().getFieldCount() + right.getRowType().getFieldCount());
            // left
            addNullLiterals(rexBuilder, empty, projects, columnNames);
            // right
            populateProjects(rexBuilder, right.getRowType(), projects, columnNames);

            RelNode project = call.builder().push(right).project(projects, columnNames).build();
            call.transformTo(project);
            return;
          }
          call.transformTo(call.builder().push(join).empty().build());
        }
      };
    }
  }

  private static void addNullLiterals(
          RexBuilder rexBuilder, HiveValues empty, List<RexNode> projectFields, List<String> newColumnNames) {
    for (int i = 0; i < empty.getRowType().getFieldList().size(); ++i) {
      RelDataTypeField relDataTypeField = empty.getRowType().getFieldList().get(i);
      RexNode nullLiteral = rexBuilder.makeNullLiteral(relDataTypeField.getType());
      projectFields.add(nullLiteral);
      newColumnNames.add(empty.getRowType().getFieldList().get(i).getName());
    }
  }

  public static final RelOptRule JOIN_RIGHT_INSTANCE =
          JoinRightEmptyRuleConfig.EMPTY
                  .withOperandSupplier(b0 ->
                          b0.operand(Join.class).inputs(
                                  b1 -> b1.operand(RelNode.class).anyInputs(),
                                  b2 -> b2.operand(Values.class).predicate(Values::isEmpty)
                                          .noInputs()))
                  .withDescription("HivePruneEmptyJoin(right)")
                  .as(JoinRightEmptyRuleConfig.class)
                  .withRelBuilderFactory(HiveRelFactories.HIVE_BUILDER)
                  .toRule();

  /**
   * Improved version of Calcite's {@link PruneEmptyRules.JoinRightEmptyRuleConfig}.
   * In case of left outer join if the right branch is empty the join operator can be removed
   * and take the left branch only.
   *
   * select * from emp right join (select * from dept where 1=0)
   * ->
   * select emp.*, null as dept.col0 ... null as dept.coln from emp
   */
  public interface JoinRightEmptyRuleConfig extends PruneEmptyRule.Config {
    @Override default PruneEmptyRule toRule() {
      return new PruneEmptyRule(this) {
        @Override public void onMatch(RelOptRuleCall call) {
          Join join = call.rel(0);
          RelNode left = call.rel(1);
          HiveValues empty = call.rel(2);
          RexBuilder rexBuilder = call.builder().getRexBuilder();
          if (join.getJoinType().generatesNullsOnRight()) {
            // "select * from emp left join dept" is not necessarily empty if
            // dept is empty
            List<RexNode> projects = new ArrayList<>(
                    left.getRowType().getFieldCount() + empty.getRowType().getFieldCount());
            List<String> columnNames = new ArrayList<>(
                    left.getRowType().getFieldCount() + empty.getRowType().getFieldCount());
            // left
            populateProjects(rexBuilder, left.getRowType(), projects, columnNames);
            // right
            addNullLiterals(rexBuilder, empty, projects, columnNames);

            RelNode project = call.builder().push(left).project(projects, columnNames).build();
            call.transformTo(project);
            return;
          }
          if (join.getJoinType() == JoinRelType.ANTI) {
            // In case of anti join: Join(X, Empty, ANTI) becomes X
            call.transformTo(join.getLeft());
            return;
          }
          call.transformTo(call.builder().push(join).empty().build());
        }
      };
    }
  }

  public static final RelOptRule SORT_INSTANCE =
          PruneEmptyRules.RemoveEmptySingleRule.Config.EMPTY
                  .withDescription("HivePruneEmptySort")
                  .as(PruneEmptyRules.RemoveEmptySingleRule.Config.class)
                  .withOperandFor(Sort.class, singleRel -> true)
                  .withRelBuilderFactory(HiveRelFactories.HIVE_BUILDER)
                  .toRule();

  public static final RelOptRule SORT_FETCH_ZERO_INSTANCE =
          PruneEmptyRules.SortFetchZeroRuleConfig.EMPTY
                  .withOperandSupplier(b ->
                          b.operand(Sort.class).anyInputs())
                  .withDescription("HivePruneSortLimit0")
                  .as(PruneEmptyRules.SortFetchZeroRuleConfig.class)
                  .withRelBuilderFactory(HiveRelFactories.HIVE_BUILDER)
                  .toRule();

  public static final RelOptRule AGGREGATE_INSTANCE =
          PruneEmptyRules.RemoveEmptySingleRule.Config.EMPTY
                  .withDescription("HivePruneEmptyAggregate")
                  .as(PruneEmptyRules.RemoveEmptySingleRule.Config.class)
                  .withOperandFor(Aggregate.class, Aggregate::isNotGrandTotal)
                  .withRelBuilderFactory(HiveRelFactories.HIVE_BUILDER)
                  .toRule();

  public static final RelOptRule UNION_INSTANCE =
          HiveUnionEmptyPruneRuleConfig.EMPTY
                  .withOperandSupplier(b0 ->
                          b0.operand(Union.class).unorderedInputs(b1 ->
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
