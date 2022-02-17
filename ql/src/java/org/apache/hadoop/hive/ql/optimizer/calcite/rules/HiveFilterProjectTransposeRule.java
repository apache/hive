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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexSimplify;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.Util;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelOptUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelOptUtil.RewritablePKFKJoinInfo;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;

public class HiveFilterProjectTransposeRule extends FilterProjectTransposeRule {

  public static final HiveFilterProjectTransposeRule DETERMINISTIC_WINDOWING_ON_NON_FILTERING_JOIN =
      new HiveFilterProjectTransposeRule(
          operand(Filter.class, operand(Project.class, operand(Join.class, any()))),
          HiveRelFactories.HIVE_BUILDER, true, true);

  public static final HiveFilterProjectTransposeRule DETERMINISTIC_WINDOWING =
          new HiveFilterProjectTransposeRule(Filter.class, HiveProject.class,
                  HiveRelFactories.HIVE_BUILDER, true, true);

  public static final HiveFilterProjectTransposeRule DETERMINISTIC_ON_NON_FILTERING_JOIN =
      new HiveFilterProjectTransposeRule(
          operand(Filter.class, operand(Project.class, operand(Join.class, any()))),
          HiveRelFactories.HIVE_BUILDER, true, false);

  public static final HiveFilterProjectTransposeRule DETERMINISTIC =
          new HiveFilterProjectTransposeRule(Filter.class, HiveProject.class,
                  HiveRelFactories.HIVE_BUILDER, true, false);

  public static final HiveFilterProjectTransposeRule INSTANCE =
          new HiveFilterProjectTransposeRule(Filter.class, HiveProject.class,
                  HiveRelFactories.HIVE_BUILDER, false, false);

  private final boolean onlyDeterministic;

  private final boolean pushThroughWindowing;

  private HiveFilterProjectTransposeRule(Class<? extends Filter> filterClass,
      Class<? extends Project> projectClass, RelBuilderFactory relBuilderFactory,
      boolean onlyDeterministic,boolean pushThroughWindowing) {
    super(filterClass, projectClass, false, false, relBuilderFactory);
    this.onlyDeterministic = onlyDeterministic;
    this.pushThroughWindowing = pushThroughWindowing;
  }

  private HiveFilterProjectTransposeRule(RelOptRuleOperand operand,
      RelBuilderFactory relBuilderFactory,
      boolean onlyDeterministic, boolean pushThroughWindowing) {
    super(operand, false, false, relBuilderFactory);
    this.onlyDeterministic = onlyDeterministic;
    this.pushThroughWindowing = pushThroughWindowing;
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    final Filter filterRel = call.rel(0);

    // The condition fetched here can reference a udf that is not deterministic, but defined
    // as part of the select list when a view is in play.  But the condition after the pushdown
    // will resolve to using the udf from select list.  The check here for deterministic filters
    // should be based on the resolved expression.  Refer to test case cbo_ppd_non_deterministic.q.
    RexNode condition = RelOptUtil.pushPastProject(filterRel.getCondition(), call.rel(1));

    if (this.onlyDeterministic && !HiveCalciteUtil.isDeterministic(condition)) {
      return false;
    }

    if (call.rels.length > 2) {
      final Join joinRel = call.rel(2);
      RewritablePKFKJoinInfo joinInfo = HiveRelOptUtil.isRewritablePKFKJoin(
          joinRel, joinRel.getLeft(), joinRel.getRight(), call.getMetadataQuery());
      if (!joinInfo.rewritable) {
        return false;
      }
    }

    return super.matches(call);
  }

  public void onMatch(RelOptRuleCall call) {
    final Filter filter = call.rel(0);
    final Project origproject = call.rel(1);
    RexNode filterCondToPushBelowProj = filter.getCondition();
    RexNode unPushedFilCondAboveProj = null;

    if (RexUtil.containsCorrelation(filterCondToPushBelowProj)) {
      // If there is a correlation condition anywhere in the filter, don't
      // push this filter past project since in some cases it can prevent a
      // Correlate from being de-correlated.
      return;
    }

    if (RexOver.containsOver(origproject.getProjects(), null)) {
      RexNode origFilterCond = filterCondToPushBelowProj;
      filterCondToPushBelowProj = null;
      if (pushThroughWindowing) {
        Set<Integer> commonPartitionKeys = getCommonPartitionCols(origproject.getProjects());
        List<RexNode> newPartKeyFilConds = new ArrayList<>();
        List<RexNode> unpushedFilConds = new ArrayList<>();

        // TODO:
        // 1) Handle compound partition keys (partition by k1+k2)
        // 2) When multiple window clauses are present in same select Even if
        // Predicate can not pushed past all of them, we might still able to
        // push
        // it below some of them.
        // Ex: select * from (select key, value, avg(c_int) over (partition by
        // key), sum(c_float) over(partition by value) from t1)t1 where value <
        // 10
        // --> select * from (select key, value, avg(c_int) over (partition by
        // key) from (select key, value, sum(c_float) over(partition by value)
        // from t1 where value < 10)t1)t2
        if (!commonPartitionKeys.isEmpty()) {
          for (RexNode ce : RelOptUtil.conjunctions(origFilterCond)) {
            RexNode newCondition = RelOptUtil.pushPastProject(ce, origproject);
            if (HiveCalciteUtil.isDeterministicFuncWithSingleInputRef(newCondition,
                commonPartitionKeys)) {
              newPartKeyFilConds.add(ce);
            } else {
              unpushedFilConds.add(ce);
            }
          }

          if (!newPartKeyFilConds.isEmpty()) {
            filterCondToPushBelowProj = RexUtil.composeConjunction(filter.getCluster().getRexBuilder(),
                    newPartKeyFilConds, true);
          }
          if (!unpushedFilConds.isEmpty()) {
            unPushedFilCondAboveProj = RexUtil.composeConjunction(filter.getCluster().getRexBuilder(),
                    unpushedFilConds, true);
          }
        }
      }
    }

    if (filterCondToPushBelowProj != null
        && !isRedundantIsNotNull(origproject, filterCondToPushBelowProj)) {

      RelNode newProjRel = getNewProject(filterCondToPushBelowProj, unPushedFilCondAboveProj, origproject, filter.getCluster()
          .getTypeFactory(), call.builder());

      call.transformTo(newProjRel);
    }
  }

  private static RelNode getNewProject(RexNode filterCondToPushBelowProj, RexNode unPushedFilCondAboveProj, Project oldProj,
      RelDataTypeFactory typeFactory, RelBuilder relBuilder) {

    // convert the filter to one that references the child of the project.
    RexNode newPushedCondition = RelOptUtil.pushPastProject(filterCondToPushBelowProj, oldProj);

    // Remove cast of BOOLEAN NOT NULL to BOOLEAN or vice versa. Filter accepts
    // nullable and not-nullable conditions, but a CAST might get in the way of
    // other rewrites.
    if (RexUtil.isNullabilityCast(typeFactory, newPushedCondition)) {
      newPushedCondition = ((RexCall) newPushedCondition).getOperands().get(0);
    }

    RelNode newPushedFilterRel = relBuilder.push(oldProj.getInput()).filter(newPushedCondition).build();

    RelNode newProjRel = relBuilder.push(newPushedFilterRel)
        .project(oldProj.getProjects(), oldProj.getRowType().getFieldNames()).build();

    if (unPushedFilCondAboveProj != null) {
      // Remove cast of BOOLEAN NOT NULL to BOOLEAN or vice versa. Filter accepts
      // nullable and not-nullable conditions, but a CAST might get in the way of
      // other rewrites.
      if (RexUtil.isNullabilityCast(typeFactory, newPushedCondition)) {
        unPushedFilCondAboveProj = ((RexCall) unPushedFilCondAboveProj).getOperands().get(0);
      }
      newProjRel = relBuilder.push(newProjRel).filter(unPushedFilCondAboveProj).build();
    }

    return newProjRel;
  }

  private static Set<Integer> getCommonPartitionCols(List<RexNode> projections) {
    RexOver overClause;
    boolean firstOverClause = true;
    Set<Integer> commonPartitionKeys = new HashSet<>();

    for (RexNode expr : projections) {
      if (expr instanceof RexOver) {
        overClause = (RexOver) expr;

        if (firstOverClause) {
          firstOverClause = false;
          commonPartitionKeys.addAll(getPartitionCols(overClause.getWindow().partitionKeys));
        } else {
          commonPartitionKeys.retainAll(getPartitionCols(overClause.getWindow().partitionKeys));
        }
      }
    }

    return commonPartitionKeys;
  }

  private static List<Integer> getPartitionCols(List<RexNode> partitionKeys) {
    List<Integer> pCols = new ArrayList<>();
    for (RexNode key : partitionKeys) {
      if (key instanceof RexInputRef) {
        pCols.add(((RexInputRef) key).getIndex());
      }
    }
    return pCols;
  }

  // in cases when a filter using IS NOT NULL is applied to an input $i down the subtree,
  // creating another filter IS NOT NULL(FUNC_CALL+($i)) is of a doubtful usefulness and
  // might lead to infinite loops in predicate pull-up and push-down, like in HIVE-25275
  private static boolean isRedundantIsNotNull(Project project, RexNode newCondition) {
    if (!newCondition.isA(SqlKind.IS_NOT_NULL)) {
      return false;
    }

    // we handle expressions over a single input ref
    if (HiveCalciteUtil.getInputRefs(newCondition).size() != 1) {
      return false;
    }

    RedundancyChecker redundancyChecker = new RedundancyChecker(newCondition);
    redundancyChecker.go(project);
    return redundancyChecker.isRedundant;
  }

  private static class RedundancyChecker extends RelVisitor {
    private boolean isRedundant;

    final RexNode newCondition;
    final Map<RelNode, RexNode> filter2newConditionMap = new HashMap<>();

    protected RedundancyChecker(RexNode newCondition) {
      this.newCondition = newCondition;
    }

    @Override
    public void visit(RelNode node, int ordinal, RelNode parent) {
      RexNode filterCondition =
          filter2newConditionMap.isEmpty() ? newCondition : filter2newConditionMap.get(node);

      if (isRedundant) {
        return;
      }

      if (node instanceof HepRelVertex) {
        // unwrap HepRelVertex and replace it with the associated RelNode
        RelNode currNode = ((HepRelVertex) node).getCurrentRel();
        filter2newConditionMap.put(currNode, filter2newConditionMap.remove(node));
        visit(currNode, ordinal, parent);
      } else {
        if (node instanceof Filter) {
          check((Filter) node);
        } else if (node instanceof Project) {
          filterCondition = RelOptUtil.pushPastProject(filterCondition, (Project) node);
        } else {
          // we do not support other operators for now
          return;
        }
        RexNode finalFilterCondition = filterCondition;
        node.getInputs().forEach(i -> filter2newConditionMap.put(i, finalFilterCondition));

        super.visit(node, ordinal, parent);
      }
    }

    private void check(Filter filter) {
      final RelOptCluster cluster = filter.getCluster();
      final RexBuilder rexBuilder = cluster.getRexBuilder();

      final RexSimplify simplify = new RexSimplify(rexBuilder, RelOptPredicateList.EMPTY,
          Util.first(cluster.getPlanner().getExecutor(), RexUtil.EXECUTOR));
      final RexNode newCondition = simplify.simplify(filter2newConditionMap.get(filter));

      // if the condition simplifies to a literal, bail out
      if (RexUtil.isLiteral(newCondition, true)) {
        return;
      }

      // IS NOT NULL($i) is always safe, it is either needed, or existing already
      if (newCondition instanceof RexCall
          && ((RexCall) newCondition).getOperands().get(0).isA(SqlKind.INPUT_REF)) {
        return;
      }

      final RexNode filterCondition = simplify.simplify(filter.getCondition());

      final Set<Integer> inputRefs = HiveCalciteUtil.getInputRefs(newCondition);
      // if the new IS NOT NULL has no input ref, there is no redundancy here, bail out
      if (inputRefs.isEmpty()) {
        return;
      }

      final RexInputRef rexInputRef =
          rexBuilder.makeInputRef(filter.getInput(), inputRefs.iterator().next());
      final RexNode baseCondition = rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, rexInputRef);

      // if they are identical, pushing it down is harmless and can remove upper (redundant) filters
      if (newCondition.toString().equals(filterCondition.toString())) {
        return;
      }

      isRedundant = !isPredicateIncluded(newCondition, filterCondition)
          && isPredicateIncluded(baseCondition, filterCondition);
    }
  }

  private static boolean isPredicateIncluded(RexNode includedPred, RexNode containingPred) {
    SubsumptionChecker inclusionChecker = new SubsumptionChecker(includedPred);
    return containingPred.accept(inclusionChecker);
  }

  private static class SubsumptionChecker extends RexVisitorImpl<Boolean> {
    private final String includedPredDigest;

    protected SubsumptionChecker(RexNode includedPred) {
      super(true);
      this.includedPredDigest = includedPred.toString();
    }

    public Boolean visitInputRef(RexInputRef inputRef) {
      return false;
    }

    public Boolean visitLiteral(RexLiteral literal) {
      return false;
    }

    public Boolean visitCorrelVariable(RexCorrelVariable correlVariable) {
      return false;
    }

    public Boolean visitCall(RexCall call) {
      if (call.isA(SqlKind.AND)) {
        return call.getOperands().stream().anyMatch(o -> o.accept(this));
      } else if (call.isA(SqlKind.OR)) {
        return call.getOperands().stream().allMatch(o -> o.accept(this));
      }
      return includedPredDigest.equals(call.toString());
    }

    public Boolean visitDynamicParam(RexDynamicParam dynamicParam) {
      return false;
    }

    public Boolean visitRangeRef(RexRangeRef rangeRef) {
      return false;
    }

    public Boolean visitFieldAccess(RexFieldAccess fieldAccess) {
      return false;
    }
  }
}
