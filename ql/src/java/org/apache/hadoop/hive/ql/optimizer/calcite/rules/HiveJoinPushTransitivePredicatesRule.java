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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.RelFactories.FilterFactory;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.util.Util;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSemiJoin;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotNull;
import org.apache.hive.common.util.AnnotationUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

/**
 * Planner rule that infers predicates from on a
 * {@link org.apache.calcite.rel.core.Join} and creates
 * {@link org.apache.calcite.rel.core.Filter}s if those predicates can be pushed
 * to its inputs.
 *
 * <p>Uses {@link org.apache.calcite.rel.metadata.RelMdPredicates} to infer
 * the predicates,
 * returns them in a {@link org.apache.calcite.plan.RelOptPredicateList}
 * and applies them appropriately.
 */
public class HiveJoinPushTransitivePredicatesRule extends RelOptRule {

  public static final HiveJoinPushTransitivePredicatesRule INSTANCE_JOIN =
          new HiveJoinPushTransitivePredicatesRule(HiveJoin.class, HiveRelFactories.HIVE_FILTER_FACTORY);

  public static final HiveJoinPushTransitivePredicatesRule INSTANCE_SEMIJOIN =
          new HiveJoinPushTransitivePredicatesRule(HiveSemiJoin.class, HiveRelFactories.HIVE_FILTER_FACTORY);

  private final FilterFactory filterFactory;

  public HiveJoinPushTransitivePredicatesRule(Class<? extends Join> clazz,
      FilterFactory filterFactory) {
    super(operand(clazz, any()));
    this.filterFactory = filterFactory;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    Join join = call.rel(0);

    RelOptPredicateList preds = RelMetadataQuery.instance().getPulledUpPredicates(join);

    HiveRulesRegistry registry = call.getPlanner().getContext().unwrap(HiveRulesRegistry.class);
    assert registry != null;
    RexBuilder rB = join.getCluster().getRexBuilder();
    RelNode lChild = join.getLeft();
    RelNode rChild = join.getRight();

    Set<String> leftPushedPredicates = Sets.newHashSet(registry.getPushedPredicates(join, 0));
    List<RexNode> leftPreds = getValidPreds(join.getCluster(), lChild,
            leftPushedPredicates, preds.leftInferredPredicates, lChild.getRowType());
    Set<String> rightPushedPredicates = Sets.newHashSet(registry.getPushedPredicates(join, 1));
    List<RexNode> rightPreds = getValidPreds(join.getCluster(), rChild,
            rightPushedPredicates, preds.rightInferredPredicates, rChild.getRowType());

    RexNode newLeftPredicate = RexUtil.composeConjunction(rB, leftPreds, false);
    RexNode newRightPredicate = RexUtil.composeConjunction(rB, rightPreds, false);
    if (newLeftPredicate.isAlwaysTrue() && newRightPredicate.isAlwaysTrue()) {
      return;
    }

    if (!newLeftPredicate.isAlwaysTrue()) {
      RelNode curr = lChild;
      lChild = filterFactory.createFilter(lChild, newLeftPredicate);
      call.getPlanner().onCopy(curr, lChild);
    }

    if (!newRightPredicate.isAlwaysTrue()) {
      RelNode curr = rChild;
      rChild = filterFactory.createFilter(rChild, newRightPredicate);
      call.getPlanner().onCopy(curr, rChild);
    }

    RelNode newRel = join.copy(join.getTraitSet(), join.getCondition(),
        lChild, rChild, join.getJoinType(), join.isSemiJoinDone());
    call.getPlanner().onCopy(join, newRel);

    // Register information about pushed predicates
    registry.getPushedPredicates(newRel, 0).addAll(leftPushedPredicates);
    registry.getPushedPredicates(newRel, 1).addAll(rightPushedPredicates);

    call.transformTo(newRel);
  }

  private ImmutableList<RexNode> getValidPreds(RelOptCluster cluster, RelNode child,
      Set<String> predicatesToExclude, List<RexNode> rexs, RelDataType rType) {
    InputRefValidator validator = new InputRefValidator(rType.getFieldList());
    List<RexNode> valids = new ArrayList<RexNode>(rexs.size());
    for (RexNode rex : rexs) {
      try {
        rex.accept(validator);
        valids.add(rex);
      } catch (Util.FoundOne e) {
        Util.swallow(e, null);
      }
    }

    // We need to filter i) those that have been pushed already as stored in the join,
    // and ii) those that were already in the subtree rooted at child
    ImmutableList<RexNode> toPush = HiveCalciteUtil.getPredsNotPushedAlready(predicatesToExclude,
            child, valids);
    return toPush;
  }

  private RexNode getTypeSafePred(RelOptCluster cluster, RexNode rex, RelDataType rType) {
    RexNode typeSafeRex = rex;
    if ((typeSafeRex instanceof RexCall) && HiveCalciteUtil.isComparisonOp((RexCall) typeSafeRex)) {
      RexBuilder rb = cluster.getRexBuilder();
      List<RexNode> fixedPredElems = new ArrayList<RexNode>();
      RelDataType commonType = cluster.getTypeFactory().leastRestrictive(
          RexUtil.types(((RexCall) rex).getOperands()));
      for (RexNode rn : ((RexCall) rex).getOperands()) {
        fixedPredElems.add(rb.ensureType(commonType, rn, true));
      }

      typeSafeRex = rb.makeCall(((RexCall) typeSafeRex).getOperator(), fixedPredElems);
    }

    return typeSafeRex;
  }

  private static class InputRefValidator  extends RexVisitorImpl<Void> {

    private final List<RelDataTypeField> types;
    protected InputRefValidator(List<RelDataTypeField> types) {
      super(true);
      this.types = types;
    }

    @Override
    public Void visitCall(RexCall call) {

      if(AnnotationUtils.getAnnotation(GenericUDFOPNotNull.class, Description.class).name().equals(call.getOperator().getName())) {
        if(call.getOperands().get(0) instanceof RexInputRef &&
            !types.get(((RexInputRef)call.getOperands().get(0)).getIndex()).getType().isNullable()) {
          // No need to add not null filter for a constant.
          throw new Util.FoundOne(call);
        }
      }
      return super.visitCall(call);
    }
  }
}

