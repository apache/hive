/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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

import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.util.Util;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotNull;
import org.apache.hive.common.util.AnnotationUtils;

import com.google.common.collect.ImmutableList;

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
  private final RelFactories.FilterFactory filterFactory;

  /** The singleton. */
  public static final HiveJoinPushTransitivePredicatesRule INSTANCE =
      new HiveJoinPushTransitivePredicatesRule(Join.class,
          RelFactories.DEFAULT_FILTER_FACTORY);

  public HiveJoinPushTransitivePredicatesRule(Class<? extends Join> clazz,
      RelFactories.FilterFactory filterFactory) {
    super(operand(clazz, operand(RelNode.class, any()),
        operand(RelNode.class, any())));
    this.filterFactory = filterFactory;
  }

  @Override public void onMatch(RelOptRuleCall call) {
    Join join = call.rel(0);
    RelOptPredicateList preds = RelMetadataQuery.getPulledUpPredicates(join);

    RexBuilder rB = join.getCluster().getRexBuilder();
    RelNode lChild = call.rel(1);
    RelNode rChild = call.rel(2);

    List<RexNode> leftPreds = getValidPreds(preds.leftInferredPredicates, lChild.getRowType().getFieldList());
    List<RexNode> rightPreds = getValidPreds(preds.rightInferredPredicates, rChild.getRowType().getFieldList());

    if (leftPreds.isEmpty() && rightPreds.isEmpty()) {
      return;
    }

    if (leftPreds.size() > 0) {
      RelNode curr = lChild;
      lChild = filterFactory.createFilter(lChild, RexUtil.composeConjunction(rB, leftPreds, false));
      call.getPlanner().onCopy(curr, lChild);
    }

    if (rightPreds.size() > 0) {
      RelNode curr = rChild;
      rChild = filterFactory.createFilter(rChild, RexUtil.composeConjunction(rB, rightPreds, false));
      call.getPlanner().onCopy(curr, rChild);
    }

    RelNode newRel = join.copy(join.getTraitSet(), join.getCondition(),
        lChild, rChild, join.getJoinType(), join.isSemiJoinDone());
    call.getPlanner().onCopy(join, newRel);

    call.transformTo(newRel);
  }

  private ImmutableList<RexNode> getValidPreds (List<RexNode> rexs, List<RelDataTypeField> types) {
    InputRefValidator validator = new InputRefValidator(types);
    List<RexNode> valids = new ArrayList<RexNode>(rexs.size());
    for (RexNode rex : rexs) {
      try {
        rex.accept(validator);
        valids.add(rex);
      } catch (Util.FoundOne e) {
        Util.swallow(e, null);
      }
    }
    return ImmutableList.copyOf(valids);
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

