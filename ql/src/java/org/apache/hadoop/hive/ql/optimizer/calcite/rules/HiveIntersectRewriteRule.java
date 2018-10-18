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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.hadoop.hive.ql.optimizer.calcite.CalciteSemanticException;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.TraitsUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveAggregate;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveIntersect;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveRelNode;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableFunctionScan;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveUnion;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.SqlFunctionConverter;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.TypeConverter;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.Lists;

/**
 * Planner rule that rewrite
 * {@link org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveIntersect}
 * Rewrite: (GB-Union All-GB)-GB-UDTF (on all attributes) 

    Example: R1 Intersect All R2
    R3 = GB(R1 on all attributes + count() as c) union all GB(R2 on all attributes + count() as c)
    R4 = GB(R3 on all attributes + count(c) as cnt  + min(c) as m)
    R5 = Fil ( cnt == #branch )
    
    If it is intersect all then
    R6 = UDTF (R5) which will explode the tuples based on min(c).
    R7 = Proj(R6 on all attributes)
    Else
    R6 = Proj(R5 on all attributes)
else
 */
public class HiveIntersectRewriteRule extends RelOptRule {

  public static final HiveIntersectRewriteRule INSTANCE = new HiveIntersectRewriteRule();

  protected static final Logger LOG = LoggerFactory.getLogger(HiveIntersectRewriteRule.class);
  

  // ~ Constructors -----------------------------------------------------------

  private HiveIntersectRewriteRule() {
    super(operand(HiveIntersect.class, any()));
  }

  // ~ Methods ----------------------------------------------------------------

  public void onMatch(RelOptRuleCall call) {
    final HiveIntersect hiveIntersect = call.rel(0);

    final RelOptCluster cluster = hiveIntersect.getCluster();
    final RexBuilder rexBuilder = cluster.getRexBuilder();
    int numOfBranch = hiveIntersect.getInputs().size();
    Builder<RelNode> bldr = new ImmutableList.Builder<RelNode>();

    // 1st level GB: create a GB (col0, col1, count(1) as c) for each branch
    for (int index = 0; index < numOfBranch; index++) {
      RelNode input = hiveIntersect.getInputs().get(index);
      final List<RexNode> gbChildProjLst = Lists.newArrayList();
      final List<Integer> groupSetPositions = Lists.newArrayList();
      for (int cInd = 0; cInd < input.getRowType().getFieldList().size(); cInd++) {
        gbChildProjLst.add(rexBuilder.makeInputRef(input, cInd));
        groupSetPositions.add(cInd);
      }
      gbChildProjLst.add(rexBuilder.makeBigintLiteral(new BigDecimal(1)));

      // create the project before GB because we need a new project with extra column '1'.
      RelNode gbInputRel = null;
      try {
        gbInputRel = HiveProject.create(input, gbChildProjLst, null);
      } catch (CalciteSemanticException e) {
        LOG.debug(e.toString());
        throw new RuntimeException(e);
      }

      // groupSetPosition includes all the positions
      final ImmutableBitSet groupSet = ImmutableBitSet.of(groupSetPositions);

      List<AggregateCall> aggregateCalls = Lists.newArrayList();
      RelDataType aggFnRetType = TypeConverter.convert(TypeInfoFactory.longTypeInfo,
          cluster.getTypeFactory());
      
      // count(1), 1's position is input.getRowType().getFieldList().size()
      AggregateCall aggregateCall = HiveCalciteUtil.createSingleArgAggCall("count", cluster,
          TypeInfoFactory.longTypeInfo, input.getRowType().getFieldList().size(), aggFnRetType);
      aggregateCalls.add(aggregateCall);
      
      HiveRelNode aggregateRel = new HiveAggregate(cluster,
          cluster.traitSetOf(HiveRelNode.CONVENTION), gbInputRel, groupSet, null,
          aggregateCalls);
      bldr.add(aggregateRel);
    }

    // create a union above all the branches
    HiveRelNode union = new HiveUnion(cluster, TraitsUtil.getDefaultTraitSet(cluster), bldr.build());

    // 2nd level GB: create a GB (col0, col1, count(c)) for each branch
    final List<Integer> groupSetPositions = Lists.newArrayList();
    // the index of c
    int cInd = union.getRowType().getFieldList().size() - 1;
    for (int index = 0; index < union.getRowType().getFieldList().size(); index++) {
      if (index != cInd) {
        groupSetPositions.add(index);
      }
    }

    List<AggregateCall> aggregateCalls = Lists.newArrayList();
    RelDataType aggFnRetType = TypeConverter.convert(TypeInfoFactory.longTypeInfo,
        cluster.getTypeFactory());
    
    AggregateCall aggregateCall = HiveCalciteUtil.createSingleArgAggCall("count", cluster,
        TypeInfoFactory.longTypeInfo, cInd, aggFnRetType);
    aggregateCalls.add(aggregateCall);
    if (hiveIntersect.all) {
      aggregateCall = HiveCalciteUtil.createSingleArgAggCall("min", cluster,
          TypeInfoFactory.longTypeInfo, cInd, aggFnRetType);
      aggregateCalls.add(aggregateCall);
    }
    
    final ImmutableBitSet groupSet = ImmutableBitSet.of(groupSetPositions);
    HiveRelNode aggregateRel = new HiveAggregate(cluster,
        cluster.traitSetOf(HiveRelNode.CONVENTION), union, groupSet, null, aggregateCalls);

    // add a filter count(c) = #branches
    int countInd = cInd;
    List<RexNode> childRexNodeLst = new ArrayList<RexNode>();
    RexInputRef ref = rexBuilder.makeInputRef(aggregateRel, countInd);
    RexLiteral literal = rexBuilder.makeBigintLiteral(new BigDecimal(numOfBranch));
    childRexNodeLst.add(ref);
    childRexNodeLst.add(literal);
    ImmutableList.Builder<RelDataType> calciteArgTypesBldr = new ImmutableList.Builder<RelDataType>();
    calciteArgTypesBldr.add(TypeConverter.convert(TypeInfoFactory.longTypeInfo,
        cluster.getTypeFactory()));
    calciteArgTypesBldr.add(TypeConverter.convert(TypeInfoFactory.longTypeInfo,
        cluster.getTypeFactory()));
    RexNode factoredFilterExpr = null;
    try {
      factoredFilterExpr = rexBuilder
          .makeCall(
              SqlFunctionConverter.getCalciteFn("=", calciteArgTypesBldr.build(),
                  TypeConverter.convert(TypeInfoFactory.longTypeInfo, cluster.getTypeFactory()),
                  true, false), childRexNodeLst);
    } catch (CalciteSemanticException e) {
      LOG.debug(e.toString());
      throw new RuntimeException(e);
    }

    RelNode filterRel = new HiveFilter(cluster, cluster.traitSetOf(HiveRelNode.CONVENTION),
        aggregateRel, factoredFilterExpr);
    
    if (!hiveIntersect.all) {
      // the schema for intersect distinct is like this
      // R3 on all attributes + count(c) as cnt
      // finally add a project to project out the last column
      Set<Integer> projectOutColumnPositions = new HashSet<>();
      projectOutColumnPositions.add(filterRel.getRowType().getFieldList().size() - 1);
      try {
        call.transformTo(HiveCalciteUtil.createProjectWithoutColumn(filterRel,projectOutColumnPositions));
      } catch (CalciteSemanticException e) {
        LOG.debug(e.toString());
        throw new RuntimeException(e);
      }
    } else {
      // the schema for intersect all is like this
      // R3 + count(c) as cnt + min(c) as m
      // we create a input project for udtf whose schema is like this
      // min(c) as m + R3
      List<RexNode> originalInputRefs = Lists.transform(filterRel.getRowType().getFieldList(),
          new Function<RelDataTypeField, RexNode>() {
            @Override
            public RexNode apply(RelDataTypeField input) {
              return new RexInputRef(input.getIndex(), input.getType());
            }
          });

      List<RexNode> copyInputRefs = new ArrayList<>();
      copyInputRefs.add(originalInputRefs.get(originalInputRefs.size() - 1));
      for (int i = 0; i < originalInputRefs.size() - 2; i++) {
        copyInputRefs.add(originalInputRefs.get(i));
      }
      RelNode srcRel = null;
      try {
        srcRel = HiveProject.create(filterRel, copyInputRefs, null);
        HiveTableFunctionScan udtf = HiveCalciteUtil.createUDTFForSetOp(cluster, srcRel);
        // finally add a project to project out the 1st column
        Set<Integer> projectOutColumnPositions = new HashSet<>();
        projectOutColumnPositions.add(0);
        call.transformTo(HiveCalciteUtil
            .createProjectWithoutColumn(udtf, projectOutColumnPositions));
      } catch (SemanticException e) {
        LOG.debug(e.toString());
        throw new RuntimeException(e);
      }
    }
  }
}
