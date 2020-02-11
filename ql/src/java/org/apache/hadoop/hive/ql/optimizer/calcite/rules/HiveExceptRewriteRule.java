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
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveExcept;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
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
 * {@link org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveExcept}
 * Note, we only have 2 branches because of except's semantic.
 * R1 Except(all) R2
 * R1 introduce VCol ‘2’, R2 introduce VCol ‘1’
 * R3 = GB(R1 on all keys + VCol + count(VCol) as c) union all GB(R2 on all keys + VCol + count(VCol) as c)
 * R4 = GB(R3 on all keys + sum(c) as a + sum(VCol*c) as b) we
 * have m+n=a, 2m+n=b where m is the #row in R1 and n is the #row in R2 then
 * m=b-a, n=2a-b, m-n=2b-3a
 * if it is except (distinct)
 * then R5 = Fil (b-a&gt;0 &amp;&amp; 2a-b=0) R6 = select only keys from R5
 * else R5 = Fil (2b-3a&gt; 0) R6 = UDTF (R5) which will explode the tuples based on 2b-3a.
 * Note that NULLs are handled the same as other values. Please refer to the test cases.
 */
public class HiveExceptRewriteRule extends RelOptRule {

  public static final HiveExceptRewriteRule INSTANCE = new HiveExceptRewriteRule();

  protected static final Logger LOG = LoggerFactory.getLogger(HiveIntersectRewriteRule.class);


  // ~ Constructors -----------------------------------------------------------

  private HiveExceptRewriteRule() {
    super(operand(HiveExcept.class, any()));
  }

  // ~ Methods ----------------------------------------------------------------

  public void onMatch(RelOptRuleCall call) {
    final HiveExcept hiveExcept = call.rel(0);

    final RelOptCluster cluster = hiveExcept.getCluster();
    final RexBuilder rexBuilder = cluster.getRexBuilder();
    Builder<RelNode> bldr = new ImmutableList.Builder<RelNode>();

    // 1st level GB: create a GB(R1 on all keys + VCol + count() as c) for each
    // branch
    try {
      bldr.add(createFirstGB(hiveExcept.getInputs().get(0), true, cluster, rexBuilder));
      bldr.add(createFirstGB(hiveExcept.getInputs().get(1), false, cluster, rexBuilder));
    } catch (CalciteSemanticException e) {
      LOG.debug(e.toString());
      throw new RuntimeException(e);
    }

    // create a union above all the branches
    // the schema of union looks like this
    // all keys + VCol + c
    HiveRelNode union = new HiveUnion(cluster, TraitsUtil.getDefaultTraitSet(cluster), bldr.build());

    // 2nd level GB: create a GB (all keys + sum(c) as a + sum(VCol*c) as b) for
    // each branch
    final List<RexNode> gbChildProjLst = Lists.newArrayList();
    final List<Integer> groupSetPositions = Lists.newArrayList();
    int unionColumnSize = union.getRowType().getFieldList().size();
    for (int cInd = 0; cInd < unionColumnSize; cInd++) {
      gbChildProjLst.add(rexBuilder.makeInputRef(union, cInd));
      // the last 2 columns are VCol and c
      if (cInd < unionColumnSize - 2) {
        groupSetPositions.add(cInd);
      }
    }

    try {
      gbChildProjLst.add(multiply(rexBuilder.makeInputRef(union, unionColumnSize - 2),
          rexBuilder.makeInputRef(union, unionColumnSize - 1), cluster, rexBuilder));
    } catch (CalciteSemanticException e) {
      LOG.debug(e.toString());
      throw new RuntimeException(e);
    }

    RelNode gbInputRel = null;
    try {
      // Here we create a project for the following reasons:
      // (1) GBy only accepts arg as a position of the input, however, we need to sum on VCol*c
      // (2) This can better reuse the function createSingleArgAggCall.
      gbInputRel = HiveProject.create(union, gbChildProjLst, null);
    } catch (CalciteSemanticException e) {
      LOG.debug(e.toString());
      throw new RuntimeException(e);
    }

    // gbInputRel's schema is like this
    // all keys + VCol + c + VCol*c
    List<AggregateCall> aggregateCalls = Lists.newArrayList();
    RelDataType aggFnRetType = TypeConverter.convert(TypeInfoFactory.longTypeInfo,
        cluster.getTypeFactory());

    // sum(c)
    AggregateCall aggregateCall = HiveCalciteUtil.createSingleArgAggCall("sum", cluster,
        TypeInfoFactory.longTypeInfo, unionColumnSize - 1, aggFnRetType);
    aggregateCalls.add(aggregateCall);

    // sum(VCol*c)
    aggregateCall = HiveCalciteUtil.createSingleArgAggCall("sum", cluster,
        TypeInfoFactory.longTypeInfo, unionColumnSize, aggFnRetType);
    aggregateCalls.add(aggregateCall);

    final ImmutableBitSet groupSet = ImmutableBitSet.of(groupSetPositions);
    HiveRelNode aggregateRel = new HiveAggregate(cluster,
        cluster.traitSetOf(HiveRelNode.CONVENTION), gbInputRel, groupSet, null,
        aggregateCalls);

    // the schema after GB is like this
    // all keys + sum(c) as a + sum(VCol*c) as b
    // the column size is the same as unionColumnSize;
    // (1) for except distinct add a filter (b-a>0 && 2a-b=0)
    // i.e., a > 0 && 2a = b
    // then add the project
    // (2) for except all add a project to change it to
    // (2b-3a) + all keys
    // then add the UDTF

    if (!hiveExcept.all) {
      RelNode filterRel = null;
      try {
        filterRel = new HiveFilter(cluster, cluster.traitSetOf(HiveRelNode.CONVENTION),
            aggregateRel, makeFilterExprForExceptDistinct(aggregateRel, unionColumnSize, cluster,
                rexBuilder));
      } catch (CalciteSemanticException e) {
        LOG.debug(e.toString());
        throw new RuntimeException(e);
      }

      // finally add a project to project out the last 2 columns
      Set<Integer> projectOutColumnPositions = new HashSet<>();
      projectOutColumnPositions.add(filterRel.getRowType().getFieldList().size() - 2);
      projectOutColumnPositions.add(filterRel.getRowType().getFieldList().size() - 1);
      try {
        call.transformTo(HiveCalciteUtil.createProjectWithoutColumn(filterRel,
            projectOutColumnPositions));
      } catch (CalciteSemanticException e) {
        LOG.debug(e.toString());
        throw new RuntimeException(e);
      }
    } else {
      List<RexNode> originalInputRefs = Lists.transform(aggregateRel.getRowType().getFieldList(),
          new Function<RelDataTypeField, RexNode>() {
            @Override
            public RexNode apply(RelDataTypeField input) {
              return new RexInputRef(input.getIndex(), input.getType());
            }
          });

      List<RexNode> copyInputRefs = new ArrayList<>();
      try {
        copyInputRefs.add(makeExprForExceptAll(aggregateRel, unionColumnSize, cluster, rexBuilder));
      } catch (CalciteSemanticException e) {
        LOG.debug(e.toString());
        throw new RuntimeException(e);
      }
      for (int i = 0; i < originalInputRefs.size() - 2; i++) {
        copyInputRefs.add(originalInputRefs.get(i));
      }
      RelNode srcRel = null;
      try {
        srcRel = HiveProject.create(aggregateRel, copyInputRefs, null);
        HiveTableFunctionScan udtf = HiveCalciteUtil.createUDTFForSetOp(cluster, srcRel);
        // finally add a project to project out the 1st columns
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

  private RelNode createFirstGB(RelNode input, boolean left, RelOptCluster cluster,
      RexBuilder rexBuilder) throws CalciteSemanticException {
    final List<RexNode> gbChildProjLst = Lists.newArrayList();
    final List<Integer> groupSetPositions = Lists.newArrayList();
    for (int cInd = 0; cInd < input.getRowType().getFieldList().size(); cInd++) {
      gbChildProjLst.add(rexBuilder.makeInputRef(input, cInd));
      groupSetPositions.add(cInd);
    }
    if (left) {
      gbChildProjLst.add(rexBuilder.makeBigintLiteral(new BigDecimal(2)));
    } else {
      gbChildProjLst.add(rexBuilder.makeBigintLiteral(new BigDecimal(1)));
    }

    // also add the last VCol
    groupSetPositions.add(input.getRowType().getFieldList().size());

    // create the project before GB
    RelNode gbInputRel = HiveProject.create(input, gbChildProjLst, null);

    // groupSetPosition includes all the positions
    final ImmutableBitSet groupSet = ImmutableBitSet.of(groupSetPositions);

    List<AggregateCall> aggregateCalls = Lists.newArrayList();
    RelDataType aggFnRetType = TypeConverter.convert(TypeInfoFactory.longTypeInfo,
        cluster.getTypeFactory());

    AggregateCall aggregateCall = HiveCalciteUtil.createSingleArgAggCall("count", cluster,
        TypeInfoFactory.longTypeInfo, input.getRowType().getFieldList().size(), aggFnRetType);
    aggregateCalls.add(aggregateCall);
    return new HiveAggregate(cluster, cluster.traitSetOf(HiveRelNode.CONVENTION), gbInputRel,
        groupSet, null, aggregateCalls);
  }

  private RexNode multiply(RexNode r1, RexNode r2, RelOptCluster cluster, RexBuilder rexBuilder)
      throws CalciteSemanticException {
    List<RexNode> childRexNodeLst = new ArrayList<RexNode>();
    childRexNodeLst.add(r1);
    childRexNodeLst.add(r2);
    ImmutableList.Builder<RelDataType> calciteArgTypesBldr = new ImmutableList.Builder<RelDataType>();
    calciteArgTypesBldr.add(TypeConverter.convert(TypeInfoFactory.longTypeInfo,
        cluster.getTypeFactory()));
    calciteArgTypesBldr.add(TypeConverter.convert(TypeInfoFactory.longTypeInfo,
        cluster.getTypeFactory()));
    return rexBuilder.makeCall(
        SqlFunctionConverter.getCalciteFn("*", calciteArgTypesBldr.build(),
            TypeConverter.convert(TypeInfoFactory.longTypeInfo, cluster.getTypeFactory()), true, false),
        childRexNodeLst);
  }

  private RexNode makeFilterExprForExceptDistinct(HiveRelNode input, int columnSize,
      RelOptCluster cluster, RexBuilder rexBuilder) throws CalciteSemanticException {
    List<RexNode> childRexNodeLst = new ArrayList<RexNode>();
    RexInputRef a = rexBuilder.makeInputRef(input, columnSize - 2);
    RexLiteral zero = rexBuilder.makeBigintLiteral(new BigDecimal(0));
    childRexNodeLst.add(a);
    childRexNodeLst.add(zero);
    ImmutableList.Builder<RelDataType> calciteArgTypesBldr = new ImmutableList.Builder<RelDataType>();
    calciteArgTypesBldr.add(TypeConverter.convert(TypeInfoFactory.longTypeInfo,
        cluster.getTypeFactory()));
    calciteArgTypesBldr.add(TypeConverter.convert(TypeInfoFactory.longTypeInfo,
        cluster.getTypeFactory()));
    // a>0
    RexNode aMorethanZero = rexBuilder.makeCall(
        SqlFunctionConverter.getCalciteFn(">", calciteArgTypesBldr.build(),
            TypeConverter.convert(TypeInfoFactory.longTypeInfo, cluster.getTypeFactory()), false, false),
        childRexNodeLst);
    childRexNodeLst = new ArrayList<RexNode>();
    RexLiteral two = rexBuilder.makeBigintLiteral(new BigDecimal(2));
    childRexNodeLst.add(a);
    childRexNodeLst.add(two);
    // 2*a
    RexNode twoa = rexBuilder.makeCall(
        SqlFunctionConverter.getCalciteFn("*", calciteArgTypesBldr.build(),
            TypeConverter.convert(TypeInfoFactory.longTypeInfo, cluster.getTypeFactory()), false, false),
        childRexNodeLst);
    childRexNodeLst = new ArrayList<RexNode>();
    RexInputRef b = rexBuilder.makeInputRef(input, columnSize - 1);
    childRexNodeLst.add(twoa);
    childRexNodeLst.add(b);
    // 2a=b
    RexNode twoaEqualTob = rexBuilder.makeCall(
        SqlFunctionConverter.getCalciteFn("=", calciteArgTypesBldr.build(),
            TypeConverter.convert(TypeInfoFactory.longTypeInfo, cluster.getTypeFactory()), false, false),
        childRexNodeLst);
    childRexNodeLst = new ArrayList<RexNode>();
    childRexNodeLst.add(aMorethanZero);
    childRexNodeLst.add(twoaEqualTob);
    // a>0 && 2a=b
    return rexBuilder.makeCall(
        SqlFunctionConverter.getCalciteFn("and", calciteArgTypesBldr.build(),
            TypeConverter.convert(TypeInfoFactory.longTypeInfo, cluster.getTypeFactory()), false, false),
        childRexNodeLst);
  }

  private RexNode makeExprForExceptAll(HiveRelNode input, int columnSize, RelOptCluster cluster,
      RexBuilder rexBuilder) throws CalciteSemanticException {
    List<RexNode> childRexNodeLst = new ArrayList<RexNode>();
    ImmutableList.Builder<RelDataType> calciteArgTypesBldr = new ImmutableList.Builder<RelDataType>();
    calciteArgTypesBldr.add(TypeConverter.convert(TypeInfoFactory.longTypeInfo,
        cluster.getTypeFactory()));
    calciteArgTypesBldr.add(TypeConverter.convert(TypeInfoFactory.longTypeInfo,
        cluster.getTypeFactory()));
    RexInputRef a = rexBuilder.makeInputRef(input, columnSize - 2);
    RexLiteral three = rexBuilder.makeBigintLiteral(new BigDecimal(3));
    childRexNodeLst.add(three);
    childRexNodeLst.add(a);
    RexNode threea = rexBuilder.makeCall(
        SqlFunctionConverter.getCalciteFn("*", calciteArgTypesBldr.build(),
            TypeConverter.convert(TypeInfoFactory.longTypeInfo, cluster.getTypeFactory()), false, false),
        childRexNodeLst);

    RexLiteral two = rexBuilder.makeBigintLiteral(new BigDecimal(2));
    RexInputRef b = rexBuilder.makeInputRef(input, columnSize - 1);

    // 2*b
    childRexNodeLst = new ArrayList<RexNode>();
    childRexNodeLst.add(two);
    childRexNodeLst.add(b);
    RexNode twob = rexBuilder.makeCall(
        SqlFunctionConverter.getCalciteFn("*", calciteArgTypesBldr.build(),
            TypeConverter.convert(TypeInfoFactory.longTypeInfo, cluster.getTypeFactory()), false, false),
        childRexNodeLst);

    // 2b-3a
    childRexNodeLst = new ArrayList<RexNode>();
    childRexNodeLst.add(twob);
    childRexNodeLst.add(threea);
    return rexBuilder.makeCall(
        SqlFunctionConverter.getCalciteFn("-", calciteArgTypesBldr.build(),
            TypeConverter.convert(TypeInfoFactory.longTypeInfo, cluster.getTypeFactory()), false, false),
        childRexNodeLst);
  }
}
