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

import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.Pair;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;
import org.apache.hadoop.hive.ql.plan.impala.funcmapper.Calcite2302;
import org.apache.hadoop.hive.ql.plan.impala.funcmapper.ImpalaBuiltins;
import org.apache.hadoop.hive.ql.plan.impala.funcmapper.ImpalaFunctionMapper;
import org.apache.hadoop.hive.ql.plan.impala.funcmapper.ImpalaFunctionSignatureFactory;
import org.apache.hadoop.hive.ql.plan.impala.funcmapper.ImpalaFunctionSignature;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Impala specific rule to add a cast when needed for Impala functions.
 * For example, Impala supports multiplication between two expressions of the
 * same type only. For example, it supports INT * INT but does not support INT * TINYINT)
 * If we see this case, this rule will add  the CAST TINYINT AS INT function so that
 * it matches the impala signature.
 */
public abstract class ImpalaRexCastRule extends RelOptRule {
  protected static final Logger LOG = LoggerFactory.getLogger(ImpalaRexCastRule.class);

  public static final ImpalaProjectRexCastRule PROJECT_INSTANCE =
      new ImpalaProjectRexCastRule();

  public static final ImpalaFilterRexCastRule FILTER_INSTANCE =
      new ImpalaFilterRexCastRule();

  public static final ImpalaJoinRexCastRule JOIN_INSTANCE =
      new ImpalaJoinRexCastRule();
  public static final String UPCAST_NOT_ALLOWED = "Cannot yet support propagation of " +
     "upcasting of return types for function ";
  public static class ImpalaProjectRexCastRule extends ImpalaRexCastRule {

    private ImpalaProjectRexCastRule() {
      super(operand(HiveProject.class, any()), HiveRelFactories.HIVE_BUILDER);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      super.onMatch(call);
      final HiveProject project = call.rel(0);
      List<RexNode> newProjects = getConvertedRexNodes(project, project.getProjects());
      RelBuilder relBuilder = call.builder();

      if (newProjects != null) {
        RelNode newNode = relBuilder.push(project.getInput(0)).project(newProjects).build();
        HiveRulesRegistry registry = call.getPlanner().getContext().unwrap(HiveRulesRegistry.class);
        registry.registerVisited(this, newNode);
        call.transformTo(newNode);
      }
    }
  }

  public static class ImpalaFilterRexCastRule extends ImpalaRexCastRule {

    private ImpalaFilterRexCastRule() {
      super(operand(HiveFilter.class, any()), HiveRelFactories.HIVE_BUILDER);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      super.onMatch(call);
      final HiveFilter filter = call.rel(0);
      List<RexNode> newFilters = getConvertedRexNodes(filter, filter.getChildExps());
      RelBuilder relBuilder = call.builder();

      if (newFilters != null) {
        RelNode newNode = relBuilder.push(filter.getInput(0)).filter(newFilters).build();
        HiveRulesRegistry registry = call.getPlanner().getContext().unwrap(HiveRulesRegistry.class);
        registry.registerVisited(this, newNode);
        call.transformTo(newNode);
      }
    }
  }

  public static class ImpalaJoinRexCastRule extends ImpalaRexCastRule {

    private ImpalaJoinRexCastRule() {
      super(operand(HiveJoin.class, any()), HiveRelFactories.HIVE_BUILDER);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      super.onMatch(call);
      final HiveJoin join = call.rel(0);
      List<RexNode> newJoinExps = getConvertedRexNodes(join, join.getChildExps());
      RelBuilder relBuilder = call.builder();

      if (newJoinExps != null) {
        RelNode newNode =
            relBuilder.push(join.getLeft()).push(join.getRight()).join(join.getJoinType(), newJoinExps).build();
        HiveRulesRegistry registry = call.getPlanner().getContext().unwrap(HiveRulesRegistry.class);
        registry.registerVisited(this, newNode);
        call.transformTo(newNode);
      }
    }
  }

  public static List<RexNode> getConvertedRexNodes(RelNode relNode, List<RexNode> rexNodes) {
    List<RexNode> convertedRexNodes = Lists.newArrayList();
    boolean isChanged = false;
    for (RexNode rexNode : rexNodes) {
      ImpalaCastRexShuttle shuttle = new ImpalaCastRexShuttle(relNode.getCluster());
      RexNode appliedRexNode = shuttle.apply(rexNode);
      if (appliedRexNode != rexNode) {
        isChanged = true;
      }
      convertedRexNodes.add(appliedRexNode);
    }

    if (!isChanged) {
      return null;
    }
    return convertedRexNodes;
  }

  public static class ImpalaCastRexShuttle extends RexShuttle {
    private final RelOptCluster cluster;

    public ImpalaCastRexShuttle(RelOptCluster cluster) {
      this.cluster = cluster;
    }

    @Override
    public RexNode visitCall(RexCall rexCall) {
      // the list of RelDataType operands for current call
      List<RelDataType> currentOperandTypes =
          getRelDataTypesFromNodes(rexCall.getOperands());
      // the list of SqlTypeName operands for current call
      List<SqlTypeName> currentSqlOperandTypes =
          getSqlTypeNamesFromNodes(rexCall.getOperands());
      // The return SqlTypeName for the current call.
      SqlTypeName currentRetSqlType = rexCall.getType().getSqlTypeName();
      // apply the rex casting to all of the operand first, then apply to this RexNode.
      List<RexNode> newOperands = apply(rexCall.getOperands());

      String opName = rexCall.getOperator().getName();
      ImpalaFunctionSignature ifs = null;
      try {
        ifs = ImpalaFunctionSignatureFactory.create(rexCall.getOperator(),
           getSqlTypeNamesFromNodes(newOperands), currentRetSqlType);
      } catch (HiveException e) {
        throw new RuntimeException(e);
      }
      ImpalaFunctionMapper ifm = new ImpalaFunctionMapper(ifs);

      // The main call to check if this RexCall maps to a known Impala function signature.
      List<SqlTypeName> mappedOperandTypes =
          ifm.mapOperands(ImpalaBuiltins.SCALAR_BUILTINS_INSTANCE);

      // create the new RexNode operands.  These new operands will either have a cast of the
      // old operand or remain the same as the old operand.
      List<RexNode> newCastedOperands =
          createRexNodes(newOperands, currentOperandTypes, mappedOperandTypes);

      return cluster.getRexBuilder().makeCall(rexCall.getType(),
           rexCall.getOperator(), newCastedOperands);
    }

    /**
     * Create rex nodes for the operands.  Either it will be the original operand or a cast
     * operand.
     */
    private List<RexNode> createRexNodes(List<RexNode> operands,
       List<RelDataType> currentOperandTypes,  List<SqlTypeName> mappedOperandSqlTypes) {
      Preconditions.checkState(operands.size() == mappedOperandSqlTypes.size());
      List<RexNode> result = Lists.newArrayList();
      for (int i = 0; i < operands.size(); ++i) {
        List<RexNode> currRexNode = Lists.newArrayList(operands.get(i));
        RelDataType currentOperandType = currentOperandTypes.get(i);
        SqlTypeName mappedOperandSqlType = mappedOperandSqlTypes.get(i);
        if (typesAreEquivalent(currentOperandType.getSqlTypeName(), mappedOperandSqlType)) {
          // no casting needed if the operand type matches what exists
          result.add(currRexNode.get(0));
        } else {
          RelDataType castedRelDataType =
              getCastedDataType(cluster.getTypeFactory(), mappedOperandSqlType, currentOperandType);
          // cast to appropriate type
          result.add(cluster.getRexBuilder().makeCast(castedRelDataType, operands.get(i), true));
        }
      }
      return result;
    }

    private boolean typesAreEquivalent(SqlTypeName currentType, SqlTypeName mappedType) {
      // All INTERVAL_TYPES are mapped to a BIGINT when we convert to Impala.
      if (SqlTypeName.INTERVAL_TYPES.contains(currentType)) {
        return mappedType.equals(SqlTypeName.BIGINT);
      }
      return currentType.equals(mappedType);
    }

    /**
     * Return the casted RelDatatype of the provided postCastSqlTypeName
     */
    private RelDataType getCastedDataType(RelDataTypeFactory dtFactory,
        SqlTypeName postCastSqlTypeName, RelDataType preCastRelDataType) {
      // In the case where we are casting to Decimal, we need to provide a precision
      // and scale.  The Calcite method provides the DecimalRelDataType with the
      // appropriate precision and scale with the provided datatype that will be casted.
      if (postCastSqlTypeName == SqlTypeName.DECIMAL) {
        return Calcite2302.decimalOf(dtFactory, preCastRelDataType);
      }
      return dtFactory.createSqlType(postCastSqlTypeName);
    }
  }

  private static List<RelDataType> getRelDataTypesFromNodes(List<RexNode> rexNodes) {
    List<RelDataType> result = Lists.newArrayList();
    for (RexNode rexNode : rexNodes) {
      result.add(rexNode.getType());
    }
    return result;
  }

  private static List<SqlTypeName> getSqlTypeNamesFromNodes(List<RexNode> rexNodes) {
    List<SqlTypeName> result = Lists.newArrayList();
    for (RexNode rexNode : rexNodes) {
      result.add(rexNode.getType().getSqlTypeName());
    }
    return result;
  }

  protected ImpalaRexCastRule(RelOptRuleOperand operand,
      RelBuilderFactory relBuilderFactory) {
    super(operand, relBuilderFactory, null);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    HiveRulesRegistry registry = call.getPlanner().getContext().unwrap(HiveRulesRegistry.class);

    // If this operator has been visited already by the rule,
    // we do not need to apply the optimization
    if (registry != null && registry.getVisited(this).contains(call.rel(0))) {
      return false;
    }
    return super.matches(call);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    HiveRulesRegistry registry = call.getPlanner().getContext().unwrap(HiveRulesRegistry.class);
    if (registry != null) {
      registry.registerVisited(this, call.rel(0));
    }
  }
}
