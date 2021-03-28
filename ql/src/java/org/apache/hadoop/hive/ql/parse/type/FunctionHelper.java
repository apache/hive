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
package org.apache.hadoop.hive.ql.parse.type;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.hadoop.hive.common.classification.InterfaceStability.Evolving;
import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.PartitionPruneRuleHelper;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.util.List;
import java.util.Set;

/**
 * Interface to handle function information while generating
 * Calcite {@link RexNode}.
 */
@Evolving
public interface FunctionHelper {

  /**
   * Returns whether engine is strict with operand types for functions,
   * i.e., operand types need to match.
   */
  boolean isStrictOperandTypes();

  /**
   * Returns RexNodeExprFactory.
   */
  RexNodeExprFactory getRexNodeExprFactory();

  /**
   * Returns RexExecutor that is used for constant folding.
   */
  RexExecutor getRexExecutor();

  /**
   * Returns function information based on function text.
   */
  FunctionInfo getFunctionInfo(String functionText) throws SemanticException;

  /**
   * Given function information and its inputs, it returns
   * the type of the output of the function.
   */
  RelDataType getReturnType(FunctionInfo functionInfo, List<RexNode> inputs)
      throws SemanticException;

  /**
   * Given function information, the inputs to that function, and the
   * expected return type, it will return the list of inputs with any
   * necessary adjustments, e.g., casting of expressions.
   */
  List<RexNode> convertInputs(FunctionInfo functionInfo, List<RexNode> inputs,
      RelDataType returnType)
      throws SemanticException;

  /**
   * Given function information and text, inputs to a function, and the
   * expected return type, it will return an expression node containing
   * the function call.
   */
  RexNode getExpression(String functionText, FunctionInfo functionInfo,
      List<RexNode> inputs, RelDataType returnType)
      throws SemanticException;

  /**
   * Given the function name and multiple parameters, it will return the
   * corresponding SQL operator.
   */
  SqlOperator getCalciteFunction(String functionName, List<RelDataType> argTypes,
      RelDataType retType, boolean deterministic, boolean runtimeConstant)
      throws SemanticException;

  /**
   * Given the aggregate function name and multiple parameters, it will return the
   * corresponding SQL aggregate operator.
   */
  SqlAggFunction getCalciteAggregateFunction(String functionName, boolean isDistinct,
      List<RelDataType> argTypes, RelDataType retType);

  /**
   * Given a builder, a SQL operator, and the inputs to a function, it will
   * return an expression node containing the function call.
   */
  RexNode makeCall(RexBuilder builder, SqlOperator operator, List<RexNode> operandList)
      throws SemanticException;

  /**
   * Given a whole number, it returns smaller exact numeric
   * that can hold this value.
   */
  RexNode getExactWholeNumber(String value);

  /**
   * Create a RexNode decimal expression from a string. If allowNullValueConstantExpr
   * is false, this method will return null if the value cannot be parsed. If it is
   * true, it will return a RexNode Decimal type of value null.
   */
  public RexNode createDecimalConstantExpr(String value, boolean allowNullValueConstantExpr);

  /**
   * Returns aggregation information based on given parameters.
   */
  AggregateInfo getAggregateFunctionInfo(boolean isDistinct, boolean isAllColumns,
      String aggregateName, List<RexNode> aggregateParameters)
      throws SemanticException;

  /**
   * Returns aggregation information for analytical function based on given parameters.
   */
  AggregateInfo getWindowAggregateFunctionInfo(boolean isDistinct, boolean isAllColumns,
      String aggregateName, List<RexNode> aggregateParameters)
      throws SemanticException;

  /**
   * Returns the aggregations that can be rewritten into simpler forms.
   */
  Set<SqlKind> getAggReduceSupported();

  /**
   * Returns the default standard deviation function (population or sample)
   * for this engine.
   */
  SqlKind getDefaultStandardDeviation();

  /**
   * Returns the default variance function (population or sample) for this engine.
   */
  SqlKind getDefaultVariance();

  /**
   * Returns the helper needed to do partition pruning.
   */
  PartitionPruneRuleHelper getPartitionPruneRuleHelper();

  /**
   * Folds expression according to function semantics.
   */
  default RexNode foldExpression(RexNode expr) {
    return expr;
  }

  /**
   * Returns whether we should generate multi-column IN clauses.
   */
  boolean isMultiColumnClauseSupported();

  /**
   * returns true if FunctionInfo is an And function.
   */
  boolean isAndFunction(FunctionInfo fi);

  /**
   * returns true if FunctionInfo is an Or function.
   */
  boolean isOrFunction(FunctionInfo fi);

  /**
   * returns true if FunctionInfo is an In function.
   */
  boolean isInFunction(FunctionInfo fi);

  /**
   * returns true if FunctionInfo is a compare function (e.g. '<=')
   */
  boolean isCompareFunction(FunctionInfo fi);

  /**
   * returns true if FunctionInfo is an == function.
   */
  boolean isEqualFunction(FunctionInfo fi);

  /**
   * Returns whether the expression, for a single query, returns the same result given
   * the same arguments/children. This includes deterministic functions as well as runtime
   * constants (which may not be deterministic across queries).
   */
  boolean isConsistentWithinQuery(FunctionInfo fi);

  /**
   * returns true if FunctionInfo is a stateful function.
   */
  boolean isStateful(FunctionInfo fi);

  void validateFunction(String functionName, boolean windowSpec) throws SemanticException;
  boolean isAggregateFunction(String functionName);

  /**
   * Class to store aggregate function related information.
   */
  class AggregateInfo {
    private final List<RexNode> parameters;
    private final TypeInfo returnType;
    private final String aggregateName;
    private final boolean distinct;

    public AggregateInfo(List<RexNode> parameters, TypeInfo returnType, String aggregateName,
        boolean distinct) {
      this.parameters = ImmutableList.copyOf(parameters);
      this.returnType = returnType;
      this.aggregateName = aggregateName;
      this.distinct = distinct;
    }

    public List<RexNode> getParameters() {
      return parameters;
    }

    public TypeInfo getReturnType() {
      return returnType;
    }

    public String getAggregateName() {
      return aggregateName;
    }

    public boolean isDistinct() {
      return distinct;
    }
  }

}
