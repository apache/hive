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

import java.util.Collections;
import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.util.NullOrdering;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

/**
 * Interface to handle function information while generating
 * Calcite {@link RexNode}.
 */
public interface FunctionHelper {

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
   * Returns aggregation information based on given parameters.
   */
  AggregateInfo getAggregateFunctionInfo(boolean isDistinct, boolean isAllColumns,
                                         String aggregateName, List<RexNode> aggregateParameters, List<FieldCollation> fieldCollations)
      throws SemanticException;

  /**
   * Returns aggregation information for analytical function based on given parameters.
   */
  AggregateInfo getWindowAggregateFunctionInfo(boolean isDistinct, boolean isAllColumns,
      String aggregateName, List<RexNode> aggregateParameters)
      throws SemanticException;

  /**
   * Returns RexCall for UDTF from the appropriate function registry based on given parameters
   */
  RexCall getUDTFFunction(String functionName, List<RexNode> operands)
      throws SemanticException;

  /**
   * Folds expression according to function semantics.
   */
  default RexNode foldExpression(RexNode expr) {
    return expr;
  }

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
   * returns true if FunctionInfo is a compare function (e.g. '&lt;=')
   */
  boolean isCompareFunction(FunctionInfo fi);

  /**
   * returns true if FunctionInfo is an == function.
   */
  boolean isEqualFunction(FunctionInfo fi);


  boolean isNSCompareFunction(FunctionInfo fi);

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

  class FieldCollation {
    private final RexNode sortExpression;
    private final int sortDirection;
    private final NullOrdering nullOrdering;

    public FieldCollation(RexNode sortExpression, int sortDirection, NullOrdering nullSortOrder) {
      this.sortExpression = sortExpression;
      this.sortDirection = sortDirection;
      this.nullOrdering = nullSortOrder;
    }

    public RexNode getSortExpression() {
      return sortExpression;
    }

    public int getSortDirection() {
      return sortDirection;
    }

    public NullOrdering getNullOrdering() {
      return nullOrdering;
    }
  }

  /**
   * Class to store aggregate function related information.
   */
  class AggregateInfo {
    private final List<RexNode> parameters;
    private final TypeInfo returnType;
    private final String aggregateName;
    private final boolean distinct;
    private final List<FieldCollation> collation;

    public AggregateInfo(List<RexNode> parameters, TypeInfo returnType, String aggregateName,
        boolean distinct) {
      this.parameters = ImmutableList.copyOf(parameters);
      this.returnType = returnType;
      this.aggregateName = aggregateName;
      this.distinct = distinct;
      this.collation = Collections.emptyList();
    }

    public AggregateInfo(List<RexNode> parameters, TypeInfo returnType, String aggregateName,
        boolean distinct, List<FieldCollation> collation) {
      this.parameters = ImmutableList.copyOf(parameters);
      this.returnType = returnType;
      this.aggregateName = aggregateName;
      this.distinct = distinct;
      this.collation = collation;
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

    public List<FieldCollation> getCollation() {
      return collation;
    }
  }

}
