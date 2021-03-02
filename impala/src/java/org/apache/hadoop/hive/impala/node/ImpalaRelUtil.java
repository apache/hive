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
package org.apache.hadoop.hive.impala.node;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.hadoop.hive.impala.funcmapper.ImpalaFunctionSignature;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.impala.funcmapper.AggFunctionDetails;
import org.apache.hadoop.hive.impala.funcmapper.ImpalaBuiltinsDb;
import org.apache.hadoop.hive.impala.funcmapper.ImpalaTypeConverter;
import org.apache.hadoop.hive.impala.rex.ImpalaRexVisitor.ImpalaInferMappingRexVisitor;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.Expr;
import org.apache.impala.catalog.AggregateFunction;
import org.apache.impala.catalog.BuiltinsDb;
import org.apache.impala.catalog.ScalarType;
import org.apache.impala.catalog.Type;

import java.util.List;
import java.util.stream.Collectors;

/**
 * This is a class to hold utility functions that assist with handling and
 * translation of {@link ImpalaPlanRel} nodes.
 */
public class ImpalaRelUtil {

  /**
   * Returns the aggregation function for the provided parameters. In Impala,
   * this could be either an aggregation or analytic function.
   */
  public static AggregateFunction getAggregateFunction(SqlAggFunction aggFunction, RelDataType retType,
      List<RelDataType> operandTypes) throws HiveException {

    AggFunctionDetails funcDetails = AggFunctionDetails.get(aggFunction.getName(), operandTypes,
        retType);

    if (funcDetails == null) {
      throw new SemanticException("Could not find function \"" + aggFunction.getName() + "\"");
    }

    List<Type> argTypes = ImpalaTypeConverter.createImpalaTypes(operandTypes);
    Type impalaRetType = ImpalaTypeConverter.createImpalaType(retType);
    int intermediateTypePrecision = funcDetails.intermediateTypeLength != 0 
        ? funcDetails.intermediateTypeLength
        : retType.getPrecision();
    Type intermediateType = ImpalaTypeConverter.createImpalaType(funcDetails.getIntermediateType(),
        intermediateTypePrecision, retType.getScale());

    return createAggFunction(funcDetails,aggFunction.getName(), argTypes, impalaRetType, intermediateType);
  }

  public static AggregateFunction getAggregateFunction(String aggFuncName, Type retType,
      List<Type> operandTypes) throws HiveException {

    AggFunctionDetails funcDetails = AggFunctionDetails.get(aggFuncName, operandTypes, retType, false);

    if (funcDetails == null) {
      throw new SemanticException("Could not find function \"" + aggFuncName + "\" " +
          "for operands: " + operandTypes + " and return type: " + retType);
    }

    int intermediateTypePrecision = funcDetails.intermediateTypeLength != 0 ?
        funcDetails.intermediateTypeLength :
        // use getColumnSize() instead of getPrecision() because getPrecision()
        // is only applicable to numeric types
        retType.getColumnSize();

    int intermediateTypeScale = retType.isDecimal() ? ((ScalarType) retType).decimalScale() : 0;
    Type intermediateType = ImpalaTypeConverter
        .createImpalaType(funcDetails.getIntermediateType(), intermediateTypePrecision,
            intermediateTypeScale);

    return createAggFunction(funcDetails, aggFuncName, operandTypes, retType, intermediateType);
  }

  private static AggregateFunction createAggFunction(AggFunctionDetails funcDetails, String aggFuncName,
      List<Type> operandTypes, Type retType, Type intermediateType) {
    Preconditions.checkState(funcDetails.isAgg || funcDetails.isAnalyticFn);
    if (!funcDetails.isAgg) {
      return AggregateFunction
          .createAnalyticBuiltin(ImpalaBuiltinsDb.getInstance(), aggFuncName, operandTypes, retType, intermediateType,
              funcDetails.initFnSymbol, funcDetails.updateFnSymbol, funcDetails.removeFnSymbol, funcDetails.getValueFnSymbol,
              funcDetails.finalizeFnSymbol);
    }
    // Use the createRewrittenBuiltin() method for grouping_id since it gets
    // rewritten internally in Impala.
    if (aggFuncName.equalsIgnoreCase("grouping_id")) {
      return AggregateFunction.createRewrittenBuiltin(ImpalaBuiltinsDb.getInstance(),
          aggFuncName, operandTypes, retType, funcDetails.ignoresDistinct, funcDetails.isAnalyticFn,
          funcDetails.returnsNonNullOnEmpty);
    }
    // Some agg functions are used both in analytic functions and regular aggregations (e.g. count)
    // We can treat them both as a regular builtin.
    return AggregateFunction
        .createBuiltin(ImpalaBuiltinsDb.getInstance(), aggFuncName, operandTypes, retType, intermediateType, funcDetails.initFnSymbol,
            funcDetails.updateFnSymbol, funcDetails.mergeFnSymbol, funcDetails.serializeFnSymbol, funcDetails.getValueFnSymbol, funcDetails.removeFnSymbol,
            funcDetails.finalizeFnSymbol, funcDetails.ignoresDistinct, funcDetails.isAnalyticFn, funcDetails.returnsNonNullOnEmpty);
  }

  /**
   * Given an input and analyzer instance, translate a rex node into
   * an Impala expression.
   */
  protected static Expr getExpr(RexNode exp, Analyzer analyzer, ImpalaPlanRel input) {
    ImpalaInferMappingRexVisitor visitor = new ImpalaInferMappingRexVisitor(
        analyzer, ImmutableList.of(input), input.getCluster().getRexBuilder());
    return exp.accept(visitor);
  }

  /**
   * Given an input and analyzer instance, translate a rex node list into
   * an Impala expression list.
   */
  protected static List<Expr> getExprs(List<RexNode> exp, Analyzer analyzer, ImpalaPlanRel input) {
    ImpalaInferMappingRexVisitor visitor = new ImpalaInferMappingRexVisitor(
        analyzer, ImmutableList.of(input), input.getCluster().getRexBuilder());
    return exp.stream().map(e -> e.accept(visitor)).collect(Collectors.toList());
  }

  /**
   * Gather all Impala Hdfs table scans in the plan starting from a root node and populate
   * the supplied tableScans list
   */
  public static void gatherTableScans(ImpalaPlanRel rootRelNode, List<ImpalaHdfsScanRel> tableScans) {
    if (rootRelNode instanceof ImpalaHdfsScanRel) {
      tableScans.add((ImpalaHdfsScanRel) rootRelNode);
      return;
    }
    for (RelNode child : rootRelNode.getInputs()) {
      gatherTableScans((ImpalaPlanRel) child, tableScans);
    }
  }

}
