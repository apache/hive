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
package org.apache.hadoop.hive.ql.plan.impala.node;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.impala.funcmapper.AggFunctionDetails;
import org.apache.hadoop.hive.ql.plan.impala.funcmapper.ImpalaTypeConverter;
import org.apache.hadoop.hive.ql.plan.impala.rex.ImpalaRexVisitor.ImpalaInferMappingRexVisitor;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.Expr;
import org.apache.impala.catalog.AggregateFunction;
import org.apache.impala.catalog.BuiltinsDb;
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
  protected static AggregateFunction getAggregateFunction(SqlAggFunction aggFunction, RelDataType retType,
      List<SqlTypeName> operandTypes) throws HiveException {

    AggFunctionDetails funcDetails = AggFunctionDetails.get(aggFunction.getName(), operandTypes,
        retType.getSqlTypeName());

    if (funcDetails == null) {
      throw new SemanticException("Could not find function \"" + aggFunction.getName() + "\"");
    }

    List<Type> argTypes = ImpalaTypeConverter.getImpalaTypesList(funcDetails.argTypes);
    Type impalaRetType =
        ImpalaTypeConverter.getImpalaType(funcDetails.retType, retType.getPrecision(), retType.getScale());
    Integer intermediateTypePrecision = funcDetails.intermediateTypeLength != null
        ? funcDetails.intermediateTypeLength
        : retType.getPrecision();
    Type intermediateType = ImpalaTypeConverter.getImpalaType(funcDetails.intermediateType,
        intermediateTypePrecision, retType.getScale());

    Preconditions.checkState(funcDetails.isAgg || funcDetails.isAnalyticFn);
    if (!funcDetails.isAgg) {
      return AggregateFunction.createAnalyticBuiltin(BuiltinsDb.getInstance(true), aggFunction.getName(),
          argTypes, impalaRetType, intermediateType, funcDetails.initFnSymbol,
          funcDetails.updateFnSymbol, funcDetails.removeFnSymbol, funcDetails.getValueFnSymbol,
          funcDetails.finalizeFnSymbol);
    }
    // Some agg functions are used both in analytic functions and regular aggregations (e.g. count)
    // We can treat them both as a regular builtin.
    return AggregateFunction.createBuiltin(BuiltinsDb.getInstance(true), aggFunction.getName(),
        argTypes, impalaRetType, intermediateType, funcDetails.initFnSymbol,
        funcDetails.updateFnSymbol, funcDetails.mergeFnSymbol, funcDetails.serializeFnSymbol,
        funcDetails.getValueFnSymbol, funcDetails.removeFnSymbol, funcDetails.finalizeFnSymbol,
        funcDetails.ignoresDistinct, funcDetails.isAnalyticFn, funcDetails.returnsNonNullOnEmpty);
  }

  /**
   * Given an input and analyzer instance, translate a rex node into
   * an Impala expression.
   */
  protected static Expr getExpr(RexNode exp, Analyzer analyzer, ImpalaPlanRel input) {
    ImpalaInferMappingRexVisitor visitor = new ImpalaInferMappingRexVisitor(
        analyzer, ImmutableList.of(input));
    return exp.accept(visitor);
  }

  /**
   * Given an input and analyzer instance, translate a rex node list into
   * an Impala expression list.
   */
  protected static List<Expr> getExprs(List<RexNode> exp, Analyzer analyzer, ImpalaPlanRel input) {
    ImpalaInferMappingRexVisitor visitor = new ImpalaInferMappingRexVisitor(
        analyzer, ImmutableList.of(input));
    return exp.stream().map(e -> e.accept(visitor)).collect(Collectors.toList());
  }

}
