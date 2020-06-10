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

package org.apache.hadoop.hive.ql.plan.impala.funcmapper;

import com.google.common.base.Preconditions;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.type.FunctionHelper;
import org.apache.hadoop.hive.ql.parse.type.RexNodeExprFactory;

import java.util.List;
import java.util.Map;

/**
 * Function resolver for an "internal_interval" function which is a
 * hive internal function which handles time intervals.
 */
public class InternalIntervalFunctionResolver extends ImpalaFunctionResolverImpl {

  private final RexNodeExprFactory factory;

  public InternalIntervalFunctionResolver(FunctionHelper helper, List<RexNode> inputNodes) {
    super(helper, null, inputNodes);
    this.factory = helper.getRexNodeExprFactory();
  }

  @Override
  public ImpalaFunctionSignature getFunction(
      Map<ImpalaFunctionSignature, ? extends FunctionDetails> functionDetailsMap) {
    // there is no impala function for this.
    return null;
  }

  @Override
  public List<RexNode> getConvertedInputs(ImpalaFunctionSignature function) throws HiveException {
    return inputNodes;
  }

  /**
   * Derive the return type from the given signature and operands.
   */
  @Override
  public RelDataType getRetType(ImpalaFunctionSignature function, List<RexNode> operands) {
    return null;
  }

  /**
   * Create the RexNode. This should be the last call made to this interface as the
   * whole purpose of the interface is to produce a RexNode using Impala semnatics.
   */
  @Override
  public RexNode createRexNode(ImpalaFunctionSignature function, List<RexNode> inputs,
      RelDataType returnDataType) {
    Preconditions.checkState(inputs.size() == 2);
    int intervalType = RexLiteral.intValue(inputs.get(0));
    switch (intervalType) {
      case HiveParser.TOK_INTERVAL_YEAR_LITERAL:
        return factory.createIntervalYearConstantExpr(
	    Integer.toString(RexLiteral.intValue(inputs.get(1))));
      case HiveParser.TOK_INTERVAL_MONTH_LITERAL:
        return factory.createIntervalMonthConstantExpr(
	   Integer.toString(RexLiteral.intValue(inputs.get(1))));
      case HiveParser.TOK_INTERVAL_DAY_LITERAL:
        return factory.createIntervalDayConstantExpr(
	    Integer.toString(RexLiteral.intValue(inputs.get(1))));
      case HiveParser.TOK_INTERVAL_HOUR_LITERAL:
        return factory.createIntervalHourConstantExpr(
	    Integer.toString(RexLiteral.intValue(inputs.get(1))));
      case HiveParser.TOK_INTERVAL_MINUTE_LITERAL:
        return factory.createIntervalMinuteConstantExpr(
	    Integer.toString(RexLiteral.intValue(inputs.get(1))));
      case HiveParser.TOK_INTERVAL_SECOND_LITERAL:
        return factory.createIntervalSecondConstantExpr(
	    Integer.toString(RexLiteral.intValue(inputs.get(1))));
      default:
        throw new RuntimeException("Unknown interval type: " + intervalType);
    }
  }
}
