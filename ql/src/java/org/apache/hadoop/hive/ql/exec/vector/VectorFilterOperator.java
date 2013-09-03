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

package org.apache.hadoop.hive.ql.exec.vector;

import java.io.Serializable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.vector.expressions.ConstantVectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.FilterDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.io.LongWritable;

/**
 * Filter operator implementation.
 **/
public class VectorFilterOperator extends Operator<FilterDesc> implements
    Serializable {

  private static final long serialVersionUID = 1L;

  /**
   * Counter.
   *
   */
  public static enum Counter {
    FILTERED, PASSED
  }

  private final transient LongWritable filtered_count, passed_count;
  private VectorExpression conditionEvaluator = null;
  transient int heartbeatInterval;

  // filterMode is 1 if condition is always true, -1 if always false
  // and 0 if condition needs to be computed.
  transient private int filterMode = 0;

  public VectorFilterOperator(VectorizationContext vContext, OperatorDesc conf)
      throws HiveException {
    this();
    vContext.setOperatorType(OperatorType.FILTER);
    ExprNodeDesc oldExpression = ((FilterDesc) conf).getPredicate();
    conditionEvaluator = vContext.getVectorExpression(oldExpression);
  }

  public VectorFilterOperator() {
    super();
    filtered_count = new LongWritable();
    passed_count = new LongWritable();
    this.conf = (FilterDesc) conf;
  }


  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {
    try {
      heartbeatInterval = HiveConf.getIntVar(hconf,
          HiveConf.ConfVars.HIVESENDHEARTBEAT);
      statsMap.put(Counter.FILTERED, filtered_count);
      statsMap.put(Counter.PASSED, passed_count);
    } catch (Throwable e) {
      throw new HiveException(e);
    }
    if (conditionEvaluator instanceof ConstantVectorExpression) {
      ConstantVectorExpression cve = (ConstantVectorExpression) this.conditionEvaluator;
      if (cve.getLongValue() == 1) {
        filterMode = 1;
      } else {
        filterMode = -1;
      }
    }
    initializeChildren(hconf);
  }

  public void setFilterCondition(VectorExpression expr) {
    this.conditionEvaluator = expr;
  }

  @Override
  public void processOp(Object row, int tag) throws HiveException {

    VectorizedRowBatch vrg = (VectorizedRowBatch) row;
    //Evaluate the predicate expression
    //The selected vector represents selected rows.
    switch (filterMode) {
      case 0:
        conditionEvaluator.evaluate(vrg);
        break;
      case -1:
        // All will be filtered out
        vrg.size = 0;
        break;
      case 1:
      default:
        // All are selected, do nothing
    }
    if (vrg.size > 0) {
      forward(vrg, null);
    }
  }

  /**
   * @return the name of the operator
   */
  @Override
  public String getName() {
    return getOperatorName();
  }

  static public String getOperatorName() {
    return "FIL";
  }

  @Override
  public OperatorType getType() {
    return OperatorType.FILTER;
  }

  public VectorExpression getConditionEvaluator() {
    return conditionEvaluator;
  }

  public void setConditionEvaluator(VectorExpression conditionEvaluator) {
    this.conditionEvaluator = conditionEvaluator;
  }
}
