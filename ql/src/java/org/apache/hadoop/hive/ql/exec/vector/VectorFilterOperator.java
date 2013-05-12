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
  private transient VectorExpression conditionEvaluator;
  transient int heartbeatInterval;
  private final VectorizationContext vContext;

  public VectorFilterOperator(VectorizationContext ctxt, OperatorDesc conf) {
    super();
    this.vContext = ctxt;
    filtered_count = new LongWritable();
    passed_count = new LongWritable();
    this.conf = (FilterDesc) conf;
  }

  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {
    try {
      heartbeatInterval = HiveConf.getIntVar(hconf,
          HiveConf.ConfVars.HIVESENDHEARTBEAT);
      ExprNodeDesc oldExpression = conf.getPredicate();
      vContext.setOperatorType(OperatorType.FILTER);
      conditionEvaluator = vContext.getVectorExpression(oldExpression);
      statsMap.put(Counter.FILTERED, filtered_count);
      statsMap.put(Counter.PASSED, passed_count);
    } catch (Throwable e) {
      throw new HiveException(e);
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
    conditionEvaluator.evaluate(vrg);
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
}
