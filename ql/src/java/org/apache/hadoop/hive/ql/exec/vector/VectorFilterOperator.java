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

import java.util.Collection;
import java.util.concurrent.Future;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.vector.expressions.ConstantVectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.FilterDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;

/**
 * Filter operator implementation.
 **/
public class VectorFilterOperator extends FilterOperator {

  private static final long serialVersionUID = 1L;

  private VectorExpression conditionEvaluator = null;

  // Temporary selected vector
  private transient int[] temporarySelected;

  // filterMode is 1 if condition is always true, -1 if always false
  // and 0 if condition needs to be computed.
  transient private int filterMode = 0;

  public VectorFilterOperator(VectorizationContext vContext, OperatorDesc conf)
      throws HiveException {
    this();
    ExprNodeDesc oldExpression = ((FilterDesc) conf).getPredicate();
    conditionEvaluator = vContext.getVectorExpression(oldExpression, VectorExpressionDescriptor.Mode.FILTER);
    this.conf = (FilterDesc) conf;
  }

  public VectorFilterOperator() {
    super();
  }


  @Override
  protected Collection<Future<?>> initializeOp(Configuration hconf) throws HiveException {
    Collection<Future<?>> result = super.initializeOp(hconf);
    try {
      heartbeatInterval = HiveConf.getIntVar(hconf,
          HiveConf.ConfVars.HIVESENDHEARTBEAT);
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

    temporarySelected = new int [VectorizedRowBatch.DEFAULT_SIZE];

    return result;
  }

  public void setFilterCondition(VectorExpression expr) {
    this.conditionEvaluator = expr;
  }

  @Override
  public void process(Object row, int tag) throws HiveException {

    VectorizedRowBatch vrg = (VectorizedRowBatch) row;

    //The selected vector represents selected rows.
    //Clone the selected vector
    System.arraycopy(vrg.selected, 0, temporarySelected, 0, vrg.size);
    int [] selectedBackup = vrg.selected;
    vrg.selected = temporarySelected;
    int sizeBackup = vrg.size;
    boolean selectedInUseBackup = vrg.selectedInUse;

    //Evaluate the predicate expression
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

    // Restore the original selected vector
    vrg.selected = selectedBackup;
    vrg.size = sizeBackup;
    vrg.selectedInUse = selectedInUseBackup;
  }

  static public String getOperatorName() {
    return "FIL";
  }

  public VectorExpression getConditionEvaluator() {
    return conditionEvaluator;
  }

  public void setConditionEvaluator(VectorExpression conditionEvaluator) {
    this.conditionEvaluator = conditionEvaluator;
  }
}
