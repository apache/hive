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

package org.apache.hadoop.hive.ql.exec.vector.ptf;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.common.type.FastHiveDecimal;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector.Type;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ptf.WindowFrameDef;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;

import com.google.common.base.Preconditions;

/**
 * This class evaluates HiveDecimal last_value() for a PTF group.
 *
 * We capture the last value from the last batch.  It can be NULL.
 * It becomes the group value.
 */
public class VectorPTFEvaluatorDecimalLastValue extends VectorPTFEvaluatorBase {

  private static final long serialVersionUID = 1L;
  private static final String CLASS_NAME = VectorPTFEvaluatorDecimalLastValue.class.getName();
  private static final Log LOG = LogFactory.getLog(CLASS_NAME);

  protected boolean isGroupResultNull;
  protected HiveDecimalWritable lastValue;

  public VectorPTFEvaluatorDecimalLastValue(WindowFrameDef windowFrameDef,
      VectorExpression inputVecExpr, int outputColumnNum) {
    super(windowFrameDef, inputVecExpr, outputColumnNum);
    lastValue = new HiveDecimalWritable();
    resetEvaluator();
  }

  public void evaluateGroupBatch(VectorizedRowBatch batch, boolean isLastGroupBatch)
      throws HiveException {

    evaluateInputExpr(batch);

    // Last row of last batch determines isGroupResultNull and decimal lastValue.

    // We do not filter when PTF is in reducer.
    Preconditions.checkState(!batch.selectedInUse);

    if (!isLastGroupBatch) {
      return;
    }
    final int size = batch.size;
    if (size == 0) {
      return;
    }
    DecimalColumnVector decimalColVector = ((DecimalColumnVector) batch.cols[inputColumnNum]);
    if (decimalColVector.isRepeating) {

      if (decimalColVector.noNulls || !decimalColVector.isNull[0]) {
        lastValue.set(decimalColVector.vector[0]);
        isGroupResultNull = false;
      } else {
        isGroupResultNull = true;
      }
    } else if (decimalColVector.noNulls) {
      lastValue.set(decimalColVector.vector[size - 1]);
      isGroupResultNull = false;
    } else {
      final int lastBatchIndex = size - 1;
      if (!decimalColVector.isNull[lastBatchIndex]) {
        lastValue.set(decimalColVector.vector[lastBatchIndex]);
        isGroupResultNull = false;
      } else {
        isGroupResultNull = true;
      }
    }
  }

  @Override
  public boolean isGroupResultNull() {
    return isGroupResultNull;
  }

  @Override
  public Type getResultColumnVectorType() {
    return Type.DECIMAL;
  }

  @Override
  public HiveDecimalWritable getDecimalGroupResult() {
    return lastValue;
  }

  @Override
  public void resetEvaluator() {
    isGroupResultNull = true;
    lastValue.set(HiveDecimal.ZERO);
  }
}