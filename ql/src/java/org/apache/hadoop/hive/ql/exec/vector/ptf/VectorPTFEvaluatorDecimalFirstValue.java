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

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector.Type;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ptf.WindowFrameDef;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;

import com.google.common.base.Preconditions;

/**
 * This class evaluates HiveDecimal first_value() for a PTF group.
 *
 * We capture the first value from the first batch.  It can be NULL.
 * We then set (stream) the output column with that value as repeated in each batch.
 */
public class VectorPTFEvaluatorDecimalFirstValue extends VectorPTFEvaluatorBase {

  protected boolean haveFirstValue;
  protected boolean isGroupResultNull;
  protected HiveDecimalWritable firstValue;

  public VectorPTFEvaluatorDecimalFirstValue(WindowFrameDef windowFrameDef,
      VectorExpression inputVecExpr, int outputColumnNum) {
    super(windowFrameDef, inputVecExpr, outputColumnNum);
    resetEvaluator();
  }

  @Override
  public void evaluateGroupBatch(VectorizedRowBatch batch)
      throws HiveException {

    evaluateInputExpr(batch);

    // First row determines isGroupResultNull and decimal firstValue; stream fill result as repeated.

    // We do not filter when PTF is in reducer.
    Preconditions.checkState(!batch.selectedInUse);

    if (!haveFirstValue) {
      final int size = batch.size;
      if (size == 0) {
        return;
      }
      DecimalColumnVector decimalColVector = ((DecimalColumnVector) batch.cols[inputColumnNum]);
      if (decimalColVector.isRepeating) {
        if (decimalColVector.noNulls || !decimalColVector.isNull[0]) {
          firstValue.set(decimalColVector.vector[0]);
          isGroupResultNull = false;
        }
      } else if (decimalColVector.noNulls) {
        firstValue.set(decimalColVector.vector[0]);
        isGroupResultNull = false;
      } else if (doesRespectNulls()) {
        if (!decimalColVector.isNull[0]) {
          firstValue.set(decimalColVector.vector[0]);
          isGroupResultNull = false;
        }
      } else {
        // If we do not respect nulls, we just take the first value and ignore nulls.
        for (int i = 0; i < size; i++) {
          if (!decimalColVector.isNull[i]) {
            firstValue.set(decimalColVector.vector[i]);
            isGroupResultNull = false;
            break;
          }
        }
      }
      // If nulls are respected, we set haveFirstValue to true as we don't need to look for a non-null value
      // Otherwise, we should keep looking for a non-null value in the next batches, i.e.,
      // until group result is not null.
      haveFirstValue = doesRespectNulls() || !isGroupResultNull;
    }

    /*
     * Do careful maintenance of the outputColVector.noNulls flag.
     */

    // First value is repeated for all batches.
    DecimalColumnVector outputColVector = (DecimalColumnVector) batch.cols[outputColumnNum];
    outputColVector.isRepeating = true;
    if (isGroupResultNull) {
      outputColVector.noNulls = false;
      outputColVector.isNull[0] = true;
    } else {
      outputColVector.isNull[0] = false;
      outputColVector.set(0, firstValue);
    }
  }

  @Override
  public boolean streamsResult() {
    return true;
  }

  public boolean isGroupResultNull() {
    return isGroupResultNull;
  }

  @Override
  public Type getResultColumnVectorType() {
    return Type.DECIMAL;
  }

  @Override
  public Object getGroupResult() {
    return firstValue;
  }

  @Override
  public void resetEvaluator() {
    haveFirstValue = false;
    isGroupResultNull = true;
    firstValue = new HiveDecimalWritable();
  }

  // this is not necessarily needed, because first_value is evaluated in a streaming way, therefore
  // VectorPTFGroupBatches won't hit this codepath, so this is just for clarity's sake (behavior is
  // other than default VectorPTFEvaluatorBase)
  public boolean isCacheableForRange() {
    throw new RuntimeException("isCacheableForRange check is not expected for first_value");
  }
}