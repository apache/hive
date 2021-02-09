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

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector.Type;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ptf.WindowFrameDef;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;

import com.google.common.base.Preconditions;

/**
 * This class evaluates HiveDecimal max() for a PTF group.
 */
public class VectorPTFEvaluatorDecimalMax extends VectorPTFEvaluatorBase {

  protected boolean isGroupResultNull;
  protected HiveDecimalWritable max;

  public VectorPTFEvaluatorDecimalMax(WindowFrameDef windowFrameDef, VectorExpression inputVecExpr,
      int outputColumnNum) {
    super(windowFrameDef, inputVecExpr, outputColumnNum);
    max = new HiveDecimalWritable();
    resetEvaluator();
  }

  @Override
  public void evaluateGroupBatch(VectorizedRowBatch batch)
      throws HiveException {

    evaluateInputExpr(batch);

    // Determine maximum of all non-null decimal column values; maintain isGroupResultNull.

    // We do not filter when PTF is in reducer.
    Preconditions.checkState(!batch.selectedInUse);

    final int size = batch.size;
    if (size == 0) {
      return;
    }
    DecimalColumnVector decimalColVector = ((DecimalColumnVector) batch.cols[inputColumnNum]);
    if (decimalColVector.isRepeating) {

      if (decimalColVector.noNulls || !decimalColVector.isNull[0]) {
        if (isGroupResultNull) {
          max.set(decimalColVector.vector[0]);
          isGroupResultNull = false;
        } else {
          HiveDecimalWritable repeatedMax = decimalColVector.vector[0];
          if (repeatedMax.compareTo(max) == 1) {
            max.set(repeatedMax);
          }
        }
      }
    } else if (decimalColVector.noNulls) {
      HiveDecimalWritable[] vector = decimalColVector.vector;
      if (isGroupResultNull) {
        max.set(vector[0]);
        isGroupResultNull = false;
      } else {
        final HiveDecimalWritable dec = vector[0];
        if (dec.compareTo(max) == 1) {
          max.set(dec);
        }
      }
      for (int i = 1; i < size; i++) {
        final HiveDecimalWritable dec = vector[i];
        if (dec.compareTo(max) == 1) {
          max.set(dec);
        }
      }
    } else {
      boolean[] batchIsNull = decimalColVector.isNull;
      int i = 0;
      while (batchIsNull[i]) {
        if (++i >= size) {
          return;
        }
      }

      HiveDecimalWritable[] vector = decimalColVector.vector;

      final HiveDecimalWritable firstValue = vector[i++];
      if (isGroupResultNull) {
        max.set(firstValue);
        isGroupResultNull = false;
      } else if (firstValue.compareTo(max) == 1) {
        max.set(firstValue);
      }
      for (; i < size; i++) {
        if (!batchIsNull[i]) {
          final HiveDecimalWritable dec = vector[i];
          if (dec.compareTo(max) == 1) {
            max.set(dec);
          }
        }
      }
    }
  }

  @Override
  public boolean streamsResult() {
    // We must evaluate whole group before producing a result.
    return false;
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
  public Object getGroupResult() {
    return max;
  }

  @Override
  public void resetEvaluator() {
    isGroupResultNull = true;
    max.setFromLong(0);
  }
}