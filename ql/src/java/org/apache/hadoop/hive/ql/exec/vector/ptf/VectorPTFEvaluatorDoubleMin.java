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

package org.apache.hadoop.hive.ql.exec.vector.ptf;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector.Type;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.plan.ptf.WindowFrameDef;

import com.google.common.base.Preconditions;

/**
 * This class evaluates double min() for a PTF group.
 */
public class VectorPTFEvaluatorDoubleMin extends VectorPTFEvaluatorBase {

  private static final long serialVersionUID = 1L;
  private static final String CLASS_NAME = VectorPTFEvaluatorDoubleMin.class.getName();
  private static final Log LOG = LogFactory.getLog(CLASS_NAME);

  protected boolean isGroupResultNull;
  protected double min;

  public VectorPTFEvaluatorDoubleMin(WindowFrameDef windowFrameDef, VectorExpression inputVecExpr,
      int outputColumnNum) {
    super(windowFrameDef, inputVecExpr, outputColumnNum);
    resetEvaluator();
  }

  public void evaluateGroupBatch(VectorizedRowBatch batch, boolean isLastGroupBatch) {
    evaluateInputExpr(batch);

    // Determine minimum of all non-null double column values; maintain isGroupResultNull.

    // We do not filter when PTF is in reducer.
    Preconditions.checkState(!batch.selectedInUse);

    final int size = batch.size;
    if (size == 0) {
      return;
    }
    DoubleColumnVector doubleColVector = ((DoubleColumnVector) batch.cols[inputColumnNum]);
    if (doubleColVector.isRepeating) {
      if (doubleColVector.noNulls) {
        if (isGroupResultNull) {
          min = doubleColVector.vector[0];
          isGroupResultNull = false;
        } else {
          final double repeatedMin = doubleColVector.vector[0];
          if (repeatedMin < min) {
            min = repeatedMin;
          }
        }
      }
    } else if (doubleColVector.noNulls) {
      double[] vector = doubleColVector.vector;
      double varMin = vector[0];
      for (int i = 1; i < size; i++) {
        final double d = vector[i];
        if (d < varMin) {
          varMin = d;
        }
      }
      if (isGroupResultNull) {
        min = varMin;
        isGroupResultNull = false;
      } else if (varMin < min) {
        min = varMin;
      }
    } else {
      boolean[] batchIsNull = doubleColVector.isNull;
      int i = 0;
      while (batchIsNull[i]) {
        if (++i >= size) {
          return;
        }
      }
      double[] vector = doubleColVector.vector;
      double varMin = vector[i++];
      for (; i < size; i++) {
        if (!batchIsNull[i]) {
          final double d = vector[i];
          if (d < varMin) {
            varMin = d;
          }
        }
      }
      if (isGroupResultNull) {
        min = varMin;
        isGroupResultNull = false;
      } else if (varMin < min) {
        min = varMin;
      }
    }
  }

  @Override
  public boolean isGroupResultNull() {
    return isGroupResultNull;
  }

  @Override
  public Type getResultColumnVectorType() {
    return Type.DOUBLE;
  }

  @Override
  public double getDoubleGroupResult() {
    return min;
  }

  @Override
  public void resetEvaluator() {
    isGroupResultNull = true;
    min = Double.MAX_VALUE;
  }
}