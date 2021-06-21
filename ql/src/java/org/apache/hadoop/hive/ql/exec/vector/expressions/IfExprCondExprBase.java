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
package org.apache.hadoop.hive.ql.exec.vector.expressions;

import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.metadata.HiveException;

/**
 * Base class that supports conditional execution of the THEN/ELSE vector expressions of
 * a SQL IF statement.
 */
public abstract class IfExprCondExprBase extends VectorExpression {
  private static final long serialVersionUID = 1L;

  // Whether the IF statement boolean expression was repeating.
  protected transient boolean isIfStatementResultRepeated;
  protected transient boolean isIfStatementResultThen;

  // The batchIndex for the rows that are for the THEN/ELSE rows respectively.
  // Temporary work arrays.
  protected transient int thenSelectedCount;
  protected transient int[] thenSelected;
  protected transient int elseSelectedCount;
  protected transient int[] elseSelected;

  public IfExprCondExprBase(int arg1Column, int outputColumnNum) {
    super(arg1Column, outputColumnNum);
  }

  /* These constructors are used by subclasses */
  public IfExprCondExprBase(int arg1Column, int arg2Column, int outputColumnNum) {
    super(arg1Column, arg2Column, outputColumnNum);
  }

  public IfExprCondExprBase(int arg1Column, int arg2Column, int arg3Column, int outputColumnNum) {
    super(arg1Column, arg2Column, arg3Column, outputColumnNum);
  }

  public IfExprCondExprBase() {
    super();
  }

  public void conditionalEvaluate(VectorizedRowBatch batch, VectorExpression condVecExpr,
      int[] condSelected, int condSize) throws HiveException {

    int saveSize = batch.size;
    boolean saveSelectedInUse = batch.selectedInUse;
    int[] saveSelected = batch.selected;

    batch.size = condSize;
    batch.selectedInUse = true;
    batch.selected = condSelected;

    condVecExpr.evaluate(batch);

    batch.size = saveSize;
    batch.selectedInUse = saveSelectedInUse;
    batch.selected = saveSelected;
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) throws HiveException {

    // NOTE: We do conditional vector expression so we do not call super.evaluateChildren(batch).

    thenSelectedCount = 0;
    elseSelectedCount = 0;
    isIfStatementResultRepeated = false;
    isIfStatementResultThen = false;   // Give it a value.

    int n = batch.size;
    if (n <= 0) {
      // Nothing to do
      return;
    }

    // Child #1 is the IF boolean expression.
    childExpressions[0].evaluate(batch);
    LongColumnVector ifExprColVector = (LongColumnVector) batch.cols[inputColumnNum[0]];
    if (ifExprColVector.isRepeating) {
      isIfStatementResultRepeated = true;
      isIfStatementResultThen =
          ((ifExprColVector.noNulls || !ifExprColVector.isNull[0]) &&
              ifExprColVector.vector[0] == 1);
      return;
    }

    if (thenSelected == null || n > thenSelected.length) {

      // (Re)allocate larger to be a multiple of 1024 (DEFAULT_SIZE).
      final int roundUpSize =
          ((n + VectorizedRowBatch.DEFAULT_SIZE - 1) / VectorizedRowBatch.DEFAULT_SIZE)
              * VectorizedRowBatch.DEFAULT_SIZE;
      thenSelected = new int[roundUpSize];
      elseSelected = new int[roundUpSize];
    }

    int[] sel = batch.selected;
    long[] vector = ifExprColVector.vector;

    if (ifExprColVector.noNulls) {
      if (batch.selectedInUse) {
        for (int j = 0; j < n; j++) {
          final int i = sel[j];
          if (vector[i] == 1) {
            thenSelected[thenSelectedCount++] = i;
          } else {
            elseSelected[elseSelectedCount++] = i;
          }
        }
      } else {
        for (int i = 0; i < n; i++) {
          if (vector[i] == 1) {
            thenSelected[thenSelectedCount++] = i;
          } else {
            elseSelected[elseSelectedCount++] = i;
          }
        }
      }
    } else {
      boolean[] isNull = ifExprColVector.isNull;
      if (batch.selectedInUse) {
        for (int j = 0; j < n; j++) {
          final int i = sel[j];
          if (!isNull[i] && vector[i] == 1) {
            thenSelected[thenSelectedCount++] = i;
          } else {
            elseSelected[elseSelectedCount++] = i;
          }
        }
      } else {
        for (int i = 0; i < n; i++) {
          if (!isNull[i] && vector[i] == 1) {
            thenSelected[thenSelectedCount++] = i;
          } else {
            elseSelected[elseSelectedCount++] = i;
          }
        }
      }
    }

    if (thenSelectedCount == 0) {
      isIfStatementResultRepeated = true;
      isIfStatementResultThen = false;
    } else if (elseSelectedCount == 0) {
      isIfStatementResultRepeated = true;
      isIfStatementResultThen = true;
    }
  }

  @Override
  public VectorExpressionDescriptor.Descriptor getDescriptor() {

    // Descriptor is not defined because it takes variable number of arguments with different
    // data types.
    throw new UnsupportedOperationException("Undefined descriptor");
  }
}
