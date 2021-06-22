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

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.metadata.HiveException;

import com.google.common.base.Preconditions;

import java.util.Arrays;

/**
 * This expression returns the value of the first non-null expression
 * in the given set of inputs expressions.
 */
public class VectorCoalesce extends VectorExpression {
  private static final long serialVersionUID = 1L;

  // The unassigned batchIndex for the rows that have not received a non-NULL value yet.
  // A temporary work array.
  private transient int[] unassignedBatchIndices;

  public VectorCoalesce(int [] inputColumns, int outputColumnNum) {
    super(inputColumns, outputColumnNum);
    Preconditions.checkArgument(inputColumns.length > 0);
  }

  public VectorCoalesce() {
    super();
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) throws HiveException {

    if (childExpressions != null) {
      super.evaluateChildren(batch);
    }

    int[] sel = batch.selected;
    int n = batch.size;
    ColumnVector outputColVector = batch.cols[outputColumnNum];
    boolean[] outputIsNull = outputColVector.isNull;
    if (n <= 0) {
      // Nothing to do
      return;
    }

    if (unassignedBatchIndices == null || n > unassignedBatchIndices.length) {

      // (Re)allocate larger to be a multiple of 1024 (DEFAULT_SIZE).
      final int roundUpSize =
          ((n + VectorizedRowBatch.DEFAULT_SIZE - 1) / VectorizedRowBatch.DEFAULT_SIZE)
              * VectorizedRowBatch.DEFAULT_SIZE;
      unassignedBatchIndices = new int[roundUpSize];
    }

    // We do not need to do a column reset since we are carefully changing the output.
    outputColVector.isRepeating = false;

    // CONSIDER: Should be do this for all vector expressions that can
    //           work on BytesColumnVector output columns???
    outputColVector.init();

    final int columnCount = inputColumnNum.length;

    /*
     * Process the input columns to find a non-NULL value for each row.
     *
     * We track the unassigned batchIndex of the rows that have not received
     * a non-NULL value yet.  Similar to a selected array.
     */
    boolean isAllUnassigned = true;
    int unassignedColumnCount = 0;

    for (int k = 0; k < inputColumnNum.length; k++) {
      ColumnVector cv = batch.cols[inputColumnNum[k]];
      if (cv.isRepeating) {

        if (cv.noNulls || !cv.isNull[0]) {

          /*
           * With a repeating value we can finish all remaining rows.
           */
          if (isAllUnassigned) {

            // No other columns provided non-NULL values.  We can return repeated output.
            outputIsNull[0] = false;
            outputColVector.setElement(0, 0, cv);
            outputColVector.isRepeating = true;
            return;
          } else {

            // Some rows have already been assigned values. Assign the remaining.
            // We cannot use copySelected method here.
            for (int i = 0; i < unassignedColumnCount; i++) {
              final int batchIndex = unassignedBatchIndices[i];
              outputIsNull[batchIndex] = false;

              // Our input is repeating (i.e. inputColNumber = 0).
              outputColVector.setElement(batchIndex, 0, cv);
            }
            return;
          }
        } else {

          // Repeated NULLs -- skip this input column.
        }
      } else {

        /*
         * Non-repeating input column. Use any non-NULL values for unassigned rows.
         */
        if (isAllUnassigned) {

          /*
           * No other columns provided non-NULL values.  We *may* be able to finish all rows
           * with this input column...
           */
          if (cv.noNulls){

            // Since no NULLs, we can provide values for all rows.
            if (batch.selectedInUse) {
              for (int i = 0; i < n; i++) {
                final int batchIndex = sel[i];
                outputIsNull[batchIndex] = false;
                outputColVector.setElement(batchIndex, batchIndex, cv);
              }
            } else {
              Arrays.fill(outputIsNull, 0, n, false);
              for (int batchIndex = 0; batchIndex < n; batchIndex++) {
                outputColVector.setElement(batchIndex, batchIndex, cv);
              }
            }
            return;
          } else {

            // We might not be able to assign all rows because of input NULLs.  Start tracking any
            // unassigned rows.
            boolean[] inputIsNull = cv.isNull;
            if (batch.selectedInUse) {
              for (int i = 0; i < n; i++) {
                final int batchIndex = sel[i];
                if (!inputIsNull[batchIndex]) {
                  outputIsNull[batchIndex] = false;
                  outputColVector.setElement(batchIndex, batchIndex, cv);
                } else {
                  unassignedBatchIndices[unassignedColumnCount++] = batchIndex;
                }
              }
            } else {
              for (int batchIndex = 0; batchIndex < n; batchIndex++) {
                if (!inputIsNull[batchIndex]) {
                  outputIsNull[batchIndex] = false;
                  outputColVector.setElement(batchIndex, batchIndex, cv);
                } else {
                  unassignedBatchIndices[unassignedColumnCount++] = batchIndex;
                }
              }
            }
            if (unassignedColumnCount == 0) {
              return;
            }
            isAllUnassigned = false;
          }
        } else {

          /*
           * We previously assigned *some* rows with non-NULL values. The batch indices of
           * the unassigned row were tracked.
           */
          if (cv.noNulls) {

            // Assign all remaining rows.
            for (int i = 0; i < unassignedColumnCount; i++) {
              final int batchIndex = unassignedBatchIndices[i];
              outputIsNull[batchIndex] = false;
              outputColVector.setElement(batchIndex, batchIndex, cv);
            }
            return;
          } else {

            // Use any non-NULL values found; remember the remaining unassigned.
            boolean[] inputIsNull = cv.isNull;
            int newUnassignedColumnCount = 0;
            for (int i = 0; i < unassignedColumnCount; i++) {
              final int batchIndex = unassignedBatchIndices[i];
              if (!inputIsNull[batchIndex]) {
                outputIsNull[batchIndex] = false;
                outputColVector.setElement(batchIndex, batchIndex, cv);
              } else {
                unassignedBatchIndices[newUnassignedColumnCount++] = batchIndex;
              }
            }
            if (newUnassignedColumnCount == 0) {
              return;
            }
            unassignedColumnCount = newUnassignedColumnCount;
          }
        }
      }
    }

    // NULL out the remaining columns.
    outputColVector.noNulls = false;
    if (isAllUnassigned) {
      outputIsNull[0] = true;
      outputColVector.isRepeating = true;
    } else {
      for (int i = 0; i < unassignedColumnCount; i++) {
        final int batchIndex = unassignedBatchIndices[i];
        outputIsNull[batchIndex] = true;
      }
    }
  }

  @Override
  public String vectorExpressionParameters() {
    return "columns " + Arrays.toString(inputColumnNum);
  }

  @Override
  public VectorExpressionDescriptor.Descriptor getDescriptor() {

    // Descriptor is not defined because it takes variable number of arguments with different
    // data types.
    throw new UnsupportedOperationException("Undefined descriptor");
  }
}
