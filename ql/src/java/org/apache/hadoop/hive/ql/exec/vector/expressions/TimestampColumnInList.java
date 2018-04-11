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

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashSet;

import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor.Descriptor;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

/**
 * Output a boolean value indicating if a column is IN a list of constants.
 */
public class TimestampColumnInList extends VectorExpression implements ITimestampInExpr {
  private static final long serialVersionUID = 1L;

  private final int inputCol;

  private Timestamp[] inListValues;

  // The set object containing the IN list.
  private transient HashSet<Timestamp> inSet;

  public TimestampColumnInList() {
    super();

    // Dummy final assignments.
    inputCol = -1;
  }

  /**
   * After construction you must call setInListValues() to add the values to the IN set.
   */
  public TimestampColumnInList(int colNum, int outputColumnNum) {
    super(outputColumnNum);
    this.inputCol = colNum;
    inSet = null;
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) {

    if (childExpressions != null) {
      super.evaluateChildren(batch);
    }

    if (inSet == null) {
      inSet = new HashSet<Timestamp>(inListValues.length);
      for (Timestamp val : inListValues) {
        inSet.add(val);
      }
    }

    TimestampColumnVector inputColVector = (TimestampColumnVector) batch.cols[inputCol];
    LongColumnVector outputColVector = (LongColumnVector) batch.cols[outputColumnNum];
    int[] sel = batch.selected;
    boolean[] inputIsNull = inputColVector.isNull;
    boolean[] outputIsNull = outputColVector.isNull;
    int n = batch.size;
    long[] outputVector = outputColVector.vector;

    // return immediately if batch is empty
    if (n == 0) {
      return;
    }

    // We do not need to do a column reset since we are carefully changing the output.
    outputColVector.isRepeating = false;

    if (inputColVector.isRepeating) {
      if (inputColVector.noNulls || !inputIsNull[0]) {
        // Set isNull before call in case it changes it mind.
        outputIsNull[0] = false;
        outputVector[0] = inSet.contains(inputColVector.asScratchTimestamp(0)) ? 1 : 0;
      } else {
        outputIsNull[0] = true;
        outputColVector.noNulls = false;
      }
      outputColVector.isRepeating = true;
      return;
    }

    if (inputColVector.noNulls) {
      if (batch.selectedInUse) {

        // CONSIDER: For large n, fill n or all of isNull array and use the tighter ELSE loop.

        if (!outputColVector.noNulls) {
          for(int j = 0; j != n; j++) {
           final int i = sel[j];
           // Set isNull before call in case it changes it mind.
           outputIsNull[i] = false;
           outputVector[i] = inSet.contains(inputColVector.asScratchTimestamp(i)) ? 1 : 0;
         }
        } else {
          for(int j = 0; j != n; j++) {
            final int i = sel[j];
            outputVector[i] = inSet.contains(inputColVector.asScratchTimestamp(i)) ? 1 : 0;
          }
        }
      } else {
        if (!outputColVector.noNulls) {

          // Assume it is almost always a performance win to fill all of isNull so we can
          // safely reset noNulls.
          Arrays.fill(outputIsNull, false);
          outputColVector.noNulls = true;
        }
        for(int i = 0; i != n; i++) {
          outputVector[i] = inSet.contains(inputColVector.asScratchTimestamp(i)) ? 1 : 0;
        }
      }
    } else /* there are nulls in the inputColVector */ {

      // Carefully handle NULLs...
      outputColVector.noNulls = false;

      if (batch.selectedInUse) {
        for(int j = 0; j != n; j++) {
          int i = sel[j];
          outputIsNull[i] = inputIsNull[i];
          if (!inputIsNull[i]) {
            outputVector[i] = inSet.contains(inputColVector.asScratchTimestamp(i)) ? 1 : 0;
          }
        }
      } else {
        System.arraycopy(inputIsNull, 0, outputIsNull, 0, n);
        for(int i = 0; i != n; i++) {
          if (!inputIsNull[i]) {
            outputVector[i] = inSet.contains(inputColVector.asScratchTimestamp(i)) ? 1 : 0;
          }
        }
      }
    }
  }

  @Override
  public Descriptor getDescriptor() {

    // This VectorExpression (IN) is a special case, so don't return a descriptor.
    return null;
  }

  public void setInListValues(Timestamp[] a) {
    this.inListValues = a;
  }

  @Override
  public String vectorExpressionParameters() {
    return getColumnParamString(0, inputCol) + ", values " + Arrays.toString(inListValues);
  }
}
