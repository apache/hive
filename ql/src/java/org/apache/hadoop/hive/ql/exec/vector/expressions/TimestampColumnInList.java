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
  private int inputCol;
  private Timestamp[] inListValues;
  private int outputColumn;

  // The set object containing the IN list.
  private transient HashSet<Timestamp> inSet;

  public TimestampColumnInList() {
    super();
    inSet = null;
  }

  /**
   * After construction you must call setInListValues() to add the values to the IN set.
   */
  public TimestampColumnInList(int colNum, int outputColumn) {
    this.inputCol = colNum;
    this.outputColumn = outputColumn;
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
    LongColumnVector outputColVector = (LongColumnVector) batch.cols[outputColumn];
    int[] sel = batch.selected;
    boolean[] nullPos = inputColVector.isNull;
    boolean[] outNulls = outputColVector.isNull;
    int n = batch.size;
    long[] outputVector = outputColVector.vector;

    // return immediately if batch is empty
    if (n == 0) {
      return;
    }

    outputColVector.isRepeating = false;
    outputColVector.noNulls = inputColVector.noNulls;
    if (inputColVector.noNulls) {
      if (inputColVector.isRepeating) {

        // All must be selected otherwise size would be zero
        // Repeating property will not change.
        outputVector[0] = inSet.contains(inputColVector.asScratchTimestamp(0)) ? 1 : 0;
        outputColVector.isRepeating = true;
      } else if (batch.selectedInUse) {
        for(int j = 0; j != n; j++) {
          int i = sel[j];
          outputVector[i] = inSet.contains(inputColVector.asScratchTimestamp(i)) ? 1 : 0;
        }
      } else {
        for(int i = 0; i != n; i++) {
          outputVector[i] = inSet.contains(inputColVector.asScratchTimestamp(i)) ? 1 : 0;
        }
      }
    } else {
      if (inputColVector.isRepeating) {

        //All must be selected otherwise size would be zero
        //Repeating property will not change.
        if (!nullPos[0]) {
          outputVector[0] = inSet.contains(inputColVector.asScratchTimestamp(0)) ? 1 : 0;
          outNulls[0] = false;
        } else {
          outNulls[0] = true;
        }
        outputColVector.isRepeating = true;
      } else if (batch.selectedInUse) {
        for(int j = 0; j != n; j++) {
          int i = sel[j];
          outNulls[i] = nullPos[i];
          if (!nullPos[i]) {
            outputVector[i] = inSet.contains(inputColVector.asScratchTimestamp(i)) ? 1 : 0;
          }
        }
      } else {
        System.arraycopy(nullPos, 0, outNulls, 0, n);
        for(int i = 0; i != n; i++) {
          if (!nullPos[i]) {
            outputVector[i] = inSet.contains(inputColVector.asScratchTimestamp(i)) ? 1 : 0;
          }
        }
      }
    }
  }


  @Override
  public String getOutputType() {
    return "boolean";
  }

  @Override
  public int getOutputColumn() {
    return outputColumn;
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
    return "col " + inputCol + ", values " + Arrays.toString(inListValues);
  }
}
