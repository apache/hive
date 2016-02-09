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
import java.util.HashSet;

import org.apache.hadoop.hive.common.type.PisaTimestamp;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor.Descriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

/**
 * Evaluate IN filter on a batch for a vector of timestamps.
 */
public class FilterTimestampColumnInList extends VectorExpression implements ITimestampInExpr {
  private static final long serialVersionUID = 1L;
  private int inputCol;
  private Timestamp[] inListValues;

  // The set object containing the IN list.
  private transient HashSet<PisaTimestamp> inSet;

  public FilterTimestampColumnInList() {
    super();
    inSet = null;
  }

  /**
   * After construction you must call setInListValues() to add the values to the IN set.
   */
  public FilterTimestampColumnInList(int colNum) {
    this.inputCol = colNum;
    inSet = null;
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) {

    if (childExpressions != null) {
      super.evaluateChildren(batch);
    }

    if (inSet == null) {
      inSet = new HashSet<PisaTimestamp>(inListValues.length);
      for (Timestamp val : inListValues) {
        inSet.add(new PisaTimestamp(val));
      }
    }

    TimestampColumnVector inputColVector = (TimestampColumnVector) batch.cols[inputCol];
    int[] sel = batch.selected;
    boolean[] nullPos = inputColVector.isNull;
    int n = batch.size;

    // return immediately if batch is empty
    if (n == 0) {
      return;
    }

    PisaTimestamp scratchTimestamp = new PisaTimestamp();

    if (inputColVector.noNulls) {
      if (inputColVector.isRepeating) {

        // All must be selected otherwise size would be zero
        // Repeating property will not change.

        inputColVector.pisaTimestampUpdate(scratchTimestamp, 0);
        if (!(inSet.contains(scratchTimestamp))) {
          //Entire batch is filtered out.
          batch.size = 0;
        }
      } else if (batch.selectedInUse) {
        int newSize = 0;
        for(int j = 0; j != n; j++) {
          int i = sel[j];
          inputColVector.pisaTimestampUpdate(scratchTimestamp, i);
          if (inSet.contains(scratchTimestamp)) {
            sel[newSize++] = i;
          }
        }
        batch.size = newSize;
      } else {
        int newSize = 0;
        for(int i = 0; i != n; i++) {
          inputColVector.pisaTimestampUpdate(scratchTimestamp, i);
          if (inSet.contains(scratchTimestamp)) {
            sel[newSize++] = i;
          }
        }
        if (newSize < n) {
          batch.size = newSize;
          batch.selectedInUse = true;
        }
      }
    } else {
      if (inputColVector.isRepeating) {

        //All must be selected otherwise size would be zero
        //Repeating property will not change.
        if (!nullPos[0]) {
          inputColVector.pisaTimestampUpdate(scratchTimestamp, 0);
          if (!inSet.contains(scratchTimestamp)) {

            //Entire batch is filtered out.
            batch.size = 0;
          }
        } else {
          batch.size = 0;
        }
      } else if (batch.selectedInUse) {
        int newSize = 0;
        for(int j = 0; j != n; j++) {
          int i = sel[j];
          if (!nullPos[i]) {
            inputColVector.pisaTimestampUpdate(scratchTimestamp, i);
           if (inSet.contains(scratchTimestamp)) {
             sel[newSize++] = i;
           }
          }
        }

        // Change the selected vector
        batch.size = newSize;
      } else {
        int newSize = 0;
        for(int i = 0; i != n; i++) {
          if (!nullPos[i]) {
            inputColVector.pisaTimestampUpdate(scratchTimestamp, i);
            if (inSet.contains(scratchTimestamp)) {
              sel[newSize++] = i;
            }
          }
        }
        if (newSize < n) {
          batch.size = newSize;
          batch.selectedInUse = true;
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
    return -1;
  }

  @Override
  public Descriptor getDescriptor() {

    // This VectorExpression (IN) is a special case, so don't return a descriptor.
    return null;
  }

  public void setInListValues(Timestamp[] a) {
    this.inListValues = a;
  }
}
