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

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor.Descriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.metadata.HiveException;

/**
 * Evaluate an IN filter on a batch for a vector of strings.
 * This is optimized so that no objects have to be created in
 * the inner loop, and there is a hash table implemented
 * with Cuckoo hashing that has fast lookup to do the IN test.
 */
public class FilterStringColumnInList extends VectorExpression implements IStringInExpr {
  private static final long serialVersionUID = 1L;
  private int inputCol;
  private byte[][] inListValues;

  // The set object containing the IN list. This is optimized for lookup
  // of the data type of the column.
  private transient CuckooSetBytes inSet;

  public FilterStringColumnInList() {
    super();
    inSet = null;
  }

  /**
   * After construction you must call setInListValues() to add the values to the IN set.
   */
  public FilterStringColumnInList(int colNum) {
    this.inputCol = colNum;
    inSet = null;
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) throws HiveException {

    if (childExpressions != null) {
      super.evaluateChildren(batch);
    }

    if (inSet == null) {
      inSet = new CuckooSetBytes(inListValues.length);
      inSet.load(inListValues);
    }

    BytesColumnVector inputColVector = (BytesColumnVector) batch.cols[inputCol];
    int[] sel = batch.selected;
    boolean[] nullPos = inputColVector.isNull;
    int n = batch.size;
    byte[][] vector = inputColVector.vector;
    int[] start = inputColVector.start;
    int[] len = inputColVector.length;

    // return immediately if batch is empty
    if (n == 0) {
      return;
    }

    if (inputColVector.noNulls) {
      if (inputColVector.isRepeating) {

        // All must be selected otherwise size would be zero
        // Repeating property will not change.
        if (!(inSet.lookup(vector[0], start[0], len[0]))) {

          // Entire batch is filtered out.
          batch.size = 0;
        }
      } else if (batch.selectedInUse) {
        int newSize = 0;
        for(int j = 0; j != n; j++) {
          int i = sel[j];
          if (inSet.lookup(vector[i], start[i], len[i])) {
            sel[newSize++] = i;
          }
        }
        batch.size = newSize;
      } else {
        int newSize = 0;
        for(int i = 0; i != n; i++) {
          if (inSet.lookup(vector[i], start[i], len[i])) {
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

        // All must be selected otherwise size would be zero
        // Repeating property will not change.
        if (!nullPos[0]) {
          if (!inSet.lookup(vector[0], start[0], len[0])) {

            // Entire batch is filtered out.
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
           if (inSet.lookup(vector[i], start[i], len[i] )) {
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
            if (inSet.lookup(vector[i], start[i], len[i])) {
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

  public void setInputColumn(int inputCol) {
    this.inputCol = inputCol;
  }

  @Override
  public Descriptor getDescriptor() {

    // This VectorExpression (IN) is a special case, so don't return a descriptor.
    return null;
  }

  public byte[][] getInListValues() {
    return this.inListValues;
  }

  public void setInListValues(byte [][] a) {
    this.inListValues = a;
  }

  @Override
  public String vectorExpressionParameters() {
    StringBuilder sb = new StringBuilder();
    sb.append("col ");
    sb.append(inputCol);
    sb.append(", values ");
    sb.append(displayArrayOfUtf8ByteArrays(inListValues));
    return sb.toString();
  }
}
