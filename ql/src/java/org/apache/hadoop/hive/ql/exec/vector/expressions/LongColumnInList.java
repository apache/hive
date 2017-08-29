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

import java.util.Arrays;

import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;

/**
 * Output a boolean value indicating if a column is IN a list of constants.
 */
public class LongColumnInList extends VectorExpression implements ILongInExpr {

  private static final long serialVersionUID = 1L;

  private int colNum;
  private int outputColumn;
  private long[] inListValues;

  // The set object containing the IN list. This is optimized for lookup
  // of the data type of the column.
  private transient CuckooSetLong inSet;

  public LongColumnInList(int colNum, int outputColumn) {
    this.colNum = colNum;
    this.outputColumn = outputColumn;
  }

  public LongColumnInList() {
    super();
    inSet = null;
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) {

    if (childExpressions != null) {
      super.evaluateChildren(batch);
    }

    if (inSet == null) {
      inSet = new CuckooSetLong(inListValues.length);
      inSet.load(inListValues);
    }

    LongColumnVector inputColVector = (LongColumnVector) batch.cols[colNum];
    LongColumnVector outputColVector = (LongColumnVector) batch.cols[outputColumn];
    int[] sel = batch.selected;
    boolean[] nullPos = inputColVector.isNull;
    boolean[] outNulls = outputColVector.isNull;
    int n = batch.size;
    long[] vector = inputColVector.vector;
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
        outputVector[0] = inSet.lookup(vector[0]) ? 1 : 0;
        outputColVector.isRepeating = true;
      } else if (batch.selectedInUse) {
        for(int j = 0; j != n; j++) {
          int i = sel[j];
          outputVector[i] = inSet.lookup(vector[i]) ? 1 : 0;
        }
      } else {
        for(int i = 0; i != n; i++) {
          outputVector[i] = inSet.lookup(vector[i]) ? 1 : 0;
        }
      }
    } else {
      if (inputColVector.isRepeating) {

        // All must be selected otherwise size would be zero
        // Repeating property will not change.
        if (!nullPos[0]) {
          outputVector[0] = inSet.lookup(vector[0]) ? 1 : 0;
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
            outputVector[i] = inSet.lookup(vector[i]) ? 1 : 0;
          }
        }
      } else {
        System.arraycopy(nullPos, 0, outNulls, 0, n);
        for(int i = 0; i != n; i++) {
          if (!nullPos[i]) {
            outputVector[i] = inSet.lookup(vector[i]) ? 1 : 0;
          }
        }
      }
    }
  }

  @Override
  public int getOutputColumn() {
    return outputColumn;
  }

  @Override
  public String getOutputType() {
    return "boolean";
  }

  public int getColNum() {
    return colNum;
  }

  public void setColNum(int colNum) {
    this.colNum = colNum;
  }

  public void setOutputColumn(int outputColumn) {
    this.outputColumn = outputColumn;
  }

  public long[] getInListValues() {
    return this.inListValues;
  }

  public void setInListValues(long [] a) {
    this.inListValues = a;
  }

  public String vectorExpressionParameters() {
    return "col " + colNum + ", values " + Arrays.toString(inListValues);
  }

  @Override
  public VectorExpressionDescriptor.Descriptor getDescriptor() {

    // return null since this will be handled as a special case in VectorizationContext
    return null;
  }
}
