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

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor.Descriptor;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFLike;
import org.apache.hadoop.io.Text;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Evaluate an IN boolean expression (not a filter) on a batch for a vector of strings.
 * This is optimized so that no objects have to be created in
 * the inner loop, and there is a hash table implemented
 * with Cuckoo hashing that has fast lookup to do the IN test.
 */
public class StringColumnInList extends VectorExpression implements IStringInExpr {
  private static final long serialVersionUID = 1L;
  private byte[][] inListValues;

  // The set object containing the IN list. This is optimized for lookup
  // of the data type of the column.
  private transient CuckooSetBytes inSet;

  public StringColumnInList() {
    super();
  }

  /**
   * After construction you must call setInListValues() to add the values to the IN set.
   */
  public StringColumnInList(int colNum, int outputColumnNum) {
    super(colNum, outputColumnNum);
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

    BytesColumnVector inputColVector = (BytesColumnVector) batch.cols[inputColumnNum[0]];
    LongColumnVector outputColVector = (LongColumnVector) batch.cols[outputColumnNum];
    int[] sel = batch.selected;
    boolean[] inputIsNull = inputColVector.isNull;
    int n = batch.size;
    byte[][] vector = inputColVector.vector;
    int[] start = inputColVector.start;
    int[] len = inputColVector.length;
    long[] outputVector = outputColVector.vector;
    boolean[] outputIsNull = outputColVector.isNull;

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
        outputVector[0] = inSet.lookup(vector[0], start[0], len[0]) ? 1 : 0;
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
           outputVector[i] = inSet.lookup(vector[i], start[i], len[i]) ? 1 : 0;
         }
        } else {
          for(int j = 0; j != n; j++) {
            final int i = sel[j];
            outputVector[i] = inSet.lookup(vector[i], start[i], len[i]) ? 1 : 0;
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
          outputVector[i] = inSet.lookup(vector[i], start[i], len[i]) ? 1 : 0;
        }
      }
    } else /* there are nulls in the inputColVector */ {

      // Carefully handle NULLs...
      outputColVector.noNulls = false;

      if (batch.selectedInUse) {
        for(int j = 0; j != n; j++) {
          int i = sel[j];
          outputColVector.isNull[i] = inputIsNull[i];
          if (!inputIsNull[i]) {
            outputVector[i] = inSet.lookup(vector[i], start[i], len[i]) ? 1 : 0;
          }
        }
      } else {
        System.arraycopy(inputIsNull, 0, outputColVector.isNull, 0, n);
        for(int i = 0; i != n; i++) {
          if (!inputIsNull[i]) {
            outputVector[i] = inSet.lookup(vector[i], start[i], len[i]) ? 1 : 0;
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

  public void setInListValues(byte [][] a) {
    this.inListValues = a;
  }

  @Override
  public String vectorExpressionParameters() {
    StringBuilder sb = new StringBuilder();
    sb.append("col ");
    sb.append(inputColumnNum[0]);
    sb.append(", values ");
    sb.append(displayArrayOfUtf8ByteArrays(inListValues));
    return sb.toString();
  }
}
