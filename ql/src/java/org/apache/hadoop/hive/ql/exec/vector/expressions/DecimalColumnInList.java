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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor.Descriptor;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;

import java.util.Arrays;
import java.util.HashSet;

/**
 * Output a boolean value indicating if a column is IN a list of constants.
 */
public class DecimalColumnInList extends VectorExpression implements IDecimalInExpr {
  private static final long serialVersionUID = 1L;
  private HiveDecimal[] inListValues;

  // The set object containing the IN list.
  // We use a HashSet of HiveDecimalWritable objects instead of HiveDecimal objects so
  // we can lookup DecimalColumnVector HiveDecimalWritable quickly without creating
  // a HiveDecimal lookup object.
  private transient HashSet<HiveDecimalWritable> inSet;

  public DecimalColumnInList() {
    super();
  }

  /**
   * After construction you must call setInListValues() to add the values to the IN set.
   */
  public DecimalColumnInList(int colNum, int outputColumnNum) {
    super(colNum, outputColumnNum);
  }

  @Override
  public void transientInit(Configuration conf) throws HiveException {
    super.transientInit(conf);

    inSet = new HashSet<HiveDecimalWritable>(inListValues.length);
    for (HiveDecimal val : inListValues) {
      inSet.add(new HiveDecimalWritable(val));
    }
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) throws HiveException {

    if (childExpressions != null) {
      super.evaluateChildren(batch);
    }

    DecimalColumnVector inputColumnVector = (DecimalColumnVector) batch.cols[inputColumnNum[0]];
    LongColumnVector outputColVector = (LongColumnVector) batch.cols[outputColumnNum];
    int[] sel = batch.selected;
    boolean[] inputIsNull = inputColumnVector.isNull;
    boolean[] outputIsNull = outputColVector.isNull;
    int n = batch.size;
    HiveDecimalWritable[] vector = inputColumnVector.vector;
    long[] outputVector = outputColVector.vector;

    // return immediately if batch is empty
    if (n == 0) {
      return;
    }

    // We do not need to do a column reset since we are carefully changing the output.
    outputColVector.isRepeating = false;

    if (inputColumnVector.isRepeating) {
      if (inputColumnVector.noNulls || !inputIsNull[0]) {
        outputIsNull[0] = false;
        outputVector[0] = inSet.contains(vector[0]) ? 1 : 0;
      } else {
        outputIsNull[0] = true;
        outputColVector.noNulls = false;
      }
      outputColVector.isRepeating = true;
      return;
    }

    if (inputColumnVector.noNulls) {
      if (batch.selectedInUse) {

        // CONSIDER: For large n, fill n or all of isNull array and use the tighter ELSE loop.

        if (!outputColVector.noNulls) {
          for(int j = 0; j != n; j++) {
           final int i = sel[j];
           // Set isNull before call in case it changes it mind.
           outputIsNull[i] = false;
           outputVector[i] = inSet.contains(vector[i]) ? 1 : 0;
         }
        } else {
          for(int j = 0; j != n; j++) {
            final int i = sel[j];
            outputVector[i] = inSet.contains(vector[i]) ? 1 : 0;
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
          outputVector[i] = inSet.contains(vector[i]) ? 1 : 0;
        }
      }
    } else /* there are NULLs in the inputColVector */ {

      // Carefully handle NULLs...
      outputColVector.noNulls = false;

      if (batch.selectedInUse) {
        for(int j = 0; j != n; j++) {
          int i = sel[j];
          outputIsNull[i] = inputIsNull[i];
          if (!inputIsNull[i]) {
            outputVector[i] = inSet.contains(vector[i]) ? 1 : 0;
          }
        }
      } else {
        System.arraycopy(inputIsNull, 0, outputIsNull, 0, n);
        for(int i = 0; i != n; i++) {
          if (!inputIsNull[i]) {
            outputVector[i] = inSet.contains(vector[i]) ? 1 : 0;
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

  public void setInListValues(HiveDecimal[] a) {
    this.inListValues = a;
  }

  @Override
  public String vectorExpressionParameters() {
    return getColumnParamString(0, inputColumnNum[0]) + ", values " + Arrays.toString(inListValues);
  }

}
