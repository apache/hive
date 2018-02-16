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

import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor.Descriptor;
import org.apache.hadoop.hive.ql.exec.vector.expressions.AbstractFilterStringColLikeStringScalar.Checker;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

public class SelectStringColLikeStringScalar extends VectorExpression {

  private static final long serialVersionUID = 1L;

  private final int colNum;

  private byte[] pattern;

  transient Checker checker = null;

  public SelectStringColLikeStringScalar() {
    super();

    // Dummy final assignments.
    colNum = -1;
  }

  public SelectStringColLikeStringScalar(int colNum, byte[] pattern, int outputColumnNum) {
    super(outputColumnNum);
    this.colNum = colNum;
    this.pattern = pattern;
  }

  @Override
	public void evaluate(VectorizedRowBatch batch) {
    if (checker == null) {
      checker = borrowChecker();
    }
    
    if (childExpressions != null) {
      super.evaluateChildren(batch);
    }

    BytesColumnVector inputColVector = (BytesColumnVector) batch.cols[colNum];
    int[] sel = batch.selected;
    boolean[] nullPos = inputColVector.isNull;
    int n = batch.size;
    byte[][] vector = inputColVector.vector;
    int[] length = inputColVector.length;
    int[] start = inputColVector.start;

    LongColumnVector outV = (LongColumnVector) batch.cols[outputColumnNum];
    long[] outputVector = outV.vector;
    boolean[] inputIsNull = inputColVector.isNull;
    boolean[] outputIsNull = outV.isNull;

    // return immediately if batch is empty
    if (n == 0) {
      return;
    }

    // We do not need to do a column reset since we are carefully changing the output.
    outV.isRepeating = false;

    if (inputColVector.isRepeating) {
      if (inputColVector.noNulls || !inputIsNull[0]) {
        // Set isNull before call in case it changes it mind.
        outputIsNull[0] = false;
        outputVector[0] = (checker.check(vector[0], start[0], length[0]) ? 1 : 0);
      } else {
        outputIsNull[0] = true;
        outV.noNulls = false;
      }
      outV.isRepeating = true;
      return;
    }

    if (inputColVector.noNulls) {
      if (batch.selectedInUse) {
        for (int j = 0; j != n; j++) {
          int i = sel[j];
          outV.isNull[i] = false;
          outputVector[i] = (checker.check(vector[i], start[i], length[i]) ? 1 : 0);
        }
      } else {
        Arrays.fill(outV.isNull, 0, n, false);
        for (int i = 0; i != n; i++) {
          outputVector[i] = (checker.check(vector[i], start[i], length[i]) ? 1 : 0);
        }
      }
    } else /* there are nulls in the inputColVector */ {

      /*
       * Do careful maintenance of the outputColVector.noNulls flag.
       */

      if (batch.selectedInUse) {
        for (int j = 0; j != n; j++) {
          int i = sel[j];
          if (!nullPos[i]) {
            outputVector[i] = (checker.check(vector[i], start[i], length[i]) ? 1 : 0);
            outV.isNull[i] = false;
          } else {
            outputVector[i] = LongColumnVector.NULL_VALUE;
            outV.isNull[i] = true;
            outV.noNulls = false;
          }
        }
      } else {
        for (int i = 0; i != n; i++) {
          if (!nullPos[i]) {
            outputVector[i] = (checker.check(vector[i], start[i], length[i]) ? 1 : 0);
            outV.isNull[i] = false;
          } else {
            outputVector[i] = LongColumnVector.NULL_VALUE;
            outV.isNull[i] = true;
            outV.noNulls = false;
          }
        }
      }
    }
  }

  private Checker borrowChecker() {
    FilterStringColLikeStringScalar fil = new FilterStringColLikeStringScalar();
    return fil.createChecker(new String(pattern, StandardCharsets.UTF_8));
  }

  public void setPattern(byte[] pattern) {
    this.pattern = pattern;
  }

  public String vectorExpressionParameters() {
    return getColumnParamString(0, colNum);
  }

  @Override
  public Descriptor getDescriptor() {
      return (new VectorExpressionDescriptor.Builder())
          .setMode(
              VectorExpressionDescriptor.Mode.PROJECTION)
          .setNumArguments(2)
          .setArgumentTypes(
              VectorExpressionDescriptor.ArgumentType.STRING_FAMILY,
              VectorExpressionDescriptor.ArgumentType.STRING)
          .setInputExpressionTypes(
              VectorExpressionDescriptor.InputExpressionType.COLUMN,
              VectorExpressionDescriptor.InputExpressionType.SCALAR).build();
  }
}
