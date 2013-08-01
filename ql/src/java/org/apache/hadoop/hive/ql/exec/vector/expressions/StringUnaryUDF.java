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

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.udf.IUDFUnaryString;
import org.apache.hadoop.io.Text;

/**
 * Expression for vectorized evaluation of unary UDFs on strings.
 * An object of {@link IUDFUnaryString} is applied to every element of
 * the vector.
 */
public class StringUnaryUDF extends VectorExpression {
  private final int colNum;
  private final int outputColumn;
  private final IUDFUnaryString func;
  private final Text s;

  StringUnaryUDF(int colNum, int outputColumn, IUDFUnaryString func) {
    this.colNum = colNum;
    this.outputColumn = outputColumn;
    this.func = func;
    s = new Text();
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) {

    if (childExpressions != null) {
      super.evaluateChildren(batch);
    }

    BytesColumnVector inputColVector = (BytesColumnVector) batch.cols[colNum];
    int[] sel = batch.selected;
    int n = batch.size;
    byte[][] vector = inputColVector.vector;
    int [] start = inputColVector.start;
    int [] length = inputColVector.length;
    BytesColumnVector outV = (BytesColumnVector) batch.cols[outputColumn];
    Text t;

    if (n == 0) {
      //Nothing to do
      return;
    }

    // Design Note: In the future, if this function can be implemented
    // directly to translate input to output without creating new
    // objects, performance can probably be improved significantly.
    // It's implemented in the simplest way now, just calling the
    // existing built-in function.

    if (inputColVector.noNulls) {
      outV.noNulls = true;
      if (inputColVector.isRepeating) {
        outV.isRepeating = true;
        s.set(vector[0], start[0], length[0]);
        t = func.evaluate(s);
        outV.setRef(0, t.getBytes(), 0, t.getLength());
      } else if (batch.selectedInUse) {
        for(int j=0; j != n; j++) {
          int i = sel[j];
          s.set(vector[i], start[i], length[i]);
          t = func.evaluate(s);
          outV.setRef(i, t.getBytes(), 0, t.getLength());
        }
        outV.isRepeating = false;
      } else {
        for(int i = 0; i != n; i++) {
          s.set(vector[i], start[i], length[i]);
          t = func.evaluate(s);
          outV.setRef(i, t.getBytes(), 0, t.getLength());
        }
        outV.isRepeating = false;
      }
    } else {
      // Handle case with nulls. Don't do function if the value is null, to save time,
      // because calling the function can be expensive.
      outV.noNulls = false;
      if (inputColVector.isRepeating) {
        outV.isRepeating = true;
        outV.isNull[0] = inputColVector.isNull[0];
        if (!inputColVector.isNull[0]) {
          s.set(vector[0], start[0], length[0]);
          t = func.evaluate(s);
          outV.setRef(0, t.getBytes(), 0, t.getLength());
        }
      } else if (batch.selectedInUse) {
        for(int j=0; j != n; j++) {
          int i = sel[j];
          if (!inputColVector.isNull[i]) {
            s.set(vector[i], start[i], length[i]);
            t = func.evaluate(s);
            outV.setRef(i, t.getBytes(), 0, t.getLength());
          }
          outV.isNull[i] = inputColVector.isNull[i];
        }
        outV.isRepeating = false;
      } else {
        for(int i = 0; i != n; i++) {
          if (!inputColVector.isNull[i]) {
            s.set(vector[i], start[i], length[i]);
            t = func.evaluate(s);
            outV.setRef(i, t.getBytes(), 0, t.getLength());
          }
          outV.isNull[i] = inputColVector.isNull[i];
        }
        outV.isRepeating = false;
      }
    }
  }

  @Override
  public int getOutputColumn() {
    return outputColumn;
  }

  @Override
  public String getOutputType() {
    return "String";
  }


}
