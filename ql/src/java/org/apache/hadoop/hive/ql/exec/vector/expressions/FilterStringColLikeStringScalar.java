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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hive.ql.udf.UDFLike;

/**
 * Evaluate LIKE filter on a batch for a vector of strings.
 */
public class FilterStringColLikeStringScalar extends VectorExpression {
  private int colNum;
  private Text likePattern;
  private Text s;
  private UDFLike likeFunc;

  public FilterStringColLikeStringScalar(int colNum, Text likePattern) {
    this.colNum = colNum;
    this.likePattern = likePattern;
    likeFunc = new UDFLike();
    s = new Text();
  }

  /*
   * This vectorized version of LIKE calls the standard LIKE
   * function code. In the future, as an optimization, consider
   * unwinding some of that logic here, e.g. to determine
   * if the LIKE pattern is a simple one like 'abc%' so that
   * can be executed more efficiently as a special case.
   */

  private boolean like(byte[] bytes, int start, int len) {
    s.set(bytes, start, len);
    return (likeFunc.evaluate(s, likePattern)).get();
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) {
    BytesColumnVector inputColVector = (BytesColumnVector) batch.cols[colNum];
    int[] sel = batch.selected;
    boolean[] nullPos = inputColVector.isNull;
    int n = batch.size;
    byte[][] vector = inputColVector.vector;
    int[] length = inputColVector.length;
    int[] start = inputColVector.start;


    // return immediately if batch is empty
    if (n == 0) {
      return;
    }

    if (inputColVector.noNulls) {
      if (inputColVector.isRepeating) {

        // All must be selected otherwise size would be zero Repeating property will not change.
        if (!like(vector[0], start[0], length[0])) {

          //Entire batch is filtered out.
          batch.size = 0;
        }
      } else if (batch.selectedInUse) {
        int newSize = 0;
        for(int j=0; j != n; j++) {
          int i = sel[j];
          if (like(vector[i], start[i], length[i])) {
            sel[newSize++] = i;
          }
        }
        batch.size = newSize;
      } else {
        int newSize = 0;
        for(int i = 0; i != n; i++) {
          if (like(vector[i], start[i], length[i])) {
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

        //All must be selected otherwise size would be zero. Repeating property will not change.
        if (!nullPos[0]) {
          if (!like(vector[0], start[0], length[0])) {

            //Entire batch is filtered out.
            batch.size = 0;
          }
        } else {
          batch.size = 0;
        }
      } else if (batch.selectedInUse) {
        int newSize = 0;
        for(int j=0; j != n; j++) {
          int i = sel[j];
          if (!nullPos[i]) {
           if (like(vector[i], start[i], length[i])) {
             sel[newSize++] = i;
           }
          }
        }

        //Change the selected vector
        batch.size = newSize;
      } else {
        int newSize = 0;
        for(int i = 0; i != n; i++) {
          if (!nullPos[i]) {
            if (like(vector[i], start[i], length[i])) {
              sel[newSize++] = i;
            }
          }
        }
        if (newSize < n) {
          batch.size = newSize;
          batch.selectedInUse = true;
        }

        /* If every row qualified (newSize==n), then we can ignore the sel vector to streamline
         * future operations. So selectedInUse will remain false.
         */
      }
    }
  }

  @Override
  public int getOutputColumn() {
    return -1;
  }

  @Override
  public String getOutputType() {
    return "boolean";
  }
}
