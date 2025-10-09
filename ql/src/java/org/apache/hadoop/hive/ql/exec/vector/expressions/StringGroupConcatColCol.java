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

import java.util.Arrays;

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.metadata.HiveException;

/**
 * Vectorized instruction to concatenate two string columns and put
 * the output in a third column.
 */
public class StringGroupConcatColCol extends VectorExpression {
  private static final long serialVersionUID = 1L;

  public StringGroupConcatColCol(int colNum1, int colNum2, int outputColumnNum) {
    super(colNum1, colNum2, outputColumnNum);
  }

  public StringGroupConcatColCol() {
    super();
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) throws HiveException {

    if (childExpressions != null) {
      super.evaluateChildren(batch);
    }

    BytesColumnVector inV1 = (BytesColumnVector) batch.cols[inputColumnNum[0]];
    BytesColumnVector inV2 = (BytesColumnVector) batch.cols[inputColumnNum[1]];
    BytesColumnVector outV = (BytesColumnVector) batch.cols[outputColumnNum];
    boolean[] outputIsNull = outV.isNull;
    int[] sel = batch.selected;
    int n = batch.size;
    byte[][] vector1 = inV1.vector;
    byte[][] vector2 = inV2.vector;
    int[] len1 = inV1.length;
    int[] len2 = inV2.length;
    int[] start1 = inV1.start;
    int[] start2 = inV2.start;

    // return immediately if batch is empty
    if (n == 0) {
      return;
    }

    // prepare output buffer to accept results
    outV.initBuffer();

    /* Handle default case for isRepeating setting for output. This will be set to true
     * later in the special cases where that is necessary.
     */
    outV.isRepeating = false;

    if (inV1.noNulls && !inV2.noNulls) {

      // Carefully handle NULLs...

      /* We'll assume that there *may* be nulls in the input if !noNulls is true
       * for an input vector. This is to be more forgiving of errors in loading
       * the vectors. A properly-written vectorized iterator will make sure that
       * isNull[0] is set if !noNulls and isRepeating are true for the vector.
       */
      outV.noNulls = false;

      if (inV2.isRepeating) {
        if (inV2.isNull[0]) {

          // Output will also be repeating and null
          outV.isNull[0] = true;
          outV.isRepeating = true;

          //return as no further processing is needed
          return;
        }
      } else {
        propagateNulls(batch.selectedInUse, n, sel, inV2, outV);
      }

      // perform data operation
      if (inV1.isRepeating && inV2.isRepeating) {

        /* All must be selected otherwise size would be zero.
         * Repeating property will not change.
         */
        if (!inV2.isNull[0]) {
          outV.setConcat(0, vector1[0], start1[0], len1[0], vector2[0], start2[0], len2[0]);
        }
        outV.isRepeating = true;
      } else if (inV1.isRepeating) {
        if (batch.selectedInUse) {
          for(int j = 0; j != n; j++) {
            int i = sel[j];
            if (!inV2.isNull[i]) {
              outV.setConcat(i, vector1[0], start1[0], len1[0], vector2[i], start2[i], len2[i]);
            }
          }
        } else {
          for(int i = 0; i != n; i++) {
            if (!inV2.isNull[i]) {
              outV.setConcat(i, vector1[0], start1[0], len1[0], vector2[i], start2[i], len2[i]);
            }
          }
        }
      } else if (inV2.isRepeating) {
        if (batch.selectedInUse) {
          for(int j = 0; j != n; j++) {
            int i = sel[j];
            if (!inV2.isNull[i]) {
              outV.setConcat(i, vector1[i], start1[i], len1[i], vector2[0], start2[0], len2[0]);
            }
          }
        } else {
          for(int i = 0; i != n; i++) {
            if (!inV2.isNull[i]) {
              outV.setConcat(i, vector1[i], start1[i], len1[i], vector2[0], start2[0], len2[0]);
            }
          }
        }
      } else {
        if (batch.selectedInUse) {
          for(int j=0; j != n; j++) {
            int i = sel[j];
            if (!inV2.isNull[i]) {
              outV.setConcat(i, vector1[i], start1[i], len1[i], vector2[i], start2[i], len2[i]);
            }
          }
        } else {
          for(int i = 0; i != n; i++) {
            if (!inV2.isNull[i]) {
              outV.setConcat(i, vector1[i], start1[i], len1[i], vector2[i], start2[i], len2[i]);
            }
          }
        }
      }
    } else if (!inV1.noNulls && inV2.noNulls) {

      // propagate nulls
      outV.noNulls = false;
      if (inV1.isRepeating) {

        //Output will also be repeating and null
        outV.isRepeating = true;
        outV.isNull[0] = true;

        //return as no further processing is needed
        return;
      } else {
        propagateNulls(batch.selectedInUse, n, sel, inV1, outV);
      }

      // perform data operation
      if (inV1.isRepeating && inV2.isRepeating) {
        //All must be selected otherwise size would be zero
        //Repeating property will not change.
        if (!inV1.isNull[0]) {
          outV.setConcat(0, vector1[0], start1[0], len1[0], vector2[0], start2[0], len2[0]);
        }
        outV.isRepeating = true;
      } else if (inV1.isRepeating) {
        if (batch.selectedInUse) {
          for(int j = 0; j != n; j++) {
            int i = sel[j];
            if (!inV1.isNull[0]) {
              outV.setConcat(i, vector1[0], start1[0], len1[0], vector2[i], start2[i], len2[i]);
            }
          }
        } else {
          for(int i = 0; i != n; i++) {
            if (!inV1.isNull[0]) {
              outV.setConcat(i, vector1[0], start1[0], len1[0], vector2[i], start2[i], len2[i]);
            }
          }
        }
      } else if (inV2.isRepeating) {
        if (batch.selectedInUse) {
          for(int j = 0; j != n; j++) {
            int i = sel[j];
            if (!inV1.isNull[i]) {
              outV.setConcat(i, vector1[i], start1[i], len1[i], vector2[0], start2[0], len2[0]);
            }
          }
        } else {
          for(int i = 0; i != n; i++) {
            if (!inV1.isNull[i]) {
              outV.setConcat(i, vector1[i], start1[i], len1[i], vector2[0], start2[0], len2[0]);
            }
          }
        }
      } else {
        if (batch.selectedInUse) {
          for(int j=0; j != n; j++) {
            int i = sel[j];
            if (!inV1.isNull[i]) {
              outV.setConcat(i, vector1[i], start1[i], len1[i], vector2[i], start2[i], len2[i]);
            }
          }
        } else {
          for(int i = 0; i != n; i++) {
            if (!inV1.isNull[i]) {
              outV.setConcat(i, vector1[i], start1[i], len1[i], vector2[i], start2[i], len2[i]);
            }
          }
        }
      }
    } else if (!inV1.noNulls && !inV2.noNulls) {

      // propagate nulls
      outV.noNulls = false;
      if (inV1.isRepeating && inV2.isRepeating) {
        outV.isNull[0] = inV1.isNull[0] || inV2.isNull[0];

        //Output will also be repeating
        outV.isRepeating = true;

        // return if output is null because no additional work is needed
        if (outV.isNull[0]) {
          return;
        }
      } else if (inV1.isRepeating) {
        if (inV1.isNull[0]) {            // then all output will be null
          outV.isRepeating = true;
          outV.isNull[0] = true;
          return;
        } else {
          outV.isRepeating = false;
          propagateNulls(batch.selectedInUse, n, sel, inV2, outV);
        }
      } else if (inV2.isRepeating) {
        if (inV2.isNull[0]) {
          outV.isRepeating = true;
          outV.isNull[0] = true;
          return;
        } else {
          outV.isRepeating = false;
          propagateNulls(batch.selectedInUse, n, sel, inV1, outV);
        }
      } else {
        propagateNullsCombine(batch.selectedInUse, n, sel, inV1, inV2, outV);
      }

      // perform data operation
      if (inV1.isRepeating && inV2.isRepeating) {

        // All must be selected otherwise size would be zero. Repeating property will not change.
        if (!inV1.isNull[0] && !inV2.isNull[0]) {
          outV.setConcat(0, vector1[0], start1[0], len1[0], vector2[0], start2[0], len2[0]);
        }
        outV.isRepeating = true;
      } else if (inV1.isRepeating) {
        if (batch.selectedInUse) {
          for(int j = 0; j != n; j++) {
            int i = sel[j];
            if (!inV1.isNull[0] && !inV2.isNull[i]) {
              outV.setConcat(i, vector1[0], start1[0], len1[0], vector2[i], start2[i], len2[i]);
            }
          }
        } else {
          for(int i = 0; i != n; i++) {
            if (!inV1.isNull[0] && !inV2.isNull[i]) {
              outV.setConcat(i, vector1[0], start1[0], len1[0], vector2[i], start2[i], len2[i]);
            }
          }
        }
      } else if (inV2.isRepeating) {
        if (batch.selectedInUse) {
          for(int j = 0; j != n; j++) {
            int i = sel[j];
            if (!inV1.isNull[i] && !inV2.isNull[0]) {
              outV.setConcat(i, vector1[i], start1[i], len1[i], vector2[0], start2[0], len2[0]);
            }
          }
        } else {
          for(int i = 0; i != n; i++) {
            if (!inV1.isNull[i] && !inV2.isNull[0]) {
              outV.setConcat(i, vector1[i], start1[i], len1[i], vector2[0], start2[0], len2[0]);
            }
          }
        }
      } else {
        if (batch.selectedInUse) {
          for(int j=0; j != n; j++) {
            int i = sel[j];
            if (!inV1.isNull[i] && !inV2.isNull[i]) {
              outV.setConcat(i, vector1[i], start1[i], len1[i], vector2[i], start2[i], len2[i]);
            }
          }
        } else {
          for(int i = 0; i != n; i++) {
            if (!inV1.isNull[i] && !inV2.isNull[i]) {
              outV.setConcat(i, vector1[i], start1[i], len1[i], vector2[i], start2[i], len2[i]);
            }
          }
        }
      }
    } else {      // there are no nulls in either input vector

      /*
       * Do careful maintenance of the outputColVector.noNulls flag.
       */

      // perform data operation
      if (inV1.isRepeating && inV2.isRepeating) {

        // All must be selected otherwise size would be zero. Repeating property will not change.
        outV.setConcat(0, vector1[0], start1[0], len1[0], vector2[0], start2[0], len2[0]);
        outV.isRepeating = true;
        outputIsNull[0] = false;
      } else if (inV1.isRepeating) {
        if (batch.selectedInUse) {
          for(int j = 0; j != n; j++) {
            int i = sel[j];
            outputIsNull[i] = false;
            outV.setConcat(i, vector1[0], start1[0], len1[0], vector2[i], start2[i], len2[i]);
          }
        } else {
          Arrays.fill(outputIsNull, 0, n, false);
          for(int i = 0; i != n; i++) {
            outV.setConcat(i, vector1[0], start1[0], len1[0], vector2[i], start2[i], len2[i]);
          }
        }
      } else if (inV2.isRepeating) {
        if (batch.selectedInUse) {
          for(int j = 0; j != n; j++) {
            int i = sel[j];
            outputIsNull[i] = false;
            outV.setConcat(i, vector1[i], start1[i], len1[i], vector2[0], start2[0], len2[0]);
          }
        } else {
          Arrays.fill(outputIsNull, 0, n, false);
          for(int i = 0; i != n; i++) {
            outV.setConcat(i, vector1[i], start1[i], len1[i], vector2[0], start2[0], len2[0]);
          }
        }
      } else {
        if (batch.selectedInUse) {
          for(int j=0; j != n; j++) {
            int i = sel[j];
            outputIsNull[i] = false;
            outV.setConcat(i, vector1[i], start1[i], len1[i], vector2[i], start2[i], len2[i]);
          }
        } else {
          Arrays.fill(outputIsNull, 0, n, false);
          for(int i = 0; i != n; i++) {
            outV.setConcat(i, vector1[i], start1[i], len1[i], vector2[i], start2[i], len2[i]);
          }
        }
      }
    }
  }

  /**
   * Propagate the logic OR of null vectors from two inputs to output.
   *
   * @param selectedInUse true/false flag to tell if sel[] is in use
   * @param n number of qualifying rows
   * @param sel selected value position array
   * @param inV1 input vector 1
   * @param inV2 input vector 2
   * @param outV output vector
   */
  private static void propagateNullsCombine(boolean selectedInUse, int n, int[] sel,
      ColumnVector inV1, ColumnVector inV2, BytesColumnVector outV) {
    if (selectedInUse) {
      for(int j = 0; j != n; j++) {
        int i = sel[j];
        outV.isNull[i] = inV1.isNull[i] || inV2.isNull[i];
      }
    } else {
      for(int i = 0; i != n; i++) {
        outV.isNull[i] = inV1.isNull[i] || inV2.isNull[i];
      }
    }
  }

  /**
   * Propagate nulls from input vector inV to output vector outV.
   *
   * @param selectedInUse true/false flag to tell if sel[] is in use
   * @param sel selected value position array
   * @param n number of qualifying rows
   * @param inV input vector
   * @param outV output vector
   */
  private static void propagateNulls(boolean selectedInUse, int n, int[] sel, ColumnVector inV,
      ColumnVector outV) {
    if (selectedInUse) {
      for(int j = 0; j != n; j++) {
        int i = sel[j];
        outV.isNull[i] = inV.isNull[i];
      }
    } else {
      System.arraycopy(inV.isNull, 0, outV.isNull, 0, n);
    }
  }

  @Override
  public String vectorExpressionParameters() {
    return getColumnParamString(0, inputColumnNum[0]) + ", " + getColumnParamString(1, inputColumnNum[1]);
  }

  @Override
  public VectorExpressionDescriptor.Descriptor getDescriptor() {
    return (new VectorExpressionDescriptor.Builder())
        .setMode(
            VectorExpressionDescriptor.Mode.PROJECTION)
        .setNumArguments(2)
        .setArgumentTypes(
            VectorExpressionDescriptor.ArgumentType.STRING_FAMILY,
            VectorExpressionDescriptor.ArgumentType.STRING_FAMILY)
        .setInputExpressionTypes(
            VectorExpressionDescriptor.InputExpressionType.COLUMN,
            VectorExpressionDescriptor.InputExpressionType.COLUMN).build();
  }
}
