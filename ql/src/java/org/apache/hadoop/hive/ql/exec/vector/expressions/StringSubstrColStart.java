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

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.metadata.HiveException;

/**
 * This class provides the implementation of vectorized substring, with a single start index
 * parameter. If the start index is invalid (outside of the string boundaries) then an empty
 * string will be in the output.
 */
public class StringSubstrColStart extends VectorExpression {
  private static final long serialVersionUID = 1L;

  private final int colNum;

  private int startIdx;

  private static final byte[] EMPTY_STRING =
      StringUtils.EMPTY.getBytes(StandardCharsets.UTF_8);

  public StringSubstrColStart(int colNum, int startIdx, int outputColumnNum) {
    super(outputColumnNum);
    this.colNum = colNum;

    /* Switch from a 1-based start offset (the Hive end user convention) to a 0-based start offset
     * (the internal convention).
     */
    if (startIdx >= 1) {
      this.startIdx = startIdx - 1;
    } else if (startIdx == 0) {

      // If start index is 0 in query, that is equivalent to using 1 in query.
      // So internal offset is 0.
      this.startIdx = 0;
    } else {

      // start index of -n means give the last n characters of the string
      this.startIdx = startIdx;
    }
  }

  public StringSubstrColStart() {
    super();

    // Dummy final assignments.
    colNum = -1;
    startIdx = -1;
  }

  /**
   * Given the substring start index param it finds the starting offset of the passed in utf8
   * string byte array that matches the index.
   * @param utf8String byte array that holds the utf8 string
   * @param start start offset of the byte array the string starts at
   * @param len length of the bytes the string holds in the byte array
   * @param substrStart the Start index for the substring operation
   */
  static int getSubstrStartOffset(byte[] utf8String, int start, int len, int substrStart) {
    int end = start + len;

    if (substrStart < 0) {
      int length = 0;
      for (int i = start; i != end; ++i) {
        if ((utf8String[i] & 0xc0) != 0x80) {
          ++length;
        }
      }
      if (-substrStart > length) {

        /* The result is empty string if a negative start is provided
         * whose absolute value is greater than the string length.
         */
        return -1;
      }

      substrStart = length + substrStart;
    }

    int curIdx = -1;
    for (int i = start; i != end; ++i) {
      if ((utf8String[i] & 0xc0) != 0x80) {
        ++curIdx;
        if (curIdx == substrStart) {
          return i;
        }
      }
    }
    return -1;
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) throws HiveException {
    if (childExpressions != null) {
      super.evaluateChildren(batch);
    }

    BytesColumnVector inV = (BytesColumnVector) batch.cols[colNum];
    BytesColumnVector outputColVector = (BytesColumnVector) batch.cols[outputColumnNum];

    int n = batch.size;

    if (n == 0) {
      return;
    }

    byte[][] vector = inV.vector;
    int[] sel = batch.selected;
    int[] len = inV.length;
    int[] start = inV.start;
    outputColVector.initBuffer();
    boolean[] outputIsNull = outputColVector.isNull;

    // We do not need to do a column reset since we are carefully changing the output.
    outputColVector.isRepeating = false;

    if (inV.isRepeating) {
      if (inV.noNulls || !inV.isNull[0]) {
        outputIsNull[0] = false;
        int offset = getSubstrStartOffset(vector[0], start[0], len[0], startIdx);
        if (offset != -1) {
          outputColVector.setVal(0, vector[0], offset, len[0] - (offset - start[0]));
        } else {
          outputColVector.setVal(0, EMPTY_STRING, 0, EMPTY_STRING.length);
        }
      } else {
        outputIsNull[0] = true;
        outputColVector.noNulls = false;
        outputColVector.setVal(0, EMPTY_STRING, 0, EMPTY_STRING.length);
      }
      outputColVector.isRepeating = true;
      return;
    }

    if (batch.selectedInUse) {
      if (!inV.noNulls) /* there are nulls in the inputColVector */ {

        // Carefully handle NULLs...

        for (int i = 0; i != n; ++i) {
          int selected = sel[i];
          if (!inV.isNull[selected]) {
            outputIsNull[selected] = false;
            int offset = getSubstrStartOffset(vector[selected], start[selected], len[selected],
                startIdx);
            outputColVector.isNull[selected] = false;
            if (offset != -1) {
              outputColVector.setVal(selected, vector[selected], offset,
                  len[selected] - (offset - start[selected]));
            } else {
              outputColVector.setVal(selected, EMPTY_STRING, 0, EMPTY_STRING.length);
            }
          } else {
            outputColVector.isNull[selected] = true;
            outputColVector.noNulls = false;
          }
        }
      } else {
        for (int i = 0; i != n; ++i) {
          int selected = sel[i];
          outputColVector.isNull[selected] = false;
          int offset = getSubstrStartOffset(vector[selected], start[selected], len[selected],
              startIdx);
          if (offset != -1) {
            outputColVector.setVal(selected, vector[selected], offset,
                len[selected] - (offset - start[selected]));
          } else {
            outputColVector.setVal(selected, EMPTY_STRING, 0, EMPTY_STRING.length);
          }
        }
      }
    } else {
      if (!inV.noNulls)  /* there are nulls in the inputColVector */ {

        // Carefully handle NULLs...

        for (int i = 0; i != n; ++i) {
          if (!inV.isNull[i]) {
            outputColVector.isNull[i] = false;
            int offset = getSubstrStartOffset(vector[i], start[i], len[i], startIdx);
            if (offset != -1) {
              outputColVector.setVal(i, vector[i], offset, len[i] - (offset - start[i]));
            } else {
              outputColVector.setVal(i, EMPTY_STRING, 0, EMPTY_STRING.length);
            }
          } else {
            outputColVector.isNull[i] = true;
            outputColVector.noNulls = false;
          }
        }
      } else {
        if (!outputColVector.noNulls) {

          // Assume it is almost always a performance win to fill all of isNull so we can
          // safely reset noNulls.
          Arrays.fill(outputIsNull, false);
          outputColVector.noNulls = true;
        }
        for (int i = 0; i != n; ++i) {
          int offset = getSubstrStartOffset(vector[i], start[i], len[i], startIdx);
          if (offset != -1) {
            outputColVector.setVal(i, vector[i], offset, len[i] - (offset - start[i]));
          } else {
            outputColVector.setVal(i, EMPTY_STRING, 0, EMPTY_STRING.length);
          }
        }
      }
    }
  }

  @Override
  public String vectorExpressionParameters() {
    return getColumnParamString(0, colNum) + ", start " + startIdx;
  }

  @Override
  public VectorExpressionDescriptor.Descriptor getDescriptor() {
    VectorExpressionDescriptor.Builder b = new VectorExpressionDescriptor.Builder();
    b.setMode(VectorExpressionDescriptor.Mode.PROJECTION)
        .setNumArguments(2)
        .setArgumentTypes(
            VectorExpressionDescriptor.ArgumentType.STRING_FAMILY,
            VectorExpressionDescriptor.ArgumentType.INT_FAMILY)
        .setInputExpressionTypes(
            VectorExpressionDescriptor.InputExpressionType.COLUMN,
            VectorExpressionDescriptor.InputExpressionType.SCALAR);
    return b.build();
  }
}
