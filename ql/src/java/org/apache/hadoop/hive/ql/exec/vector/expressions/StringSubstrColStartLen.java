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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.metadata.HiveException;

/**
 * This class provides the implementation of vectorized substring, with a start index and length
 * parameters. If the start index is invalid (outside of the string boundaries) then an empty
 * string will be in the output.
 * If the length provided is longer then the string boundary, then it will replace it with the
 * ending index.
 */
public class StringSubstrColStartLen extends VectorExpression {
  private static final long serialVersionUID = 1L;

  private final int colNum;

  private final int startIdx;
  private final int length;
  private final int[] offsetArray;

  private static final byte[] EMPTY_STRING =
      StringUtils.EMPTY.getBytes(StandardCharsets.UTF_8);

  public StringSubstrColStartLen(int colNum, int startIdx, int length, int outputColumnNum) {
    super(outputColumnNum);
    this.colNum = colNum;
    offsetArray = new int[2];

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

    this.length = length;
  }

  public StringSubstrColStartLen() {
    super();

    // Dummy final assignments.
    colNum = -1;
    startIdx = -1;
    length = 0;
    offsetArray = null;
  }

  /**
   * Populates the substring start and end offsets based on the substring start and length params.
   *
   * @param utf8String byte array that holds the utf8 string
   * @param start start offset of the byte array the string starts at
   * @param len length of the bytes the string holds in the byte array
   * @param substrStart the Start index for the substring operation
   * @param substrLen the length of the substring
   * @param offsetArray the array that indexes are populated to. Assume its length >= 2.
   */
  static void populateSubstrOffsets(byte[] utf8String, int start, int len, int substrStart,
      int substrLength, int[] offsetArray) {
    int curIdx = -1;
    offsetArray[0] = -1;
    offsetArray[1] = -1;
    int end = start + len;

    if (substrStart < 0) {
      int length = 0;
      for (int i = start; i != end; ++i) {
        if ((utf8String[i] & 0xc0) != 0x80) {
          ++length;
        }
      }

      if (-substrStart > length) {
        return;
      }

      substrStart = length + substrStart;
    }

    if (substrLength == 0) {
      return;
    }

    int endIdx = substrStart + substrLength - 1;
    for (int i = start; i != end; ++i) {
      if ((utf8String[i] & 0xc0) != 0x80) {
        ++curIdx;
        if (curIdx == substrStart) {
          offsetArray[0] = i;
        } else if (curIdx - 1 == endIdx) {
          offsetArray[1] = i - offsetArray[0];
        }
      }
    }

    if (offsetArray[1] == -1) {
      offsetArray[1] = end - offsetArray[0];
    }
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
        populateSubstrOffsets(vector[0], start[0], len[0], startIdx, length, offsetArray);
        if (offsetArray[0] != -1) {
          outputColVector.setVal(0, vector[0], offsetArray[0], offsetArray[1]);
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
            populateSubstrOffsets(vector[selected], start[selected], len[selected], startIdx,
                length, offsetArray);
            if (offsetArray[0] != -1) {
              outputColVector.setVal(selected, vector[selected], offsetArray[0], offsetArray[1]);
            } else {
              outputColVector.setVal(selected, EMPTY_STRING, 0, EMPTY_STRING.length);
            }
          } else {
            outputIsNull[selected] = true;
            outputColVector.noNulls = false;
          }
        }
      } else {
        for (int i = 0; i != n; ++i) {
          int selected = sel[i];
          outputColVector.isNull[selected] = false;
          populateSubstrOffsets(vector[selected], start[selected], len[selected], startIdx,
              length, offsetArray);
          if (offsetArray[0] != -1) {
            outputColVector.setVal(selected, vector[selected], offsetArray[0], offsetArray[1]);
          } else {
            outputColVector.setVal(selected, EMPTY_STRING, 0, EMPTY_STRING.length);
          }
        }
      }
    } else {
      if (!inV.noNulls) /* there are nulls in the inputColVector */ {

        // Carefully handle NULLs...

        for (int i = 0; i != n; ++i) {
          if (!inV.isNull[i]) {
            outputIsNull[i] = false;
            populateSubstrOffsets(vector[i], start[i], len[i], startIdx, length, offsetArray);
            if (offsetArray[0] != -1) {
              outputColVector.setVal(i, vector[i], offsetArray[0], offsetArray[1]);
            } else {
              outputColVector.setVal(i, EMPTY_STRING, 0, EMPTY_STRING.length);
            }
          } else {
            outputIsNull[i] = true;
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
          populateSubstrOffsets(vector[i], start[i], len[i], startIdx, length, offsetArray);
          if (offsetArray[0] != -1) {
            outputColVector.setVal(i, vector[i], offsetArray[0], offsetArray[1]);
          } else {
            outputColVector.setVal(i, EMPTY_STRING, 0, EMPTY_STRING.length);
          }
        }
      }
    }
  }

  @Override
  public String vectorExpressionParameters() {
    return getColumnParamString(0, colNum) + ", start " + startIdx + ", length " + length;
  }

  @Override
  public VectorExpressionDescriptor.Descriptor getDescriptor() {
    VectorExpressionDescriptor.Builder b = new VectorExpressionDescriptor.Builder();
    b.setMode(VectorExpressionDescriptor.Mode.PROJECTION)
        .setNumArguments(3)
        .setArgumentTypes(
            VectorExpressionDescriptor.ArgumentType.STRING_FAMILY,
            VectorExpressionDescriptor.ArgumentType.INT_FAMILY,
            VectorExpressionDescriptor.ArgumentType.INT_FAMILY)
        .setInputExpressionTypes(
            VectorExpressionDescriptor.InputExpressionType.COLUMN,
            VectorExpressionDescriptor.InputExpressionType.SCALAR,
            VectorExpressionDescriptor.InputExpressionType.SCALAR);
    return b.build();
  }
}
