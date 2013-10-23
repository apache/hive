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

import java.io.UnsupportedEncodingException;

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

/**
 * This class provides the implementation of vectorized substring, with a single start index
 * parameter. If the start index is invalid (outside of the string boundaries) then an empty
 * string will be in the output.
 */
public class StringSubstrColStart extends VectorExpression {
  private static final long serialVersionUID = 1L;
  private int startIdx;
  private int colNum;
  private int outputColumn;
  private transient static byte[] EMPTY_STRING;

  // Populating the Empty string bytes. Putting it as static since it should be immutable and can
  // be shared.
  static {
    try {
      EMPTY_STRING = "".getBytes("UTF-8");
    } catch(UnsupportedEncodingException e) {
      e.printStackTrace();
    }
  }

  public StringSubstrColStart(int colNum, int startIdx, int outputColumn) {
    this();
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
    this.outputColumn = outputColumn;
  }

  public StringSubstrColStart() {
    super();
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
  public void evaluate(VectorizedRowBatch batch) {
    if (childExpressions != null) {
      super.evaluateChildren(batch);
    }

    BytesColumnVector inV = (BytesColumnVector) batch.cols[colNum];
    BytesColumnVector outV = (BytesColumnVector) batch.cols[outputColumn];

    int n = batch.size;

    if (n == 0) {
      return;
    }

    byte[][] vector = inV.vector;
    int[] sel = batch.selected;
    int[] len = inV.length;
    int[] start = inV.start;
    outV.initBuffer();

    if (inV.isRepeating) {
      outV.isRepeating = true;
      if (!inV.noNulls && inV.isNull[0]) {
        outV.isNull[0] = true;
        outV.noNulls = false;
        outV.setVal(0, EMPTY_STRING, 0, EMPTY_STRING.length);
        return;
      } else {
        outV.noNulls = true;
        int offset = getSubstrStartOffset(vector[0], start[0], len[0], startIdx);
        if (offset != -1) {
          outV.setVal(0, vector[0], offset, len[0] - (offset - start[0]));
        } else {
          outV.setVal(0, EMPTY_STRING, 0, EMPTY_STRING.length);
        }
      }
    } else {
      outV.isRepeating = false;
      if (batch.selectedInUse) {
        if (!inV.noNulls) {
          outV.noNulls = false;
          for (int i = 0; i != n; ++i) {
            int selected = sel[i];
            if (!inV.isNull[selected]) {
              int offset = getSubstrStartOffset(vector[selected], start[selected], len[selected],
                  startIdx);
              outV.isNull[selected] = false;
              if (offset != -1) {
                outV.setVal(selected, vector[selected], offset,
                    len[selected] - (offset - start[selected]));
              } else {
                outV.setVal(selected, EMPTY_STRING, 0, EMPTY_STRING.length);
              }
            } else {
              outV.isNull[selected] = true;
            }
          }
        } else {
          outV.noNulls = true;
          for (int i = 0; i != n; ++i) {
            int selected = sel[i];
            int offset = getSubstrStartOffset(vector[selected], start[selected], len[selected],
                startIdx);
            if (offset != -1) {
              outV.setVal(selected, vector[selected], offset,
                  len[selected] - (offset - start[selected]));
            } else {
              outV.setVal(selected, EMPTY_STRING, 0, EMPTY_STRING.length);
            }
          }
        }
      } else {
        if (!inV.noNulls) {
          outV.noNulls = false;
          System.arraycopy(inV.isNull, 0, outV.isNull, 0, n);
          for (int i = 0; i != n; ++i) {
            if (!inV.isNull[i]) {
              int offset = getSubstrStartOffset(vector[i], start[i], len[i], startIdx);
              if (offset != -1) {
                outV.setVal(i, vector[i], offset, len[i] - (offset - start[i]));
              } else {
                outV.setVal(i, EMPTY_STRING, 0, EMPTY_STRING.length);
              }
            }
          }
        } else {
          outV.noNulls = true;
          for (int i = 0; i != n; ++i) {
            int offset = getSubstrStartOffset(vector[i], start[i], len[i], startIdx);
            if (offset != -1) {
              outV.setVal(i, vector[i], offset, len[i] - (offset - start[i]));
            } else {
              outV.setVal(i, EMPTY_STRING, 0, EMPTY_STRING.length);
            }
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
    return "string";
  }

  public int getStartIdx() {
    return startIdx;
  }

  public void setStartIdx(int startIdx) {
    this.startIdx = startIdx;
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

  @Override
  public VectorExpressionDescriptor.Descriptor getDescriptor() {
    VectorExpressionDescriptor.Builder b = new VectorExpressionDescriptor.Builder();
    b.setMode(VectorExpressionDescriptor.Mode.PROJECTION)
        .setNumArguments(2)
        .setArgumentTypes(
            VectorExpressionDescriptor.ArgumentType.STRING,
            VectorExpressionDescriptor.ArgumentType.LONG)
        .setInputExpressionTypes(
            VectorExpressionDescriptor.InputExpressionType.COLUMN,
            VectorExpressionDescriptor.InputExpressionType.SCALAR);
    return b.build();
  }
}
