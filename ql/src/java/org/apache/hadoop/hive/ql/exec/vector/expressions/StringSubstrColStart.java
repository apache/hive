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
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

/**
 * This class provides the implementation of vectorized substring, with a single start index
 * parameter. If the start index is invalid (outside of the string boundaries) then an empty
 * string will be in the output.
 */
public class StringSubstrColStart extends VectorExpression {
  private final int startIdx;
  private final int colNum;
  private final int outputColumn;
  private static byte[] EMPTYSTRING;

  // Populating the Empty string bytes. Putting it as static since it should be immutable and can
  // be shared.
  static {
    try {
      EMPTYSTRING = "".getBytes("UTF-8");
    } catch(UnsupportedEncodingException e) {
      e.printStackTrace();
    }
  }

  public StringSubstrColStart(int colNum, int startIdx, int outputColumn) {
    this.colNum = colNum;
    this.startIdx = startIdx;
    this.outputColumn = outputColumn;
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
    int curIdx = -1;

    if (substrStart < 0) {
      int length = 0;
      for (int i = start; i != len; ++i) {
        if ((utf8String[i] & 0xc0) != 0x80) {
          ++length;
        }
      }

      if (-length > substrStart) {
        return -1;
      }

      substrStart = length + substrStart;
    }

    int end = start + len;
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

    if (inV.isRepeating) {
      outV.isRepeating = true;
      if (!inV.noNulls && inV.isNull[0]) {
        outV.isNull[0] = true;
        outV.noNulls = false;
        outV.setRef(0, EMPTYSTRING, 0, EMPTYSTRING.length);
        return;
      } else {
        outV.noNulls = true;
        int offset = getSubstrStartOffset(vector[0], sel[0], len[0], startIdx);
        if (offset != -1) {
          outV.setRef(0, vector[0], offset, len[0] - offset);
        } else {
          outV.setRef(0, EMPTYSTRING, 0, EMPTYSTRING.length);
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
                outV.setRef(selected, vector[selected], offset, len[selected] - offset);
              } else {
                outV.setRef(selected, EMPTYSTRING, 0, EMPTYSTRING.length);
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
              outV.setRef(selected, vector[selected], offset, len[selected] - offset);
            } else {
              outV.setRef(selected, EMPTYSTRING, 0, EMPTYSTRING.length);
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
                outV.setRef(i, vector[i], offset, len[i] - offset);
              } else {
                outV.setRef(i, EMPTYSTRING, 0, EMPTYSTRING.length);
              }
            }
          }
        } else {
          outV.noNulls = true;
          for (int i = 0; i != n; ++i) {
            int offset = getSubstrStartOffset(vector[i], start[i], len[i], startIdx);
            if (offset != -1) {
              outV.setRef(i, vector[i], offset, len[i] - offset);
            } else {
              outV.setRef(i, EMPTYSTRING, 0, EMPTYSTRING.length);
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
}
