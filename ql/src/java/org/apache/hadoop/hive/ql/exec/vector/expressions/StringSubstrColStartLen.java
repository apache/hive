package org.apache.hadoop.hive.ql.exec.vector.expressions;

import java.io.UnsupportedEncodingException;

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

/**
 * This class provides the implementation of vectorized substring, with a start index and length parameters.
 * If the start index is invalid (outside of the string boundaries) then an empty string will be in the output.
 * If the length provided is longer then the string boundary, then it will replace it with the ending index.
 */
public class StringSubstrColStartLen extends VectorExpression {
  private final int startIdx;
  private final int colNum;
  private final int length;
  private final int outputColumn;
  private final int[] offsetArray;
  private static byte[] EMPTY_STRING;

  // Populating the Empty string bytes. Putting it as static since it should be immutable and can be shared
  static {
    try {
      EMPTY_STRING = "".getBytes("UTF-8");
    } catch(UnsupportedEncodingException e) {
      e.printStackTrace();
    }
  }

  public StringSubstrColStartLen(int colNum, int startIdx, int length, int outputColumn) {
    this.colNum = colNum;
    this.startIdx = startIdx;
    this.length = length;
    this.outputColumn = outputColumn;
    offsetArray = new int[2];
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
  static void populateSubstrOffsets(byte[] utf8String, int start, int len, int substrStart, int substrLength, int[] offsetArray) {
    int curIdx = -1;
    offsetArray[0] = -1;
    offsetArray[1] = -1;

    if (substrStart < 0) {
      int length = 0;
      for (int i = start; i != len; ++i) {
        if ((utf8String[i] & 0xc0) != 0x80) {
          ++length;
        }
      }

      if (-length > substrStart) {
        return;
      }

      substrStart = length + substrStart;
    }


    int endIdx = substrStart + substrLength - 1;
    int end = start + len;
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
        outV.setRef(0, EMPTY_STRING, 0, EMPTY_STRING.length);
        return;
      } else {
        outV.noNulls = true;
        populateSubstrOffsets(vector[0], sel[0], len[0], startIdx, length, offsetArray);
        if (offsetArray[0] != -1) {
          outV.setRef(0, vector[0], offsetArray[0], offsetArray[1]);
        } else {
          outV.setRef(0, EMPTY_STRING, 0, EMPTY_STRING.length);
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
              outV.isNull[selected] = false;
              populateSubstrOffsets(vector[selected], start[selected], len[selected], startIdx, length, offsetArray);
              if (offsetArray[0] != -1) {
                outV.setRef(selected, vector[selected], offsetArray[0], offsetArray[1]);
              } else {
                outV.setRef(selected, EMPTY_STRING, 0, EMPTY_STRING.length);
              }
            } else {
              outV.isNull[selected] = true;
            }
          }
        } else {
          outV.noNulls = true;
          for (int i = 0; i != n; ++i) {
            int selected = sel[i];
            outV.isNull[selected] = false;
            populateSubstrOffsets(vector[selected], start[selected], len[selected], startIdx, length, offsetArray);
            if (offsetArray[0] != -1) {
              outV.setRef(selected, vector[selected], offsetArray[0], offsetArray[1]);
            } else {
              outV.setRef(selected, EMPTY_STRING, 0, EMPTY_STRING.length);
            }
          }
        }
      } else {
        if (!inV.noNulls) {
          System.arraycopy(inV.isNull, 0, outV.isNull, 0, n);
          outV.noNulls = false;
          for (int i = 0; i != n; ++i) {
            if (!inV.isNull[i]) {
              populateSubstrOffsets(vector[i], start[i], len[i], startIdx, length, offsetArray);
              if (offsetArray[0] != -1) {
                outV.setRef(i, vector[i], offsetArray[0], offsetArray[1]);
              } else {
                outV.setRef(i, EMPTY_STRING, 0, EMPTY_STRING.length);
              }
            }
          }
        } else {
          outV.noNulls = true;
          for (int i = 0; i != n; ++i) {
            outV.isNull[i] = false;
            populateSubstrOffsets(vector[i], start[i], len[i], startIdx, length, offsetArray);
            if (offsetArray[0] != -1) {
              outV.setRef(i, vector[i], offsetArray[0], offsetArray[1]);
            } else {
              outV.setRef(i, EMPTY_STRING, 0, EMPTY_STRING.length);
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
