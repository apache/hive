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

import static org.apache.hadoop.hive.ql.udf.UDFLike.likePatternToRegExp;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.util.regex.Pattern;

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.io.Text;

/**
 * Evaluate LIKE filter on a batch for a vector of strings.
 */
public class FilterStringColLikeStringScalar extends VectorExpression {
  private static final long serialVersionUID = 1L;
  private int colNum;
  private Pattern compiledPattern;
  private PatternType type = PatternType.NONE;
  private String simpleStringPattern;

  private transient Text simplePattern = new Text();
  private transient ByteBuffer byteBuffer;
  private transient CharBuffer charBuffer;
  private transient CharsetDecoder decoder;

  // Doing characters comparison directly instead of regular expression
  // matching for simple patterns like "%abc%".
  public enum PatternType {
    NONE, // "abc"
    BEGIN, // "abc%"
    END, // "%abc"
    MIDDLE, // "%abc%"
    COMPLEX, // all other cases, such as "ab%c_de"
  }

  public FilterStringColLikeStringScalar(int colNum, Text likePattern) {
    this();
    this.colNum = colNum;
    String stringLikePattern = likePattern.toString();
    parseSimplePattern(stringLikePattern);
    if (type == PatternType.COMPLEX) {
      compiledPattern = Pattern.compile(likePatternToRegExp(stringLikePattern));
    }
  }

  public FilterStringColLikeStringScalar() {
    super();
    decoder = Charset.forName("UTF-8").newDecoder()
        .onMalformedInput(CodingErrorAction.REPLACE)
        .onUnmappableCharacter(CodingErrorAction.REPLACE);
    byteBuffer = ByteBuffer.allocate(4);
    charBuffer = CharBuffer.allocate(4);
  }

  public PatternType getType() {
    return type;
  }

  private boolean like(byte[] bytes, int start, int len) {
    switch (type) {
      case NONE:
        return noneLike(bytes, start, len, simplePattern.getBytes());
      case BEGIN:
        return beginLike(bytes, start, len, simplePattern.getBytes());
      case END:
        return endLike(bytes, start, len, simplePattern.getBytes());
      case MIDDLE:
        return midLike(bytes, start, len, simplePattern.getBytes());
      case COMPLEX:
        return complexLike(bytes, start, len);
      default:
        return false;
    }
  }

  private static boolean noneLike(byte[] byteS, int start, int len, byte[] byteSub) {
    int lenSub = byteSub.length;
    if (len != lenSub) {
      return false;
    }
    for (int i = start, j = 0; j < len; i++, j++) {
      if (byteS[i] != byteSub[j]) {
        return false;
      }
    }
    return true;
  }

  private static boolean beginLike(byte[] byteS, int start, int len, byte[] byteSub) {
    if (len < byteSub.length) {
      return false;
    }
    for (int i = start, j = 0; j < byteSub.length; i++, j++) {
      if (byteS[i] != byteSub[j]) {
        return false;
      }
    }
    return true;
  }

  private static boolean endLike(byte[] byteS, int start, int len, byte[] byteSub) {
    int lenSub = byteSub.length;
    if (len < lenSub) {
      return false;
    }
    for (int i = start + len - lenSub, j = 0; j < lenSub; i++, j++) {
      if (byteS[i] != byteSub[j]) {
        return false;
      }
    }
    return true;
  }

  private static boolean midLike(byte[] byteS, int start, int len, byte[] byteSub) {
    int lenSub = byteSub.length;
    if (len < lenSub) {
      return false;
    }
    int end = start + len - lenSub + 1;
    boolean match = false;
    for (int i = start; (i < end) && (!match); i++) {
      match = true;
      for (int j = 0; j < lenSub; j++) {
        if (byteS[i + j] != byteSub[j]) {
          match = false;
          break;
        }
      }
    }
    return match;
  }

  /**
   * Matches the byte array against the complex like pattern. This method uses
   * {@link #compiledPattern} to match. For decoding performance, it caches
   * {@link #compiledPattern}, {@link #byteBuffer} and {@link #charBuffer}.
   * When the length to decode is greater than the capacity of
   * {@link #byteBuffer}, it creates new {@link #byteBuffer} and
   * {@link #charBuffer}. The capacity of the new {@link #byteBuffer} is the
   * double of the length, for fewer object creations and higher memory
   * utilization.
   *
   * @param byteS
   *          A byte array that contains a UTF-8 string.
   * @param start
   *          A position to start decoding.
   * @param len
   *          A length to decode.
   * @return
   *          true if the byte array matches the complex like pattern,
   *          otherwise false.
   */
  private boolean complexLike(byte[] byteS, int start, int len) {
    // Prepare buffers
    if (byteBuffer.capacity() < len) {
      byteBuffer = ByteBuffer.allocate(len * 2);
    }
    byteBuffer.clear();
    byteBuffer.put(byteS, start, len);
    byteBuffer.flip();

    int maxChars = (int) (byteBuffer.capacity() * decoder.maxCharsPerByte());
    if (charBuffer.capacity() < maxChars) {
      charBuffer = CharBuffer.allocate(maxChars);
    }
    charBuffer.clear();

    // Decode UTF-8
    decoder.reset();
    decoder.decode(byteBuffer, charBuffer, true);
    decoder.flush(charBuffer);
    charBuffer.flip();

    // Match the given bytes with the like pattern
    return compiledPattern.matcher(charBuffer).matches();
  }

  /**
   * Parses the likePattern. Based on it is a simple pattern or not, the
   * function might change two member variables. {@link #type} will be changed
   * to the corresponding pattern type; {@link #simplePattern} will record the
   * string in it for later pattern matching if it is a simple pattern.
   * <p>
   * Examples: <blockquote>
   *
   * <pre>
   * parseSimplePattern("%abc%") changes {@link #type} to PatternType.MIDDLE
   * and changes {@link #simplePattern} to "abc"
   * parseSimplePattern("%ab_c%") changes {@link #type} to PatternType.COMPLEX
   * and does not change {@link #simplePattern}
   * </pre>
   *
   * </blockquote>
   *
   * @param likePattern
   *          the input LIKE query pattern
   */
  private void parseSimplePattern(String likePattern) {
    int length = likePattern.length();
    int beginIndex = 0;
    int endIndex = length;
    char lastChar = 'a';
    String strPattern = new String();
    type = PatternType.NONE;

    for (int i = 0; i < length; i++) {
      char n = likePattern.charAt(i);
      if (n == '_') { // such as "a_b"
        if (lastChar != '\\') { // such as "a%bc"
          type = PatternType.COMPLEX;
          return;
        } else { // such as "abc\%de%"
          strPattern += likePattern.substring(beginIndex, i - 1);
          beginIndex = i;
        }
      } else if (n == '%') {
        if (i == 0) { // such as "%abc"
          type = PatternType.END;
          beginIndex = 1;
        } else if (i < length - 1) {
          if (lastChar != '\\') { // such as "a%bc"
            type = PatternType.COMPLEX;
            return;
          } else { // such as "abc\%de%"
            strPattern += likePattern.substring(beginIndex, i - 1);
            beginIndex = i;
          }
        } else {
          if (lastChar != '\\') {
            endIndex = length - 1;
            if (type == PatternType.END) { // such as "%abc%"
              type = PatternType.MIDDLE;
            } else {
              type = PatternType.BEGIN; // such as "abc%"
            }
          } else { // such as "abc\%"
            strPattern += likePattern.substring(beginIndex, i - 1);
            beginIndex = i;
            endIndex = length;
          }
        }
      }
      lastChar = n;
    }

    strPattern += likePattern.substring(beginIndex, endIndex);
    simpleStringPattern = strPattern;
    simplePattern.set(simpleStringPattern);
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) {

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
    byte[] simplePatternBytes = simplePattern.getBytes();

    // return immediately if batch is empty
    if (n == 0) {
      return;
    }

    if (inputColVector.noNulls) {
      if (inputColVector.isRepeating) {

        // All must be selected otherwise size would be zero Repeating property will not change.
        if (!like(vector[0], start[0], length[0])) {

          // Entire batch is filtered out.
          batch.size = 0;
        }
      } else if (batch.selectedInUse) {
        int newSize = 0;

        switch (type) {
          case NONE:
            for (int j = 0; j != n; j++) {
              int i = sel[j];
              if (noneLike(vector[i], start[i], length[i], simplePatternBytes)) {
                sel[newSize++] = i;
              }
            }
            break;
          case BEGIN:
            for (int j = 0; j != n; j++) {
              int i = sel[j];
              if (beginLike(vector[i], start[i], length[i], simplePatternBytes)) {
                sel[newSize++] = i;
              }
            }
            break;
          case END:
            for (int j = 0; j != n; j++) {
              int i = sel[j];
              if (endLike(vector[i], start[i], length[i], simplePatternBytes)) {
                sel[newSize++] = i;
              }
            }
            break;
          case MIDDLE:
            for (int j = 0; j != n; j++) {
              int i = sel[j];
              if (midLike(vector[i], start[i], length[i], simplePatternBytes)) {
                sel[newSize++] = i;
              }
            }
            break;
          case COMPLEX:
            for (int j = 0; j != n; j++) {
              int i = sel[j];
              if (complexLike(vector[i], start[i], length[i])) {
                sel[newSize++] = i;
              }
            }
            break;
        }

        batch.size = newSize;
      } else {
        int newSize = 0;

        switch (type) {
          case NONE:
            for (int i = 0; i != n; i++) {
              if (noneLike(vector[i], start[i], length[i], simplePatternBytes)) {
                sel[newSize++] = i;
              }
            }
            break;
          case BEGIN:
            for (int i = 0; i != n; i++) {
              if (beginLike(vector[i], start[i], length[i], simplePatternBytes)) {
                sel[newSize++] = i;
              }
            }
            break;
          case END:
            for (int i = 0; i != n; i++) {
              if (endLike(vector[i], start[i], length[i], simplePatternBytes)) {
                sel[newSize++] = i;
              }
            }
            break;
          case MIDDLE:
            for (int i = 0; i != n; i++) {
              if (midLike(vector[i], start[i], length[i], simplePatternBytes)) {
                sel[newSize++] = i;
              }
            }
            break;
          case COMPLEX:
            for (int i = 0; i != n; i++) {
              if (complexLike(vector[i], start[i], length[i])) {
                sel[newSize++] = i;
              }
            }
            break;
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

        switch (type) {
          case NONE:
            for (int j = 0; j != n; j++) {
              int i = sel[j];
              if (!nullPos[i]) {
                if (noneLike(vector[i], start[i], length[i], simplePatternBytes)) {
                  sel[newSize++] = i;
                }
              }
            }
            break;
          case BEGIN:
            for (int j = 0; j != n; j++) {
              int i = sel[j];
              if (!nullPos[i]) {
                if (beginLike(vector[i], start[i], length[i], simplePatternBytes)) {
                  sel[newSize++] = i;
                }
              }
            }
            break;
          case END:
            for (int j = 0; j != n; j++) {
              int i = sel[j];
              if (!nullPos[i]) {
                if (endLike(vector[i], start[i], length[i], simplePatternBytes)) {
                  sel[newSize++] = i;
                }
              }
            }
            break;
          case MIDDLE:
            for (int j = 0; j != n; j++) {
              int i = sel[j];
              if (!nullPos[i]) {
                if (midLike(vector[i], start[i], length[i], simplePatternBytes)) {
                  sel[newSize++] = i;
                }
              }
            }
            break;
          case COMPLEX:
            for (int j = 0; j != n; j++) {
              int i = sel[j];
              if (!nullPos[i]) {
                if (complexLike(vector[i], start[i], length[i])) {
                  sel[newSize++] = i;
                }
              }
            }
            break;
        }

        //Change the selected vector
        batch.size = newSize;
      } else {
        int newSize = 0;

        switch (type) {
          case NONE:
            for (int i = 0; i != n; i++) {
              if (!nullPos[i]) {
                if (noneLike(vector[i], start[i], length[i], simplePatternBytes)) {
                  sel[newSize++] = i;
                }
              }
            }
            break;
          case BEGIN:
            for (int i = 0; i != n; i++) {
              if (!nullPos[i]) {
                if (beginLike(vector[i], start[i], length[i], simplePatternBytes)) {
                  sel[newSize++] = i;
                }
              }
            }
            break;
          case END:
            for (int i = 0; i != n; i++) {
              if (!nullPos[i]) {
                if (endLike(vector[i], start[i], length[i], simplePatternBytes)) {
                  sel[newSize++] = i;
                }
              }
            }
            break;
          case MIDDLE:
            for (int i = 0; i != n; i++) {
              if (!nullPos[i]) {
                if (midLike(vector[i], start[i], length[i], simplePatternBytes)) {
                  sel[newSize++] = i;
                }
              }
            }
            break;
          case COMPLEX:
            for (int i = 0; i != n; i++) {
              if (!nullPos[i]) {
                if (complexLike(vector[i], start[i], length[i])) {
                  sel[newSize++] = i;
                }
              }
            }
            break;
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

  public int getColNum() {
    return colNum;
  }

  public void setColNum(int colNum) {
    this.colNum = colNum;
  }

  public Pattern getCompiledPattern() {
    return compiledPattern;
  }

  public void setCompiledPattern(Pattern compiledPattern) {
    this.compiledPattern = compiledPattern;
  }

  public void setType(PatternType type) {
    this.type = type;
  }

  public String getSimpleStringPattern() {
    return simpleStringPattern;
  }

  public void setSimpleStringPattern(String simpleStringPattern) {
    this.simpleStringPattern = simpleStringPattern;
    simplePattern.set(simpleStringPattern);
  }
}
