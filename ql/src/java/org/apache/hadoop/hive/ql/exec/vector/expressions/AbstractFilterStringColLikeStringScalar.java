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
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

/**
 * An abstract class for LIKE and REGEXP expressions. LIKE and REGEXP expression share similar
 * functions, but they have different grammars. AbstractFilterStringColLikeStringScalar class
 * provides shared classes and methods. Each subclass handles its grammar.
 */
public abstract class AbstractFilterStringColLikeStringScalar extends VectorExpression {
  private static final long serialVersionUID = 1L;

  private int colNum;
  private String pattern;
  transient Checker checker;

  public AbstractFilterStringColLikeStringScalar() {
    super();
  }

  public AbstractFilterStringColLikeStringScalar(int colNum, String pattern) {
    this.colNum = colNum;
    this.pattern = pattern;
  }

  protected abstract List<CheckerFactory> getCheckerFactories();

  /**
   * Selects an optimized checker for a given string.
   * @param pattern
   * @return
   */
  private Checker createChecker(String pattern) {
    for (CheckerFactory checkerFactory : getCheckerFactories()) {
      Checker checker = checkerFactory.tryCreate(pattern);
      if (checker != null) {
        return checker;
      }
    }
    return null;
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) {
    this.checker = createChecker(pattern);

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

    // return immediately if batch is empty
    if (n == 0) {
      return;
    }

    if (inputColVector.noNulls) {
      if (inputColVector.isRepeating) {

        // All must be selected otherwise size would be zero Repeating property will not change.
        if (!checker.check(vector[0], start[0], length[0])) {

          // Entire batch is filtered out.
          batch.size = 0;
        }
      } else if (batch.selectedInUse) {
        int newSize = 0;

        for (int j = 0; j != n; j++) {
          int i = sel[j];
          if (checker.check(vector[i], start[i], length[i])) {
            sel[newSize++] = i;
          }
        }

        batch.size = newSize;
      } else {
        int newSize = 0;
        for (int i = 0; i != n; i++) {
          if (checker.check(vector[i], start[i], length[i])) {
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
          if (!checker.check(vector[0], start[0], length[0])) {

            //Entire batch is filtered out.
            batch.size = 0;
          }
        } else {
          batch.size = 0;
        }
      } else if (batch.selectedInUse) {
        int newSize = 0;

        for (int j = 0; j != n; j++) {
          int i = sel[j];
          if (!nullPos[i]) {
            if (checker.check(vector[i], start[i], length[i])) {
              sel[newSize++] = i;
            }
          }
        }

        //Change the selected vector
        batch.size = newSize;
      } else {
        int newSize = 0;

        for (int i = 0; i != n; i++) {
          if (!nullPos[i]) {
            if (checker.check(vector[i], start[i], length[i])) {
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

  /**
   * A Checker contains a pattern and checks whether a given string matches or not.
   */
  public interface Checker {
    /**
     * Checks whether the given string matches with its pattern.
     * @param byteS The byte array that contains the string
     * @param start The start position of the string
     * @param len The length of the string
     * @return Whether it matches or not.
     */
    boolean check(byte[] byteS, int start, int len);
  }

  /**
   * A CheckerFactory creates checkers of its kind.
   */
  protected interface CheckerFactory {
    /**
     * If the given pattern is acceptable for its checker class, it creates and returns a checker.
     * Otherwise, it returns <code>null</code>.
     * @param pattern
     * @return If the pattern is acceptable, a <code>Checker</code> object. Otherwise
     * <code>null</code>.
     */
    Checker tryCreate(String pattern);
  }

  /**
   * Matches the whole string to its pattern.
   */
  protected static class NoneChecker implements Checker {
    byte [] byteSub;

    NoneChecker(String pattern) {
      try {
        byteSub = pattern.getBytes("UTF-8");
      } catch (UnsupportedEncodingException e) {
        throw new RuntimeException(e);
      }
    }

    public boolean check(byte[] byteS, int start, int len) {
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
  }

  /**
   * Matches the beginning of each string to a pattern.
   */
  protected static class BeginChecker implements Checker {
    byte[] byteSub;

    BeginChecker(String pattern) {
      try {
        byteSub = pattern.getBytes("UTF-8");
      } catch (UnsupportedEncodingException e) {
        throw new RuntimeException(e);
      }
    }

    public boolean check(byte[] byteS, int start, int len) {
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
  }

  /**
   * Matches the ending of each string to its pattern.
   */
  protected static class EndChecker implements Checker {
    byte[] byteSub;
    EndChecker(String pattern) {
      try {
        byteSub = pattern.getBytes("UTF-8");
      } catch (UnsupportedEncodingException e) {
        throw new RuntimeException(e);
      }
    }

    public boolean check(byte[] byteS, int start, int len) {
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
  }

  /**
   * Matches the middle of each string to its pattern.
   */
  protected static class MiddleChecker implements Checker {
    byte[] byteSub;
    int lenSub;

    MiddleChecker(String pattern) {
      try {
        byteSub = pattern.getBytes("UTF-8");
        lenSub = byteSub.length;
      } catch (UnsupportedEncodingException e) {
        throw new RuntimeException(e);
      }
    }

    public boolean check(byte[] byteS, int start, int len) {
      if (len < lenSub) {
        return false;
      }
      int end = start + len - lenSub + 1;
      boolean match = false;
      for (int i = start; i < end; i++) {
        match = true;
        for (int j = 0; j < lenSub; j++) {
          if (byteS[i + j] != byteSub[j]) {
            match = false;
            break;
          }
        }
        if (match) {
          return true;
        }
      }
      return match;
    }
  }

  /**
   * Matches each string to a pattern with Java regular expression package.
   */
  protected static class ComplexChecker implements Checker {
    Pattern compiledPattern;
    Matcher matcher;
    FastUTF8Decoder decoder;

    ComplexChecker(String pattern) {
      compiledPattern = Pattern.compile(pattern);
      matcher = compiledPattern.matcher("");
      decoder = new FastUTF8Decoder();
    }

    public boolean check(byte[] byteS, int start, int len) {
      // Match the given bytes with the like pattern
      matcher.reset(decoder.decodeUnsafely(byteS, start, len));
      return matcher.matches();
    }
  }

  /**
   * A fast UTF-8 decoder that caches necessary objects for decoding.
   */
  private static class FastUTF8Decoder {
    CharsetDecoder decoder;
    ByteBuffer byteBuffer;
    CharBuffer charBuffer;

    public FastUTF8Decoder() {
      decoder = Charset.forName("UTF-8").newDecoder()
          .onMalformedInput(CodingErrorAction.REPLACE)
          .onUnmappableCharacter(CodingErrorAction.REPLACE);
      byteBuffer = ByteBuffer.allocate(4);
      charBuffer = CharBuffer.allocate(4);
    }

    public CharBuffer decodeUnsafely(byte[] byteS, int start, int len) {
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

      return charBuffer;
    }
  }

  public int getColNum() {
    return colNum;
  }

  public void setColNum(int colNum) {
    this.colNum = colNum;
  }

  public String getPattern() {
    return pattern;
  }

  public void setPattern(String pattern) {
    this.pattern = pattern;
  }

  @Override
  public VectorExpressionDescriptor.Descriptor getDescriptor() {
    return (new VectorExpressionDescriptor.Builder())
        .setMode(
            VectorExpressionDescriptor.Mode.FILTER)
        .setNumArguments(2)
        .setArgumentTypes(
            VectorExpressionDescriptor.ArgumentType.getType("string"),
            VectorExpressionDescriptor.ArgumentType.getType("string"))
        .setInputExpressionTypes(
            VectorExpressionDescriptor.InputExpressionType.COLUMN,
            VectorExpressionDescriptor.InputExpressionType.SCALAR).build();
  }
}
