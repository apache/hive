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
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.ArrayUtils;
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
  transient Checker checker = null;

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
  Checker createChecker(String pattern) {
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

    if (checker == null) {
      checker = createChecker(pattern);
    }

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
  protected static final class NoneChecker implements Checker {
    final byte [] byteSub;

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
  protected static final class BeginChecker implements Checker {
    final byte[] byteSub;

    BeginChecker(String pattern) {
      try {
        byteSub = pattern.getBytes("UTF-8");
      } catch (UnsupportedEncodingException e) {
        throw new RuntimeException(e);
      }
    }

    public boolean check(byte[] byteS, int start, int len) {
      int lenSub = byteSub.length;
      if (len < byteSub.length) {
        return false;
      }
      return StringExpr.equal(byteSub, 0, lenSub, byteS, start, lenSub);
    }
  }

  /**
   * Matches the ending of each string to its pattern.
   */
  protected static final class EndChecker implements Checker {
    final byte[] byteSub;

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
      return StringExpr.equal(byteSub, 0, lenSub, byteS, start + len - lenSub, lenSub);
    }
  }

  /**
   * Matches the middle of each string to its pattern.
   */
  protected static final class MiddleChecker implements Checker {
    final StringExpr.Finder finder;

    MiddleChecker(String pattern) {
      finder = StringExpr.compile(pattern.getBytes(StandardCharsets.UTF_8));
    }

    public boolean check(byte[] byteS, int start, int len) {
      return index(byteS, start, len) != -1;
    }

    /*
     * Returns absolute offset of the match
     */
    public int index(byte[] byteS, int start, int len) {
      return finder.find(byteS, start, len);
    }
  }

  /**
   * Matches a chained sequence of checkers.
   *
   * This has 4 chain scenarios cases in it (has no escaping or single char wildcards)
   *
   * 1) anchored left "abc%def%"
   * 2) anchored right "%abc%def"
   * 3) unanchored "%abc%def%"
   * 4) anchored on both sides "abc%def"
   */
  protected static final class ChainedChecker implements Checker {

    final int minLen;
    final BeginChecker begin;
    final EndChecker end;
    final MiddleChecker[] middle;
    final int[] midLens;
    final int beginLen;
    final int endLen;

    ChainedChecker(String pattern) {
      final StringTokenizer tokens = new StringTokenizer(pattern, "%");
      final boolean leftAnchor = pattern.startsWith("%") == false;
      final boolean rightAnchor = pattern.endsWith("%") == false;
      int len = 0;
      // at least 2 checkers always
      BeginChecker left = null;
      EndChecker right = null;
      int leftLen = 0; // not -1
      int rightLen = 0; // not -1
      final List<MiddleChecker> checkers = new ArrayList<MiddleChecker>(2);
      final List<Integer> lengths = new ArrayList<Integer>(2);

      for (int i = 0; tokens.hasMoreTokens(); i++) {
        String chunk = tokens.nextToken();
        if (chunk.length() == 0) {
          // %% is folded in the .*?.*? regex usually into .*?
          continue;
        }
        len += utf8Length(chunk);
        if (leftAnchor && i == 0) {
          // first item
          left = new BeginChecker(chunk);
          leftLen = utf8Length(chunk);
        } else if (rightAnchor && tokens.hasMoreTokens() == false) {
          // last item
          right = new EndChecker(chunk);
          rightLen = utf8Length(chunk);
        } else {
          // middle items in order
          checkers.add(new MiddleChecker(chunk));
          lengths.add(utf8Length(chunk));
        }
      }
      midLens = ArrayUtils.toPrimitive(lengths.toArray(ArrayUtils.EMPTY_INTEGER_OBJECT_ARRAY));
      middle = checkers.toArray(new MiddleChecker[0]);
      minLen = len;
      begin = left;
      end = right;
      beginLen = leftLen;
      endLen = rightLen;
    }

    public boolean check(byte[] byteS, final int start, final int len) {
      int pos = start;
      int mark = len;
      if (len < minLen) {
        return false;
      }
      // prefix, extend start
      if (begin != null && false == begin.check(byteS, pos, mark)) {
        // no match
        return false;
      } else {
        pos += beginLen;
        mark -= beginLen;
      }
      // suffix, reduce len
      if (end != null && false == end.check(byteS, pos, mark)) {
        // no match
        return false;
      } else {
        // no pos change - no need since we've shrunk the string with same pos
        mark -= endLen;
      }
      // loop for middles
      for (int i = 0; i < middle.length; i++) {
        int index = middle[i].index(byteS, pos, mark);
        if (index == -1) {
          // no match
          return false;
        } else {
          mark -= ((index-pos) + midLens[i]);
          pos = index + midLens[i];
        }
      }
      // if all is good
      return true;
    }

    private int utf8Length(String chunk) {
      try {
        return chunk.getBytes("UTF-8").length;
      } catch (UnsupportedEncodingException ue) {
        throw new RuntimeException(ue);
      }
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
      return matcher.find(0);
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
      decoder = StandardCharsets.UTF_8.newDecoder()
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
  public String vectorExpressionParameters() {
    return "col " + colNum + ", pattern " + pattern;
  }

  @Override
  public VectorExpressionDescriptor.Descriptor getDescriptor() {
    return (new VectorExpressionDescriptor.Builder())
        .setMode(
            VectorExpressionDescriptor.Mode.FILTER)
        .setNumArguments(2)
        .setArgumentTypes(
            VectorExpressionDescriptor.ArgumentType.STRING_FAMILY,
            VectorExpressionDescriptor.ArgumentType.STRING_FAMILY)
        .setInputExpressionTypes(
            VectorExpressionDescriptor.InputExpressionType.COLUMN,
            VectorExpressionDescriptor.InputExpressionType.SCALAR).build();
  }
}
