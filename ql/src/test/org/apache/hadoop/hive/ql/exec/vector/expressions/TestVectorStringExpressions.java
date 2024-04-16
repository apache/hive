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

import static org.junit.Assert.assertEquals;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Random;
import java.util.StringTokenizer;

import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.Assert;

import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.CharScalarEqualStringGroupColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterCharScalarEqualStringGroupColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterCharScalarGreaterStringGroupColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterCharScalarLessEqualStringGroupColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterStringGroupColEqualCharScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterStringGroupColEqualStringScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterStringGroupColEqualVarCharScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterStringGroupColGreaterEqualCharScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterStringGroupColGreaterEqualStringScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterStringGroupColGreaterEqualVarCharScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterStringGroupColLessCharScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterStringGroupColLessStringGroupColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterStringGroupColLessStringScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterStringGroupColLessVarCharScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterStringScalarEqualStringGroupColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterStringScalarGreaterStringGroupColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterStringScalarLessEqualStringGroupColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterVarCharScalarEqualStringGroupColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterVarCharScalarGreaterStringGroupColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterVarCharScalarLessEqualStringGroupColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.StringGroupColEqualCharScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.StringGroupColEqualStringScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.StringGroupColEqualVarCharScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.StringGroupColLessStringGroupColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.StringScalarEqualStringGroupColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.VarCharScalarEqualStringGroupColumn;
import org.apache.hadoop.hive.ql.exec.vector.util.VectorizedRowGroupGenUtil;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFLike;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test vectorized expression and filter evaluation for strings.
 */
public class TestVectorStringExpressions {

  private static final Logger LOG = LoggerFactory
      .getLogger(TestVectorStringExpressions.class);

  private static byte[] red;
  private static byte[] redred;
  private static byte[] red2; // second copy of red, different object
  private static byte[] green;
  private static byte[] greenred;
  private static byte[] redgreen;
  private static byte[] greengreen;
  private static byte[] blue;
  private static byte[] emptyString;
  private static byte[] mixedUp;
  private static byte[] mixedUpLower;
  private static byte[] mixedUpUpper;
  private static byte[] multiByte;
  private static byte[] mixPercentPattern;
  private static byte[] blanksLeft;
  private static byte[] blanksRight;
  private static byte[] blanksBoth;
  private static byte[] blankString;
  private static byte[] blankRanges;
  private static byte[] ascii_sentence;

  static {
    blue = "blue".getBytes(StandardCharsets.UTF_8);
    red = "red".getBytes(StandardCharsets.UTF_8);
    redred = "redred".getBytes(StandardCharsets.UTF_8);
    green = "green".getBytes(StandardCharsets.UTF_8);
    greenred = "greenred".getBytes(StandardCharsets.UTF_8);
    redgreen = "redgreen".getBytes(StandardCharsets.UTF_8);
    greengreen = "greengreen".getBytes(StandardCharsets.UTF_8);
    emptyString = "".getBytes(StandardCharsets.UTF_8);
    mixedUp = "mixedUp".getBytes(StandardCharsets.UTF_8);
    mixedUpLower = "mixedup".getBytes(StandardCharsets.UTF_8);
    mixedUpUpper = "MIXEDUP".getBytes(StandardCharsets.UTF_8);

    // for use as wildcard pattern to test LIKE
    mixPercentPattern = "mix%".getBytes(StandardCharsets.UTF_8); 

    multiByte = new byte[10];
    addMultiByteChars(multiByte);
    blanksLeft = "  foo".getBytes(StandardCharsets.UTF_8);
    blanksRight = "foo  ".getBytes(StandardCharsets.UTF_8);
    blanksBoth = "  foo  ".getBytes(StandardCharsets.UTF_8);
    blankString = "  ".getBytes(StandardCharsets.UTF_8);
    blankRanges =
        "   more  than a    bargain    ".getBytes(StandardCharsets.UTF_8);
    // 012345678901234567890123456789
    ascii_sentence =
        "The fox trotted over the fence.".getBytes(StandardCharsets.UTF_8);
    // 0123456789012345678901234567890
    red2 = new byte[red.length];
    System.arraycopy(red, 0, red2, 0, red.length);
  }

  // add some multi-byte characters to test length routine later.
  // total characters = 4; byte length = 10
  static void addMultiByteChars(byte[] b) {
    int i = 0;
    b[i++] = (byte) 0x41; // letter "A" (1 byte)
    b[i++] = (byte) 0xC3; // Latin capital A with grave (2 bytes)
    b[i++] = (byte) 0x80;
    b[i++] = (byte) 0xE2; // Euro sign (3 bytes)
    b[i++] = (byte) 0x82;
    b[i++] = (byte) 0xAC;
    b[i++] = (byte) 0xF0; // Asian character U+24B62 (4 bytes)
    b[i++] = (byte) 0xA4;
    b[i++] = (byte) 0xAD;
    b[i++] = (byte) 0xA2;
  }

  //-------------------------------------------------------------
  
  // total characters = 2; byte length = 3
  static void addMultiByteCharLeftPadded1_1(byte[] b) {
    int i = 0;
    b[i++] = (byte) 0x20; // blank " " (1 byte)
    b[i++] = (byte) 0xD0; // Cyrillic Capital DJE U+402 (2 bytes)
    b[i++] = (byte) 0x82;
  }

  // total characters = 3; byte length = 9
  static void addMultiByteCharLeftPadded1_2(byte[] b) {
    int i = 0;
    b[i++] = (byte) 0x20; // blank " " (1 byte)
    b[i++] = (byte) 0xF0; // Smiling Face with Open Mouth and Smiling Eyes U+1F604 (4 bytes)
    b[i++] = (byte) 0x9F;
    b[i++] = (byte) 0x98;
    b[i++] = (byte) 0x84;
    b[i++] = (byte) 0xF0; // Grimacing Face U+1F62C (4 bytes)
    b[i++] = (byte) 0x9F;
    b[i++] = (byte) 0x98;
    b[i++] = (byte) 0xAC;
  }

  // total characters = 4; byte length = 6
  static void addMultiByteCharLeftPadded3_1(byte[] b) {
    int i = 0;
    b[i++] = (byte) 0x20; // blank " " (1 byte)
    b[i++] = (byte) 0x20; // blank " " (1 byte)
    b[i++] = (byte) 0x20; // blank " " (1 byte)
    b[i++] = (byte) 0xE4; // Asian character U+4824 (3 bytes)
    b[i++] = (byte) 0xA0;
    b[i++] = (byte) 0xA4;
  }

  //-------------------------------------------------------------
  
  // total characters = 2; byte length = 4
  static void addMultiByteCharRightPadded1_1(byte[] b) {
    int i = 0;
    b[i++] = (byte) 0xE0; // Tamil Om U+0BD0 (3 bytes)
    b[i++] = (byte) 0xAF;
    b[i++] = (byte) 0x90;
    b[i++] = (byte) 0x20; // blank " " (1 byte)
  }

  // total characters = 3; byte length = 5
  static void addMultiByteCharRightPadded1_2(byte[] b) {
    int i = 0;
    b[i++] = (byte) 0xEA; // Va Syllable MEE U+A521 (3 bytes)
    b[i++] = (byte) 0x94;
    b[i++] = (byte) 0xA1;
    b[i++] = (byte) 0x5A; // Latin Capital Letter Z U+005A (1 bytes)
    b[i++] = (byte) 0x20; // blank " " (1 byte)
  }

  // total characters = 4; byte length = 9
  static void addMultiByteCharRightPadded1_3(byte[] b) {
    int i = 0;
    b[i++] = (byte) 0xCC; // COMBINING ACUTE ACENT U+0301 (2 bytes)
    b[i++] = (byte) 0x81;
    b[i++] = (byte) 0xE0; // DEVENAGARI LETTER KA U+0915 (3 bytes)
    b[i++] = (byte) 0xA4;
    b[i++] = (byte) 0x95;
    b[i++] = (byte) 0xE0; // DEVENAGARI SIGN VIRAMA U+094D (3 bytes)
    b[i++] = (byte) 0xA5;
    b[i++] = (byte) 0x8D;
    b[i++] = (byte) 0x20; // blank " " (1 byte)
  }

  // total characters = 10; byte length = 26
  static int addMultiByteCharSentenceOne(byte[] b, int start) {
    int i = start;
    b[i++] = (byte) 0xE1; // NEW TAI LUE LETTER THA U+1992 (3 bytes)
    b[i++] = (byte) 0xA6;
    b[i++] = (byte) 0x92;
    b[i++] = (byte) 0xE1; // NEW TAI LUE LETTER LOW XA U+1986 (3 bytes)
    b[i++] = (byte) 0xA6;
    b[i++] = (byte) 0x86;
    b[i++] = (byte) 0xE1; // NEW TAI LUE LETTER HIGH MA U+1996 (3 bytes)
    b[i++] = (byte) 0xA6;
    b[i++] = (byte) 0x96;
    b[i++] = (byte) 0x20; // blank " " (1 byte)
    b[i++] = (byte) 0xE1; // NEW TAI LUE LETTER LOW QA U+1981 (3 bytes)
    b[i++] = (byte) 0xA6;
    b[i++] = (byte) 0x81;
    b[i++] = (byte) 0x20; // blank " " (1 byte)
    b[i++] = (byte) 0xE1; // NEW TAI LUE LETTER LOW BA U+19A5 (3 bytes)
    b[i++] = (byte) 0xA6;
    b[i++] = (byte) 0xA5;
    b[i++] = (byte) 0xE1; // NEW TAI LUE LETTER HIGH LA U+199C (3 bytes)
    b[i++] = (byte) 0xA6;
    b[i++] = (byte) 0x9C;
    b[i++] = (byte) 0xE1; // NEW TAI LUE LETTER LOW KVA U+19A8 (3 bytes)
    b[i++] = (byte) 0xA6;
    b[i++] = (byte) 0xA8;
    b[i++] = (byte) 0xE1; // NEW TAI LUE LETTER LOW FA U+199D (3 bytes)
    b[i++] = (byte) 0xA6;
    b[i++] = (byte) 0x9D;
    return i;
  }

  // total characters = 13; byte length = 24
  static int addMultiByteCharSentenceTwo(byte[] b, int start) {
    int i = start;
    b[i++] = (byte) 0xC9; // LATIN SMALL LETTER TURNED A U+0250 (2 bytes)
    b[i++] = (byte) 0x90;
    b[i++] = (byte) 0xC9; // LATIN SMALL LETTER GAMMA U+0263 (2 bytes)
    b[i++] = (byte) 0xA3;
    b[i++] = (byte) 0xC9; // LATIN SMALL LETTER TURNED M U+026F (2 bytes)
    b[i++] = (byte) 0xAF;
    b[i++] = (byte) 0xCA; // LATIN SMALL LETTER S WITH HOOK U+0282 (2 bytes)
    b[i++] = (byte) 0x82;
    b[i++] = (byte) 0x20; // blank " " (1 byte)
    b[i++] = (byte) 0xCA; // LATIN LETTER SMALL CAPITAL L U+029F (2 bytes)
    b[i++] = (byte) 0x9F;
    b[i++] = (byte) 0xCB; // MODIFIER LETTER TRIANGULAR COLON U+02D0 (2 bytes)
    b[i++] = (byte) 0x90;
    b[i++] = (byte) 0x20; // blank " " (1 byte)
    b[i++] = (byte) 0xCB; // RING ABOVE U+02DA (2 bytes)
    b[i++] = (byte) 0x9A;
    b[i++] = (byte) 0xCB; // MODIFIER LETTER SMALL L U+02E1 (2 bytes)
    b[i++] = (byte) 0xA1;
    b[i++] = (byte) 0xCB; // MODIFIER LETTER SMALL X U+02E3 (2 bytes)
    b[i++] = (byte) 0xA3;
    b[i++] = (byte) 0xCB; // MODIFIER LETTER UP ARROWHEAD U+02C4 (2 bytes)
    b[i++] = (byte) 0x84;
    b[i++] = (byte) 0x2E; // FULL STOP "." (1 byte)
    return i;
  }

  // total characters = 17; byte length = 30
  static int addMultiByteCharSentenceBlankRanges(byte[] b, int start) {
    int i = start;
    b[i++] = (byte) 0xF0; // INSCRIPTIONAL YODH U+10B49 (4 bytes)
    b[i++] = (byte) 0x90;
    b[i++] = (byte) 0xAD;
    b[i++] = (byte) 0x89;
    b[i++] = (byte) 0xE1; // NEW TAI LUE LETTER LOW FA U+199D (3 bytes)
    b[i++] = (byte) 0xA6;
    b[i++] = (byte) 0x9D;
    b[i++] = (byte) 0x20; // blank " " (1 byte)
    b[i++] = (byte) 0x20; // blank " " (1 byte)
    b[i++] = (byte) 0x20; // blank " " (1 byte)
    b[i++] = (byte) 0x2D; // hyphen-minus "-" U-002D (1 byte)
    b[i++] = (byte) 0x20; // blank " " (1 byte)
    b[i++] = (byte) 0x60; // grave accent "-" U-0060 (1 byte)
    b[i++] = (byte) 0xE2; // BLACK SUN WITH RAYS U+2600 (3 bytes)
    b[i++] = (byte) 0x98;
    b[i++] = (byte) 0x80;
    b[i++] = (byte) 0xE2; // BALLOT BOX WITH X U+2612 (3 bytes)
    b[i++] = (byte) 0x98;
    b[i++] = (byte) 0x92;
    b[i++] = (byte) 0x20; // blank " " (1 byte)
    b[i++] = (byte) 0x20; // blank " " (1 byte)
    b[i++] = (byte) 0x20; // blank " " (1 byte)
    b[i++] = (byte) 0x20; // blank " " (1 byte)
    b[i++] = (byte) 0x20; // blank " " (1 byte)
    b[i++] = (byte) 0xE2; // WHITE START U+2606 (3 bytes)
    b[i++] = (byte) 0x98;
    b[i++] = (byte) 0x86;
    b[i++] = (byte) 0xE2; // WHITE FLAG WITH HORIZONTAL MIDDLE BLACK STRIPE U+26FF (3 bytes)
    b[i++] = (byte) 0x9B;
    b[i++] = (byte) 0xBF;
    return i;
  }


  static int addPads(byte[] b, int start, int count) {
    int i = start;
    int end = start + count;
    for ( ; i < end; i++) {
      b[i] = (byte) 0x20; // blank " " (1 byte)
    }
    return i;
  }

  private HiveConf hiveConf = new HiveConf();

  private boolean vectorEqual(BytesColumnVector vector, int i, byte[] bytes, int offset, int length) {
    byte[] bytesSlice = new byte[length];
    System.arraycopy(bytes, offset, bytesSlice, 0, length);
    int vectorLength = vector.length[i];
    byte[] vectorSlice = new byte[vectorLength];
    System.arraycopy(vector.vector[i], vector.start[i], vectorSlice, 0, vectorLength);
    boolean equals = Arrays.equals(bytesSlice, vectorSlice);
    if (!equals) {
      System.out.println("vectorEqual offset " + offset + " length " + length + " vectorSlice.length " + vectorSlice.length);
      System.out.println("vectorEqual bytesSlice " + Hex.encodeHexString(bytesSlice));
      System.out.println("vectorEqual vectorSlice " + Hex.encodeHexString(vectorSlice));
    }
    return equals;
  }

  private int vectorCharacterCount(BytesColumnVector vector, int i) {
    return StringExpr.characterCount(vector.vector[i], vector.start[i], vector.length[i]);
  }

  @Test
  // Test basic assign to vector.
  public void testAssignBytesColumnVector()  {
      BytesColumnVector outV = new BytesColumnVector(VectorizedRowBatch.DEFAULT_SIZE);
      outV.initBuffer(35); // initialize with estimated element size 35

      int i = 0;

      int expectedResultLen;

      Assert.assertTrue(StringExpr.characterCount(blue, 0, blue.length) == 4);
      StringExpr.assign(outV, i, blue, 0, blue.length);
      expectedResultLen = blue.length;
      Assert.assertTrue(vectorEqual(outV, i, blue, 0, expectedResultLen));
      i++;
      Assert.assertTrue(StringExpr.characterCount(redgreen, 0, redgreen.length) == 8);
      StringExpr.assign(outV, i, redgreen, 0, redgreen.length);
      expectedResultLen =  redgreen.length;
      Assert.assertTrue(vectorEqual(outV, i, redgreen, 0, expectedResultLen));
      i++;
      Assert.assertTrue(StringExpr.characterCount(ascii_sentence, 0, ascii_sentence.length) == 31);
      StringExpr.assign(outV, i, ascii_sentence, 0, ascii_sentence.length);
      expectedResultLen =  ascii_sentence.length;
      Assert.assertTrue(vectorEqual(outV, i, ascii_sentence, 0, expectedResultLen));
      i++;
      Assert.assertTrue(StringExpr.characterCount(blanksLeft, 0, blanksLeft.length) == 5);
      StringExpr.assign(outV, i, blanksLeft, 0, blanksLeft.length);
      expectedResultLen =  blanksLeft.length;
      Assert.assertTrue(vectorEqual(outV, i, blanksLeft, 0, expectedResultLen));
      i++;

      // Multi-byte characters with blank ranges.
      byte[] sentenceBlankRanges = new byte[100];
      int sentenceBlankRangesLen = addMultiByteCharSentenceBlankRanges(sentenceBlankRanges, 0);

      Assert.assertTrue(StringExpr.characterCount(sentenceBlankRanges, 0, sentenceBlankRangesLen) == 17);
      StringExpr.assign(outV, i, sentenceBlankRanges, 0, sentenceBlankRangesLen);
      expectedResultLen = sentenceBlankRangesLen;
      Assert.assertTrue(vectorEqual(outV, i, sentenceBlankRanges, 0, expectedResultLen));
      i++;
      Assert.assertTrue(StringExpr.characterCount(sentenceBlankRanges, 0, sentenceBlankRangesLen - 3) == 16);
      StringExpr.assign(outV, i, sentenceBlankRanges, 0, sentenceBlankRangesLen - 3);
      expectedResultLen = sentenceBlankRangesLen - 3;
      Assert.assertTrue(vectorEqual(outV, i, sentenceBlankRanges, 0, expectedResultLen));
      i++;

      // Some non-zero offsets.
      Assert.assertTrue(StringExpr.characterCount(sentenceBlankRanges, 4, sentenceBlankRangesLen - 4) == 16);
      StringExpr.assign(outV, i, sentenceBlankRanges, 4, sentenceBlankRangesLen - 4);
      expectedResultLen = sentenceBlankRangesLen - 4;
      Assert.assertTrue(vectorEqual(outV, i, sentenceBlankRanges, 4, expectedResultLen));
      Assert.assertTrue(vectorCharacterCount(outV, i) == 16);
      i++;
      Assert.assertTrue(StringExpr.characterCount(sentenceBlankRanges, 7, 17) == 13);
      StringExpr.assign(outV, i, sentenceBlankRanges, 7, 17);
      expectedResultLen = 17;
      Assert.assertTrue(vectorEqual(outV, i, sentenceBlankRanges, 7, expectedResultLen));
      Assert.assertTrue(vectorCharacterCount(outV, i) == 13);
      i++;
  }

  @Test
  // Test basic right trim of bytes slice.
  public void testRightTrimBytesSlice()  {
      int resultLen;
      // Nothing to trim (ASCII).
      Assert.assertTrue(StringExpr.characterCount(blue, 0, blue.length) == 4);
      resultLen = StringExpr.rightTrim(blue, 0, blue.length);
      Assert.assertTrue(resultLen == blue.length);
      Assert.assertTrue(StringExpr.characterCount(blue, 0, resultLen) == 4);

      Assert.assertTrue(StringExpr.characterCount(redgreen, 0, redgreen.length) == 8);
      resultLen = StringExpr.rightTrim(redgreen, 0, redgreen.length);
      Assert.assertTrue(resultLen == redgreen.length);

      Assert.assertTrue(StringExpr.characterCount(ascii_sentence, 0, ascii_sentence.length) == 31);
      resultLen = StringExpr.rightTrim(ascii_sentence, 0, ascii_sentence.length);
      Assert.assertTrue(resultLen == ascii_sentence.length);

      Assert.assertTrue(StringExpr.characterCount(blanksLeft, 0, blanksLeft.length) == 5);
      resultLen = StringExpr.rightTrim(blanksLeft, 0, blanksLeft.length);
      Assert.assertTrue(resultLen == blanksLeft.length);

      // Simple trims.
      Assert.assertTrue(StringExpr.characterCount(blanksRight, 0, blanksRight.length) == 5);
      resultLen = StringExpr.rightTrim(blanksRight, 0, blanksRight.length);
      Assert.assertTrue(resultLen == 3);
      Assert.assertTrue(StringExpr.characterCount(blanksRight, 0, resultLen) == 3);

      Assert.assertTrue(StringExpr.characterCount(blanksBoth, 0, blanksBoth.length) == 7);
      resultLen = StringExpr.rightTrim(blanksBoth, 0, blanksBoth.length);
      Assert.assertTrue(resultLen == 5);
      Assert.assertTrue(StringExpr.characterCount(blanksBoth, 0, resultLen) == 5);
     
      Assert.assertTrue(StringExpr.characterCount(blankString, 0, blankString.length) == 2);
      resultLen = StringExpr.rightTrim(blankString, 0, blankString.length);
      Assert.assertTrue(resultLen == 0);
      Assert.assertTrue(StringExpr.characterCount(blankString, 0, resultLen) == 0);

      Assert.assertTrue(StringExpr.characterCount(blankRanges, 0, blankRanges.length) == 30);
      resultLen = StringExpr.rightTrim(blankRanges, 0, blankRanges.length);
      Assert.assertTrue(resultLen == blankRanges.length - 4);
      Assert.assertTrue(StringExpr.characterCount(blankRanges, 0, resultLen) == 26);

      // Offset trims.
      Assert.assertTrue(StringExpr.characterCount(blanksRight, 1, blanksRight.length - 1) == 4);
      resultLen = StringExpr.rightTrim(blanksRight, 1, blanksRight.length - 1);
      Assert.assertTrue(resultLen == 2);
      Assert.assertTrue(StringExpr.characterCount(blanksRight, 1, resultLen) == 2);

      Assert.assertTrue(StringExpr.characterCount(blanksBoth, 4, blanksBoth.length - 4) == 3);
      resultLen = StringExpr.rightTrim(blanksBoth, 4, blanksBoth.length - 4);
      Assert.assertTrue(resultLen == 1);
      Assert.assertTrue(StringExpr.characterCount(blanksBoth, 4, resultLen) == 1);

      Assert.assertTrue(StringExpr.characterCount(blanksBoth, 5, blanksBoth.length - 5) == 2);
      resultLen = StringExpr.rightTrim(blanksBoth, 5, blanksBoth.length -5 );
      Assert.assertTrue(resultLen == 0);
      Assert.assertTrue(StringExpr.characterCount(blanksBoth, 5, resultLen) == 0);

      Assert.assertTrue(StringExpr.characterCount(blankString, 1, blankString.length - 1) == 1);
      resultLen = StringExpr.rightTrim(blankString, 1, blankString.length - 1);
      Assert.assertTrue(resultLen == 0);
      Assert.assertTrue(StringExpr.characterCount(blankString, 1, resultLen) == 0);

      Assert.assertTrue(StringExpr.characterCount(blankRanges, 4, blankRanges.length - 4) == 26);
      resultLen = StringExpr.rightTrim(blankRanges, 4, blankRanges.length - 4);
      Assert.assertTrue(resultLen == blankRanges.length - 4 -4);
      Assert.assertTrue(StringExpr.characterCount(blankRanges, 4, resultLen) == 22);

      Assert.assertTrue(StringExpr.characterCount(blankRanges, 6, blankRanges.length - 6) == 24);
      resultLen = StringExpr.rightTrim(blankRanges, 6, blankRanges.length- 6);
      Assert.assertTrue(resultLen == blankRanges.length - 6 - 4);
      Assert.assertTrue(StringExpr.characterCount(blankRanges, 6, resultLen) == 20);

      Assert.assertTrue(StringExpr.characterCount(blankRanges, 7, blankRanges.length - 7) == 23);
      resultLen = StringExpr.rightTrim(blankRanges, 7, blankRanges.length - 7);
      Assert.assertTrue(resultLen == blankRanges.length - 7 - 4);
      Assert.assertTrue(StringExpr.characterCount(blankRanges, 7, resultLen) == 19);

      Assert.assertTrue(StringExpr.characterCount(blankRanges, 7, 8 - 7) == 1);
      resultLen = StringExpr.rightTrim(blankRanges, 7, 8 - 7);
      Assert.assertTrue(resultLen == 0);
      Assert.assertTrue(StringExpr.characterCount(blankRanges, 7, resultLen) == 0);

      // Multi-byte trims.
      byte[] multiByte = new byte[100];

      addMultiByteCharRightPadded1_1(multiByte);
      Assert.assertTrue(StringExpr.characterCount(multiByte, 0, 4) == 2);
      resultLen = StringExpr.rightTrim(multiByte, 0, 4);
      Assert.assertTrue(resultLen == 3);
      Assert.assertTrue(StringExpr.characterCount(multiByte, 0, resultLen) == 1);

      addMultiByteCharRightPadded1_2(multiByte);
      Assert.assertTrue(StringExpr.characterCount(multiByte, 0, 5) == 3);
      resultLen = StringExpr.rightTrim(multiByte, 0, 5);
      Assert.assertTrue(resultLen == 4);
      Assert.assertTrue(StringExpr.characterCount(multiByte, 0, resultLen) == 2);

      addMultiByteCharRightPadded1_3(multiByte);
      Assert.assertTrue(StringExpr.characterCount(multiByte, 0, 9) == 4);
      resultLen = StringExpr.rightTrim(multiByte, 0, 9);
      Assert.assertTrue(resultLen == 8);
      Assert.assertTrue(StringExpr.characterCount(multiByte, 0, resultLen) == 3);

      addMultiByteCharRightPadded1_1(multiByte);
      Assert.assertTrue(StringExpr.characterCount(multiByte, 3, 1) == 1);
      resultLen = StringExpr.rightTrim(multiByte, 3, 1);
      Assert.assertTrue(resultLen == 0);
      Assert.assertTrue(StringExpr.characterCount(multiByte, 0, resultLen) == 0);

      addMultiByteCharRightPadded1_2(multiByte);
      Assert.assertTrue(StringExpr.characterCount(multiByte, 3, 2) == 2);
      resultLen = StringExpr.rightTrim(multiByte, 3, 2);
      Assert.assertTrue(resultLen == 1);
      Assert.assertTrue(StringExpr.characterCount(multiByte, 3, resultLen) == 1);

      byte[] sentenceOne = new byte[100];
      int sentenceOneLen = addMultiByteCharSentenceOne(sentenceOne, 0);

      Assert.assertTrue(StringExpr.characterCount(sentenceOne, 0, sentenceOneLen) == 10);
      resultLen = StringExpr.rightTrim(sentenceOne, 0, sentenceOneLen);
      Assert.assertTrue(resultLen == sentenceOneLen);

      Assert.assertTrue(StringExpr.characterCount(sentenceOne, 0, sentenceOneLen - 3) == 9);
      resultLen = StringExpr.rightTrim(sentenceOne, 0, sentenceOneLen - 3);
      Assert.assertTrue(resultLen == sentenceOneLen - 3);

      byte[] sentenceTwo = new byte[100];
      int sentenceTwoLen = addMultiByteCharSentenceTwo(sentenceTwo, 0);

      Assert.assertTrue(StringExpr.characterCount(sentenceTwo, 0, sentenceTwoLen) == 13);
      resultLen = StringExpr.rightTrim(sentenceTwo, 0, sentenceTwoLen);
      Assert.assertTrue(resultLen == sentenceTwoLen);

      Assert.assertTrue(StringExpr.characterCount(sentenceTwo, 0, sentenceTwoLen- 5) == 10);
      resultLen = StringExpr.rightTrim(sentenceTwo, 0, sentenceTwoLen - 5);
      Assert.assertTrue(resultLen == sentenceTwoLen - 5);

      int start;

      // Left pad longer strings with multi-byte characters.
      byte[] sentenceOnePaddedLeft = new byte[100];
      start = addPads(sentenceOnePaddedLeft, 0, 3);
      int sentenceOnePaddedLeftLen = addMultiByteCharSentenceOne(sentenceOnePaddedLeft, start);

      Assert.assertTrue(StringExpr.characterCount(sentenceOnePaddedLeft, 0, sentenceOnePaddedLeftLen) == 3 + 10);
      resultLen = StringExpr.rightTrim(sentenceOnePaddedLeft, 0, sentenceOnePaddedLeftLen);
      Assert.assertTrue(resultLen == sentenceOnePaddedLeftLen);

      Assert.assertTrue(StringExpr.characterCount(sentenceOnePaddedLeft, 0, sentenceOnePaddedLeftLen - 3) == 3 + 9);
      resultLen = StringExpr.rightTrim(sentenceOnePaddedLeft, 0, sentenceOnePaddedLeftLen - 3);
      Assert.assertTrue(resultLen == sentenceOnePaddedLeftLen - 3);

      byte[] sentenceTwoPaddedLeft = new byte[100];
      start = addPads(sentenceTwoPaddedLeft, 0, 2);
      int sentenceTwoPaddedLeftLen = addMultiByteCharSentenceTwo(sentenceTwoPaddedLeft, start);

      Assert.assertTrue(StringExpr.characterCount(sentenceTwoPaddedLeft, 0, sentenceTwoPaddedLeftLen) == 2 + 13);
      resultLen = StringExpr.rightTrim(sentenceTwoPaddedLeft, 0, sentenceTwoPaddedLeftLen);
      Assert.assertTrue(resultLen == sentenceTwoPaddedLeftLen);

      Assert.assertTrue(StringExpr.characterCount(sentenceTwoPaddedLeft, 0, sentenceTwoPaddedLeftLen - 5) == 2 + 10);
      resultLen = StringExpr.rightTrim(sentenceTwoPaddedLeft, 0, sentenceTwoPaddedLeftLen - 5);
      Assert.assertTrue(resultLen == sentenceTwoPaddedLeftLen - 5);

      // Right pad longer strings with multi-byte characters.
      byte[] sentenceOnePaddedRight = new byte[100];
      start = addMultiByteCharSentenceOne(sentenceOnePaddedRight, 0);
      int sentenceOnePaddedRightLen = addPads(sentenceOnePaddedRight, start, 4);

      Assert.assertTrue(StringExpr.characterCount(sentenceOnePaddedRight, 0, sentenceOnePaddedRightLen) == 10 + 4);
      resultLen = StringExpr.rightTrim(sentenceOnePaddedRight, 0, sentenceOnePaddedRightLen);
      Assert.assertTrue(resultLen == sentenceOnePaddedRightLen - 4);

      Assert.assertTrue(StringExpr.characterCount(sentenceOnePaddedRight, 0, sentenceOnePaddedRightLen - 3 - 4) == 9);
      resultLen = StringExpr.rightTrim(sentenceOnePaddedRight, 0, sentenceOnePaddedRightLen - 3 - 4);
      Assert.assertTrue(resultLen == sentenceOnePaddedRightLen - 3 - 4);

      byte[] sentenceTwoPaddedRight = new byte[100];
      start = addMultiByteCharSentenceTwo(sentenceTwoPaddedRight, 0);
      int sentenceTwoPaddedRightLen = addPads(sentenceTwoPaddedRight, start, 1);

      Assert.assertTrue(StringExpr.characterCount(sentenceTwoPaddedRight, 0, sentenceTwoPaddedRightLen) == 13 + 1);
      resultLen = StringExpr.rightTrim(sentenceTwoPaddedRight, 0, sentenceTwoPaddedRightLen);
      Assert.assertTrue(resultLen == sentenceTwoPaddedRightLen - 1);

      Assert.assertTrue(StringExpr.characterCount(sentenceTwoPaddedRight, 0, sentenceTwoPaddedRightLen - 5 - 1) == 10);
      resultLen = StringExpr.rightTrim(sentenceTwoPaddedRight, 0, sentenceTwoPaddedRightLen - 5 - 1);
      Assert.assertTrue(resultLen == sentenceTwoPaddedRightLen - 5 - 1);

      // Multi-byte characters with blank ranges.
      byte[] sentenceBlankRanges = new byte[100];
      int sentenceBlankRangesLen = addMultiByteCharSentenceBlankRanges(sentenceBlankRanges, 0);

      Assert.assertTrue(StringExpr.characterCount(sentenceBlankRanges, 0, sentenceBlankRangesLen) == 17);
      resultLen = StringExpr.rightTrim(sentenceBlankRanges, 0, sentenceBlankRangesLen);
      Assert.assertTrue(resultLen == sentenceBlankRangesLen);

      Assert.assertTrue(StringExpr.characterCount(sentenceBlankRanges, 0, sentenceBlankRangesLen - 3) == 16);
      resultLen = StringExpr.rightTrim(sentenceBlankRanges, 0, sentenceBlankRangesLen - 3);
      Assert.assertTrue(resultLen == sentenceBlankRangesLen - 3);

      Assert.assertTrue(StringExpr.characterCount(sentenceBlankRanges, 7, 17) == 13);
      resultLen = StringExpr.rightTrim(sentenceBlankRanges, 7, 17);
      Assert.assertTrue(resultLen == 12);
      Assert.assertTrue(StringExpr.characterCount(sentenceBlankRanges, 7, resultLen) == 8);
  }

  @Test
  // Test basic right trim to vector.
  public void testRightTrimBytesColumnVector()  {
      BytesColumnVector outV = new BytesColumnVector(VectorizedRowBatch.DEFAULT_SIZE);
      outV.initBuffer(30); // initialize with estimated element size 35

      int i = 0;
      int expectedResultLen;

      // Nothing to trim (ASCII).
      Assert.assertTrue(StringExpr.characterCount(blue, 0, blue.length) == 4);
      StringExpr.rightTrim(outV, i, blue, 0, blue.length);
      expectedResultLen = blue.length;
      Assert.assertTrue(vectorEqual(outV, i, blue, 0, expectedResultLen));
      Assert.assertTrue(vectorCharacterCount(outV, i) == 4);
      i++;
      Assert.assertTrue(StringExpr.characterCount(redgreen, 0, redgreen.length) == 8);
      StringExpr.rightTrim(outV, i, redgreen, 0, redgreen.length);
      expectedResultLen = redgreen.length;
      Assert.assertTrue(vectorEqual(outV, i, redgreen, 0, expectedResultLen));
      i++;
      Assert.assertTrue(StringExpr.characterCount(ascii_sentence, 0, ascii_sentence.length) == 31);
      StringExpr.rightTrim(outV, i, ascii_sentence, 0, ascii_sentence.length);
      expectedResultLen = ascii_sentence.length;
      Assert.assertTrue(vectorEqual(outV, i, ascii_sentence, 0, expectedResultLen));
      i++;
      Assert.assertTrue(StringExpr.characterCount(blanksLeft, 0, blanksLeft.length) == 5);
      StringExpr.rightTrim(outV, i, blanksLeft, 0, blanksLeft.length);
      expectedResultLen = blanksLeft.length;
      Assert.assertTrue(vectorEqual(outV, i, blanksLeft, 0, expectedResultLen));
      i++;

      // Simple trims.
      Assert.assertTrue(StringExpr.characterCount(blanksRight, 0, blanksRight.length) == 5);
      StringExpr.rightTrim(outV, i, blanksRight, 0, blanksRight.length);
      expectedResultLen = 3;
      Assert.assertTrue(vectorEqual(outV, i, blanksRight, 0, expectedResultLen));
      Assert.assertTrue(vectorCharacterCount(outV, i) == 3);
      i++;
      Assert.assertTrue(StringExpr.characterCount(blanksBoth, 0, blanksBoth.length) == 7);
      StringExpr.rightTrim(outV, i, blanksBoth, 0, blanksBoth.length);
      expectedResultLen = 5;
      Assert.assertTrue(vectorEqual(outV, i, blanksBoth, 0, expectedResultLen));
      Assert.assertTrue(vectorCharacterCount(outV, i) == 5);
      i++;
      Assert.assertTrue(StringExpr.characterCount(blankString, 0, blankString.length) == 2);
      StringExpr.rightTrim(outV, i, blankString, 0, blankString.length);
      expectedResultLen = 0;
      Assert.assertTrue(vectorEqual(outV, i, blankString, 0, expectedResultLen));
      Assert.assertTrue(vectorCharacterCount(outV, i) == 0);
      i++;
      Assert.assertTrue(StringExpr.characterCount(blankRanges, 0, blankRanges.length) == 30);
      StringExpr.rightTrim(outV, i, blankRanges, 0, blankRanges.length);
      expectedResultLen = blankRanges.length - 4;
      Assert.assertTrue(vectorEqual(outV, i, blankRanges, 0, expectedResultLen));
      Assert.assertTrue(vectorCharacterCount(outV, i) == 26);
      i++;

      // Offset trims.
      Assert.assertTrue(StringExpr.characterCount(blanksRight, 1, blanksRight.length - 1) == 4);
      StringExpr.rightTrim(outV, i, blanksRight, 1, blanksRight.length - 1);
      expectedResultLen = 2;
      Assert.assertTrue(vectorEqual(outV, i, blanksRight, 1, expectedResultLen));
      Assert.assertTrue(vectorCharacterCount(outV, i) == 2);
      i++;
      Assert.assertTrue(StringExpr.characterCount(blanksBoth, 4, blanksBoth.length - 4) == 3);
      StringExpr.rightTrim(outV, i, blanksBoth, 4, blanksBoth.length - 4);
      expectedResultLen = 1;
      Assert.assertTrue(vectorEqual(outV, i, blanksBoth, 4, expectedResultLen));
      Assert.assertTrue(vectorCharacterCount(outV, i) == 1);
      i++;
      Assert.assertTrue(StringExpr.characterCount(blanksBoth, 5, blanksBoth.length - 5) == 2);
      StringExpr.rightTrim(outV, i, blanksBoth, 5, blanksBoth.length -5 );
      expectedResultLen = 0;
      Assert.assertTrue(vectorEqual(outV, i, blanksBoth, 5, expectedResultLen));
      Assert.assertTrue(vectorCharacterCount(outV, i) == 0);
      i++;
      Assert.assertTrue(StringExpr.characterCount(blankString, 1, blankString.length - 1) == 1);
      StringExpr.rightTrim(outV, i, blankString, 1, blankString.length - 1);
      expectedResultLen = 0;
      Assert.assertTrue(vectorEqual(outV, i, blankString, 1, expectedResultLen));
      Assert.assertTrue(vectorCharacterCount(outV, i) == 0);
      i++;
      Assert.assertTrue(StringExpr.characterCount(blankRanges, 4, blankRanges.length - 4) == 26);
      StringExpr.rightTrim(outV, i, blankRanges, 4, blankRanges.length - 4);
      expectedResultLen = blankRanges.length - 4 -4;
      Assert.assertTrue(vectorEqual(outV, i, blankRanges, 4, expectedResultLen));
      Assert.assertTrue(vectorCharacterCount(outV, i) == 22);
      i++;
      Assert.assertTrue(StringExpr.characterCount(blankRanges, 6, blankRanges.length - 6) == 24);
      StringExpr.rightTrim(outV, i, blankRanges, 6, blankRanges.length- 6);
      expectedResultLen = blankRanges.length - 6 - 4;
      Assert.assertTrue(vectorEqual(outV, i, blankRanges, 6, expectedResultLen));
      Assert.assertTrue(vectorCharacterCount(outV, i) == 20);
      i++;
      Assert.assertTrue(StringExpr.characterCount(blankRanges, 7, blankRanges.length - 7) == 23);
      StringExpr.rightTrim(outV, i, blankRanges, 7, blankRanges.length - 7);
      expectedResultLen = blankRanges.length - 7 - 4;
      Assert.assertTrue(vectorEqual(outV, i, blankRanges, 7, expectedResultLen));
      Assert.assertTrue(vectorCharacterCount(outV, i) == 19);
      i++;
      Assert.assertTrue(StringExpr.characterCount(blankRanges, 7, 8 - 7) == 1);
      StringExpr.rightTrim(outV, i, blankRanges, 7, 8 - 7);
      expectedResultLen = 0;
      Assert.assertTrue(vectorEqual(outV, i, blankRanges, 7, expectedResultLen));
      Assert.assertTrue(vectorCharacterCount(outV, i) == 0);
      i++;

      // Multi-byte trims.
      byte[] multiByte = new byte[100];

      addMultiByteCharRightPadded1_1(multiByte);
      Assert.assertTrue(StringExpr.characterCount(multiByte, 0, 4) == 2);
      StringExpr.rightTrim(outV, i, multiByte, 0, 4);
      expectedResultLen = 3;
      Assert.assertTrue(vectorEqual(outV, i, multiByte, 0, expectedResultLen));
      Assert.assertTrue(vectorCharacterCount(outV, i) == 1);
      i++;
      addMultiByteCharRightPadded1_2(multiByte);
      Assert.assertTrue(StringExpr.characterCount(multiByte, 0, 5) == 3);
      StringExpr.rightTrim(outV, i, multiByte, 0, 5);
      expectedResultLen = 4;
      Assert.assertTrue(vectorEqual(outV, i, multiByte, 0, expectedResultLen));
      Assert.assertTrue(vectorCharacterCount(outV, i) == 2);
      i++;
      addMultiByteCharRightPadded1_3(multiByte);
      Assert.assertTrue(StringExpr.characterCount(multiByte, 0, 9) == 4);
      StringExpr.rightTrim(outV, i, multiByte, 0, 9);
      expectedResultLen = 8;
      Assert.assertTrue(vectorEqual(outV, i, multiByte, 0, expectedResultLen));
      Assert.assertTrue(vectorCharacterCount(outV, i) == 3);
      i++;
      addMultiByteCharRightPadded1_1(multiByte);
      Assert.assertTrue(StringExpr.characterCount(multiByte, 3, 1) == 1);
      StringExpr.rightTrim(outV, i, multiByte, 3, 1);
      expectedResultLen = 0;
      Assert.assertTrue(vectorEqual(outV, i, multiByte, 3, expectedResultLen));
      Assert.assertTrue(vectorCharacterCount(outV, i) == 0);
      i++;
      addMultiByteCharRightPadded1_2(multiByte);
      Assert.assertTrue(StringExpr.characterCount(multiByte, 3, 2) == 2);
      StringExpr.rightTrim(outV, i, multiByte, 3, 2);
      expectedResultLen = 1;
      Assert.assertTrue(vectorEqual(outV, i, multiByte, 3, expectedResultLen));
      Assert.assertTrue(vectorCharacterCount(outV, i) == 1);
      i++;

      byte[] sentenceOne = new byte[100];
      int sentenceOneLen = addMultiByteCharSentenceOne(sentenceOne, 0);

      Assert.assertTrue(StringExpr.characterCount(sentenceOne, 0, sentenceOneLen) == 10);
      StringExpr.rightTrim(outV, i, sentenceOne, 0, sentenceOneLen);
      expectedResultLen = sentenceOneLen;
      Assert.assertTrue(vectorEqual(outV, i, sentenceOne, 0, expectedResultLen));
      i++;
      Assert.assertTrue(StringExpr.characterCount(sentenceOne, 0, sentenceOneLen - 3) == 9);
      StringExpr.rightTrim(outV, i, sentenceOne, 0, sentenceOneLen - 3);
      expectedResultLen = sentenceOneLen - 3;
      Assert.assertTrue(vectorEqual(outV, i, sentenceOne, 0, expectedResultLen));
      i++;

      byte[] sentenceTwo = new byte[100];
      int sentenceTwoLen = addMultiByteCharSentenceTwo(sentenceTwo, 0);

      Assert.assertTrue(StringExpr.characterCount(sentenceTwo, 0, sentenceTwoLen) == 13);
      StringExpr.rightTrim(outV, i, sentenceTwo, 0, sentenceTwoLen);
      expectedResultLen = sentenceTwoLen;
      Assert.assertTrue(vectorEqual(outV, i, sentenceTwo, 0, expectedResultLen));
      i++;
      Assert.assertTrue(StringExpr.characterCount(sentenceTwo, 0, sentenceTwoLen- 5) == 10);
      StringExpr.rightTrim(outV, i, sentenceTwo, 0, sentenceTwoLen - 5);
      expectedResultLen = sentenceTwoLen - 5;
      Assert.assertTrue(vectorEqual(outV, i, sentenceTwo, 0, expectedResultLen));
      i++;

      int start;

      // Left pad longer strings with multi-byte characters.
      byte[] sentenceOnePaddedLeft = new byte[100];
      start = addPads(sentenceOnePaddedLeft, 0, 3);
      int sentenceOnePaddedLeftLen = addMultiByteCharSentenceOne(sentenceOnePaddedLeft, start);

      Assert.assertTrue(StringExpr.characterCount(sentenceOnePaddedLeft, 0, sentenceOnePaddedLeftLen) == 3 + 10);
      StringExpr.rightTrim(outV, i, sentenceOnePaddedLeft, 0, sentenceOnePaddedLeftLen);
      expectedResultLen = sentenceOnePaddedLeftLen;
      Assert.assertTrue(vectorEqual(outV, i, sentenceOnePaddedLeft, 0, expectedResultLen));
      i++;
      Assert.assertTrue(StringExpr.characterCount(sentenceOnePaddedLeft, 0, sentenceOnePaddedLeftLen - 3) == 3 + 9);
      StringExpr.rightTrim(outV, i, sentenceOnePaddedLeft, 0, sentenceOnePaddedLeftLen - 3);
      expectedResultLen = sentenceOnePaddedLeftLen - 3;
      Assert.assertTrue(vectorEqual(outV, i, sentenceOnePaddedLeft, 0, expectedResultLen));
      i++;

      byte[] sentenceTwoPaddedLeft = new byte[100];
      start = addPads(sentenceTwoPaddedLeft, 0, 2);
      int sentenceTwoPaddedLeftLen = addMultiByteCharSentenceTwo(sentenceTwoPaddedLeft, start);

      Assert.assertTrue(StringExpr.characterCount(sentenceTwoPaddedLeft, 0, sentenceTwoPaddedLeftLen) == 2 + 13);
      StringExpr.rightTrim(outV, i, sentenceTwoPaddedLeft, 0, sentenceTwoPaddedLeftLen);
      expectedResultLen = sentenceTwoPaddedLeftLen;
      Assert.assertTrue(vectorEqual(outV, i, sentenceTwoPaddedLeft, 0, expectedResultLen));
      i++;
      Assert.assertTrue(StringExpr.characterCount(sentenceTwoPaddedLeft, 0, sentenceTwoPaddedLeftLen - 5) == 2 + 10);
      StringExpr.rightTrim(outV, i, sentenceTwoPaddedLeft, 0, sentenceTwoPaddedLeftLen - 5);
      expectedResultLen = sentenceTwoPaddedLeftLen - 5;
      Assert.assertTrue(vectorEqual(outV, i, sentenceTwoPaddedLeft, 0, expectedResultLen));
      i++;

      // Right pad longer strings with multi-byte characters.
      byte[] sentenceOnePaddedRight = new byte[100];
      start = addMultiByteCharSentenceOne(sentenceOnePaddedRight, 0);
      int sentenceOnePaddedRightLen = addPads(sentenceOnePaddedRight, start, 4);

      Assert.assertTrue(StringExpr.characterCount(sentenceOnePaddedRight, 0, sentenceOnePaddedRightLen) == 10 + 4);
      StringExpr.rightTrim(outV, i, sentenceOnePaddedRight, 0, sentenceOnePaddedRightLen);
      expectedResultLen = sentenceOnePaddedRightLen - 4;
      Assert.assertTrue(vectorEqual(outV, i, sentenceOnePaddedRight, 0, expectedResultLen));
      i++;
      Assert.assertTrue(StringExpr.characterCount(sentenceOnePaddedRight, 0, sentenceOnePaddedRightLen - 3 - 4) == 9);
      StringExpr.rightTrim(outV, i, sentenceOnePaddedRight, 0, sentenceOnePaddedRightLen - 3 - 4);
      expectedResultLen = sentenceOnePaddedRightLen - 3 - 4;
      Assert.assertTrue(vectorEqual(outV, i, sentenceOnePaddedRight, 0, expectedResultLen));
      i++;

      byte[] sentenceTwoPaddedRight = new byte[100];
      start = addMultiByteCharSentenceTwo(sentenceTwoPaddedRight, 0);
      int sentenceTwoPaddedRightLen = addPads(sentenceTwoPaddedRight, start, 1);

      Assert.assertTrue(StringExpr.characterCount(sentenceTwoPaddedRight, 0, sentenceTwoPaddedRightLen) == 13 + 1);
      StringExpr.rightTrim(outV, i, sentenceTwoPaddedRight, 0, sentenceTwoPaddedRightLen);
      expectedResultLen = sentenceTwoPaddedRightLen - 1;
      Assert.assertTrue(vectorEqual(outV, i, sentenceTwoPaddedRight, 0, expectedResultLen));
      i++;
      Assert.assertTrue(StringExpr.characterCount(sentenceTwoPaddedRight, 0, sentenceTwoPaddedRightLen - 5 - 1) == 10);
      StringExpr.rightTrim(outV, i, sentenceTwoPaddedRight, 0, sentenceTwoPaddedRightLen - 5 - 1);
      expectedResultLen = sentenceTwoPaddedRightLen - 5 - 1;
      Assert.assertTrue(vectorEqual(outV, i, sentenceTwoPaddedRight, 0, expectedResultLen));
      i++;

      // Multi-byte characters with blank ranges.
      byte[] sentenceBlankRanges = new byte[100];
      int sentenceBlankRangesLen = addMultiByteCharSentenceBlankRanges(sentenceBlankRanges, 0);

      Assert.assertTrue(StringExpr.characterCount(sentenceBlankRanges, 0, sentenceBlankRangesLen) == 17);
      StringExpr.rightTrim(outV, i, sentenceBlankRanges, 0, sentenceBlankRangesLen);
      expectedResultLen = sentenceBlankRangesLen;
      Assert.assertTrue(vectorEqual(outV, i, sentenceBlankRanges, 0, expectedResultLen));
      i++;
      Assert.assertTrue(StringExpr.characterCount(sentenceBlankRanges, 0, sentenceBlankRangesLen - 3) == 16);
      StringExpr.rightTrim(outV, i, sentenceBlankRanges, 0, sentenceBlankRangesLen - 3);
      expectedResultLen = sentenceBlankRangesLen - 3;
      Assert.assertTrue(vectorEqual(outV, i, sentenceBlankRanges, 0, expectedResultLen));
      i++;
      Assert.assertTrue(StringExpr.characterCount(sentenceBlankRanges, 7, 17) == 13);
      StringExpr.rightTrim(outV, i, sentenceBlankRanges, 7, 17);
      expectedResultLen = 12;
      Assert.assertTrue(vectorEqual(outV, i, sentenceBlankRanges, 7, expectedResultLen));
      Assert.assertTrue(vectorCharacterCount(outV, i) == 8);
  }

  @Test
  // Test basic truncate of bytes slice.
  public void testTruncateBytesSlice()  {
      int largeMaxLength = 100;
      int resultLen;

      // No truncate (ASCII) -- maximum length large.
      Assert.assertTrue(StringExpr.characterCount(blue, 0, blue.length) == 4);
      resultLen = StringExpr.truncate(blue, 0, blue.length, largeMaxLength);
      Assert.assertTrue(resultLen == blue.length);
      Assert.assertTrue(StringExpr.characterCount(blue, 0, resultLen) == 4);

      Assert.assertTrue(StringExpr.characterCount(redgreen, 0, redgreen.length) == 8);
      resultLen = StringExpr.truncate(redgreen, 0, redgreen.length, largeMaxLength);
      Assert.assertTrue(resultLen == redgreen.length);

      Assert.assertTrue(StringExpr.characterCount(ascii_sentence, 0, ascii_sentence.length) == 31);
      resultLen = StringExpr.truncate(ascii_sentence, 0, ascii_sentence.length, largeMaxLength);
      Assert.assertTrue(resultLen == ascii_sentence.length);

      Assert.assertTrue(StringExpr.characterCount(blanksLeft, 0, blanksLeft.length) == 5);
      resultLen = StringExpr.truncate(blanksLeft, 0, blanksLeft.length, largeMaxLength);
      Assert.assertTrue(resultLen == blanksLeft.length);

      // No truncate (ASCII) -- same maximum length.
      Assert.assertTrue(StringExpr.characterCount(blue, 0, blue.length) == 4);
      resultLen = StringExpr.truncate(blue, 0, blue.length, 4);
      Assert.assertTrue(resultLen == blue.length);
      Assert.assertTrue(StringExpr.characterCount(blue, 0, resultLen) == 4);

      Assert.assertTrue(StringExpr.characterCount(redgreen, 0, redgreen.length) == 8);
      resultLen = StringExpr.truncate(redgreen, 0, redgreen.length, 8);
      Assert.assertTrue(resultLen == redgreen.length);

      Assert.assertTrue(StringExpr.characterCount(ascii_sentence, 0, ascii_sentence.length) == 31);
      resultLen = StringExpr.truncate(ascii_sentence, 0, ascii_sentence.length, 31);
      Assert.assertTrue(resultLen == ascii_sentence.length);

      Assert.assertTrue(StringExpr.characterCount(blanksLeft, 0, blanksLeft.length) == 5);
      resultLen = StringExpr.truncate(blanksLeft, 0, blanksLeft.length, 5);
      Assert.assertTrue(resultLen == blanksLeft.length);

      // Simple truncation.
      Assert.assertTrue(StringExpr.characterCount(blue, 0, blue.length) == 4);
      resultLen = StringExpr.truncate(blue, 0, blue.length, 3);
      Assert.assertTrue(resultLen == 3);
      Assert.assertTrue(StringExpr.characterCount(blue, 0, resultLen) == 3);

      Assert.assertTrue(StringExpr.characterCount(redgreen, 0, redgreen.length) == 8);
      resultLen = StringExpr.truncate(redgreen, 0, redgreen.length, 6);
      Assert.assertTrue(resultLen == 6);

      Assert.assertTrue(StringExpr.characterCount(ascii_sentence, 0, ascii_sentence.length) == 31);
      resultLen = StringExpr.truncate(ascii_sentence, 0, ascii_sentence.length, 14);
      Assert.assertTrue(resultLen == 14);

      Assert.assertTrue(StringExpr.characterCount(blanksLeft, 0, blanksLeft.length) == 5);
      resultLen = StringExpr.truncate(blanksLeft, 0, blanksLeft.length, 2);
      Assert.assertTrue(resultLen == 2);

      Assert.assertTrue(StringExpr.characterCount(blanksRight, 0, blanksRight.length) == 5);
      resultLen = StringExpr.truncate(blanksRight, 0, blanksRight.length, 4);
      Assert.assertTrue(resultLen == 4);
      Assert.assertTrue(StringExpr.characterCount(blanksRight, 0, resultLen) == 4);

      Assert.assertTrue(StringExpr.characterCount(blanksBoth, 0, blanksBoth.length) == 7);
      resultLen = StringExpr.truncate(blanksBoth, 0, blanksBoth.length, 2);
      Assert.assertTrue(resultLen == 2);
      Assert.assertTrue(StringExpr.characterCount(blanksBoth, 0, resultLen) == 2);
     
      Assert.assertTrue(StringExpr.characterCount(blankString, 0, blankString.length) == 2);
      resultLen = StringExpr.truncate(blankString, 0, blankString.length, 1);
      Assert.assertTrue(resultLen == 1);
      Assert.assertTrue(StringExpr.characterCount(blankString, 0, resultLen) == 1);

      Assert.assertTrue(StringExpr.characterCount(blankRanges, 0, blankRanges.length) == 30);
      resultLen = StringExpr.truncate(blankRanges, 0, blankRanges.length, 29);
      Assert.assertTrue(resultLen == 29);
      Assert.assertTrue(StringExpr.characterCount(blankRanges, 0, resultLen) == 29);

      // Offset truncation.
      Assert.assertTrue(StringExpr.characterCount(blanksRight, 1, blanksRight.length - 1) == 4);
      resultLen = StringExpr.truncate(blanksRight, 1, blanksRight.length - 1, 3);
      Assert.assertTrue(resultLen == 3);
      Assert.assertTrue(StringExpr.characterCount(blanksRight, 1, resultLen) == 3);

      Assert.assertTrue(StringExpr.characterCount(blanksBoth, 4, blanksBoth.length - 4) == 3);
      resultLen = StringExpr.truncate(blanksBoth, 4, blanksBoth.length - 4, 2);
      Assert.assertTrue(resultLen == 2);
      Assert.assertTrue(StringExpr.characterCount(blanksBoth, 4, resultLen) == 2);

      Assert.assertTrue(StringExpr.characterCount(blanksBoth, 5, blanksBoth.length - 5) == 2);
      resultLen = StringExpr.truncate(blanksBoth, 5, blanksBoth.length -5, 1);
      Assert.assertTrue(resultLen == 1);
      Assert.assertTrue(StringExpr.characterCount(blanksBoth, 5, resultLen) == 1);

      Assert.assertTrue(StringExpr.characterCount(blankRanges, 4, blankRanges.length - 4) == 26);
      resultLen = StringExpr.truncate(blankRanges, 4, blankRanges.length - 4, 22);
      Assert.assertTrue(resultLen == 22);
      Assert.assertTrue(StringExpr.characterCount(blankRanges, 4, resultLen) == 22);

      Assert.assertTrue(StringExpr.characterCount(blankRanges, 6, blankRanges.length - 6) == 24);
      resultLen = StringExpr.truncate(blankRanges, 6, blankRanges.length- 6, 7);
      Assert.assertTrue(resultLen == 7);
      Assert.assertTrue(StringExpr.characterCount(blankRanges, 6, resultLen) == 7);

      Assert.assertTrue(StringExpr.characterCount(blankRanges, 7, blankRanges.length - 7) == 23);
      resultLen = StringExpr.truncate(blankRanges, 7, blankRanges.length - 7, 20);
      Assert.assertTrue(resultLen == 20);
      Assert.assertTrue(StringExpr.characterCount(blankRanges, 7, resultLen) == 20);

      // Multi-byte truncation.
      byte[] multiByte = new byte[100];

      addMultiByteCharRightPadded1_1(multiByte);
      Assert.assertTrue(StringExpr.characterCount(multiByte, 0, 4) == 2);
      resultLen = StringExpr.truncate(multiByte, 0, 4, 1);
      Assert.assertTrue(resultLen == 3);
      Assert.assertTrue(StringExpr.characterCount(multiByte, 0, resultLen) == 1);

      addMultiByteCharRightPadded1_2(multiByte);
      Assert.assertTrue(StringExpr.characterCount(multiByte, 0, 5) == 3);
      resultLen = StringExpr.truncate(multiByte, 0, 5, 2);
      Assert.assertTrue(resultLen == 4);
      Assert.assertTrue(StringExpr.characterCount(multiByte, 0, resultLen) == 2);

      addMultiByteCharRightPadded1_3(multiByte);
      Assert.assertTrue(StringExpr.characterCount(multiByte, 0, 9) == 4);
      resultLen = StringExpr.truncate(multiByte, 0, 9, 2);
      Assert.assertTrue(resultLen == 5);
      Assert.assertTrue(StringExpr.characterCount(multiByte, 0, resultLen) == 2);

      addMultiByteCharRightPadded1_2(multiByte);
      Assert.assertTrue(StringExpr.characterCount(multiByte, 3, 2) == 2);
      resultLen = StringExpr.truncate(multiByte, 3, 2, 1);
      Assert.assertTrue(resultLen == 1);
      Assert.assertTrue(StringExpr.characterCount(multiByte, 3, resultLen) == 1);

      byte[] sentenceOne = new byte[100];
      int sentenceOneLen = addMultiByteCharSentenceOne(sentenceOne, 0);

      Assert.assertTrue(StringExpr.characterCount(sentenceOne, 0, sentenceOneLen) == 10);
      resultLen = StringExpr.truncate(sentenceOne, 0, sentenceOneLen, 8);
      Assert.assertTrue(resultLen == 20);

      Assert.assertTrue(StringExpr.characterCount(sentenceOne, 0, sentenceOneLen - 3) == 9);
      resultLen = StringExpr.truncate(sentenceOne, 0, sentenceOneLen - 3, 3);
      Assert.assertTrue(resultLen == 9);

      byte[] sentenceTwo = new byte[100];
      int sentenceTwoLen = addMultiByteCharSentenceTwo(sentenceTwo, 0);

      Assert.assertTrue(StringExpr.characterCount(sentenceTwo, 0, sentenceTwoLen) == 13);
      resultLen = StringExpr.truncate(sentenceTwo, 0, sentenceTwoLen, 9);
      Assert.assertTrue(resultLen == 16);

      Assert.assertTrue(StringExpr.characterCount(sentenceTwo, 0, sentenceTwoLen- 5) == 10);
      resultLen = StringExpr.truncate(sentenceTwo, 0, sentenceTwoLen - 5, 6);
      Assert.assertTrue(resultLen == 11);

      int start;

      // Left pad longer strings with multi-byte characters.
      byte[] sentenceOnePaddedLeft = new byte[100];
      start = addPads(sentenceOnePaddedLeft, 0, 3);
      int sentenceOnePaddedLeftLen = addMultiByteCharSentenceOne(sentenceOnePaddedLeft, start);

      Assert.assertTrue(StringExpr.characterCount(sentenceOnePaddedLeft, 0, sentenceOnePaddedLeftLen) == 3 + 10);
      resultLen = StringExpr.truncate(sentenceOnePaddedLeft, 0, sentenceOnePaddedLeftLen, 4);
      Assert.assertTrue(resultLen == 6);

      Assert.assertTrue(StringExpr.characterCount(sentenceOnePaddedLeft, 0, sentenceOnePaddedLeftLen - 3) == 3 + 9);
      resultLen = StringExpr.truncate(sentenceOnePaddedLeft, 0, sentenceOnePaddedLeftLen - 3, 7);
      Assert.assertTrue(resultLen == 13);

      byte[] sentenceTwoPaddedLeft = new byte[100];
      start = addPads(sentenceTwoPaddedLeft, 0, 2);
      int sentenceTwoPaddedLeftLen = addMultiByteCharSentenceTwo(sentenceTwoPaddedLeft, start);

      Assert.assertTrue(StringExpr.characterCount(sentenceTwoPaddedLeft, 0, sentenceTwoPaddedLeftLen) == 2 + 13);
      resultLen = StringExpr.truncate(sentenceTwoPaddedLeft, 0, sentenceTwoPaddedLeftLen, 14);
      Assert.assertTrue(resultLen == 24);

      Assert.assertTrue(StringExpr.characterCount(sentenceTwoPaddedLeft, 0, sentenceTwoPaddedLeftLen - 5) == 2 + 10);
      resultLen = StringExpr.truncate(sentenceTwoPaddedLeft, 0, sentenceTwoPaddedLeftLen - 5, 9);
      Assert.assertTrue(resultLen == 15);

      // Right pad longer strings with multi-byte characters.
      byte[] sentenceOnePaddedRight = new byte[100];
      start = addMultiByteCharSentenceOne(sentenceOnePaddedRight, 0);
      int sentenceOnePaddedRightLen = addPads(sentenceOnePaddedRight, start, 4);

      Assert.assertTrue(StringExpr.characterCount(sentenceOnePaddedRight, 0, sentenceOnePaddedRightLen) == 10 + 4);
      resultLen = StringExpr.truncate(sentenceOnePaddedRight, 0, sentenceOnePaddedRightLen, 1);
      Assert.assertTrue(resultLen == 3);

      Assert.assertTrue(StringExpr.characterCount(sentenceOnePaddedRight, 0, sentenceOnePaddedRightLen - 3 - 4) == 9);
      resultLen = StringExpr.truncate(sentenceOnePaddedRight, 0, sentenceOnePaddedRightLen - 3 - 4, 5);
      Assert.assertTrue(resultLen == 13);

      byte[] sentenceTwoPaddedRight = new byte[100];
      start = addMultiByteCharSentenceTwo(sentenceTwoPaddedRight, 0);
      int sentenceTwoPaddedRightLen = addPads(sentenceTwoPaddedRight, start, 1);

      Assert.assertTrue(StringExpr.characterCount(sentenceTwoPaddedRight, 0, sentenceTwoPaddedRightLen) == 13 + 1);
      resultLen = StringExpr.truncate(sentenceTwoPaddedRight, 0, sentenceTwoPaddedRightLen, 6);
      Assert.assertTrue(resultLen == 11);

      Assert.assertTrue(StringExpr.characterCount(sentenceTwoPaddedRight, 0, sentenceTwoPaddedRightLen - 5 - 1) == 10);
      resultLen = StringExpr.truncate(sentenceTwoPaddedRight, 0, sentenceTwoPaddedRightLen - 5 - 1, 8);
      Assert.assertTrue(resultLen == 14);

      // Multi-byte characters with blank ranges.
      byte[] sentenceBlankRanges = new byte[100];
      int sentenceBlankRangesLen = addMultiByteCharSentenceBlankRanges(sentenceBlankRanges, 0);

      Assert.assertTrue(StringExpr.characterCount(sentenceBlankRanges, 0, sentenceBlankRangesLen) == 17);
      resultLen = StringExpr.truncate(sentenceBlankRanges, 0, sentenceBlankRangesLen, 4);
      Assert.assertTrue(resultLen == 9);

      Assert.assertTrue(StringExpr.characterCount(sentenceBlankRanges, 0, sentenceBlankRangesLen - 3) == 16);
      resultLen = StringExpr.truncate(sentenceBlankRanges, 0, sentenceBlankRangesLen - 3, 14);
      Assert.assertTrue(resultLen == 23);

      Assert.assertTrue(StringExpr.characterCount(sentenceBlankRanges, 7, 17) == 13);
      resultLen = StringExpr.truncate(sentenceBlankRanges, 7, 17, 11);
      Assert.assertTrue(resultLen == 15);
      Assert.assertTrue(StringExpr.characterCount(sentenceBlankRanges, 7, resultLen) == 11);
  }

  @Test
  // Test basic truncate to vector.
  public void testTruncateBytesColumnVector()  {
      BytesColumnVector outV = new BytesColumnVector(VectorizedRowBatch.DEFAULT_SIZE);
      outV.initBuffer(35); // initialize with estimated element size 35

      int i = 0;
      int largeMaxLength = 100;

      int expectedResultLen;

      // No truncate (ASCII) -- maximum length large.
      Assert.assertTrue(StringExpr.characterCount(blue, 0, blue.length) == 4);
      StringExpr.truncate(outV, i, blue, 0, blue.length, largeMaxLength);
      expectedResultLen = blue.length;
      Assert.assertTrue(vectorEqual(outV, i, blue, 0, expectedResultLen));
      Assert.assertTrue(vectorCharacterCount(outV, i) == 4);
      i++;
      Assert.assertTrue(StringExpr.characterCount(redgreen, 0, redgreen.length) == 8);
      StringExpr.truncate(outV, i, redgreen, 0, redgreen.length, largeMaxLength);
      expectedResultLen = redgreen.length;
      Assert.assertTrue(vectorEqual(outV, i, redgreen, 0, expectedResultLen));
      i++;
      Assert.assertTrue(StringExpr.characterCount(ascii_sentence, 0, ascii_sentence.length) == 31);
      StringExpr.truncate(outV, i, ascii_sentence, 0, ascii_sentence.length, largeMaxLength);
      expectedResultLen = ascii_sentence.length;
      Assert.assertTrue(vectorEqual(outV, i, ascii_sentence, 0, expectedResultLen));
      i++;
      Assert.assertTrue(StringExpr.characterCount(blanksLeft, 0, blanksLeft.length) == 5);
      StringExpr.truncate(outV, i, blanksLeft, 0, blanksLeft.length, largeMaxLength);
      expectedResultLen = blanksLeft.length;
      Assert.assertTrue(vectorEqual(outV, i, blanksLeft, 0, expectedResultLen));
      i++;

      // No truncate (ASCII) -- same maximum length.
      Assert.assertTrue(StringExpr.characterCount(blue, 0, blue.length) == 4);
      StringExpr.truncate(outV, i, blue, 0, blue.length, 4);
      expectedResultLen = blue.length;
      Assert.assertTrue(vectorEqual(outV, i, blue, 0, expectedResultLen));
      Assert.assertTrue(vectorCharacterCount(outV, i) == 4);
      i++;
      Assert.assertTrue(StringExpr.characterCount(redgreen, 0, redgreen.length) == 8);
      StringExpr.truncate(outV, i, redgreen, 0, redgreen.length, 8);
      expectedResultLen = redgreen.length;
      Assert.assertTrue(vectorEqual(outV, i, redgreen, 0, expectedResultLen));
      i++;
      Assert.assertTrue(StringExpr.characterCount(ascii_sentence, 0, ascii_sentence.length) == 31);
      StringExpr.truncate(outV, i, ascii_sentence, 0, ascii_sentence.length, 31);
      expectedResultLen = ascii_sentence.length;
      Assert.assertTrue(vectorEqual(outV, i, ascii_sentence, 0, expectedResultLen));
      i++;
      Assert.assertTrue(StringExpr.characterCount(blanksLeft, 0, blanksLeft.length) == 5);
      StringExpr.truncate(outV, i, blanksLeft, 0, blanksLeft.length, 5);
      expectedResultLen = blanksLeft.length;
      Assert.assertTrue(vectorEqual(outV, i, blanksLeft, 0, expectedResultLen));
      i++;

      // Simple truncation.
      Assert.assertTrue(StringExpr.characterCount(blue, 0, blue.length) == 4);
      StringExpr.truncate(outV, i, blue, 0, blue.length, 3);
      expectedResultLen = 3;
      Assert.assertTrue(vectorEqual(outV, i, blue, 0, expectedResultLen));
      Assert.assertTrue(vectorCharacterCount(outV, i) == 3);
      i++;
      Assert.assertTrue(StringExpr.characterCount(redgreen, 0, redgreen.length) == 8);
      StringExpr.truncate(outV, i, redgreen, 0, redgreen.length, 6);
      expectedResultLen = 6;
      Assert.assertTrue(vectorEqual(outV, i, redgreen, 0, expectedResultLen));
      i++;
      Assert.assertTrue(StringExpr.characterCount(ascii_sentence, 0, ascii_sentence.length) == 31);
      StringExpr.truncate(outV, i, ascii_sentence, 0, ascii_sentence.length, 14);
      expectedResultLen = 14;
      Assert.assertTrue(vectorEqual(outV, i, ascii_sentence, 0, expectedResultLen));
      i++;
      Assert.assertTrue(StringExpr.characterCount(blanksLeft, 0, blanksLeft.length) == 5);
      StringExpr.truncate(outV, i, blanksLeft, 0, blanksLeft.length, 2);
      expectedResultLen = 2;
      Assert.assertTrue(vectorEqual(outV, i, blanksLeft, 0, expectedResultLen));
      i++;
      Assert.assertTrue(StringExpr.characterCount(blanksRight, 0, blanksRight.length) == 5);
      StringExpr.truncate(outV, i, blanksRight, 0, blanksRight.length, 4);
      expectedResultLen = 4;
      Assert.assertTrue(vectorCharacterCount(outV, i) == 4);
      i++;
      Assert.assertTrue(StringExpr.characterCount(blanksBoth, 0, blanksBoth.length) == 7);
      StringExpr.truncate(outV, i, blanksBoth, 0, blanksBoth.length, 2);
      expectedResultLen = 2;
      Assert.assertTrue(vectorEqual(outV, i, blanksBoth, 0, expectedResultLen));
      Assert.assertTrue(vectorCharacterCount(outV, i) == 2);
      i++;
      Assert.assertTrue(StringExpr.characterCount(blankString, 0, blankString.length) == 2);
      StringExpr.truncate(outV, i, blankString, 0, blankString.length, 1);
      expectedResultLen = 1;
      Assert.assertTrue(vectorEqual(outV, i, blankString, 0, expectedResultLen));
      Assert.assertTrue(vectorCharacterCount(outV, i) == 1);
      i++;
      Assert.assertTrue(StringExpr.characterCount(blankRanges, 0, blankRanges.length) == 30);
      StringExpr.truncate(outV, i, blankRanges, 0, blankRanges.length, 29);
      expectedResultLen = 29;
      Assert.assertTrue(vectorEqual(outV, i, blankRanges, 0, expectedResultLen));
      Assert.assertTrue(vectorCharacterCount(outV, i) == 29);
      i++;

      // Offset truncation.
      Assert.assertTrue(StringExpr.characterCount(blanksRight, 1, blanksRight.length - 1) == 4);
      StringExpr.truncate(outV, i, blanksRight, 1, blanksRight.length - 1, 3);
      expectedResultLen = 3;
      Assert.assertTrue(vectorEqual(outV, i, blanksRight, 1, expectedResultLen));
      Assert.assertTrue(vectorCharacterCount(outV, i) == 3);
      i++;
      Assert.assertTrue(StringExpr.characterCount(blanksBoth, 4, blanksBoth.length - 4) == 3);
      StringExpr.truncate(outV, i, blanksBoth, 4, blanksBoth.length - 4, 2);
      expectedResultLen = 2;
      Assert.assertTrue(vectorEqual(outV, i, blanksBoth, 4, expectedResultLen));
      Assert.assertTrue(vectorCharacterCount(outV, i) == 2);
      i++;
      Assert.assertTrue(StringExpr.characterCount(blanksBoth, 5, blanksBoth.length - 5) == 2);
      StringExpr.truncate(outV, i, blanksBoth, 5, blanksBoth.length -5, 1);
      expectedResultLen = 1;
      Assert.assertTrue(vectorEqual(outV, i, blanksBoth, 5, expectedResultLen));
      Assert.assertTrue(vectorCharacterCount(outV, i) == 1);
      i++;
      Assert.assertTrue(StringExpr.characterCount(blankRanges, 4, blankRanges.length - 4) == 26);
      StringExpr.truncate(outV, i, blankRanges, 4, blankRanges.length - 4, 22);
      expectedResultLen = 22;
      Assert.assertTrue(vectorEqual(outV, i, blankRanges, 4, expectedResultLen));
      Assert.assertTrue(vectorCharacterCount(outV, i) == 22);
      i++;
      Assert.assertTrue(StringExpr.characterCount(blankRanges, 6, blankRanges.length - 6) == 24);
      StringExpr.truncate(outV, i, blankRanges, 6, blankRanges.length- 6, 7);
      expectedResultLen = 7;
      Assert.assertTrue(vectorEqual(outV, i, blankRanges, 6, expectedResultLen));
      Assert.assertTrue(vectorCharacterCount(outV, i) == 7);
      i++;
      Assert.assertTrue(StringExpr.characterCount(blankRanges, 7, blankRanges.length - 7) == 23);
      StringExpr.truncate(outV, i, blankRanges, 7, blankRanges.length - 7, 20);
      expectedResultLen = 20;
      Assert.assertTrue(vectorEqual(outV, i, blankRanges, 7, expectedResultLen));
      Assert.assertTrue(vectorCharacterCount(outV, i) == 20);
      i++;

      // Multi-byte truncation.
      byte[] multiByte = new byte[100];

      addMultiByteCharRightPadded1_1(multiByte);
      Assert.assertTrue(StringExpr.characterCount(multiByte, 0, 4) == 2);
      StringExpr.truncate(outV, i, multiByte, 0, 4, 1);
      expectedResultLen = 3;
      Assert.assertTrue(vectorEqual(outV, i, multiByte, 0, expectedResultLen));
      Assert.assertTrue(vectorCharacterCount(outV, i) == 1);
      i++;
      addMultiByteCharRightPadded1_2(multiByte);
      Assert.assertTrue(StringExpr.characterCount(multiByte, 0, 5) == 3);
      StringExpr.truncate(outV, i, multiByte, 0, 5, 2);
      expectedResultLen = 4;
      Assert.assertTrue(vectorEqual(outV, i, multiByte, 0, expectedResultLen));
      Assert.assertTrue(vectorCharacterCount(outV, i) == 2);
      i++;
      addMultiByteCharRightPadded1_3(multiByte);
      Assert.assertTrue(StringExpr.characterCount(multiByte, 0, 9) == 4);
      StringExpr.truncate(outV, i, multiByte, 0, 9, 2);
      expectedResultLen = 5;
      Assert.assertTrue(vectorEqual(outV, i, multiByte, 0, expectedResultLen));
      Assert.assertTrue(vectorCharacterCount(outV, i) == 2);
      i++;
      addMultiByteCharRightPadded1_2(multiByte);
      Assert.assertTrue(StringExpr.characterCount(multiByte, 3, 2) == 2);
      StringExpr.truncate(outV, i, multiByte, 3, 2, 1);
      expectedResultLen = 1;
      Assert.assertTrue(vectorEqual(outV, i, multiByte, 3, expectedResultLen));
      Assert.assertTrue(vectorCharacterCount(outV, i) == 1);
      i++;

      byte[] sentenceOne = new byte[100];
      int sentenceOneLen = addMultiByteCharSentenceOne(sentenceOne, 0);

      Assert.assertTrue(StringExpr.characterCount(sentenceOne, 0, sentenceOneLen) == 10);
      StringExpr.truncate(outV, i, sentenceOne, 0, sentenceOneLen, 8);
      expectedResultLen = 20;
      Assert.assertTrue(vectorEqual(outV, i, sentenceOne, 0, expectedResultLen));
      i++;
      Assert.assertTrue(StringExpr.characterCount(sentenceOne, 0, sentenceOneLen - 3) == 9);
      StringExpr.truncate(outV, i, sentenceOne, 0, sentenceOneLen - 3, 3);
      expectedResultLen = 9;
      Assert.assertTrue(vectorEqual(outV, i, sentenceOne, 0, expectedResultLen));
      i++;

      byte[] sentenceTwo = new byte[100];
      int sentenceTwoLen = addMultiByteCharSentenceTwo(sentenceTwo, 0);

      Assert.assertTrue(StringExpr.characterCount(sentenceTwo, 0, sentenceTwoLen) == 13);
      StringExpr.truncate(outV, i, sentenceTwo, 0, sentenceTwoLen, 9);
      expectedResultLen = 16;
      Assert.assertTrue(vectorEqual(outV, i, sentenceTwo, 0, expectedResultLen));
      i++;
      Assert.assertTrue(StringExpr.characterCount(sentenceTwo, 0, sentenceTwoLen- 5) == 10);
      StringExpr.truncate(outV, i, sentenceTwo, 0, sentenceTwoLen - 5, 6);
      expectedResultLen = 11;
      Assert.assertTrue(vectorEqual(outV, i, sentenceTwo, 0, expectedResultLen));
      i++;

      int start;

      // Left pad longer strings with multi-byte characters.
      byte[] sentenceOnePaddedLeft = new byte[100];
      start = addPads(sentenceOnePaddedLeft, 0, 3);
      int sentenceOnePaddedLeftLen = addMultiByteCharSentenceOne(sentenceOnePaddedLeft, start);

      Assert.assertTrue(StringExpr.characterCount(sentenceOnePaddedLeft, 0, sentenceOnePaddedLeftLen) == 3 + 10);
      StringExpr.truncate(outV, i, sentenceOnePaddedLeft, 0, sentenceOnePaddedLeftLen, 4);
      expectedResultLen = 6;
      Assert.assertTrue(vectorEqual(outV, i, sentenceOnePaddedLeft, 0, expectedResultLen));
      i++;
      Assert.assertTrue(StringExpr.characterCount(sentenceOnePaddedLeft, 0, sentenceOnePaddedLeftLen - 3) == 3 + 9);
      StringExpr.truncate(outV, i, sentenceOnePaddedLeft, 0, sentenceOnePaddedLeftLen - 3, 7);
      expectedResultLen = 13;
      Assert.assertTrue(vectorEqual(outV, i, sentenceOnePaddedLeft, 0, expectedResultLen));
      i++;

      byte[] sentenceTwoPaddedLeft = new byte[100];
      start = addPads(sentenceTwoPaddedLeft, 0, 2);
      int sentenceTwoPaddedLeftLen = addMultiByteCharSentenceTwo(sentenceTwoPaddedLeft, start);

      Assert.assertTrue(StringExpr.characterCount(sentenceTwoPaddedLeft, 0, sentenceTwoPaddedLeftLen) == 2 + 13);
      StringExpr.truncate(outV, i, sentenceTwoPaddedLeft, 0, sentenceTwoPaddedLeftLen, 14);
      expectedResultLen = 24;
      Assert.assertTrue(vectorEqual(outV, i, sentenceTwoPaddedLeft, 0, expectedResultLen));
      i++;
      Assert.assertTrue(StringExpr.characterCount(sentenceTwoPaddedLeft, 0, sentenceTwoPaddedLeftLen - 5) == 2 + 10);
      StringExpr.truncate(outV, i, sentenceTwoPaddedLeft, 0, sentenceTwoPaddedLeftLen - 5, 9);
      expectedResultLen = 15;
      Assert.assertTrue(vectorEqual(outV, i, sentenceTwoPaddedLeft, 0, expectedResultLen));
      i++;

      // Right pad longer strings with multi-byte characters.
      byte[] sentenceOnePaddedRight = new byte[100];
      start = addMultiByteCharSentenceOne(sentenceOnePaddedRight, 0);
      int sentenceOnePaddedRightLen = addPads(sentenceOnePaddedRight, start, 4);

      Assert.assertTrue(StringExpr.characterCount(sentenceOnePaddedRight, 0, sentenceOnePaddedRightLen) == 10 + 4);
      StringExpr.truncate(outV, i, sentenceOnePaddedRight, 0, sentenceOnePaddedRightLen, 1);
      expectedResultLen = 3;
      Assert.assertTrue(vectorEqual(outV, i, sentenceOnePaddedRight, 0, expectedResultLen));
      i++;
      Assert.assertTrue(StringExpr.characterCount(sentenceOnePaddedRight, 0, sentenceOnePaddedRightLen - 3 - 4) == 9);
      StringExpr.truncate(outV, i, sentenceOnePaddedRight, 0, sentenceOnePaddedRightLen - 3 - 4, 5);
      expectedResultLen = 13;
      Assert.assertTrue(vectorEqual(outV, i, sentenceOnePaddedRight, 0, expectedResultLen));
      i++;

      byte[] sentenceTwoPaddedRight = new byte[100];
      start = addMultiByteCharSentenceTwo(sentenceTwoPaddedRight, 0);
      int sentenceTwoPaddedRightLen = addPads(sentenceTwoPaddedRight, start, 1);

      Assert.assertTrue(StringExpr.characterCount(sentenceTwoPaddedRight, 0, sentenceTwoPaddedRightLen) == 13 + 1);
      StringExpr.truncate(outV, i, sentenceTwoPaddedRight, 0, sentenceTwoPaddedRightLen, 6);
      expectedResultLen = 11;
      Assert.assertTrue(vectorEqual(outV, i, sentenceTwoPaddedRight, 0, expectedResultLen));
      i++;
      Assert.assertTrue(StringExpr.characterCount(sentenceTwoPaddedRight, 0, sentenceTwoPaddedRightLen - 5 - 1) == 10);
      StringExpr.truncate(outV, i, sentenceTwoPaddedRight, 0, sentenceTwoPaddedRightLen - 5 - 1, 8);
      expectedResultLen = 14;
      Assert.assertTrue(vectorEqual(outV, i, sentenceTwoPaddedRight, 0, expectedResultLen));
      i++;

      // Multi-byte characters with blank ranges.
      byte[] sentenceBlankRanges = new byte[100];
      int sentenceBlankRangesLen = addMultiByteCharSentenceBlankRanges(sentenceBlankRanges, 0);

      Assert.assertTrue(StringExpr.characterCount(sentenceBlankRanges, 0, sentenceBlankRangesLen) == 17);
      StringExpr.truncate(outV, i, sentenceBlankRanges, 0, sentenceBlankRangesLen, 4);
      expectedResultLen = 9;
      Assert.assertTrue(vectorEqual(outV, i, sentenceBlankRanges, 0, expectedResultLen));
      i++;
      Assert.assertTrue(StringExpr.characterCount(sentenceBlankRanges, 0, sentenceBlankRangesLen - 3) == 16);
      StringExpr.truncate(outV, i, sentenceBlankRanges, 0, sentenceBlankRangesLen - 3, 14);
      expectedResultLen = 23;
      Assert.assertTrue(vectorEqual(outV, i, sentenceBlankRanges,0, expectedResultLen));
      i++;
      Assert.assertTrue(StringExpr.characterCount(sentenceBlankRanges, 7, 17) == 13);
      StringExpr.truncate(outV, i, sentenceBlankRanges, 7, 17, 11);
      expectedResultLen = 15;
      Assert.assertTrue(vectorEqual(outV, i, sentenceBlankRanges, 7, expectedResultLen));
      Assert.assertTrue(vectorCharacterCount(outV, i) == 11);
      i++;
  }

  @Test
  // Test basic truncate to vector.
  public void testTruncateScalar()  {
      int largeMaxLength = 100;

      byte[] result;

      // No truncate (ASCII) -- maximum length large.
      Assert.assertTrue(StringExpr.characterCount(blue) == 4);
      result = StringExpr.truncateScalar(blue, largeMaxLength);
      Assert.assertTrue(Arrays.equals(blue, result));

      Assert.assertTrue(StringExpr.characterCount(redgreen) == 8);
      result = StringExpr.truncateScalar(redgreen, largeMaxLength);
      Assert.assertTrue(Arrays.equals(redgreen, result));

      Assert.assertTrue(StringExpr.characterCount(ascii_sentence) == 31);
      result = StringExpr.truncateScalar(ascii_sentence, largeMaxLength);
      Assert.assertTrue(Arrays.equals(ascii_sentence, result));

      Assert.assertTrue(StringExpr.characterCount(blanksLeft) == 5);
      result = StringExpr.truncateScalar(blanksLeft, largeMaxLength);
      Assert.assertTrue(Arrays.equals(blanksLeft, result));

      // No truncate (ASCII) -- same maximum length.
      Assert.assertTrue(StringExpr.characterCount(blue) == 4);
      result = StringExpr.truncateScalar(blue, blue.length);
      Assert.assertTrue(Arrays.equals(blue, result));

      Assert.assertTrue(StringExpr.characterCount(redgreen) == 8);
      result = StringExpr.truncateScalar(redgreen, redgreen.length);
      Assert.assertTrue(Arrays.equals(redgreen, result));

      Assert.assertTrue(StringExpr.characterCount(ascii_sentence) == 31);
      result = StringExpr.truncateScalar(ascii_sentence, ascii_sentence.length);
      Assert.assertTrue(Arrays.equals(ascii_sentence, result));

      Assert.assertTrue(StringExpr.characterCount(blanksLeft) == 5);
      result = StringExpr.truncateScalar(blanksLeft, blanksLeft.length);
      Assert.assertTrue(Arrays.equals(blanksLeft, result));

      // Simple truncation.
      result = StringExpr.truncateScalar(blue, 3);
      Assert.assertTrue(Arrays.equals(Arrays.copyOf(blue, 3), result));

      result = StringExpr.truncateScalar(redgreen, 6);
      Assert.assertTrue(Arrays.equals(Arrays.copyOf(redgreen, 6), result));

      result = StringExpr.truncateScalar(ascii_sentence, 14);
      Assert.assertTrue(Arrays.equals(Arrays.copyOf(ascii_sentence, 14), result));

      result = StringExpr.truncateScalar(blanksLeft, 2);
      Assert.assertTrue(Arrays.equals(Arrays.copyOf(blanksLeft, 2), result));

      result = StringExpr.truncateScalar(blanksRight, 4);
      Assert.assertTrue(Arrays.equals(Arrays.copyOf(blanksRight, 4), result));

      result = StringExpr.truncateScalar(blanksBoth, 2);
      Assert.assertTrue(Arrays.equals(Arrays.copyOf(blanksBoth, 2), result));

      result = StringExpr.truncateScalar(blankString, 1);
      Assert.assertTrue(Arrays.equals(Arrays.copyOf(blankString, 1), result));

      result = StringExpr.truncateScalar(blankRanges, 29);
      Assert.assertTrue(Arrays.equals(Arrays.copyOf(blankRanges, 29), result));

      // Multi-byte truncation.
      byte[] scratch = new byte[100];
      byte[] multiByte;

      addMultiByteCharRightPadded1_1(scratch);
      multiByte = Arrays.copyOf(scratch, 4);
      Assert.assertTrue(StringExpr.characterCount(multiByte, 0, 4) == 2);
      result = StringExpr.truncateScalar(multiByte, 1);
      Assert.assertTrue(Arrays.equals(Arrays.copyOf(multiByte, 3), result));

      addMultiByteCharRightPadded1_2(scratch);
      multiByte = Arrays.copyOf(scratch, 5);
      Assert.assertTrue(StringExpr.characterCount(multiByte, 0, 5) == 3);
      result = StringExpr.truncateScalar(multiByte, 2);
      Assert.assertTrue(Arrays.equals(Arrays.copyOf(multiByte, 4), result));

      addMultiByteCharRightPadded1_3(scratch);
      multiByte = Arrays.copyOf(scratch, 9);
      Assert.assertTrue(StringExpr.characterCount(multiByte, 0, 9) == 4);
      result = StringExpr.truncateScalar(multiByte, 2);
      Assert.assertTrue(Arrays.equals(Arrays.copyOf(multiByte, 5), result));

      addMultiByteCharRightPadded1_2(scratch);
      multiByte = Arrays.copyOfRange(scratch, 3, 3 + 2);
      Assert.assertTrue(StringExpr.characterCount(multiByte) == 2);
      result = StringExpr.truncateScalar(multiByte, 1);
      Assert.assertTrue(Arrays.equals(Arrays.copyOf(multiByte, 1), result));

      int sentenceOneLen = addMultiByteCharSentenceOne(scratch, 0);
      byte[] sentenceOne = Arrays.copyOf(scratch, sentenceOneLen);

      Assert.assertTrue(StringExpr.characterCount(sentenceOne) == 10);
      result = StringExpr.truncateScalar(sentenceOne, 8);
      Assert.assertTrue(Arrays.equals(Arrays.copyOf(sentenceOne, 20), result));

      byte[] sentenceOnePortion = Arrays.copyOf(sentenceOne, sentenceOneLen - 3);
      Assert.assertTrue(StringExpr.characterCount(sentenceOnePortion) == 9);
      result = StringExpr.truncateScalar(sentenceOnePortion, 3);
      Assert.assertTrue(Arrays.equals(Arrays.copyOf(sentenceOnePortion, 9), result));

      int sentenceTwoLen = addMultiByteCharSentenceTwo(scratch, 0);
      byte[] sentenceTwo = Arrays.copyOf(scratch, sentenceTwoLen);

      Assert.assertTrue(StringExpr.characterCount(sentenceTwo) == 13);
      result = StringExpr.truncateScalar(sentenceTwo, 9);
      Assert.assertTrue(Arrays.equals(Arrays.copyOf(sentenceTwo, 16), result));

      byte[] sentenceTwoPortion = Arrays.copyOf(sentenceTwo, sentenceTwoLen - 5);
      Assert.assertTrue(StringExpr.characterCount(sentenceTwoPortion) == 10);
      result = StringExpr.truncateScalar(sentenceTwoPortion, 6);
      Assert.assertTrue(Arrays.equals(Arrays.copyOf(sentenceTwoPortion, 11), result));

      int start;

      // Left pad longer strings with multi-byte characters.
      start = addPads(scratch, 0, 3);
      int sentenceOnePaddedLeftLen = addMultiByteCharSentenceOne(scratch, start);
      byte[] sentenceOnePaddedLeft = Arrays.copyOf(scratch, sentenceOnePaddedLeftLen);

      Assert.assertTrue(StringExpr.characterCount(sentenceOnePaddedLeft) == 3 + 10);
      result = StringExpr.truncateScalar(sentenceOnePaddedLeft, 4);
      Assert.assertTrue(Arrays.equals(Arrays.copyOf(sentenceOnePaddedLeft, 6), result));

      byte[] sentenceOnePaddedLeftPortion = Arrays.copyOf(sentenceOnePaddedLeft, sentenceOnePaddedLeftLen - 3);
      Assert.assertTrue(StringExpr.characterCount(sentenceOnePaddedLeftPortion) == 3 + 9);
      result = StringExpr.truncateScalar(sentenceOnePaddedLeftPortion, 7);
      Assert.assertTrue(Arrays.equals(Arrays.copyOf(sentenceOnePaddedLeftPortion, 13), result));

      start = addPads(scratch, 0, 2);
      int sentenceTwoPaddedLeftLen = addMultiByteCharSentenceTwo(scratch, start);
      byte[] sentenceTwoPaddedLeft = Arrays.copyOf(scratch, sentenceTwoPaddedLeftLen);

      Assert.assertTrue(StringExpr.characterCount(sentenceTwoPaddedLeft) == 2 + 13);
      result = StringExpr.truncateScalar(sentenceTwoPaddedLeft, 14);
      Assert.assertTrue(Arrays.equals(Arrays.copyOf(sentenceTwoPaddedLeft, 24), result));

      byte[] sentenceTwoPaddedLeftPortion = Arrays.copyOf(sentenceTwoPaddedLeft, sentenceTwoPaddedLeftLen - 5);
      Assert.assertTrue(StringExpr.characterCount(sentenceTwoPaddedLeftPortion) == 2 + 10);
      result = StringExpr.truncateScalar(sentenceTwoPaddedLeftPortion, 9);
      Assert.assertTrue(Arrays.equals(Arrays.copyOf(sentenceTwoPaddedLeftPortion, 15), result));

      // Right pad longer strings with multi-byte characters.
      start = addMultiByteCharSentenceOne(scratch, 0);
      int sentenceOnePaddedRightLen = addPads(scratch, start, 4);
      byte[] sentenceOnePaddedRight = Arrays.copyOf(scratch, sentenceOnePaddedRightLen);

      Assert.assertTrue(StringExpr.characterCount(sentenceOnePaddedRight) == 10 + 4);
      result = StringExpr.truncateScalar(sentenceOnePaddedRight, 1);
      Assert.assertTrue(Arrays.equals(Arrays.copyOf(sentenceOnePaddedRight, 3), result));

      byte[] sentenceOnePaddedRightPortion = Arrays.copyOf(sentenceOnePaddedRight, sentenceOnePaddedRightLen - 3 - 4);
      Assert.assertTrue(StringExpr.characterCount(sentenceOnePaddedRightPortion) == 9);
      result = StringExpr.truncateScalar(sentenceOnePaddedRightPortion, 5);
      Assert.assertTrue(Arrays.equals(Arrays.copyOf(sentenceOnePaddedRightPortion, 13), result));

      start = addMultiByteCharSentenceTwo(scratch, 0);
      int sentenceTwoPaddedRightLen = addPads(scratch, start, 1);
      byte[] sentenceTwoPaddedRight = Arrays.copyOf(scratch, sentenceTwoPaddedRightLen);

      Assert.assertTrue(StringExpr.characterCount(sentenceTwoPaddedRight) == 13 + 1);
      result = StringExpr.truncateScalar(sentenceTwoPaddedRight, 6);
      Assert.assertTrue(Arrays.equals(Arrays.copyOf(sentenceTwoPaddedRight, 11), result));

      byte[] sentenceTwoPaddedRightPortion = Arrays.copyOf(sentenceTwoPaddedRight, sentenceTwoPaddedRightLen - 5 - 1);
      Assert.assertTrue(StringExpr.characterCount(sentenceTwoPaddedRightPortion) == 10);
      result = StringExpr.truncateScalar(sentenceTwoPaddedRightPortion, 8);
      Assert.assertTrue(Arrays.equals(Arrays.copyOf(sentenceTwoPaddedRightPortion, 14), result));

      // Multi-byte characters with blank ranges.
      int sentenceBlankRangesLen = addMultiByteCharSentenceBlankRanges(scratch, 0);
      byte[] sentenceBlankRanges = Arrays.copyOf(scratch, sentenceBlankRangesLen);

      Assert.assertTrue(StringExpr.characterCount(sentenceBlankRanges) == 17);
      result = StringExpr.truncateScalar(sentenceBlankRanges, 4);
      Assert.assertTrue(Arrays.equals(Arrays.copyOf(sentenceBlankRanges, 9), result));

      byte[] sentenceBlankRangesPortion = Arrays.copyOf(sentenceBlankRanges, sentenceBlankRangesLen - 3);
      Assert.assertTrue(StringExpr.characterCount(sentenceBlankRangesPortion) == 16);
      result = StringExpr.truncateScalar(sentenceBlankRangesPortion, 14);
      Assert.assertTrue(Arrays.equals(Arrays.copyOf(sentenceBlankRangesPortion, 23), result));

      sentenceBlankRangesPortion = Arrays.copyOfRange(sentenceBlankRanges, 7, 7 + 17);
      Assert.assertTrue(StringExpr.characterCount(sentenceBlankRangesPortion) == 13);
      result = StringExpr.truncateScalar(sentenceBlankRangesPortion, 11);
      Assert.assertTrue(Arrays.equals(Arrays.copyOf(sentenceBlankRangesPortion, 15), result));
      Assert.assertTrue(StringExpr.characterCount(result) == 11);
  }

  @Test
  // Test basic right trim and truncate to vector.
  public void testRightTrimAndTruncateBytesSlice()  {
      // This first section repeats the tests of testRightTrimWithOffset with a large maxLength parameter.
      // (i.e. too large to have an effect).
      int largeMaxLength = 100;

      int resultLen;
      // Nothing to trim (ASCII).
      Assert.assertTrue(StringExpr.characterCount(blue, 0, blue.length) == 4);
      resultLen = StringExpr.rightTrimAndTruncate(blue, 0, blue.length, largeMaxLength);
      Assert.assertTrue(resultLen == blue.length);
      Assert.assertTrue(StringExpr.characterCount(blue, 0, resultLen) == 4);

      Assert.assertTrue(StringExpr.characterCount(redgreen, 0, redgreen.length) == 8);
      resultLen = StringExpr.rightTrimAndTruncate(redgreen, 0, redgreen.length, largeMaxLength);
      Assert.assertTrue(resultLen == redgreen.length);

      Assert.assertTrue(StringExpr.characterCount(ascii_sentence, 0, ascii_sentence.length) == 31);
      resultLen = StringExpr.rightTrimAndTruncate(ascii_sentence, 0, ascii_sentence.length, largeMaxLength);
      Assert.assertTrue(resultLen == ascii_sentence.length);

      Assert.assertTrue(StringExpr.characterCount(blanksLeft, 0, blanksLeft.length) == 5);
      resultLen = StringExpr.rightTrimAndTruncate(blanksLeft, 0, blanksLeft.length, largeMaxLength);
      Assert.assertTrue(resultLen == blanksLeft.length);

      // Simple trims.
      Assert.assertTrue(StringExpr.characterCount(blanksRight, 0, blanksRight.length) == 5);
      resultLen = StringExpr.rightTrimAndTruncate(blanksRight, 0, blanksRight.length, largeMaxLength);
      Assert.assertTrue(resultLen == 3);
      Assert.assertTrue(StringExpr.characterCount(blanksRight, 0, resultLen) == 3);

      Assert.assertTrue(StringExpr.characterCount(blanksBoth, 0, blanksBoth.length) == 7);
      resultLen = StringExpr.rightTrimAndTruncate(blanksBoth, 0, blanksBoth.length, largeMaxLength);
      Assert.assertTrue(resultLen == 5);
      Assert.assertTrue(StringExpr.characterCount(blanksBoth, 0, resultLen) == 5);
     
      Assert.assertTrue(StringExpr.characterCount(blankString, 0, blankString.length) == 2);
      resultLen = StringExpr.rightTrimAndTruncate(blankString, 0, blankString.length, largeMaxLength);
      Assert.assertTrue(resultLen == 0);
      Assert.assertTrue(StringExpr.characterCount(blankString, 0, resultLen) == 0);

      Assert.assertTrue(StringExpr.characterCount(blankRanges, 0, blankRanges.length) == 30);
      resultLen = StringExpr.rightTrimAndTruncate(blankRanges, 0, blankRanges.length, largeMaxLength);
      Assert.assertTrue(resultLen == blankRanges.length - 4);
      Assert.assertTrue(StringExpr.characterCount(blankRanges, 0, resultLen) == 26);

      // Offset trims.
      Assert.assertTrue(StringExpr.characterCount(blanksRight, 1, blanksRight.length - 1) == 4);
      resultLen = StringExpr.rightTrimAndTruncate(blanksRight, 1, blanksRight.length - 1, largeMaxLength);
      Assert.assertTrue(resultLen == 2);
      Assert.assertTrue(StringExpr.characterCount(blanksRight, 1, resultLen) == 2);

      Assert.assertTrue(StringExpr.characterCount(blanksBoth, 4, blanksBoth.length - 4) == 3);
      resultLen = StringExpr.rightTrimAndTruncate(blanksBoth, 4, blanksBoth.length - 4, largeMaxLength);
      Assert.assertTrue(resultLen == 1);
      Assert.assertTrue(StringExpr.characterCount(blanksBoth, 4, resultLen) == 1);

      Assert.assertTrue(StringExpr.characterCount(blanksBoth, 5, blanksBoth.length - 5) == 2);
      resultLen = StringExpr.rightTrimAndTruncate(blanksBoth, 5, blanksBoth.length -5, largeMaxLength);
      Assert.assertTrue(resultLen == 0);
      Assert.assertTrue(StringExpr.characterCount(blanksBoth, 5, resultLen) == 0);

      Assert.assertTrue(StringExpr.characterCount(blankString, 1, blankString.length - 1) == 1);
      resultLen = StringExpr.rightTrimAndTruncate(blankString, 1, blankString.length - 1, largeMaxLength);
      Assert.assertTrue(resultLen == 0);
      Assert.assertTrue(StringExpr.characterCount(blankString, 1, resultLen) == 0);

      Assert.assertTrue(StringExpr.characterCount(blankRanges, 4, blankRanges.length - 4) == 26);
      resultLen = StringExpr.rightTrimAndTruncate(blankRanges, 4, blankRanges.length - 4, largeMaxLength);
      Assert.assertTrue(resultLen == blankRanges.length - 4 -4);
      Assert.assertTrue(StringExpr.characterCount(blankRanges, 4, resultLen) == 22);

      Assert.assertTrue(StringExpr.characterCount(blankRanges, 6, blankRanges.length - 6) == 24);
      resultLen = StringExpr.rightTrimAndTruncate(blankRanges, 6, blankRanges.length- 6, largeMaxLength);
      Assert.assertTrue(resultLen == blankRanges.length - 6 - 4);
      Assert.assertTrue(StringExpr.characterCount(blankRanges, 6, resultLen) == 20);

      Assert.assertTrue(StringExpr.characterCount(blankRanges, 7, blankRanges.length - 7) == 23);
      resultLen = StringExpr.rightTrimAndTruncate(blankRanges, 7, blankRanges.length - 7, largeMaxLength);
      Assert.assertTrue(resultLen == blankRanges.length - 7 - 4);
      Assert.assertTrue(StringExpr.characterCount(blankRanges, 7, resultLen) == 19);

      Assert.assertTrue(StringExpr.characterCount(blankRanges, 7, 8 - 7) == 1);
      resultLen = StringExpr.rightTrimAndTruncate(blankRanges, 7, 8 - 7, largeMaxLength);
      Assert.assertTrue(resultLen == 0);
      Assert.assertTrue(StringExpr.characterCount(blankRanges, 7, resultLen) == 0);

      // Multi-byte trims.
      byte[] multiByte = new byte[100];

      addMultiByteCharRightPadded1_1(multiByte);
      Assert.assertTrue(StringExpr.characterCount(multiByte, 0, 4) == 2);
      resultLen = StringExpr.rightTrimAndTruncate(multiByte, 0, 4, largeMaxLength);
      Assert.assertTrue(resultLen == 3);
      Assert.assertTrue(StringExpr.characterCount(multiByte, 0, resultLen) == 1);

      addMultiByteCharRightPadded1_2(multiByte);
      Assert.assertTrue(StringExpr.characterCount(multiByte, 0, 5) == 3);
      resultLen = StringExpr.rightTrimAndTruncate(multiByte, 0, 5, largeMaxLength);
      Assert.assertTrue(resultLen == 4);
      Assert.assertTrue(StringExpr.characterCount(multiByte, 0, resultLen) == 2);

      addMultiByteCharRightPadded1_3(multiByte);
      Assert.assertTrue(StringExpr.characterCount(multiByte, 0, 9) == 4);
      resultLen = StringExpr.rightTrimAndTruncate(multiByte, 0, 9, largeMaxLength);
      Assert.assertTrue(resultLen == 8);
      Assert.assertTrue(StringExpr.characterCount(multiByte, 0, resultLen) == 3);

      addMultiByteCharRightPadded1_1(multiByte);
      Assert.assertTrue(StringExpr.characterCount(multiByte, 3, 1) == 1);
      resultLen = StringExpr.rightTrimAndTruncate(multiByte, 3, 1, largeMaxLength);
      Assert.assertTrue(resultLen == 0);
      Assert.assertTrue(StringExpr.characterCount(multiByte, 0, resultLen) == 0);

      addMultiByteCharRightPadded1_2(multiByte);
      Assert.assertTrue(StringExpr.characterCount(multiByte, 3, 2) == 2);
      resultLen = StringExpr.rightTrimAndTruncate(multiByte, 3, 2, largeMaxLength);
      Assert.assertTrue(resultLen == 1);
      Assert.assertTrue(StringExpr.characterCount(multiByte, 3, resultLen) == 1);

      byte[] sentenceOne = new byte[100];
      int sentenceOneLen = addMultiByteCharSentenceOne(sentenceOne, 0);

      Assert.assertTrue(StringExpr.characterCount(sentenceOne, 0, sentenceOneLen) == 10);
      resultLen = StringExpr.rightTrimAndTruncate(sentenceOne, 0, sentenceOneLen, largeMaxLength);
      Assert.assertTrue(resultLen == sentenceOneLen);

      Assert.assertTrue(StringExpr.characterCount(sentenceOne, 0, sentenceOneLen - 3) == 9);
      resultLen = StringExpr.rightTrimAndTruncate(sentenceOne, 0, sentenceOneLen - 3, largeMaxLength);
      Assert.assertTrue(resultLen == sentenceOneLen - 3);

      byte[] sentenceTwo = new byte[100];
      int sentenceTwoLen = addMultiByteCharSentenceTwo(sentenceTwo, 0);

      Assert.assertTrue(StringExpr.characterCount(sentenceTwo, 0, sentenceTwoLen) == 13);
      resultLen = StringExpr.rightTrimAndTruncate(sentenceTwo, 0, sentenceTwoLen, largeMaxLength);
      Assert.assertTrue(resultLen == sentenceTwoLen);

      Assert.assertTrue(StringExpr.characterCount(sentenceTwo, 0, sentenceTwoLen- 5) == 10);
      resultLen = StringExpr.rightTrimAndTruncate(sentenceTwo, 0, sentenceTwoLen - 5, largeMaxLength);
      Assert.assertTrue(resultLen == sentenceTwoLen - 5);

      int start;

      // Left pad longer strings with multi-byte characters.
      byte[] sentenceOnePaddedLeft = new byte[100];
      start = addPads(sentenceOnePaddedLeft, 0, 3);
      int sentenceOnePaddedLeftLen = addMultiByteCharSentenceOne(sentenceOnePaddedLeft, start);

      Assert.assertTrue(StringExpr.characterCount(sentenceOnePaddedLeft, 0, sentenceOnePaddedLeftLen) == 3 + 10);
      resultLen = StringExpr.rightTrimAndTruncate(sentenceOnePaddedLeft, 0, sentenceOnePaddedLeftLen, largeMaxLength);
      Assert.assertTrue(resultLen == sentenceOnePaddedLeftLen);

      Assert.assertTrue(StringExpr.characterCount(sentenceOnePaddedLeft, 0, sentenceOnePaddedLeftLen - 3) == 3 + 9);
      resultLen = StringExpr.rightTrimAndTruncate(sentenceOnePaddedLeft, 0, sentenceOnePaddedLeftLen - 3, largeMaxLength);
      Assert.assertTrue(resultLen == sentenceOnePaddedLeftLen - 3);

      byte[] sentenceTwoPaddedLeft = new byte[100];
      start = addPads(sentenceTwoPaddedLeft, 0, 2);
      int sentenceTwoPaddedLeftLen = addMultiByteCharSentenceTwo(sentenceTwoPaddedLeft, start);

      Assert.assertTrue(StringExpr.characterCount(sentenceTwoPaddedLeft, 0, sentenceTwoPaddedLeftLen) == 2 + 13);
      resultLen = StringExpr.rightTrimAndTruncate(sentenceTwoPaddedLeft, 0, sentenceTwoPaddedLeftLen, largeMaxLength);
      Assert.assertTrue(resultLen == sentenceTwoPaddedLeftLen);

      Assert.assertTrue(StringExpr.characterCount(sentenceTwoPaddedLeft, 0, sentenceTwoPaddedLeftLen - 5) == 2 + 10);
      resultLen = StringExpr.rightTrimAndTruncate(sentenceTwoPaddedLeft, 0, sentenceTwoPaddedLeftLen - 5, largeMaxLength);
      Assert.assertTrue(resultLen == sentenceTwoPaddedLeftLen - 5);

      // Right pad longer strings with multi-byte characters.
      byte[] sentenceOnePaddedRight = new byte[100];
      start = addMultiByteCharSentenceOne(sentenceOnePaddedRight, 0);
      int sentenceOnePaddedRightLen = addPads(sentenceOnePaddedRight, start, 4);

      Assert.assertTrue(StringExpr.characterCount(sentenceOnePaddedRight, 0, sentenceOnePaddedRightLen) == 10 + 4);
      resultLen = StringExpr.rightTrimAndTruncate(sentenceOnePaddedRight, 0, sentenceOnePaddedRightLen, largeMaxLength);
      Assert.assertTrue(resultLen == sentenceOnePaddedRightLen - 4);

      Assert.assertTrue(StringExpr.characterCount(sentenceOnePaddedRight, 0, sentenceOnePaddedRightLen - 3 - 4) == 9);
      resultLen = StringExpr.rightTrimAndTruncate(sentenceOnePaddedRight, 0, sentenceOnePaddedRightLen - 3 - 4, largeMaxLength);
      Assert.assertTrue(resultLen == sentenceOnePaddedRightLen - 3 - 4);

      byte[] sentenceTwoPaddedRight = new byte[100];
      start = addMultiByteCharSentenceTwo(sentenceTwoPaddedRight, 0);
      int sentenceTwoPaddedRightLen = addPads(sentenceTwoPaddedRight, start, 1);

      Assert.assertTrue(StringExpr.characterCount(sentenceTwoPaddedRight, 0, sentenceTwoPaddedRightLen) == 13 + 1);
      resultLen = StringExpr.rightTrimAndTruncate(sentenceTwoPaddedRight, 0, sentenceTwoPaddedRightLen, largeMaxLength);
      Assert.assertTrue(resultLen == sentenceTwoPaddedRightLen - 1);

      Assert.assertTrue(StringExpr.characterCount(sentenceTwoPaddedRight, 0, sentenceTwoPaddedRightLen - 5 - 1) == 10);
      resultLen = StringExpr.rightTrimAndTruncate(sentenceTwoPaddedRight, 0, sentenceTwoPaddedRightLen - 5 - 1, largeMaxLength);
      Assert.assertTrue(resultLen == sentenceTwoPaddedRightLen - 5 - 1);

      // Multi-byte characters with blank ranges.
      byte[] sentenceBlankRanges = new byte[100];
      int sentenceBlankRangesLen = addMultiByteCharSentenceBlankRanges(sentenceBlankRanges, 0);

      Assert.assertTrue(StringExpr.characterCount(sentenceBlankRanges, 0, sentenceBlankRangesLen) == 17);
      resultLen = StringExpr.rightTrimAndTruncate(sentenceBlankRanges, 0, sentenceBlankRangesLen, largeMaxLength);
      Assert.assertTrue(resultLen == sentenceBlankRangesLen);

      Assert.assertTrue(StringExpr.characterCount(sentenceBlankRanges, 0, sentenceBlankRangesLen - 3) == 16);
      resultLen = StringExpr.rightTrimAndTruncate(sentenceBlankRanges, 0, sentenceBlankRangesLen - 3, largeMaxLength);
      Assert.assertTrue(resultLen == sentenceBlankRangesLen - 3);

      Assert.assertTrue(StringExpr.characterCount(sentenceBlankRanges, 7, 17) == 13);
      resultLen = StringExpr.rightTrimAndTruncate(sentenceBlankRanges, 7, 17, largeMaxLength);
      Assert.assertTrue(resultLen == 12);
      Assert.assertTrue(StringExpr.characterCount(sentenceBlankRanges, 7, resultLen) == 8);

      // This next section repeats the tests of testRightTrimWithOffset with a maxLength parameter that is
      // exactly the number of current characters in the string.  This shouldn't affect the trim.

      // Nothing to trim (ASCII).
      Assert.assertTrue(StringExpr.characterCount(blue, 0, blue.length) == 4);
      resultLen = StringExpr.rightTrimAndTruncate(blue, 0, blue.length, 4);
      Assert.assertTrue(resultLen == blue.length);
      Assert.assertTrue(StringExpr.characterCount(blue, 0, resultLen) == 4);

      Assert.assertTrue(StringExpr.characterCount(redgreen, 0, redgreen.length) == 8);
      resultLen = StringExpr.rightTrimAndTruncate(redgreen, 0, redgreen.length, 8);
      Assert.assertTrue(resultLen == redgreen.length);

      Assert.assertTrue(StringExpr.characterCount(ascii_sentence, 0, ascii_sentence.length) == 31);
      resultLen = StringExpr.rightTrimAndTruncate(ascii_sentence, 0, ascii_sentence.length, 31);
      Assert.assertTrue(resultLen == ascii_sentence.length);

      Assert.assertTrue(StringExpr.characterCount(blanksLeft, 0, blanksLeft.length) == 5);
      resultLen = StringExpr.rightTrimAndTruncate(blanksLeft, 0, blanksLeft.length, 5);
      Assert.assertTrue(resultLen == blanksLeft.length);

      // Truncate everything and nothing to trim
      Assert.assertTrue(StringExpr.characterCount(blanksLeft, 0, blanksLeft.length) == 5);
      resultLen = StringExpr.rightTrimAndTruncate(blanksLeft, 0, blanksLeft.length, 0);
      Assert.assertTrue(resultLen == 0);

      // Simple trims.
      Assert.assertTrue(StringExpr.characterCount(blanksRight, 0, blanksRight.length) == 5);
      resultLen = StringExpr.rightTrimAndTruncate(blanksRight, 0, blanksRight.length, 5);
      Assert.assertTrue(resultLen == 3);
      Assert.assertTrue(StringExpr.characterCount(blanksRight, 0, resultLen) == 3);

      Assert.assertTrue(StringExpr.characterCount(blanksBoth, 0, blanksBoth.length) == 7);
      resultLen = StringExpr.rightTrimAndTruncate(blanksBoth, 0, blanksBoth.length, 7);
      Assert.assertTrue(resultLen == 5);
      Assert.assertTrue(StringExpr.characterCount(blanksBoth, 0, resultLen) == 5);
     
      Assert.assertTrue(StringExpr.characterCount(blankString, 0, blankString.length) == 2);
      resultLen = StringExpr.rightTrimAndTruncate(blankString, 0, blankString.length, 2);
      Assert.assertTrue(resultLen == 0);
      Assert.assertTrue(StringExpr.characterCount(blankString, 0, resultLen) == 0);

      Assert.assertTrue(StringExpr.characterCount(blankRanges, 0, blankRanges.length) == 30);
      resultLen = StringExpr.rightTrimAndTruncate(blankRanges, 0, blankRanges.length, 30);
      Assert.assertTrue(resultLen == blankRanges.length - 4);
      Assert.assertTrue(StringExpr.characterCount(blankRanges, 0, resultLen) == 26);

      // Offset trims.
      Assert.assertTrue(StringExpr.characterCount(blanksRight, 1, blanksRight.length - 1) == 4);
      resultLen = StringExpr.rightTrimAndTruncate(blanksRight, 1, blanksRight.length - 1, 4);
      Assert.assertTrue(resultLen == 2);
      Assert.assertTrue(StringExpr.characterCount(blanksRight, 1, resultLen) == 2);

      Assert.assertTrue(StringExpr.characterCount(blanksBoth, 4, blanksBoth.length - 4) == 3);
      resultLen = StringExpr.rightTrimAndTruncate(blanksBoth, 4, blanksBoth.length - 4, 3);
      Assert.assertTrue(resultLen == 1);
      Assert.assertTrue(StringExpr.characterCount(blanksBoth, 4, resultLen) == 1);

      Assert.assertTrue(StringExpr.characterCount(blanksBoth, 5, blanksBoth.length - 5) == 2);
      resultLen = StringExpr.rightTrimAndTruncate(blanksBoth, 5, blanksBoth.length -5, 2);
      Assert.assertTrue(resultLen == 0);
      Assert.assertTrue(StringExpr.characterCount(blanksBoth, 5, resultLen) == 0);

      Assert.assertTrue(StringExpr.characterCount(blankString, 1, blankString.length - 1) == 1);
      resultLen = StringExpr.rightTrimAndTruncate(blankString, 1, blankString.length - 1, 1);
      Assert.assertTrue(resultLen == 0);
      Assert.assertTrue(StringExpr.characterCount(blankString, 1, resultLen) == 0);

      Assert.assertTrue(StringExpr.characterCount(blankRanges, 4, blankRanges.length - 4) == 26);
      resultLen = StringExpr.rightTrimAndTruncate(blankRanges, 4, blankRanges.length - 4, 26);
      Assert.assertTrue(resultLen == blankRanges.length - 4 -4);
      Assert.assertTrue(StringExpr.characterCount(blankRanges, 4, resultLen) == 22);

      Assert.assertTrue(StringExpr.characterCount(blankRanges, 6, blankRanges.length - 6) == 24);
      resultLen = StringExpr.rightTrimAndTruncate(blankRanges, 6, blankRanges.length- 6, 24);
      Assert.assertTrue(resultLen == blankRanges.length - 6 - 4);
      Assert.assertTrue(StringExpr.characterCount(blankRanges, 6, resultLen) == 20);

      Assert.assertTrue(StringExpr.characterCount(blankRanges, 7, blankRanges.length - 7) == 23);
      resultLen = StringExpr.rightTrimAndTruncate(blankRanges, 7, blankRanges.length - 7, 23);
      Assert.assertTrue(resultLen == blankRanges.length - 7 - 4);
      Assert.assertTrue(StringExpr.characterCount(blankRanges, 7, resultLen) == 19);

      Assert.assertTrue(StringExpr.characterCount(blankRanges, 7, 8 - 7) == 1);
      resultLen = StringExpr.rightTrimAndTruncate(blankRanges, 7, 8 - 7, 1);
      Assert.assertTrue(resultLen == 0);
      Assert.assertTrue(StringExpr.characterCount(blankRanges, 7, resultLen) == 0);

      // Multi-byte trims.
      multiByte = new byte[100];

      addMultiByteCharRightPadded1_1(multiByte);
      Assert.assertTrue(StringExpr.characterCount(multiByte, 0, 4) == 2);
      resultLen = StringExpr.rightTrimAndTruncate(multiByte, 0, 4, 2);
      Assert.assertTrue(resultLen == 3);
      Assert.assertTrue(StringExpr.characterCount(multiByte, 0, resultLen) == 1);

      addMultiByteCharRightPadded1_2(multiByte);
      Assert.assertTrue(StringExpr.characterCount(multiByte, 0, 5) == 3);
      resultLen = StringExpr.rightTrimAndTruncate(multiByte, 0, 5, 3);
      Assert.assertTrue(resultLen == 4);
      Assert.assertTrue(StringExpr.characterCount(multiByte, 0, resultLen) == 2);

      addMultiByteCharRightPadded1_3(multiByte);
      Assert.assertTrue(StringExpr.characterCount(multiByte, 0, 9) == 4);
      resultLen = StringExpr.rightTrimAndTruncate(multiByte, 0, 9, 4);
      Assert.assertTrue(resultLen == 8);
      Assert.assertTrue(StringExpr.characterCount(multiByte, 0, resultLen) == 3);

      addMultiByteCharRightPadded1_1(multiByte);
      Assert.assertTrue(StringExpr.characterCount(multiByte, 3, 1) == 1);
      resultLen = StringExpr.rightTrimAndTruncate(multiByte, 3, 1, 1);
      Assert.assertTrue(resultLen == 0);
      Assert.assertTrue(StringExpr.characterCount(multiByte, 0, resultLen) == 0);

      addMultiByteCharRightPadded1_2(multiByte);
      Assert.assertTrue(StringExpr.characterCount(multiByte, 3, 2) == 2);
      resultLen = StringExpr.rightTrimAndTruncate(multiByte, 3, 2, 2);
      Assert.assertTrue(resultLen == 1);
      Assert.assertTrue(StringExpr.characterCount(multiByte, 3, resultLen) == 1);

      sentenceOne = new byte[100];
      sentenceOneLen = addMultiByteCharSentenceOne(sentenceOne, 0);

      Assert.assertTrue(StringExpr.characterCount(sentenceOne, 0, sentenceOneLen) == 10);
      resultLen = StringExpr.rightTrimAndTruncate(sentenceOne, 0, sentenceOneLen, 10);
      Assert.assertTrue(resultLen == sentenceOneLen);

      Assert.assertTrue(StringExpr.characterCount(sentenceOne, 0, sentenceOneLen - 3) == 9);
      resultLen = StringExpr.rightTrimAndTruncate(sentenceOne, 0, sentenceOneLen - 3, 9);
      Assert.assertTrue(resultLen == sentenceOneLen - 3);

      sentenceTwo = new byte[100];
      sentenceTwoLen = addMultiByteCharSentenceTwo(sentenceTwo, 0);

      Assert.assertTrue(StringExpr.characterCount(sentenceTwo, 0, sentenceTwoLen) == 13);
      resultLen = StringExpr.rightTrimAndTruncate(sentenceTwo, 0, sentenceTwoLen, 13);
      Assert.assertTrue(resultLen == sentenceTwoLen);

      Assert.assertTrue(StringExpr.characterCount(sentenceTwo, 0, sentenceTwoLen- 5) == 10);
      resultLen = StringExpr.rightTrimAndTruncate(sentenceTwo, 0, sentenceTwoLen - 5, 10);
      Assert.assertTrue(resultLen == sentenceTwoLen - 5);

      // Left pad longer strings with multi-byte characters.
      sentenceOnePaddedLeft = new byte[100];
      start = addPads(sentenceOnePaddedLeft, 0, 3);
      sentenceOnePaddedLeftLen = addMultiByteCharSentenceOne(sentenceOnePaddedLeft, start);

      Assert.assertTrue(StringExpr.characterCount(sentenceOnePaddedLeft, 0, sentenceOnePaddedLeftLen) == 3 + 10);
      resultLen = StringExpr.rightTrimAndTruncate(sentenceOnePaddedLeft, 0, sentenceOnePaddedLeftLen, 3 + 10);
      Assert.assertTrue(resultLen == sentenceOnePaddedLeftLen);

      Assert.assertTrue(StringExpr.characterCount(sentenceOnePaddedLeft, 0, sentenceOnePaddedLeftLen - 3) == 3 + 9);
      resultLen = StringExpr.rightTrimAndTruncate(sentenceOnePaddedLeft, 0, sentenceOnePaddedLeftLen - 3, 3 + 9);
      Assert.assertTrue(resultLen == sentenceOnePaddedLeftLen - 3);

      sentenceTwoPaddedLeft = new byte[100];
      start = addPads(sentenceTwoPaddedLeft, 0, 2);
      sentenceTwoPaddedLeftLen = addMultiByteCharSentenceTwo(sentenceTwoPaddedLeft, start);

      Assert.assertTrue(StringExpr.characterCount(sentenceTwoPaddedLeft, 0, sentenceTwoPaddedLeftLen) == 2 + 13);
      resultLen = StringExpr.rightTrimAndTruncate(sentenceTwoPaddedLeft, 0, sentenceTwoPaddedLeftLen, 2 + 13);
      Assert.assertTrue(resultLen == sentenceTwoPaddedLeftLen);

      Assert.assertTrue(StringExpr.characterCount(sentenceTwoPaddedLeft, 0, sentenceTwoPaddedLeftLen - 5) == 2 + 10);
      resultLen = StringExpr.rightTrimAndTruncate(sentenceTwoPaddedLeft, 0, sentenceTwoPaddedLeftLen - 5, 2 + 10);
      Assert.assertTrue(resultLen == sentenceTwoPaddedLeftLen - 5);

      // Right pad longer strings with multi-byte characters.
      sentenceOnePaddedRight = new byte[100];
      start = addMultiByteCharSentenceOne(sentenceOnePaddedRight, 0);
      sentenceOnePaddedRightLen = addPads(sentenceOnePaddedRight, start, 4);

      Assert.assertTrue(StringExpr.characterCount(sentenceOnePaddedRight, 0, sentenceOnePaddedRightLen) == 10 + 4);
      resultLen = StringExpr.rightTrimAndTruncate(sentenceOnePaddedRight, 0, sentenceOnePaddedRightLen, 10 + 4);
      Assert.assertTrue(resultLen == sentenceOnePaddedRightLen - 4);

      Assert.assertTrue(StringExpr.characterCount(sentenceOnePaddedRight, 0, sentenceOnePaddedRightLen - 3 - 4) == 9);
      resultLen = StringExpr.rightTrimAndTruncate(sentenceOnePaddedRight, 0, sentenceOnePaddedRightLen - 3 - 4, 9);
      Assert.assertTrue(resultLen == sentenceOnePaddedRightLen - 3 - 4);

      sentenceTwoPaddedRight = new byte[100];
      start = addMultiByteCharSentenceTwo(sentenceTwoPaddedRight, 0);
      sentenceTwoPaddedRightLen = addPads(sentenceTwoPaddedRight, start, 1);

      Assert.assertTrue(StringExpr.characterCount(sentenceTwoPaddedRight, 0, sentenceTwoPaddedRightLen) == 13 + 1);
      resultLen = StringExpr.rightTrimAndTruncate(sentenceTwoPaddedRight, 0, sentenceTwoPaddedRightLen, 13 + 1);
      Assert.assertTrue(resultLen == sentenceTwoPaddedRightLen - 1);

      Assert.assertTrue(StringExpr.characterCount(sentenceTwoPaddedRight, 0, sentenceTwoPaddedRightLen - 5 - 1) == 10);
      resultLen = StringExpr.rightTrimAndTruncate(sentenceTwoPaddedRight, 0, sentenceTwoPaddedRightLen - 5 - 1, 10);
      Assert.assertTrue(resultLen == sentenceTwoPaddedRightLen - 5 - 1);

      // Multi-byte characters with blank ranges.
      sentenceBlankRanges = new byte[100];
      sentenceBlankRangesLen = addMultiByteCharSentenceBlankRanges(sentenceBlankRanges, 0);

      Assert.assertTrue(StringExpr.characterCount(sentenceBlankRanges, 0, sentenceBlankRangesLen) == 17);
      resultLen = StringExpr.rightTrimAndTruncate(sentenceBlankRanges, 0, sentenceBlankRangesLen, 17);
      Assert.assertTrue(resultLen == sentenceBlankRangesLen);

      Assert.assertTrue(StringExpr.characterCount(sentenceBlankRanges, 0, sentenceBlankRangesLen - 3) == 16);
      resultLen = StringExpr.rightTrimAndTruncate(sentenceBlankRanges, 0, sentenceBlankRangesLen - 3, 16);
      Assert.assertTrue(resultLen == sentenceBlankRangesLen - 3);

      Assert.assertTrue(StringExpr.characterCount(sentenceBlankRanges, 7, 17) == 13);
      resultLen = StringExpr.rightTrimAndTruncate(sentenceBlankRanges, 7, 17, largeMaxLength);
      Assert.assertTrue(resultLen == 12);
      Assert.assertTrue(StringExpr.characterCount(sentenceBlankRanges, 7, resultLen) == 8);

      // This next section repeats the tests of testRightTrimWithOffset with a maxLength parameter that is
      // less than the number of current characters in the string and thus affects the trim.

      // Nothing to trim (ASCII).
      Assert.assertTrue(StringExpr.characterCount(blue, 0, blue.length) == 4);
      resultLen = StringExpr.rightTrimAndTruncate(blue, 0, blue.length, 3);
      Assert.assertTrue(resultLen == 3);
      Assert.assertTrue(StringExpr.characterCount(blue, 0, resultLen) == 3);

      Assert.assertTrue(StringExpr.characterCount(redgreen, 0, redgreen.length) == 8);
      resultLen = StringExpr.rightTrimAndTruncate(redgreen, 0, redgreen.length, 6);
      Assert.assertTrue(resultLen == 6);

      Assert.assertTrue(StringExpr.characterCount(ascii_sentence, 0, ascii_sentence.length) == 31);
      resultLen = StringExpr.rightTrimAndTruncate(ascii_sentence, 0, ascii_sentence.length, 30);
      Assert.assertTrue(resultLen == 30);

      Assert.assertTrue(StringExpr.characterCount(blanksLeft, 0, blanksLeft.length) == 5);
      resultLen = StringExpr.rightTrimAndTruncate(blanksLeft, 0, blanksLeft.length, 1);
      Assert.assertTrue(resultLen == 0);

      // Simple trims.
      Assert.assertTrue(StringExpr.characterCount(blanksRight, 0, blanksRight.length) == 5);
      resultLen = StringExpr.rightTrimAndTruncate(blanksRight, 0, blanksRight.length, 4);
      Assert.assertTrue(resultLen == 3);
      Assert.assertTrue(StringExpr.characterCount(blanksRight, 0, resultLen) == 3);

      Assert.assertTrue(StringExpr.characterCount(blanksBoth, 0, blanksBoth.length) == 7);
      resultLen = StringExpr.rightTrimAndTruncate(blanksBoth, 0, blanksBoth.length, 6);
      Assert.assertTrue(resultLen == 5);
      Assert.assertTrue(StringExpr.characterCount(blanksBoth, 0, resultLen) == 5);
     
      Assert.assertTrue(StringExpr.characterCount(blankString, 0, blankString.length) == 2);
      resultLen = StringExpr.rightTrimAndTruncate(blankString, 0, blankString.length, 1);
      Assert.assertTrue(resultLen == 0);
      Assert.assertTrue(StringExpr.characterCount(blankString, 0, resultLen) == 0);

      Assert.assertTrue(StringExpr.characterCount(blankRanges, 0, blankRanges.length) == 30);
      resultLen = StringExpr.rightTrimAndTruncate(blankRanges, 0, blankRanges.length, 19);
      Assert.assertTrue(resultLen == 15);
      Assert.assertTrue(StringExpr.characterCount(blankRanges, 0, resultLen) == 15);

      // Offset trims.
      Assert.assertTrue(StringExpr.characterCount(blanksRight, 1, blanksRight.length - 1) == 4);
      resultLen = StringExpr.rightTrimAndTruncate(blanksRight, 1, blanksRight.length - 1, 3);
      Assert.assertTrue(resultLen == 2);
      Assert.assertTrue(StringExpr.characterCount(blanksRight, 1, resultLen) == 2);

      Assert.assertTrue(StringExpr.characterCount(blanksBoth, 4, blanksBoth.length - 4) == 3);
      resultLen = StringExpr.rightTrimAndTruncate(blanksBoth, 4, blanksBoth.length - 4, 2);
      Assert.assertTrue(resultLen == 1);
      Assert.assertTrue(StringExpr.characterCount(blanksBoth, 4, resultLen) == 1);

      Assert.assertTrue(StringExpr.characterCount(blanksBoth, 5, blanksBoth.length - 5) == 2);
      resultLen = StringExpr.rightTrimAndTruncate(blanksBoth, 5, blanksBoth.length -5, 1);
      Assert.assertTrue(resultLen == 0);
      Assert.assertTrue(StringExpr.characterCount(blanksBoth, 5, resultLen) == 0);

      Assert.assertTrue(StringExpr.characterCount(blankString, 1, blankString.length - 1) == 1);
      resultLen = StringExpr.rightTrimAndTruncate(blankString, 1, blankString.length - 1, 1);
      Assert.assertTrue(resultLen == 0);
      Assert.assertTrue(StringExpr.characterCount(blankString, 1, resultLen) == 0);

      Assert.assertTrue(StringExpr.characterCount(blankRanges, 3, 6) == 6);
      resultLen = StringExpr.rightTrimAndTruncate(blankRanges, 3, 6, 5);
      Assert.assertTrue(resultLen == 4);
      Assert.assertTrue(StringExpr.characterCount(blankRanges, 3, resultLen) == 4);

      Assert.assertTrue(StringExpr.characterCount(blankRanges, 6, blankRanges.length - 6) == 24);
      resultLen = StringExpr.rightTrimAndTruncate(blankRanges, 6, blankRanges.length- 6, 22);
      Assert.assertTrue(resultLen == blankRanges.length - 6 - 4);
      Assert.assertTrue(StringExpr.characterCount(blankRanges, 6, resultLen) == 20);

      Assert.assertTrue(StringExpr.characterCount(blankRanges, 7, blankRanges.length - 7) == 23);
      resultLen = StringExpr.rightTrimAndTruncate(blankRanges, 7, blankRanges.length - 7, 10);
      Assert.assertTrue(resultLen == 8);
      Assert.assertTrue(StringExpr.characterCount(blankRanges, 7, resultLen) == 8);

      Assert.assertTrue(StringExpr.characterCount(blankRanges, 7, 8 - 7) == 1);
      resultLen = StringExpr.rightTrimAndTruncate(blankRanges, 7, 8 - 7, 1);
      Assert.assertTrue(resultLen == 0);
      Assert.assertTrue(StringExpr.characterCount(blankRanges, 7, resultLen) == 0);

      // Multi-byte trims.
      multiByte = new byte[100];

      addMultiByteCharRightPadded1_1(multiByte);
      Assert.assertTrue(StringExpr.characterCount(multiByte, 0, 4) == 2);
      resultLen = StringExpr.rightTrimAndTruncate(multiByte, 0, 4, 1);
      Assert.assertTrue(resultLen == 3);
      Assert.assertTrue(StringExpr.characterCount(multiByte, 0, resultLen) == 1);

      addMultiByteCharRightPadded1_2(multiByte);
      Assert.assertTrue(StringExpr.characterCount(multiByte, 0, 5) == 3);
      resultLen = StringExpr.rightTrimAndTruncate(multiByte, 0, 5, 2);
      Assert.assertTrue(resultLen == 4);
      Assert.assertTrue(StringExpr.characterCount(multiByte, 0, resultLen) == 2);

      addMultiByteCharRightPadded1_3(multiByte);
      Assert.assertTrue(StringExpr.characterCount(multiByte, 0, 9) == 4);
      resultLen = StringExpr.rightTrimAndTruncate(multiByte, 0, 9, 3);
      Assert.assertTrue(resultLen == 8);
      Assert.assertTrue(StringExpr.characterCount(multiByte, 0, resultLen) == 3);

      addMultiByteCharRightPadded1_1(multiByte);
      Assert.assertTrue(StringExpr.characterCount(multiByte, 3, 1) == 1);
      resultLen = StringExpr.rightTrimAndTruncate(multiByte, 3, 1, 1);
      Assert.assertTrue(resultLen == 0);
      Assert.assertTrue(StringExpr.characterCount(multiByte, 0, resultLen) == 0);

      addMultiByteCharRightPadded1_2(multiByte);
      Assert.assertTrue(StringExpr.characterCount(multiByte, 3, 2) == 2);
      resultLen = StringExpr.rightTrimAndTruncate(multiByte, 3, 2, 1);
      Assert.assertTrue(resultLen == 1);
      Assert.assertTrue(StringExpr.characterCount(multiByte, 3, resultLen) == 1);

      sentenceOne = new byte[100];
      sentenceOneLen = addMultiByteCharSentenceOne(sentenceOne, 0);

      Assert.assertTrue(StringExpr.characterCount(sentenceOne, 0, sentenceOneLen) == 10);
      resultLen = StringExpr.rightTrimAndTruncate(sentenceOne, 0, sentenceOneLen, 7);
      Assert.assertTrue(resultLen == sentenceOneLen - 9);

      Assert.assertTrue(StringExpr.characterCount(sentenceOne, 0, sentenceOneLen - 3) == 9);
      resultLen = StringExpr.rightTrimAndTruncate(sentenceOne, 0, sentenceOneLen - 3, 6);
      Assert.assertTrue(resultLen == 13);

      sentenceTwo = new byte[100];
      sentenceTwoLen = addMultiByteCharSentenceTwo(sentenceTwo, 0);

      Assert.assertTrue(StringExpr.characterCount(sentenceTwo, 0, sentenceTwoLen) == 13);
      resultLen = StringExpr.rightTrimAndTruncate(sentenceTwo, 0, sentenceTwoLen, 13);
      Assert.assertTrue(resultLen == sentenceTwoLen);

      Assert.assertTrue(StringExpr.characterCount(sentenceTwo, 0, sentenceTwoLen- 5) == 10);
      resultLen = StringExpr.rightTrimAndTruncate(sentenceTwo, 0, sentenceTwoLen - 5, 10);
      Assert.assertTrue(resultLen == sentenceTwoLen - 5);

      // Left pad longer strings with multi-byte characters.
      sentenceOnePaddedLeft = new byte[100];
      start = addPads(sentenceOnePaddedLeft, 0, 3);
      sentenceOnePaddedLeftLen = addMultiByteCharSentenceOne(sentenceOnePaddedLeft, start);

      Assert.assertTrue(StringExpr.characterCount(sentenceOnePaddedLeft, 0, sentenceOnePaddedLeftLen) == 3 + 10);
      resultLen = StringExpr.rightTrimAndTruncate(sentenceOnePaddedLeft, 0, sentenceOnePaddedLeftLen, 3 + 8);
      Assert.assertTrue(resultLen == sentenceOnePaddedLeftLen - 6);

      Assert.assertTrue(StringExpr.characterCount(sentenceOnePaddedLeft, 0, sentenceOnePaddedLeftLen - 3) == 3 + 9);
      resultLen = StringExpr.rightTrimAndTruncate(sentenceOnePaddedLeft, 0, sentenceOnePaddedLeftLen - 3, 3 + 6);
      Assert.assertTrue(resultLen == 16);

      sentenceTwoPaddedLeft = new byte[100];
      start = addPads(sentenceTwoPaddedLeft, 0, 2);
      sentenceTwoPaddedLeftLen = addMultiByteCharSentenceTwo(sentenceTwoPaddedLeft, start);

      Assert.assertTrue(StringExpr.characterCount(sentenceTwoPaddedLeft, 0, sentenceTwoPaddedLeftLen) == 2 + 13);
      resultLen = StringExpr.rightTrimAndTruncate(sentenceTwoPaddedLeft, 0, sentenceTwoPaddedLeftLen, 7);
      Assert.assertTrue(resultLen == 10);

      Assert.assertTrue(StringExpr.characterCount(sentenceTwoPaddedLeft, 0, sentenceTwoPaddedLeftLen - 5) == 2 + 10);
      resultLen = StringExpr.rightTrimAndTruncate(sentenceTwoPaddedLeft, 0, sentenceTwoPaddedLeftLen - 5, 6);
      Assert.assertTrue(resultLen == 10);

      // Right pad longer strings with multi-byte characters.
      sentenceOnePaddedRight = new byte[100];
      start = addMultiByteCharSentenceOne(sentenceOnePaddedRight, 0);
      sentenceOnePaddedRightLen = addPads(sentenceOnePaddedRight, start, 4);

      Assert.assertTrue(StringExpr.characterCount(sentenceOnePaddedRight, 0, sentenceOnePaddedRightLen) == 10 + 4);
      resultLen = StringExpr.rightTrimAndTruncate(sentenceOnePaddedRight, 0, sentenceOnePaddedRightLen, 10);
      Assert.assertTrue(resultLen == sentenceOnePaddedRightLen - 4);

      Assert.assertTrue(StringExpr.characterCount(sentenceOnePaddedRight, 0, sentenceOnePaddedRightLen - 3 - 4) == 9);
      resultLen = StringExpr.rightTrimAndTruncate(sentenceOnePaddedRight, 0, sentenceOnePaddedRightLen - 3 - 4, 7);
      Assert.assertTrue(resultLen == 17);

      sentenceTwoPaddedRight = new byte[100];
      start = addMultiByteCharSentenceTwo(sentenceTwoPaddedRight, 0);
      sentenceTwoPaddedRightLen = addPads(sentenceTwoPaddedRight, start, 1);

      Assert.assertTrue(StringExpr.characterCount(sentenceTwoPaddedRight, 0, sentenceTwoPaddedRightLen) == 13 + 1);
      resultLen = StringExpr.rightTrimAndTruncate(sentenceTwoPaddedRight, 0, sentenceTwoPaddedRightLen, 13);
      Assert.assertTrue(resultLen == sentenceTwoPaddedRightLen - 1);

      Assert.assertTrue(StringExpr.characterCount(sentenceTwoPaddedRight, 0, sentenceTwoPaddedRightLen - 5 - 1) == 10);
      resultLen = StringExpr.rightTrimAndTruncate(sentenceTwoPaddedRight, 0, sentenceTwoPaddedRightLen - 5 - 1, 4);
      Assert.assertTrue(resultLen == 8);

      // Multi-byte characters with blank ranges.
      sentenceBlankRanges = new byte[100];
      sentenceBlankRangesLen = addMultiByteCharSentenceBlankRanges(sentenceBlankRanges, 0);

      Assert.assertTrue(StringExpr.characterCount(sentenceBlankRanges, 0, sentenceBlankRangesLen) == 17);
      resultLen = StringExpr.rightTrimAndTruncate(sentenceBlankRanges, 0, sentenceBlankRangesLen, 4);
      Assert.assertTrue(resultLen == 7);

      Assert.assertTrue(StringExpr.characterCount(sentenceBlankRanges, 0, sentenceBlankRangesLen - 3) == 16);
      resultLen = StringExpr.rightTrimAndTruncate(sentenceBlankRanges, 0, sentenceBlankRangesLen - 3, 6);
      Assert.assertTrue(resultLen == 11);

      Assert.assertTrue(StringExpr.characterCount(sentenceBlankRanges, 4, 12) == 8);
      resultLen = StringExpr.rightTrimAndTruncate(sentenceBlankRanges, 4, 12, 6);
      Assert.assertTrue(resultLen == 7);
      Assert.assertTrue(StringExpr.characterCount(sentenceBlankRanges, 4, resultLen) == 5);

      Assert.assertTrue(StringExpr.characterCount(sentenceBlankRanges, 7, 17) == 13);
      resultLen = StringExpr.rightTrimAndTruncate(sentenceBlankRanges, 7, 17, 11);
      Assert.assertTrue(resultLen == 12);
      Assert.assertTrue(StringExpr.characterCount(sentenceBlankRanges, 7, resultLen) == 8);
  }

  @Test
  // Test basic right trim and truncate to vector.
  public void testRightTrimAndTruncateBytesColumnVector()  {
      BytesColumnVector outV = new BytesColumnVector(VectorizedRowBatch.DEFAULT_SIZE);
      outV.initBuffer(35); // initialize with estimated element size 35

      int i = 0;

      // This first section repeats the tests of testRightTrimWithOffset with a large maxLength parameter.
      // (i.e. too large to have an effect).
      int largeMaxLength = 100;

      int expectedResultLen;
      // Nothing to trim (ASCII).
      Assert.assertTrue(StringExpr.characterCount(blue, 0, blue.length) == 4);
      StringExpr.rightTrimAndTruncate(outV, i, blue, 0, blue.length, largeMaxLength);
      expectedResultLen = blue.length;
      Assert.assertTrue(vectorEqual(outV, i, blue, 0, expectedResultLen));
      i++;
      Assert.assertTrue(StringExpr.characterCount(redgreen, 0, redgreen.length) == 8);
      StringExpr.rightTrimAndTruncate(outV, i, redgreen, 0, redgreen.length, largeMaxLength);
      expectedResultLen =  redgreen.length;
      Assert.assertTrue(vectorEqual(outV, i, redgreen, 0, expectedResultLen));
      i++;
      Assert.assertTrue(StringExpr.characterCount(ascii_sentence, 0, ascii_sentence.length) == 31);
      StringExpr.rightTrimAndTruncate(outV, i, ascii_sentence, 0, ascii_sentence.length, largeMaxLength);
      expectedResultLen =  ascii_sentence.length;
      Assert.assertTrue(vectorEqual(outV, i, ascii_sentence, 0, expectedResultLen));
      i++;
      Assert.assertTrue(StringExpr.characterCount(blanksLeft, 0, blanksLeft.length) == 5);
      StringExpr.rightTrimAndTruncate(outV, i, blanksLeft, 0, blanksLeft.length, largeMaxLength);
      expectedResultLen =  blanksLeft.length;
      Assert.assertTrue(vectorEqual(outV, i, blanksLeft, 0, expectedResultLen));
      i++;

      // Simple trims.
      Assert.assertTrue(StringExpr.characterCount(blanksRight, 0, blanksRight.length) == 5);
      StringExpr.rightTrimAndTruncate(outV, i, blanksRight, 0, blanksRight.length, largeMaxLength);
      expectedResultLen = 3;
      Assert.assertTrue(vectorEqual(outV, i, blanksRight, 0, expectedResultLen));
      Assert.assertTrue(vectorCharacterCount(outV, i) == 3);
      i++;
      Assert.assertTrue(StringExpr.characterCount(blanksBoth, 0, blanksBoth.length) == 7);
      StringExpr.rightTrimAndTruncate(outV, i, blanksBoth, 0, blanksBoth.length, largeMaxLength);
      expectedResultLen = 5;
      Assert.assertTrue(vectorEqual(outV, i, blanksBoth, 0, expectedResultLen));
      Assert.assertTrue(vectorCharacterCount(outV, i) == 5);
      i++;
      Assert.assertTrue(StringExpr.characterCount(blankString, 0, blankString.length) == 2);
      StringExpr.rightTrimAndTruncate(outV, i, blankString, 0, blankString.length, largeMaxLength);
      expectedResultLen =  0;
      Assert.assertTrue(vectorEqual(outV, i, blankString, 0, expectedResultLen));
      Assert.assertTrue(vectorCharacterCount(outV, i) == 0);
      i++;
      Assert.assertTrue(StringExpr.characterCount(blankRanges, 0, blankRanges.length) == 30);
      StringExpr.rightTrimAndTruncate(outV, i, blankRanges, 0, blankRanges.length, largeMaxLength);
      expectedResultLen = blankRanges.length - 4;
      Assert.assertTrue(vectorEqual(outV, i, blankRanges, 0, expectedResultLen));
      Assert.assertTrue(vectorCharacterCount(outV, i) == 26);
      i++;
      // Offset trims.
      Assert.assertTrue(StringExpr.characterCount(blanksRight, 1, blanksRight.length - 1) == 4);
      StringExpr.rightTrimAndTruncate(outV, i, blanksRight, 1, blanksRight.length - 1, largeMaxLength);
      expectedResultLen = 2;
      Assert.assertTrue(vectorEqual(outV, i, blanksRight, 1, expectedResultLen));
      Assert.assertTrue(vectorCharacterCount(outV, i) == 2);
      i++;
      Assert.assertTrue(StringExpr.characterCount(blanksBoth, 4, blanksBoth.length - 4) == 3);
      StringExpr.rightTrimAndTruncate(outV, i, blanksBoth, 4, blanksBoth.length - 4, largeMaxLength);
      expectedResultLen = 1;
      Assert.assertTrue(vectorEqual(outV, i, blanksBoth, 4, expectedResultLen));
      Assert.assertTrue(vectorCharacterCount(outV, i) == 1);
      i++;
      Assert.assertTrue(StringExpr.characterCount(blanksBoth, 5, blanksBoth.length - 5) == 2);
      StringExpr.rightTrimAndTruncate(outV, i, blanksBoth, 5, blanksBoth.length -5, largeMaxLength);
      expectedResultLen = 0;
      Assert.assertTrue(vectorEqual(outV, i, blanksBoth, 5, expectedResultLen));
      Assert.assertTrue(vectorCharacterCount(outV, i) == 0);
      i++;
      Assert.assertTrue(StringExpr.characterCount(blankString, 1, blankString.length - 1) == 1);
      StringExpr.rightTrimAndTruncate(outV, i, blankString, 1, blankString.length - 1, largeMaxLength);
      expectedResultLen = 0;
      Assert.assertTrue(vectorEqual(outV, i, blankString, 1, expectedResultLen));
      Assert.assertTrue(vectorCharacterCount(outV, i) == 0);
      i++;
      Assert.assertTrue(StringExpr.characterCount(blankRanges, 4, blankRanges.length - 4) == 26);
      StringExpr.rightTrimAndTruncate(outV, i, blankRanges, 4, blankRanges.length - 4, largeMaxLength);
      expectedResultLen = blankRanges.length - 4 -4;
      Assert.assertTrue(vectorEqual(outV, i, blankRanges, 4, expectedResultLen));
      Assert.assertTrue(vectorCharacterCount(outV, i) == 22);
      i++;
      Assert.assertTrue(StringExpr.characterCount(blankRanges, 6, blankRanges.length - 6) == 24);
      StringExpr.rightTrimAndTruncate(outV, i, blankRanges, 6, blankRanges.length- 6, largeMaxLength);
      expectedResultLen = blankRanges.length - 6 - 4;
      Assert.assertTrue(vectorEqual(outV, i, blankRanges, 6, expectedResultLen));
      Assert.assertTrue(vectorCharacterCount(outV, i) == 20);
      i++;
      Assert.assertTrue(StringExpr.characterCount(blankRanges, 7, blankRanges.length - 7) == 23);
      StringExpr.rightTrimAndTruncate(outV, i, blankRanges, 7, blankRanges.length - 7, largeMaxLength);
      expectedResultLen = blankRanges.length - 7 - 4;
      Assert.assertTrue(vectorEqual(outV, i, blankRanges, 7, expectedResultLen));
      Assert.assertTrue(vectorCharacterCount(outV, i) == 19);
      i++;
      Assert.assertTrue(StringExpr.characterCount(blankRanges, 7, 8 - 7) == 1);
      StringExpr.rightTrimAndTruncate(outV, i, blankRanges, 7, 8 - 7, largeMaxLength);
      expectedResultLen = 0;
      Assert.assertTrue(vectorEqual(outV, i, blankRanges, 7, expectedResultLen));
      Assert.assertTrue(vectorCharacterCount(outV, i) == 0);
      i++;
      // Multi-byte trims.
      byte[] multiByte = new byte[100];

      addMultiByteCharRightPadded1_1(multiByte);
      Assert.assertTrue(StringExpr.characterCount(multiByte, 0, 4) == 2);
      StringExpr.rightTrimAndTruncate(outV, i, multiByte, 0, 4, largeMaxLength);
      expectedResultLen = 3;
      Assert.assertTrue(vectorEqual(outV, i, multiByte, 0, expectedResultLen));
      Assert.assertTrue(vectorCharacterCount(outV, i) == 1);
      i++;
      addMultiByteCharRightPadded1_2(multiByte);
      Assert.assertTrue(StringExpr.characterCount(multiByte, 0, 5) == 3);
      StringExpr.rightTrimAndTruncate(outV, i, multiByte, 0, 5, largeMaxLength);
      expectedResultLen = 4;
      Assert.assertTrue(vectorEqual(outV, i, multiByte, 0, expectedResultLen));
      Assert.assertTrue(vectorCharacterCount(outV, i) == 2);
      i++;
      addMultiByteCharRightPadded1_3(multiByte);
      Assert.assertTrue(StringExpr.characterCount(multiByte, 0, 9) == 4);
      StringExpr.rightTrimAndTruncate(outV, i, multiByte, 0, 9, largeMaxLength);
      expectedResultLen = 8;
      Assert.assertTrue(vectorEqual(outV, i, multiByte, 0, expectedResultLen));
      Assert.assertTrue(vectorCharacterCount(outV, i) == 3);
      i++;
      addMultiByteCharRightPadded1_1(multiByte);
      Assert.assertTrue(StringExpr.characterCount(multiByte, 3, 1) == 1);
      StringExpr.rightTrimAndTruncate(outV, i, multiByte, 3, 1, largeMaxLength);
      expectedResultLen = 0;
      Assert.assertTrue(vectorEqual(outV, i, multiByte, 0, expectedResultLen));
      Assert.assertTrue(vectorCharacterCount(outV, i) == 0);
      i++;
      addMultiByteCharRightPadded1_2(multiByte);
      Assert.assertTrue(StringExpr.characterCount(multiByte, 3, 2) == 2);
      StringExpr.rightTrimAndTruncate(outV, i, multiByte, 3, 2, largeMaxLength);
      expectedResultLen = 1;
      Assert.assertTrue(vectorEqual(outV, i, multiByte, 3, expectedResultLen));
      Assert.assertTrue(vectorCharacterCount(outV, i) == 1);
      i++;
      byte[] sentenceOne = new byte[100];
      int sentenceOneLen = addMultiByteCharSentenceOne(sentenceOne, 0);

      Assert.assertTrue(StringExpr.characterCount(sentenceOne, 0, sentenceOneLen) == 10);
      StringExpr.rightTrimAndTruncate(outV, i, sentenceOne, 0, sentenceOneLen, largeMaxLength);
      expectedResultLen = sentenceOneLen;
      Assert.assertTrue(vectorEqual(outV, i, sentenceOne, 0, expectedResultLen));
      i++;
      Assert.assertTrue(StringExpr.characterCount(sentenceOne, 0, sentenceOneLen - 3) == 9);
      StringExpr.rightTrimAndTruncate(outV, i, sentenceOne, 0, sentenceOneLen - 3, largeMaxLength);
      expectedResultLen = sentenceOneLen - 3;
      Assert.assertTrue(vectorEqual(outV, i, sentenceOne, 0, expectedResultLen));
      i++;

      byte[] sentenceTwo = new byte[100];
      int sentenceTwoLen = addMultiByteCharSentenceTwo(sentenceTwo, 0);

      Assert.assertTrue(StringExpr.characterCount(sentenceTwo, 0, sentenceTwoLen) == 13);
      StringExpr.rightTrimAndTruncate(outV, i, sentenceTwo, 0, sentenceTwoLen, largeMaxLength);
      expectedResultLen = sentenceTwoLen;
      Assert.assertTrue(vectorEqual(outV, i, sentenceTwo, 0, expectedResultLen));
      i++;
      Assert.assertTrue(StringExpr.characterCount(sentenceTwo, 0, sentenceTwoLen- 5) == 10);
      StringExpr.rightTrimAndTruncate(outV, i, sentenceTwo, 0, sentenceTwoLen - 5, largeMaxLength);
      expectedResultLen = sentenceTwoLen - 5;
      Assert.assertTrue(vectorEqual(outV, i, sentenceTwo, 0, expectedResultLen));
      i++;

      int start;

      // Left pad longer strings with multi-byte characters.
      byte[] sentenceOnePaddedLeft = new byte[100];
      start = addPads(sentenceOnePaddedLeft, 0, 3);
      int sentenceOnePaddedLeftLen = addMultiByteCharSentenceOne(sentenceOnePaddedLeft, start);

      Assert.assertTrue(StringExpr.characterCount(sentenceOnePaddedLeft, 0, sentenceOnePaddedLeftLen) == 3 + 10);
      StringExpr.rightTrimAndTruncate(outV, i, sentenceOnePaddedLeft, 0, sentenceOnePaddedLeftLen, largeMaxLength);
      expectedResultLen = sentenceOnePaddedLeftLen;
      Assert.assertTrue(vectorEqual(outV, i, sentenceOnePaddedLeft, 0, expectedResultLen));
      i++;
      Assert.assertTrue(StringExpr.characterCount(sentenceOnePaddedLeft, 0, sentenceOnePaddedLeftLen - 3) == 3 + 9);
      StringExpr.rightTrimAndTruncate(outV, i, sentenceOnePaddedLeft, 0, sentenceOnePaddedLeftLen - 3, largeMaxLength);
      expectedResultLen = sentenceOnePaddedLeftLen - 3;
      Assert.assertTrue(vectorEqual(outV, i, sentenceOnePaddedLeft, 0, expectedResultLen));
      i++;
      byte[] sentenceTwoPaddedLeft = new byte[100];
      start = addPads(sentenceTwoPaddedLeft, 0, 2);
      int sentenceTwoPaddedLeftLen = addMultiByteCharSentenceTwo(sentenceTwoPaddedLeft, start);
      i++;
      Assert.assertTrue(StringExpr.characterCount(sentenceTwoPaddedLeft, 0, sentenceTwoPaddedLeftLen) == 2 + 13);
      StringExpr.rightTrimAndTruncate(outV, i, sentenceTwoPaddedLeft, 0, sentenceTwoPaddedLeftLen, largeMaxLength);
      expectedResultLen = sentenceTwoPaddedLeftLen;
      Assert.assertTrue(vectorEqual(outV, i, sentenceTwoPaddedLeft, 0, expectedResultLen));
      i++;
      Assert.assertTrue(StringExpr.characterCount(sentenceTwoPaddedLeft, 0, sentenceTwoPaddedLeftLen - 5) == 2 + 10);
      StringExpr.rightTrimAndTruncate(outV, i, sentenceTwoPaddedLeft, 0, sentenceTwoPaddedLeftLen - 5, largeMaxLength);
      expectedResultLen = sentenceTwoPaddedLeftLen - 5;
      Assert.assertTrue(vectorEqual(outV, i, sentenceTwoPaddedLeft, 0, expectedResultLen));
      i++;

      // Right pad longer strings with multi-byte characters.
      byte[] sentenceOnePaddedRight = new byte[100];
      start = addMultiByteCharSentenceOne(sentenceOnePaddedRight, 0);
      int sentenceOnePaddedRightLen = addPads(sentenceOnePaddedRight, start, 4);

      Assert.assertTrue(StringExpr.characterCount(sentenceOnePaddedRight, 0, sentenceOnePaddedRightLen) == 10 + 4);
      StringExpr.rightTrimAndTruncate(outV, i, sentenceOnePaddedRight, 0, sentenceOnePaddedRightLen, largeMaxLength);
      expectedResultLen = sentenceOnePaddedRightLen - 4;
      Assert.assertTrue(vectorEqual(outV, i, sentenceOnePaddedRight, 0, expectedResultLen));
      i++;
      Assert.assertTrue(StringExpr.characterCount(sentenceOnePaddedRight, 0, sentenceOnePaddedRightLen - 3 - 4) == 9);
      StringExpr.rightTrimAndTruncate(outV, i, sentenceOnePaddedRight, 0, sentenceOnePaddedRightLen - 3 - 4, largeMaxLength);
      expectedResultLen = sentenceOnePaddedRightLen - 3 - 4;
      Assert.assertTrue(vectorEqual(outV, i, sentenceOnePaddedRight, 0, expectedResultLen));
      i++;
      byte[] sentenceTwoPaddedRight = new byte[100];
      start = addMultiByteCharSentenceTwo(sentenceTwoPaddedRight, 0);
      int sentenceTwoPaddedRightLen = addPads(sentenceTwoPaddedRight, start, 1);

      Assert.assertTrue(StringExpr.characterCount(sentenceTwoPaddedRight, 0, sentenceTwoPaddedRightLen) == 13 + 1);
      StringExpr.rightTrimAndTruncate(outV, i, sentenceTwoPaddedRight, 0, sentenceTwoPaddedRightLen, largeMaxLength);
      expectedResultLen = sentenceTwoPaddedRightLen - 1;
      Assert.assertTrue(vectorEqual(outV, i, sentenceTwoPaddedRight, 0, expectedResultLen));
      i++;
      Assert.assertTrue(StringExpr.characterCount(sentenceTwoPaddedRight, 0, sentenceTwoPaddedRightLen - 5 - 1) == 10);
      StringExpr.rightTrimAndTruncate(outV, i, sentenceTwoPaddedRight, 0, sentenceTwoPaddedRightLen - 5 - 1, largeMaxLength);
      expectedResultLen = sentenceTwoPaddedRightLen - 5 - 1;
      Assert.assertTrue(vectorEqual(outV, i, sentenceTwoPaddedRight, 0, expectedResultLen));
      i++;
      // Multi-byte characters with blank ranges.
      byte[] sentenceBlankRanges = new byte[100];
      int sentenceBlankRangesLen = addMultiByteCharSentenceBlankRanges(sentenceBlankRanges, 0);

      Assert.assertTrue(StringExpr.characterCount(sentenceBlankRanges, 0, sentenceBlankRangesLen) == 17);
      StringExpr.rightTrimAndTruncate(outV, i, sentenceBlankRanges, 0, sentenceBlankRangesLen, largeMaxLength);
      expectedResultLen = sentenceBlankRangesLen;
      Assert.assertTrue(vectorEqual(outV, i, sentenceBlankRanges, 0, expectedResultLen));
      i++;
      Assert.assertTrue(StringExpr.characterCount(sentenceBlankRanges, 0, sentenceBlankRangesLen - 3) == 16);
      StringExpr.rightTrimAndTruncate(outV, i, sentenceBlankRanges, 0, sentenceBlankRangesLen - 3, largeMaxLength);
      expectedResultLen = sentenceBlankRangesLen - 3;
      Assert.assertTrue(vectorEqual(outV, i, sentenceBlankRanges, 0, expectedResultLen));
      i++;
      Assert.assertTrue(StringExpr.characterCount(sentenceBlankRanges, 7, 17) == 13);
      StringExpr.rightTrimAndTruncate(outV, i, sentenceBlankRanges, 7, 17, largeMaxLength);
      expectedResultLen = 12;
      Assert.assertTrue(vectorEqual(outV, i, sentenceBlankRanges, 7, expectedResultLen));
      Assert.assertTrue(vectorCharacterCount(outV, i) == 8);
      i++;

      // This next section repeats the tests of testRightTrimWithOffset with a maxLength parameter that is
      // exactly the number of current characters in the string.  This shouldn't affect the trim.

      // Nothing to trim (ASCII).
      Assert.assertTrue(StringExpr.characterCount(blue, 0, blue.length) == 4);
      StringExpr.rightTrimAndTruncate(outV, i, blue, 0, blue.length, 4);
      expectedResultLen = blue.length;
      Assert.assertTrue(vectorEqual(outV, i, blue, 0, expectedResultLen));
      Assert.assertTrue(vectorCharacterCount(outV, i) == 4);
      i++;
      Assert.assertTrue(StringExpr.characterCount(redgreen, 0, redgreen.length) == 8);
      StringExpr.rightTrimAndTruncate(outV, i, redgreen, 0, redgreen.length, 8);
      expectedResultLen = redgreen.length;
      Assert.assertTrue(vectorEqual(outV, i, redgreen, 0, expectedResultLen));
      i++;
      Assert.assertTrue(StringExpr.characterCount(ascii_sentence, 0, ascii_sentence.length) == 31);
      StringExpr.rightTrimAndTruncate(outV, i, ascii_sentence, 0, ascii_sentence.length, 31);
      expectedResultLen = ascii_sentence.length;
      Assert.assertTrue(vectorEqual(outV, i, ascii_sentence, 0, expectedResultLen));
      i++;
      Assert.assertTrue(StringExpr.characterCount(blanksLeft, 0, blanksLeft.length) == 5);
      StringExpr.rightTrimAndTruncate(outV, i, blanksLeft, 0, blanksLeft.length, 5);
      expectedResultLen = blanksLeft.length;
      Assert.assertTrue(vectorEqual(outV, i, blanksLeft, 0, expectedResultLen));
      i++;

      // Simple trims.
      Assert.assertTrue(StringExpr.characterCount(blanksRight, 0, blanksRight.length) == 5);
      StringExpr.rightTrimAndTruncate(outV, i, blanksRight, 0, blanksRight.length, 5);
      expectedResultLen = 3;
      Assert.assertTrue(vectorEqual(outV, i, blanksRight, 0, expectedResultLen));
      Assert.assertTrue(vectorCharacterCount(outV, i) == 3);
      i++;
      Assert.assertTrue(StringExpr.characterCount(blanksBoth, 0, blanksBoth.length) == 7);
      StringExpr.rightTrimAndTruncate(outV, i, blanksBoth, 0, blanksBoth.length, 7);
      expectedResultLen = 5;
      Assert.assertTrue(vectorEqual(outV, i, blanksBoth, 0, expectedResultLen));
      Assert.assertTrue(vectorCharacterCount(outV, i) == 5);
      i++;
      Assert.assertTrue(StringExpr.characterCount(blankString, 0, blankString.length) == 2);
      StringExpr.rightTrimAndTruncate(outV, i, blankString, 0, blankString.length, 2);
      expectedResultLen =  0;
      Assert.assertTrue(vectorEqual(outV, i, blankString, 0, expectedResultLen));
      Assert.assertTrue(vectorCharacterCount(outV, i) == 0);
      i++;
      Assert.assertTrue(StringExpr.characterCount(blankRanges, 0, blankRanges.length) == 30);
      StringExpr.rightTrimAndTruncate(outV, i, blankRanges, 0, blankRanges.length, 30);
      expectedResultLen = blankRanges.length - 4;
      Assert.assertTrue(vectorEqual(outV, i, blankRanges, 0, expectedResultLen));
      Assert.assertTrue(vectorCharacterCount(outV, i) == 26);
      i++;

      // Offset trims.
      Assert.assertTrue(StringExpr.characterCount(blanksRight, 1, blanksRight.length - 1) == 4);
       StringExpr.rightTrimAndTruncate(outV, i, blanksRight, 1, blanksRight.length - 1, 4);
      expectedResultLen = 2;
      Assert.assertTrue(vectorEqual(outV, i, blanksRight, 1, expectedResultLen));
      Assert.assertTrue(vectorCharacterCount(outV, i) == 2);
      i++;
      Assert.assertTrue(StringExpr.characterCount(blanksBoth, 4, blanksBoth.length - 4) == 3);
      StringExpr.rightTrimAndTruncate(outV, i, blanksBoth, 4, blanksBoth.length - 4, 3);
      expectedResultLen = 1;
      Assert.assertTrue(vectorEqual(outV, i, blanksBoth, 4, expectedResultLen));
      Assert.assertTrue(vectorCharacterCount(outV, i) == 1);
      i++;
      Assert.assertTrue(StringExpr.characterCount(blanksBoth, 5, blanksBoth.length - 5) == 2);
      StringExpr.rightTrimAndTruncate(outV, i, blanksBoth, 5, blanksBoth.length -5, 2);
      expectedResultLen = 0;
      Assert.assertTrue(vectorEqual(outV, i, blanksBoth, 5, expectedResultLen));
      Assert.assertTrue(vectorCharacterCount(outV, i) == 0);
      i++;
      Assert.assertTrue(StringExpr.characterCount(blankString, 1, blankString.length - 1) == 1);
      StringExpr.rightTrimAndTruncate(outV, i, blankString, 1, blankString.length - 1, 1);
      expectedResultLen = 0;
      Assert.assertTrue(vectorEqual(outV, i, blankString, 1, expectedResultLen));
      Assert.assertTrue(vectorCharacterCount(outV, i) == 0);
      i++;
      Assert.assertTrue(StringExpr.characterCount(blankRanges, 4, blankRanges.length - 4) == 26);
      StringExpr.rightTrimAndTruncate(outV, i, blankRanges, 4, blankRanges.length - 4, 26);
      expectedResultLen = blankRanges.length - 4 -4;
      Assert.assertTrue(vectorEqual(outV, i, blankRanges, 4, expectedResultLen));
      Assert.assertTrue(vectorCharacterCount(outV, i) == 22);
      i++;
      Assert.assertTrue(StringExpr.characterCount(blankRanges, 6, blankRanges.length - 6) == 24);
      StringExpr.rightTrimAndTruncate(outV, i, blankRanges, 6, blankRanges.length- 6, 24);
      expectedResultLen = blankRanges.length - 6 - 4;
      Assert.assertTrue(vectorEqual(outV, i, blankRanges, 6, expectedResultLen));
      Assert.assertTrue(vectorCharacterCount(outV, i) == 20);
      i++;
      Assert.assertTrue(StringExpr.characterCount(blankRanges, 7, blankRanges.length - 7) == 23);
      StringExpr.rightTrimAndTruncate(outV, i, blankRanges, 7, blankRanges.length - 7, 23);
      expectedResultLen = blankRanges.length - 7 - 4;
      Assert.assertTrue(vectorEqual(outV, i, blankRanges, 7, expectedResultLen));
      Assert.assertTrue(vectorCharacterCount(outV, i) == 19);
      i++;
      Assert.assertTrue(StringExpr.characterCount(blankRanges, 7, 8 - 7) == 1);
      StringExpr.rightTrimAndTruncate(outV, i, blankRanges, 7, 8 - 7, 1);
      expectedResultLen = 0;
      Assert.assertTrue(vectorEqual(outV, i, blankRanges, 7, expectedResultLen));
      Assert.assertTrue(vectorCharacterCount(outV, i) == 0);
      i++;

      // Multi-byte trims.
      multiByte = new byte[100];

      addMultiByteCharRightPadded1_1(multiByte);
      Assert.assertTrue(StringExpr.characterCount(multiByte, 0, 4) == 2);
      StringExpr.rightTrimAndTruncate(outV, i, multiByte, 0, 4, 2);
      expectedResultLen = 3;
      Assert.assertTrue(vectorEqual(outV, i, multiByte, 0, expectedResultLen));
      Assert.assertTrue(vectorCharacterCount(outV, i) == 1);
      i++;
      addMultiByteCharRightPadded1_2(multiByte);
      Assert.assertTrue(StringExpr.characterCount(multiByte, 0, 5) == 3);
      StringExpr.rightTrimAndTruncate(outV, i, multiByte, 0, 5, 3);
      expectedResultLen = 4;
      Assert.assertTrue(vectorEqual(outV, i, multiByte, 0, expectedResultLen));
      Assert.assertTrue(vectorCharacterCount(outV, i) == 2);
      i++;
      addMultiByteCharRightPadded1_3(multiByte);
      Assert.assertTrue(StringExpr.characterCount(multiByte, 0, 9) == 4);
      StringExpr.rightTrimAndTruncate(outV, i, multiByte, 0, 9, 4);
      expectedResultLen = 8;
      Assert.assertTrue(vectorEqual(outV, i, multiByte, 0, expectedResultLen));
      Assert.assertTrue(vectorCharacterCount(outV, i) == 3);
      i++;
      addMultiByteCharRightPadded1_1(multiByte);
      Assert.assertTrue(StringExpr.characterCount(multiByte, 3, 1) == 1);
      StringExpr.rightTrimAndTruncate(outV, i, multiByte, 3, 1, 1);
      expectedResultLen = 0;
      Assert.assertTrue(vectorEqual(outV, i, multiByte, 3, expectedResultLen));
      Assert.assertTrue(vectorCharacterCount(outV, i) == 0);
      i++;
      addMultiByteCharRightPadded1_2(multiByte);
      Assert.assertTrue(StringExpr.characterCount(multiByte, 3, 2) == 2);
      StringExpr.rightTrimAndTruncate(outV, i, multiByte, 3, 2, 2);
      expectedResultLen = 1;
      Assert.assertTrue(vectorEqual(outV, i, multiByte, 3, expectedResultLen));
      Assert.assertTrue(vectorCharacterCount(outV, i) == 1);
      i++;

      sentenceOne = new byte[100];
      sentenceOneLen = addMultiByteCharSentenceOne(sentenceOne, 0);

      Assert.assertTrue(StringExpr.characterCount(sentenceOne, 0, sentenceOneLen) == 10);
      StringExpr.rightTrimAndTruncate(outV, i, sentenceOne, 0, sentenceOneLen, 10);
      expectedResultLen = sentenceOneLen;
      Assert.assertTrue(vectorEqual(outV, i, sentenceOne, 0, expectedResultLen));
      i++;
      Assert.assertTrue(StringExpr.characterCount(sentenceOne, 0, sentenceOneLen - 3) == 9);
      StringExpr.rightTrimAndTruncate(outV, i, sentenceOne, 0, sentenceOneLen - 3, 9);
      expectedResultLen = sentenceOneLen - 3;
      Assert.assertTrue(vectorEqual(outV, i, sentenceOne, 0, expectedResultLen));
      i++;

      sentenceTwo = new byte[100];
      sentenceTwoLen = addMultiByteCharSentenceTwo(sentenceTwo, 0);

      Assert.assertTrue(StringExpr.characterCount(sentenceTwo, 0, sentenceTwoLen) == 13);
      StringExpr.rightTrimAndTruncate(outV, i, sentenceTwo, 0, sentenceTwoLen, 13);
      expectedResultLen = sentenceTwoLen;
      Assert.assertTrue(vectorEqual(outV, i, sentenceTwo, 0, expectedResultLen));
      i++;
      Assert.assertTrue(StringExpr.characterCount(sentenceTwo, 0, sentenceTwoLen- 5) == 10);
      StringExpr.rightTrimAndTruncate(outV, i, sentenceTwo, 0, sentenceTwoLen - 5, 10);
      expectedResultLen = sentenceTwoLen - 5;
      Assert.assertTrue(vectorEqual(outV, i, sentenceTwo, 0, expectedResultLen));
      i++;

      // Left pad longer strings with multi-byte characters.
      sentenceOnePaddedLeft = new byte[100];
      start = addPads(sentenceOnePaddedLeft, 0, 3);
      sentenceOnePaddedLeftLen = addMultiByteCharSentenceOne(sentenceOnePaddedLeft, start);

      Assert.assertTrue(StringExpr.characterCount(sentenceOnePaddedLeft, 0, sentenceOnePaddedLeftLen) == 3 + 10);
      StringExpr.rightTrimAndTruncate(outV, i, sentenceOnePaddedLeft, 0, sentenceOnePaddedLeftLen, 3 + 10);
      expectedResultLen = sentenceOnePaddedLeftLen;
      Assert.assertTrue(vectorEqual(outV, i, sentenceOnePaddedLeft, 0, expectedResultLen));
      i++;
      Assert.assertTrue(StringExpr.characterCount(sentenceOnePaddedLeft, 0, sentenceOnePaddedLeftLen - 3) == 3 + 9);
      StringExpr.rightTrimAndTruncate(outV, i, sentenceOnePaddedLeft, 0, sentenceOnePaddedLeftLen - 3, 3 + 9);
      expectedResultLen = sentenceOnePaddedLeftLen - 3;
      Assert.assertTrue(vectorEqual(outV, i, sentenceOnePaddedLeft, 0, expectedResultLen));
      i++;
      sentenceTwoPaddedLeft = new byte[100];
      start = addPads(sentenceTwoPaddedLeft, 0, 2);
      sentenceTwoPaddedLeftLen = addMultiByteCharSentenceTwo(sentenceTwoPaddedLeft, start);
      i++;
      Assert.assertTrue(StringExpr.characterCount(sentenceTwoPaddedLeft, 0, sentenceTwoPaddedLeftLen) == 2 + 13);
      StringExpr.rightTrimAndTruncate(outV, i, sentenceTwoPaddedLeft, 0, sentenceTwoPaddedLeftLen, 2 + 13);
      expectedResultLen = sentenceTwoPaddedLeftLen;
      Assert.assertTrue(vectorEqual(outV, i, sentenceTwoPaddedLeft, 0, expectedResultLen));
      i++;
      Assert.assertTrue(StringExpr.characterCount(sentenceTwoPaddedLeft, 0, sentenceTwoPaddedLeftLen - 5) == 2 + 10);
      StringExpr.rightTrimAndTruncate(outV, i, sentenceTwoPaddedLeft, 0, sentenceTwoPaddedLeftLen - 5, 2 + 10);
      expectedResultLen = sentenceTwoPaddedLeftLen - 5;
      Assert.assertTrue(vectorEqual(outV, i, sentenceTwoPaddedLeft, 0, expectedResultLen));
      i++;

      // Right pad longer strings with multi-byte characters.
      sentenceOnePaddedRight = new byte[100];
      start = addMultiByteCharSentenceOne(sentenceOnePaddedRight, 0);
      sentenceOnePaddedRightLen = addPads(sentenceOnePaddedRight, start, 4);

      Assert.assertTrue(StringExpr.characterCount(sentenceOnePaddedRight, 0, sentenceOnePaddedRightLen) == 10 + 4);
      StringExpr.rightTrimAndTruncate(outV, i, sentenceOnePaddedRight, 0, sentenceOnePaddedRightLen, 10 + 4);
      expectedResultLen = sentenceOnePaddedRightLen - 4;
      Assert.assertTrue(vectorEqual(outV, i, sentenceOnePaddedRight, 0, expectedResultLen));
      i++;
      Assert.assertTrue(StringExpr.characterCount(sentenceOnePaddedRight, 0, sentenceOnePaddedRightLen - 3 - 4) == 9);
      StringExpr.rightTrimAndTruncate(outV, i, sentenceOnePaddedRight, 0, sentenceOnePaddedRightLen - 3 - 4, 9);
      expectedResultLen = sentenceOnePaddedRightLen - 3 - 4;
      Assert.assertTrue(vectorEqual(outV, i, sentenceOnePaddedRight, 0, expectedResultLen));
      i++;

      sentenceTwoPaddedRight = new byte[100];
      start = addMultiByteCharSentenceTwo(sentenceTwoPaddedRight, 0);
      sentenceTwoPaddedRightLen = addPads(sentenceTwoPaddedRight, start, 1);

      Assert.assertTrue(StringExpr.characterCount(sentenceTwoPaddedRight, 0, sentenceTwoPaddedRightLen) == 13 + 1);
      StringExpr.rightTrimAndTruncate(outV, i, sentenceTwoPaddedRight, 0, sentenceTwoPaddedRightLen, 13 + 1);
      expectedResultLen = sentenceTwoPaddedRightLen - 1;
      Assert.assertTrue(vectorEqual(outV, i, sentenceTwoPaddedRight, 0, expectedResultLen));
      i++;
      Assert.assertTrue(StringExpr.characterCount(sentenceTwoPaddedRight, 0, sentenceTwoPaddedRightLen - 5 - 1) == 10);
      StringExpr.rightTrimAndTruncate(outV, i, sentenceTwoPaddedRight, 0, sentenceTwoPaddedRightLen - 5 - 1, 10);
      expectedResultLen = sentenceTwoPaddedRightLen - 5 - 1;
      Assert.assertTrue(vectorEqual(outV, i, sentenceTwoPaddedRight, 0, expectedResultLen));
      i++;

      // Multi-byte characters with blank ranges.
      sentenceBlankRanges = new byte[100];
      sentenceBlankRangesLen = addMultiByteCharSentenceBlankRanges(sentenceBlankRanges, 0);

      Assert.assertTrue(StringExpr.characterCount(sentenceBlankRanges, 0, sentenceBlankRangesLen) == 17);
      StringExpr.rightTrimAndTruncate(outV, i, sentenceBlankRanges, 0, sentenceBlankRangesLen, 17);
      expectedResultLen = sentenceBlankRangesLen;
      Assert.assertTrue(vectorEqual(outV, i, sentenceBlankRanges, 0, expectedResultLen));
      i++;
      Assert.assertTrue(StringExpr.characterCount(sentenceBlankRanges, 0, sentenceBlankRangesLen - 3) == 16);
      StringExpr.rightTrimAndTruncate(outV, i, sentenceBlankRanges, 0, sentenceBlankRangesLen - 3, 16);
      expectedResultLen = sentenceBlankRangesLen - 3;
      Assert.assertTrue(vectorEqual(outV, i, sentenceBlankRanges, 0, expectedResultLen));
      i++;
      Assert.assertTrue(StringExpr.characterCount(sentenceBlankRanges, 7, 17) == 13);
      StringExpr.rightTrimAndTruncate(outV, i, sentenceBlankRanges, 7, 17, largeMaxLength);
      expectedResultLen = 12;
      Assert.assertTrue(vectorEqual(outV, i, sentenceBlankRanges, 7, expectedResultLen));
      Assert.assertTrue(vectorCharacterCount(outV, i) == 8);
      i++;

      // This next section repeats the tests of testRightTrimWithOffset with a maxLength parameter that is
      // less than the number of current characters in the string and thus affects the trim.

      // Nothing to trim (ASCII).
      Assert.assertTrue(StringExpr.characterCount(blue, 0, blue.length) == 4);
      StringExpr.rightTrimAndTruncate(outV, i, blue, 0, blue.length, 3);
      expectedResultLen = 3;
      Assert.assertTrue(vectorEqual(outV, i, blue, 0, expectedResultLen));
      Assert.assertTrue(vectorCharacterCount(outV, i) == 3);
      i++;
      Assert.assertTrue(StringExpr.characterCount(redgreen, 0, redgreen.length) == 8);
      StringExpr.rightTrimAndTruncate(outV, i, redgreen, 0, redgreen.length, 6);
      expectedResultLen = 6;
      Assert.assertTrue(vectorEqual(outV, i, redgreen, 0, expectedResultLen));
      i++;
      Assert.assertTrue(StringExpr.characterCount(ascii_sentence, 0, ascii_sentence.length) == 31);
      StringExpr.rightTrimAndTruncate(outV, i, ascii_sentence, 0, ascii_sentence.length, 30);
      expectedResultLen = 30;
      Assert.assertTrue(vectorEqual(outV, i, ascii_sentence, 0, expectedResultLen));
      i++;
      Assert.assertTrue(StringExpr.characterCount(blanksLeft, 0, blanksLeft.length) == 5);
      StringExpr.rightTrimAndTruncate(outV, i, blanksLeft, 0, blanksLeft.length, 1);
      expectedResultLen = 0;
      Assert.assertTrue(vectorEqual(outV, i, blanksLeft, 0, expectedResultLen));
      i++;

      // Simple trims.
      Assert.assertTrue(StringExpr.characterCount(blanksRight, 0, blanksRight.length) == 5);
      StringExpr.rightTrimAndTruncate(outV, i, blanksRight, 0, blanksRight.length, 4);
      expectedResultLen = 3;
      Assert.assertTrue(vectorEqual(outV, i, blanksRight, 0, expectedResultLen));
      Assert.assertTrue(vectorCharacterCount(outV, i) == 3);
      i++;
      Assert.assertTrue(StringExpr.characterCount(blanksBoth, 0, blanksBoth.length) == 7);
      StringExpr.rightTrimAndTruncate(outV, i, blanksBoth, 0, blanksBoth.length, 6);
      expectedResultLen = 5;
      Assert.assertTrue(vectorEqual(outV, i, blanksBoth, 0, expectedResultLen));
      Assert.assertTrue(vectorCharacterCount(outV, i) == 5);
      i++;
      Assert.assertTrue(StringExpr.characterCount(blankString, 0, blankString.length) == 2);
      StringExpr.rightTrimAndTruncate(outV, i, blankString, 0, blankString.length, 1);
      expectedResultLen = 0;
      Assert.assertTrue(vectorEqual(outV, i, blankString, 0, expectedResultLen));
      Assert.assertTrue(vectorCharacterCount(outV, i) == 0);
      i++;
      Assert.assertTrue(StringExpr.characterCount(blankRanges, 0, blankRanges.length) == 30);
      StringExpr.rightTrimAndTruncate(outV, i, blankRanges, 0, blankRanges.length, 19);
      expectedResultLen = 15;
      Assert.assertTrue(vectorEqual(outV, i, blankRanges, 0, expectedResultLen));
      Assert.assertTrue(vectorCharacterCount(outV, i) == 15);
      i++;

      // Offset trims.
      Assert.assertTrue(StringExpr.characterCount(blanksRight, 1, blanksRight.length - 1) == 4);
      StringExpr.rightTrimAndTruncate(outV, i, blanksRight, 1, blanksRight.length - 1, 3);
      expectedResultLen = 2;
      Assert.assertTrue(vectorEqual(outV, i, blanksRight, 1, expectedResultLen));
      Assert.assertTrue(vectorCharacterCount(outV, i) == 2);
      i++;
      Assert.assertTrue(StringExpr.characterCount(blanksBoth, 4, blanksBoth.length - 4) == 3);
      StringExpr.rightTrimAndTruncate(outV, i, blanksBoth, 4, blanksBoth.length - 4, 2);
      expectedResultLen = 1;
      Assert.assertTrue(vectorEqual(outV, i, blanksBoth, 4, expectedResultLen));
      Assert.assertTrue(vectorCharacterCount(outV, i) == 1);
      i++;
      Assert.assertTrue(StringExpr.characterCount(blanksBoth, 5, blanksBoth.length - 5) == 2);
      StringExpr.rightTrimAndTruncate(outV, i, blanksBoth, 5, blanksBoth.length -5, 1);
      expectedResultLen = 0;
      Assert.assertTrue(vectorEqual(outV, i, blanksBoth, 5, expectedResultLen));
      Assert.assertTrue(vectorCharacterCount(outV, i) == 0);
      i++;
      Assert.assertTrue(StringExpr.characterCount(blankString, 1, blankString.length - 1) == 1);
      StringExpr.rightTrimAndTruncate(outV, i, blankString, 1, blankString.length - 1, 1);
      expectedResultLen = 0;
      Assert.assertTrue(vectorEqual(outV, i, blankString, 1, expectedResultLen));
      Assert.assertTrue(vectorCharacterCount(outV, i) == 0);
      i++;
      Assert.assertTrue(StringExpr.characterCount(blankRanges, 3, 6) == 6);
      StringExpr.rightTrimAndTruncate(outV, i, blankRanges, 3, 6, 5);
      expectedResultLen = 4;
      Assert.assertTrue(vectorEqual(outV, i, blankRanges, 3, expectedResultLen));
      Assert.assertTrue(vectorCharacterCount(outV, i) == 4);
      i++;
      Assert.assertTrue(StringExpr.characterCount(blankRanges, 6, blankRanges.length - 6) == 24);
      StringExpr.rightTrimAndTruncate(outV, i, blankRanges, 6, blankRanges.length- 6, 22);
      expectedResultLen = blankRanges.length - 6 - 4;
      Assert.assertTrue(vectorEqual(outV, i, blankRanges, 6, expectedResultLen));
      Assert.assertTrue(vectorCharacterCount(outV, i) == 20);
      i++;
      Assert.assertTrue(StringExpr.characterCount(blankRanges, 7, blankRanges.length - 7) == 23);
      StringExpr.rightTrimAndTruncate(outV, i, blankRanges, 7, blankRanges.length - 7, 10);
      expectedResultLen = 8;
      Assert.assertTrue(vectorEqual(outV, i, blankRanges, 7, expectedResultLen));
      Assert.assertTrue(vectorCharacterCount(outV, i) == 8);
      i++;
      Assert.assertTrue(StringExpr.characterCount(blankRanges, 7, 8 - 7) == 1);
      StringExpr.rightTrimAndTruncate(outV, i, blankRanges, 7, 8 - 7, 1);
      expectedResultLen = 0;
      Assert.assertTrue(vectorEqual(outV, i, blankRanges, 7, expectedResultLen));
      Assert.assertTrue(vectorCharacterCount(outV, i) == 0);
      i++;

      // Multi-byte trims.
      multiByte = new byte[100];

      addMultiByteCharRightPadded1_1(multiByte);
      Assert.assertTrue(StringExpr.characterCount(multiByte, 0, 4) == 2);
      StringExpr.rightTrimAndTruncate(outV, i, multiByte, 0, 4, 1);
      expectedResultLen = 3;
      Assert.assertTrue(vectorEqual(outV, i, multiByte, 0, expectedResultLen));
      Assert.assertTrue(vectorCharacterCount(outV, i) == 1);
      i++;
      addMultiByteCharRightPadded1_2(multiByte);
      Assert.assertTrue(StringExpr.characterCount(multiByte, 0, 5) == 3);
      StringExpr.rightTrimAndTruncate(outV, i, multiByte, 0, 5, 2);
      expectedResultLen = 4;
      Assert.assertTrue(vectorEqual(outV, i, multiByte, 0, expectedResultLen));
      Assert.assertTrue(vectorCharacterCount(outV, i) == 2);
      i++;
      addMultiByteCharRightPadded1_3(multiByte);
      Assert.assertTrue(StringExpr.characterCount(multiByte, 0, 9) == 4);
      StringExpr.rightTrimAndTruncate(outV, i, multiByte, 0, 9, 3);
      expectedResultLen = 8;
      Assert.assertTrue(vectorEqual(outV, i, multiByte, 0, expectedResultLen));
      Assert.assertTrue(vectorCharacterCount(outV, i) == 3);
      i++;
      addMultiByteCharRightPadded1_1(multiByte);
      Assert.assertTrue(StringExpr.characterCount(multiByte, 3, 1) == 1);
      StringExpr.rightTrimAndTruncate(outV, i, multiByte, 3, 1, 1);
      expectedResultLen = 0;
      Assert.assertTrue(vectorEqual(outV, i, multiByte, 3, expectedResultLen));
      Assert.assertTrue(vectorCharacterCount(outV, i) == 0);
      i++;
      addMultiByteCharRightPadded1_2(multiByte);
      Assert.assertTrue(StringExpr.characterCount(multiByte, 3, 2) == 2);
      StringExpr.rightTrimAndTruncate(outV, i, multiByte, 3, 2, 1);
      expectedResultLen = 1;
      Assert.assertTrue(vectorEqual(outV, i, multiByte, 3, expectedResultLen));
      Assert.assertTrue(vectorCharacterCount(outV, i) == 1);
      i++;

      sentenceOne = new byte[100];
      sentenceOneLen = addMultiByteCharSentenceOne(sentenceOne, 0);

      Assert.assertTrue(StringExpr.characterCount(sentenceOne, 0, sentenceOneLen) == 10);
      StringExpr.rightTrimAndTruncate(outV, i, sentenceOne, 0, sentenceOneLen, 7);
      expectedResultLen = sentenceOneLen - 9;
      Assert.assertTrue(vectorEqual(outV, i, sentenceOne, 0, expectedResultLen));
      i++;
      Assert.assertTrue(StringExpr.characterCount(sentenceOne, 0, sentenceOneLen - 3) == 9);
      StringExpr.rightTrimAndTruncate(outV, i, sentenceOne, 0, sentenceOneLen - 3, 6);
      expectedResultLen = 13;
      Assert.assertTrue(vectorEqual(outV, i, sentenceOne, 0, expectedResultLen));
      i++;

      sentenceTwo = new byte[100];
      sentenceTwoLen = addMultiByteCharSentenceTwo(sentenceTwo, 0);

      Assert.assertTrue(StringExpr.characterCount(sentenceTwo, 0, sentenceTwoLen) == 13);
      StringExpr.rightTrimAndTruncate(outV, i, sentenceTwo, 0, sentenceTwoLen, 13);
      expectedResultLen = sentenceTwoLen;
      Assert.assertTrue(vectorEqual(outV, i, sentenceTwo, 0, expectedResultLen));
      i++;
      Assert.assertTrue(StringExpr.characterCount(sentenceTwo, 0, sentenceTwoLen- 5) == 10);
      StringExpr.rightTrimAndTruncate(outV, i, sentenceTwo, 0, sentenceTwoLen - 5, 10);
      expectedResultLen = sentenceTwoLen - 5;
      Assert.assertTrue(vectorEqual(outV, i, sentenceTwo, 0, expectedResultLen));
      i++;

      // Left pad longer strings with multi-byte characters.
      sentenceOnePaddedLeft = new byte[100];
      start = addPads(sentenceOnePaddedLeft, 0, 3);
      sentenceOnePaddedLeftLen = addMultiByteCharSentenceOne(sentenceOnePaddedLeft, start);

      Assert.assertTrue(StringExpr.characterCount(sentenceOnePaddedLeft, 0, sentenceOnePaddedLeftLen) == 3 + 10);
      StringExpr.rightTrimAndTruncate(outV, i, sentenceOnePaddedLeft, 0, sentenceOnePaddedLeftLen, 3 + 8);
      expectedResultLen = sentenceOnePaddedLeftLen - 6;
      Assert.assertTrue(vectorEqual(outV, i, sentenceOnePaddedLeft, 0, expectedResultLen));
      i++;
      Assert.assertTrue(StringExpr.characterCount(sentenceOnePaddedLeft, 0, sentenceOnePaddedLeftLen - 3) == 3 + 9);
      StringExpr.rightTrimAndTruncate(outV, i, sentenceOnePaddedLeft, 0, sentenceOnePaddedLeftLen - 3, 3 + 6);
      expectedResultLen = 16;
      Assert.assertTrue(vectorEqual(outV, i, sentenceOnePaddedLeft, 0, expectedResultLen));
      i++;

      sentenceTwoPaddedLeft = new byte[100];
      start = addPads(sentenceTwoPaddedLeft, 0, 2);
      sentenceTwoPaddedLeftLen = addMultiByteCharSentenceTwo(sentenceTwoPaddedLeft, start);

      Assert.assertTrue(StringExpr.characterCount(sentenceTwoPaddedLeft, 0, sentenceTwoPaddedLeftLen) == 2 + 13);
      StringExpr.rightTrimAndTruncate(outV, i, sentenceTwoPaddedLeft, 0, sentenceTwoPaddedLeftLen, 7);
      expectedResultLen = 10;
      Assert.assertTrue(vectorEqual(outV, i, sentenceTwoPaddedLeft, 0, expectedResultLen));
      i++;
      Assert.assertTrue(StringExpr.characterCount(sentenceTwoPaddedLeft, 0, sentenceTwoPaddedLeftLen - 5) == 2 + 10);
      StringExpr.rightTrimAndTruncate(outV, i, sentenceTwoPaddedLeft, 0, sentenceTwoPaddedLeftLen - 5, 6);
      expectedResultLen = 10;
      Assert.assertTrue(vectorEqual(outV, i, sentenceTwoPaddedLeft, 0, expectedResultLen));
      i++;

      // Right pad longer strings with multi-byte characters.
      sentenceOnePaddedRight = new byte[100];
      start = addMultiByteCharSentenceOne(sentenceOnePaddedRight, 0);
      sentenceOnePaddedRightLen = addPads(sentenceOnePaddedRight, start, 4);

      Assert.assertTrue(StringExpr.characterCount(sentenceOnePaddedRight, 0, sentenceOnePaddedRightLen) == 10 + 4);
      StringExpr.rightTrimAndTruncate(outV, i, sentenceOnePaddedRight, 0, sentenceOnePaddedRightLen, 10);
      expectedResultLen = sentenceOnePaddedRightLen - 4;
      Assert.assertTrue(vectorEqual(outV, i, sentenceOnePaddedRight, 0, expectedResultLen));
      i++;
      Assert.assertTrue(StringExpr.characterCount(sentenceOnePaddedRight, 0, sentenceOnePaddedRightLen - 3 - 4) == 9);
      StringExpr.rightTrimAndTruncate(outV, i, sentenceOnePaddedRight, 0, sentenceOnePaddedRightLen - 3 - 4, 7);
      expectedResultLen = 17;
      Assert.assertTrue(vectorEqual(outV, i, sentenceOnePaddedRight, 0, expectedResultLen));
      i++;

      sentenceTwoPaddedRight = new byte[100];
      start = addMultiByteCharSentenceTwo(sentenceTwoPaddedRight, 0);
      sentenceTwoPaddedRightLen = addPads(sentenceTwoPaddedRight, start, 1);

      Assert.assertTrue(StringExpr.characterCount(sentenceTwoPaddedRight, 0, sentenceTwoPaddedRightLen) == 13 + 1);
      StringExpr.rightTrimAndTruncate(outV, i, sentenceTwoPaddedRight, 0, sentenceTwoPaddedRightLen, 13);
      expectedResultLen = sentenceTwoPaddedRightLen - 1;
      Assert.assertTrue(vectorEqual(outV, i, sentenceTwoPaddedRight, 0, expectedResultLen));
      i++;
      Assert.assertTrue(StringExpr.characterCount(sentenceTwoPaddedRight, 0, sentenceTwoPaddedRightLen - 5 - 1) == 10);
      StringExpr.rightTrimAndTruncate(outV, i, sentenceTwoPaddedRight, 0, sentenceTwoPaddedRightLen - 5 - 1, 4);
      expectedResultLen = 8;
      Assert.assertTrue(vectorEqual(outV, i, sentenceTwoPaddedRight, 0, expectedResultLen));
      i++;

      // Multi-byte characters with blank ranges.
      sentenceBlankRanges = new byte[100];
      sentenceBlankRangesLen = addMultiByteCharSentenceBlankRanges(sentenceBlankRanges, 0);

      Assert.assertTrue(StringExpr.characterCount(sentenceBlankRanges, 0, sentenceBlankRangesLen) == 17);
      StringExpr.rightTrimAndTruncate(outV, i, sentenceBlankRanges, 0, sentenceBlankRangesLen, 4);
      expectedResultLen = 7;
      Assert.assertTrue(vectorEqual(outV, i, sentenceBlankRanges, 0, expectedResultLen));
      i++;
      Assert.assertTrue(StringExpr.characterCount(sentenceBlankRanges, 0, sentenceBlankRangesLen - 3) == 16);
      StringExpr.rightTrimAndTruncate(outV, i, sentenceBlankRanges, 0, sentenceBlankRangesLen - 3, 6);
      expectedResultLen = 11;
      Assert.assertTrue(vectorEqual(outV, i, sentenceBlankRanges, 0, expectedResultLen));
      i++;
      Assert.assertTrue(StringExpr.characterCount(sentenceBlankRanges, 4, 12) == 8);
      StringExpr.rightTrimAndTruncate(outV, i, sentenceBlankRanges, 4, 12, 6);
      expectedResultLen = 7;
      Assert.assertTrue(vectorEqual(outV, i, sentenceBlankRanges, 4, expectedResultLen));
      Assert.assertTrue(vectorCharacterCount(outV, i) == 5);
      i++;
      Assert.assertTrue(StringExpr.characterCount(sentenceBlankRanges, 7, 17) == 13);
      StringExpr.rightTrimAndTruncate(outV, i, sentenceBlankRanges, 7, 17, 11);
      expectedResultLen = 12;
      Assert.assertTrue(vectorEqual(outV, i, sentenceBlankRanges, 7, expectedResultLen));
      Assert.assertTrue(vectorCharacterCount(outV, i) == 8);
      i++;
  }

  @Test
  // Test basic truncate to vector.
  public void testRightTrimAndTruncateScalar()  {
      int largeMaxLength = 100;

      byte[] result;

      // No truncate (ASCII) -- maximum length large.
      Assert.assertTrue(StringExpr.characterCount(blue) == 4);
      result = StringExpr.rightTrimAndTruncateScalar(blue, largeMaxLength);
      Assert.assertTrue(Arrays.equals(blue, result));

      Assert.assertTrue(StringExpr.characterCount(redgreen) == 8);
      result = StringExpr.rightTrimAndTruncateScalar(redgreen, largeMaxLength);
      Assert.assertTrue(Arrays.equals(redgreen, result));

      Assert.assertTrue(StringExpr.characterCount(ascii_sentence) == 31);
      result = StringExpr.rightTrimAndTruncateScalar(ascii_sentence, largeMaxLength);
      Assert.assertTrue(Arrays.equals(ascii_sentence, result));

      Assert.assertTrue(StringExpr.characterCount(blanksLeft) == 5);
      result = StringExpr.rightTrimAndTruncateScalar(blanksLeft, largeMaxLength);
      Assert.assertTrue(Arrays.equals(blanksLeft, result));

      // No truncate (ASCII) -- same maximum length.
      Assert.assertTrue(StringExpr.characterCount(blue) == 4);
      result = StringExpr.rightTrimAndTruncateScalar(blue, blue.length);
      Assert.assertTrue(Arrays.equals(blue, result));

      Assert.assertTrue(StringExpr.characterCount(redgreen) == 8);
      result = StringExpr.rightTrimAndTruncateScalar(redgreen, redgreen.length);
      Assert.assertTrue(Arrays.equals(redgreen, result));

      Assert.assertTrue(StringExpr.characterCount(ascii_sentence) == 31);
      result = StringExpr.rightTrimAndTruncateScalar(ascii_sentence, ascii_sentence.length);
      Assert.assertTrue(Arrays.equals(ascii_sentence, result));

      Assert.assertTrue(StringExpr.characterCount(blanksLeft) == 5);
      result = StringExpr.rightTrimAndTruncateScalar(blanksLeft, blanksLeft.length);
      Assert.assertTrue(Arrays.equals(blanksLeft, result));

      // Simple truncation.
      result = StringExpr.rightTrimAndTruncateScalar(blue, 3);
      Assert.assertTrue(Arrays.equals(Arrays.copyOf(blue, 3), result));

      result = StringExpr.rightTrimAndTruncateScalar(redgreen, 6);
      Assert.assertTrue(Arrays.equals(Arrays.copyOf(redgreen, 6), result));

      result = StringExpr.rightTrimAndTruncateScalar(ascii_sentence, 14);
      Assert.assertTrue(Arrays.equals(Arrays.copyOf(ascii_sentence, 14), result));

      result = StringExpr.rightTrimAndTruncateScalar(blanksLeft, 2);
      Assert.assertTrue(Arrays.equals(Arrays.copyOf(blanksLeft, 0), result));

      result = StringExpr.rightTrimAndTruncateScalar(blanksRight, 4);
      Assert.assertTrue(Arrays.equals(Arrays.copyOf(blanksRight, 3), result));

      result = StringExpr.rightTrimAndTruncateScalar(blanksBoth, 2);
      Assert.assertTrue(Arrays.equals(Arrays.copyOf(blanksBoth, 0), result));

      result = StringExpr.rightTrimAndTruncateScalar(blankString, 1);
      Assert.assertTrue(Arrays.equals(Arrays.copyOf(blankString, 0), result));

      result = StringExpr.rightTrimAndTruncateScalar(blankRanges, 29);
      Assert.assertTrue(Arrays.equals(Arrays.copyOf(blankRanges, 26), result));

      // Multi-byte truncation.
      byte[] scratch = new byte[100];
      byte[] multiByte;

      addMultiByteCharRightPadded1_1(scratch);
      multiByte = Arrays.copyOf(scratch, 4);
      Assert.assertTrue(StringExpr.characterCount(multiByte, 0, 4) == 2);
      result = StringExpr.rightTrimAndTruncateScalar(multiByte, 1);
      Assert.assertTrue(Arrays.equals(Arrays.copyOf(multiByte, 3), result));

      addMultiByteCharRightPadded1_2(scratch);
      multiByte = Arrays.copyOf(scratch, 5);
      Assert.assertTrue(StringExpr.characterCount(multiByte, 0, 5) == 3);
      result = StringExpr.rightTrimAndTruncateScalar(multiByte, 2);
      Assert.assertTrue(Arrays.equals(Arrays.copyOf(multiByte, 4), result));

      addMultiByteCharRightPadded1_3(scratch);
      multiByte = Arrays.copyOf(scratch, 9);
      Assert.assertTrue(StringExpr.characterCount(multiByte, 0, 9) == 4);
      result = StringExpr.rightTrimAndTruncateScalar(multiByte, 2);
      Assert.assertTrue(Arrays.equals(Arrays.copyOf(multiByte, 5), result));

      addMultiByteCharRightPadded1_2(scratch);
      multiByte = Arrays.copyOfRange(scratch, 3, 3 + 2);
      Assert.assertTrue(StringExpr.characterCount(multiByte) == 2);
      result = StringExpr.rightTrimAndTruncateScalar(multiByte, 1);
      Assert.assertTrue(Arrays.equals(Arrays.copyOf(multiByte, 1), result));

      int sentenceOneLen = addMultiByteCharSentenceOne(scratch, 0);
      byte[] sentenceOne = Arrays.copyOf(scratch, sentenceOneLen);

      Assert.assertTrue(StringExpr.characterCount(sentenceOne) == 10);
      result = StringExpr.rightTrimAndTruncateScalar(sentenceOne, 8);
      Assert.assertTrue(Arrays.equals(Arrays.copyOf(sentenceOne, 20), result));

      byte[] sentenceOnePortion = Arrays.copyOf(sentenceOne, sentenceOneLen - 3);
      Assert.assertTrue(StringExpr.characterCount(sentenceOnePortion) == 9);
      result = StringExpr.rightTrimAndTruncateScalar(sentenceOnePortion, 3);
      Assert.assertTrue(Arrays.equals(Arrays.copyOf(sentenceOnePortion, 9), result));

      int sentenceTwoLen = addMultiByteCharSentenceTwo(scratch, 0);
      byte[] sentenceTwo = Arrays.copyOf(scratch, sentenceTwoLen);

      Assert.assertTrue(StringExpr.characterCount(sentenceTwo) == 13);
      result = StringExpr.rightTrimAndTruncateScalar(sentenceTwo, 9);
      Assert.assertTrue(Arrays.equals(Arrays.copyOf(sentenceTwo, 16), result));

      byte[] sentenceTwoPortion = Arrays.copyOf(sentenceTwo, sentenceTwoLen - 5);
      Assert.assertTrue(StringExpr.characterCount(sentenceTwoPortion) == 10);
      result = StringExpr.rightTrimAndTruncateScalar(sentenceTwoPortion, 6);
      Assert.assertTrue(Arrays.equals(Arrays.copyOf(sentenceTwoPortion, 11), result));

      int start;

      // Left pad longer strings with multi-byte characters.
      start = addPads(scratch, 0, 3);
      int sentenceOnePaddedLeftLen = addMultiByteCharSentenceOne(scratch, start);
      byte[] sentenceOnePaddedLeft = Arrays.copyOf(scratch, sentenceOnePaddedLeftLen);

      Assert.assertTrue(StringExpr.characterCount(sentenceOnePaddedLeft) == 3 + 10);
      result = StringExpr.rightTrimAndTruncateScalar(sentenceOnePaddedLeft, 4);
      Assert.assertTrue(Arrays.equals(Arrays.copyOf(sentenceOnePaddedLeft, 6), result));

      byte[] sentenceOnePaddedLeftPortion = Arrays.copyOf(sentenceOnePaddedLeft, sentenceOnePaddedLeftLen - 3);
      Assert.assertTrue(StringExpr.characterCount(sentenceOnePaddedLeftPortion) == 3 + 9);
      result = StringExpr.rightTrimAndTruncateScalar(sentenceOnePaddedLeftPortion, 7);
      Assert.assertTrue(Arrays.equals(Arrays.copyOf(sentenceOnePaddedLeftPortion, 12), result));

      start = addPads(scratch, 0, 2);
      int sentenceTwoPaddedLeftLen = addMultiByteCharSentenceTwo(scratch, start);
      byte[] sentenceTwoPaddedLeft = Arrays.copyOf(scratch, sentenceTwoPaddedLeftLen);

      Assert.assertTrue(StringExpr.characterCount(sentenceTwoPaddedLeft) == 2 + 13);
      result = StringExpr.rightTrimAndTruncateScalar(sentenceTwoPaddedLeft, 14);
      Assert.assertTrue(Arrays.equals(Arrays.copyOf(sentenceTwoPaddedLeft, 24), result));

      byte[] sentenceTwoPaddedLeftPortion = Arrays.copyOf(sentenceTwoPaddedLeft, sentenceTwoPaddedLeftLen - 5);
      Assert.assertTrue(StringExpr.characterCount(sentenceTwoPaddedLeftPortion) == 2 + 10);
      result = StringExpr.rightTrimAndTruncateScalar(sentenceTwoPaddedLeftPortion, 9);
      Assert.assertTrue(Arrays.equals(Arrays.copyOf(sentenceTwoPaddedLeftPortion, 15), result));

      // Right pad longer strings with multi-byte characters.
      start = addMultiByteCharSentenceOne(scratch, 0);
      int sentenceOnePaddedRightLen = addPads(scratch, start, 4);
      byte[] sentenceOnePaddedRight = Arrays.copyOf(scratch, sentenceOnePaddedRightLen);

      Assert.assertTrue(StringExpr.characterCount(sentenceOnePaddedRight) == 10 + 4);
      result = StringExpr.rightTrimAndTruncateScalar(sentenceOnePaddedRight, 1);
      Assert.assertTrue(Arrays.equals(Arrays.copyOf(sentenceOnePaddedRight, 3), result));

      byte[] sentenceOnePaddedRightPortion = Arrays.copyOf(sentenceOnePaddedRight, sentenceOnePaddedRightLen - 3 - 4);
      Assert.assertTrue(StringExpr.characterCount(sentenceOnePaddedRightPortion) == 9);
      result = StringExpr.rightTrimAndTruncateScalar(sentenceOnePaddedRightPortion, 5);
      Assert.assertTrue(Arrays.equals(Arrays.copyOf(sentenceOnePaddedRightPortion, 13), result));

      start = addMultiByteCharSentenceTwo(scratch, 0);
      int sentenceTwoPaddedRightLen = addPads(scratch, start, 1);
      byte[] sentenceTwoPaddedRight = Arrays.copyOf(scratch, sentenceTwoPaddedRightLen);

      Assert.assertTrue(StringExpr.characterCount(sentenceTwoPaddedRight) == 13 + 1);
      result = StringExpr.rightTrimAndTruncateScalar(sentenceTwoPaddedRight, 6);
      Assert.assertTrue(Arrays.equals(Arrays.copyOf(sentenceTwoPaddedRight, 11), result));

      byte[] sentenceTwoPaddedRightPortion = Arrays.copyOf(sentenceTwoPaddedRight, sentenceTwoPaddedRightLen - 5 - 1);
      Assert.assertTrue(StringExpr.characterCount(sentenceTwoPaddedRightPortion) == 10);
      result = StringExpr.rightTrimAndTruncateScalar(sentenceTwoPaddedRightPortion, 8);
      Assert.assertTrue(Arrays.equals(Arrays.copyOf(sentenceTwoPaddedRightPortion, 13), result));

      // Multi-byte characters with blank ranges.
      int sentenceBlankRangesLen = addMultiByteCharSentenceBlankRanges(scratch, 0);
      byte[] sentenceBlankRanges = Arrays.copyOf(scratch, sentenceBlankRangesLen);

      Assert.assertTrue(StringExpr.characterCount(sentenceBlankRanges) == 17);
      result = StringExpr.rightTrimAndTruncateScalar(sentenceBlankRanges, 4);
      Assert.assertTrue(Arrays.equals(Arrays.copyOf(sentenceBlankRanges, 7), result));

      byte[] sentenceBlankRangesPortion = Arrays.copyOf(sentenceBlankRanges, sentenceBlankRangesLen - 3);
      Assert.assertTrue(StringExpr.characterCount(sentenceBlankRangesPortion) == 16);
      result = StringExpr.rightTrimAndTruncateScalar(sentenceBlankRangesPortion, 14);
      Assert.assertTrue(Arrays.equals(Arrays.copyOf(sentenceBlankRangesPortion, 19), result));

      sentenceBlankRangesPortion = Arrays.copyOfRange(sentenceBlankRanges, 7, 7 + 17);
      Assert.assertTrue(StringExpr.characterCount(sentenceBlankRangesPortion) == 13);
      result = StringExpr.rightTrimAndTruncateScalar(sentenceBlankRangesPortion, 11);
      Assert.assertTrue(Arrays.equals(Arrays.copyOf(sentenceBlankRangesPortion, 12), result));
      Assert.assertTrue(StringExpr.characterCount(result) == 8);
  }
  @Test
  // Load a BytesColumnVector by copying in large data, enough to force
  // the buffer to expand.
  public void testLoadBytesColumnVectorByValueLargeData()  {
    BytesColumnVector bcv = new BytesColumnVector(VectorizedRowBatch.DEFAULT_SIZE);
    bcv.initBuffer(10); // initialize with estimated element size 10
    // Record initial buffer size
    int initialBufferSize = bcv.bufferSize();
    String s = "0123456789";
    while (s.length() < 500) {
      s += s;
    }
    byte[] b = s.getBytes(StandardCharsets.UTF_8);

    for (int i = 0; i != VectorizedRowBatch.DEFAULT_SIZE; i++) {
      bcv.setVal(i, b, 0, b.length);
    }
    // Current buffer size should be larger than initial size
    Assert.assertTrue(bcv.bufferSize() > initialBufferSize);
  }

  @Test
  // set values by reference, copy the data out, and verify equality
  public void testLoadBytesColumnVectorByRef() {
    BytesColumnVector bcv = new BytesColumnVector(VectorizedRowBatch.DEFAULT_SIZE);
    String s = "red";
    byte[] b = s.getBytes(StandardCharsets.UTF_8);

    for (int i = 0; i != VectorizedRowBatch.DEFAULT_SIZE; i++) {
      bcv.setRef(i, b, 0, b.length);
    }
    // verify
    byte[] v = new byte[b.length];
    for (int i = 0; i != VectorizedRowBatch.DEFAULT_SIZE; i++) {
      Assert.assertTrue(bcv.length[i] == b.length);
      System.arraycopy(bcv.vector[i], bcv.start[i], v, 0, b.length);
      Assert.assertTrue(Arrays.equals(b, v));
    }
  }

  @Test
  // Test string column to string literal comparison
  public void testStringColCompareStringScalarFilter() throws HiveException {
    VectorizedRowBatch batch = makeStringBatch();
    VectorExpression expr;
    expr = new FilterStringGroupColEqualStringScalar(0, red2);
    expr.evaluate(batch);

    // only red qualifies, and it's in entry 0
    Assert.assertTrue(batch.size == 1);
    Assert.assertTrue(batch.selected[0] == 0);

    batch = makeStringBatch();
    expr = new FilterStringGroupColLessStringScalar(0, red2);
    expr.evaluate(batch);

    // only green qualifies, and it's in entry 1
    Assert.assertTrue(batch.size == 1);
    Assert.assertTrue(batch.selected[0] == 1);

    batch = makeStringBatch();
    expr = new FilterStringGroupColGreaterEqualStringScalar(0, green);
    expr.evaluate(batch);

    // green and red qualify
    Assert.assertTrue(batch.size == 2);
    Assert.assertTrue(batch.selected[0] == 0);
    Assert.assertTrue(batch.selected[1] == 1);
  }

  @Test
  // Test string column to CHAR literal comparison
  public void testStringColCompareCharScalarFilter() throws HiveException {
    VectorizedRowBatch batch = makeStringBatch();
    VectorExpression expr;
    expr =
        new FilterStringGroupColEqualCharScalar(
            0, new HiveChar(new String(red2), 10).getStrippedValue().getBytes());
    expr.evaluate(batch);

    // only red qualifies, and it's in entry 0
    Assert.assertTrue(batch.size == 1);
    Assert.assertTrue(batch.selected[0] == 0);

    batch = makeStringBatch();
    expr =
        new FilterStringGroupColLessCharScalar(
            0, new HiveChar(new String(red2), 8).getStrippedValue().getBytes());
    expr.evaluate(batch);

    // only green qualifies, and it's in entry 1
    Assert.assertTrue(batch.size == 1);
    Assert.assertTrue(batch.selected[0] == 1);

    batch = makeStringBatch();
    expr =
        new FilterStringGroupColGreaterEqualCharScalar(
            0, new HiveChar(new String(green), 12).getStrippedValue().getBytes());
    expr.evaluate(batch);

    // green and red qualify
    Assert.assertTrue(batch.size == 2);
    Assert.assertTrue(batch.selected[0] == 0);
    Assert.assertTrue(batch.selected[1] == 1);
  }

  @Test
  // Test string column to VARCHAR literal comparison
  public void testStringColCompareVarCharScalarFilter() throws HiveException {
    VectorizedRowBatch batch = makeStringBatch();
    VectorExpression expr;
    expr =
        new FilterStringGroupColEqualVarCharScalar(
            0, new HiveVarchar(new String(red2), 10).getValue().getBytes());
    expr.evaluate(batch);

    // only red qualifies, and it's in entry 0
    Assert.assertTrue(batch.size == 1);
    Assert.assertTrue(batch.selected[0] == 0);

    batch = makeStringBatch();
    expr =
        new FilterStringGroupColLessVarCharScalar(
            0, new HiveVarchar(new String(red2), 8).getValue().getBytes());
    expr.evaluate(batch);

    // only green qualifies, and it's in entry 1
    Assert.assertTrue(batch.size == 1);
    Assert.assertTrue(batch.selected[0] == 1);

    batch = makeStringBatch();
    expr =
        new FilterStringGroupColGreaterEqualVarCharScalar(
            0, new HiveVarchar(new String(green), 12).getValue().getBytes());
    expr.evaluate(batch);

    // green and red qualify
    Assert.assertTrue(batch.size == 2);
    Assert.assertTrue(batch.selected[0] == 0);
    Assert.assertTrue(batch.selected[1] == 1);
  }

  @Test
  public void testStringColCompareStringScalarProjection() throws HiveException {
    VectorizedRowBatch batch = makeStringBatch();
    VectorExpression expr;

    expr = new StringGroupColEqualStringScalar(0, red2, 2);
    expr.evaluate(batch);
    Assert.assertEquals(3, batch.size);
    LongColumnVector outVector = (LongColumnVector) batch.cols[2];
    Assert.assertEquals(1, outVector.vector[0]);
    Assert.assertEquals(0, outVector.vector[1]);
    Assert.assertEquals(0, outVector.vector[2]);

    batch = makeStringBatch();
    expr = new StringGroupColEqualStringScalar(0, green, 2);
    expr.evaluate(batch);
    Assert.assertEquals(3, batch.size);
    outVector = (LongColumnVector) batch.cols[2];
    Assert.assertEquals(0, outVector.vector[0]);
    Assert.assertEquals(1, outVector.vector[1]);
    Assert.assertEquals(0, outVector.vector[2]);
  }

  @Test
  public void testStringColCompareCharScalarProjection() throws HiveException {
    VectorizedRowBatch batch = makeStringBatch();
    VectorExpression expr;

    expr =
        new StringGroupColEqualCharScalar(
            0, new HiveChar(new String(red2), 8).getStrippedValue().getBytes(), 2);
    expr.evaluate(batch);
    Assert.assertEquals(3, batch.size);
    LongColumnVector outVector = (LongColumnVector) batch.cols[2];
    Assert.assertEquals(1, outVector.vector[0]);
    Assert.assertEquals(0, outVector.vector[1]);
    Assert.assertEquals(0, outVector.vector[2]);

    batch = makeStringBatch();
    expr =
        new StringGroupColEqualCharScalar(
            0, new HiveChar(new String(green), 10).getStrippedValue().getBytes(), 2);
    expr.evaluate(batch);
    Assert.assertEquals(3, batch.size);
    outVector = (LongColumnVector) batch.cols[2];
    Assert.assertEquals(0, outVector.vector[0]);
    Assert.assertEquals(1, outVector.vector[1]);
    Assert.assertEquals(0, outVector.vector[2]);
  }

  @Test
  public void testStringColCompareVarCharScalarProjection() throws HiveException {
    VectorizedRowBatch batch = makeStringBatch();
    VectorExpression expr;

    expr =
        new StringGroupColEqualVarCharScalar(
            0, new HiveVarchar(new String(red2), 8).getValue().getBytes(), 2);
    expr.evaluate(batch);
    Assert.assertEquals(3, batch.size);
    LongColumnVector outVector = (LongColumnVector) batch.cols[2];
    Assert.assertEquals(1, outVector.vector[0]);
    Assert.assertEquals(0, outVector.vector[1]);
    Assert.assertEquals(0, outVector.vector[2]);

    batch = makeStringBatch();
    expr =
        new StringGroupColEqualVarCharScalar(
            0, new HiveVarchar(new String(green), 10).getValue().getBytes(), 2);
    expr.evaluate(batch);
    Assert.assertEquals(3, batch.size);
    outVector = (LongColumnVector) batch.cols[2];
    Assert.assertEquals(0, outVector.vector[0]);
    Assert.assertEquals(1, outVector.vector[1]);
    Assert.assertEquals(0, outVector.vector[2]);
  }

  @Test
  // Test string literal to string column comparison
  public void testStringScalarCompareStringCol() throws HiveException {
    VectorizedRowBatch batch = makeStringBatch();
    VectorExpression expr;
    expr = new FilterStringScalarEqualStringGroupColumn(red2, 0);
    expr.evaluate(batch);

    // only red qualifies, and it's in entry 0
    Assert.assertTrue(batch.size == 1);
    Assert.assertTrue(batch.selected[0] == 0);

    batch = makeStringBatch();
    expr = new FilterStringScalarGreaterStringGroupColumn(red2, 0);
    expr.evaluate(batch);

    // only green qualifies, and it's in entry 1
    Assert.assertTrue(batch.size == 1);
    Assert.assertTrue(batch.selected[0] == 1);

    batch = makeStringBatch();
    expr = new FilterStringScalarLessEqualStringGroupColumn(green, 0);
    expr.evaluate(batch);

    // green and red qualify
    Assert.assertTrue(batch.size == 2);
    Assert.assertTrue(batch.selected[0] == 0);
    Assert.assertTrue(batch.selected[1] == 1);
  }

  @Test
  // Test CHAR literal to string column comparison
  public void testCharScalarCompareStringCol() throws HiveException {
    VectorizedRowBatch batch = makeStringBatch();
    VectorExpression expr;
    expr =
        new FilterCharScalarEqualStringGroupColumn(
            new HiveChar(new String(red2), 8).getStrippedValue().getBytes(), 0);
    expr.evaluate(batch);

    // only red qualifies, and it's in entry 0
    Assert.assertTrue(batch.size == 1);
    Assert.assertTrue(batch.selected[0] == 0);

    batch = makeStringBatch();
    expr =
        new FilterCharScalarGreaterStringGroupColumn(
            new HiveChar(new String(red2), 8).getStrippedValue().getBytes(), 0);
    expr.evaluate(batch);

    // only green qualifies, and it's in entry 1
    Assert.assertTrue(batch.size == 1);
    Assert.assertTrue(batch.selected[0] == 1);

    batch = makeStringBatch();
    expr =
        new FilterCharScalarLessEqualStringGroupColumn(
            new HiveChar(new String(green), 10).getStrippedValue().getBytes(), 0);
    expr.evaluate(batch);

    // green and red qualify
    Assert.assertTrue(batch.size == 2);
    Assert.assertTrue(batch.selected[0] == 0);
    Assert.assertTrue(batch.selected[1] == 1);
  }

  @Test
  // Test VARCHAR literal to string column comparison
  public void testVarCharScalarCompareStringCol() throws HiveException {
    VectorizedRowBatch batch = makeStringBatch();
    VectorExpression expr;
    expr =
        new FilterVarCharScalarEqualStringGroupColumn(
            new HiveVarchar(new String(red2), 8).getValue().getBytes(), 0);
    expr.evaluate(batch);

    // only red qualifies, and it's in entry 0
    Assert.assertTrue(batch.size == 1);
    Assert.assertTrue(batch.selected[0] == 0);

    batch = makeStringBatch();
    expr =
        new FilterVarCharScalarGreaterStringGroupColumn(
            new HiveVarchar(new String(red2), 8).getValue().getBytes(), 0);
    expr.evaluate(batch);

    // only green qualifies, and it's in entry 1
    Assert.assertTrue(batch.size == 1);
    Assert.assertTrue(batch.selected[0] == 1);

    batch = makeStringBatch();
    expr =
        new FilterVarCharScalarLessEqualStringGroupColumn(
            new HiveVarchar(new String(green), 10).getValue().getBytes(), 0);
    expr.evaluate(batch);

    // green and red qualify
    Assert.assertTrue(batch.size == 2);
    Assert.assertTrue(batch.selected[0] == 0);
    Assert.assertTrue(batch.selected[1] == 1);
  }

  @Test
  public void testStringScalarCompareStringColProjection() throws HiveException {
    VectorizedRowBatch batch = makeStringBatch();
    VectorExpression expr;

    expr = new StringScalarEqualStringGroupColumn(red2, 0, 2);
    expr.evaluate(batch);
    Assert.assertEquals(3, batch.size);
    LongColumnVector outVector = (LongColumnVector) batch.cols[2];
    Assert.assertEquals(1, outVector.vector[0]);
    Assert.assertEquals(0, outVector.vector[1]);
    Assert.assertEquals(0, outVector.vector[2]);

    batch = makeStringBatch();
    expr = new StringScalarEqualStringGroupColumn(green, 0, 2);
    expr.evaluate(batch);
    Assert.assertEquals(3, batch.size);
    outVector = (LongColumnVector) batch.cols[2];
    Assert.assertEquals(0, outVector.vector[0]);
    Assert.assertEquals(1, outVector.vector[1]);
    Assert.assertEquals(0, outVector.vector[2]);
  }

  @Test
  public void testCharScalarCompareStringColProjection() throws HiveException {
    VectorizedRowBatch batch = makeStringBatch();
    VectorExpression expr;

    expr =
        new CharScalarEqualStringGroupColumn(
            new HiveChar(new String(red2), 8).getStrippedValue().getBytes(), 0, 2);
    expr.evaluate(batch);
    Assert.assertEquals(3, batch.size);
    LongColumnVector outVector = (LongColumnVector) batch.cols[2];
    Assert.assertEquals(1, outVector.vector[0]);
    Assert.assertEquals(0, outVector.vector[1]);
    Assert.assertEquals(0, outVector.vector[2]);

    batch = makeStringBatch();
    expr =
        new CharScalarEqualStringGroupColumn(
            new HiveChar(new String(green), 10).getStrippedValue().getBytes(), 0, 2);
    expr.evaluate(batch);
    Assert.assertEquals(3, batch.size);
    outVector = (LongColumnVector) batch.cols[2];
    Assert.assertEquals(0, outVector.vector[0]);
    Assert.assertEquals(1, outVector.vector[1]);
    Assert.assertEquals(0, outVector.vector[2]);
  }

  @Test
  public void testVarCharScalarCompareStringColProjection() throws HiveException {
    VectorizedRowBatch batch = makeStringBatch();
    VectorExpression expr;

    expr =
        new VarCharScalarEqualStringGroupColumn(
            new HiveVarchar(new String(red2), 8).getValue().getBytes(), 0, 2);
    expr.evaluate(batch);
    Assert.assertEquals(3, batch.size);
    LongColumnVector outVector = (LongColumnVector) batch.cols[2];
    Assert.assertEquals(1, outVector.vector[0]);
    Assert.assertEquals(0, outVector.vector[1]);
    Assert.assertEquals(0, outVector.vector[2]);

    batch = makeStringBatch();
    expr =
        new VarCharScalarEqualStringGroupColumn(
            new HiveVarchar(new String(green), 10).getValue().getBytes(), 0, 2);
    expr.evaluate(batch);
    Assert.assertEquals(3, batch.size);
    outVector = (LongColumnVector) batch.cols[2];
    Assert.assertEquals(0, outVector.vector[0]);
    Assert.assertEquals(1, outVector.vector[1]);
    Assert.assertEquals(0, outVector.vector[2]);
  }
  @Test
  public void testStringColCompareStringColFilter() throws HiveException {
    VectorizedRowBatch batch;
    VectorExpression expr;

    /* input data
     *
     * col0       col1
     * ===============
     * blue       red
     * green      green
     * red        blue
     * NULL       red            col0 data is empty string if we un-set NULL property
     */

    // nulls possible on left, right
    batch = makeStringBatchForColColCompare();
    expr = new FilterStringGroupColLessStringGroupColumn(0,1);
    expr.evaluate(batch);
    Assert.assertEquals(1, batch.size);
    Assert.assertEquals(0, batch.selected[0]);

    // no nulls possible
    batch = makeStringBatchForColColCompare();
    batch.cols[0].noNulls = true;
    batch.cols[1].noNulls = true;
    expr.evaluate(batch);
    Assert.assertEquals(2, batch.size);
    Assert.assertEquals(3, batch.selected[1]);

    // nulls on left, no nulls on right
    batch = makeStringBatchForColColCompare();
    batch.cols[1].noNulls = true;
    expr.evaluate(batch);
    Assert.assertEquals(1, batch.size);
    Assert.assertEquals(0, batch.selected[0]);

    // nulls on right, no nulls on left
    batch = makeStringBatchForColColCompare();
    batch.cols[0].noNulls = true;
    batch.cols[1].isNull[3] = true;
    expr.evaluate(batch);
    Assert.assertEquals(1, batch.size);
    Assert.assertEquals(0, batch.selected[0]);

    // Now vary isRepeating
    // nulls possible on left, right

    // left repeats
    batch = makeStringBatchForColColCompare();
    batch.cols[0].isRepeating = true;
    expr.evaluate(batch);
    Assert.assertEquals(3, batch.size);
    Assert.assertEquals(3, batch.selected[2]);

    // right repeats
    batch = makeStringBatchForColColCompare();
    batch.cols[1].isRepeating = true;
    expr.evaluate(batch);
    Assert.assertEquals(2, batch.size); // first 2 qualify
    Assert.assertEquals(1, batch.selected[1]);

    // left and right repeat
    batch = makeStringBatchForColColCompare();
    batch.cols[0].isRepeating = true;
    batch.cols[1].isRepeating = true;
    expr.evaluate(batch);
    Assert.assertEquals(4, batch.size);

    // Now vary isRepeating
    // nulls possible only on left

    // left repeats
    batch = makeStringBatchForColColCompare();
    batch.cols[0].isRepeating = true;
    batch.cols[1].noNulls = true;
    expr.evaluate(batch);
    Assert.assertEquals(3, batch.size);
    Assert.assertEquals(3, batch.selected[2]);

    // left repeats and is null
    batch = makeStringBatchForColColCompare();
    batch.cols[0].isRepeating = true;
    batch.cols[1].noNulls = true;
    batch.cols[0].isNull[0] = true;
    expr.evaluate(batch);
    Assert.assertEquals(0, batch.size);

    // right repeats
    batch = makeStringBatchForColColCompare();
    batch.cols[1].isRepeating = true;
    batch.cols[1].noNulls = true;
    expr.evaluate(batch);
    Assert.assertEquals(2, batch.size);
    Assert.assertEquals(0, batch.selected[0]);
    Assert.assertEquals(1, batch.selected[1]);

    // left and right repeat
    batch = makeStringBatchForColColCompare();
    batch.cols[0].isRepeating = true;
    batch.cols[1].isRepeating = true;
    batch.cols[1].noNulls = true;
    expr.evaluate(batch);
    Assert.assertEquals(4, batch.size);


    // Now vary isRepeating
    // nulls possible only on right

    // left repeats
    batch = makeStringBatchForColColCompare();
    batch.cols[0].isRepeating = true;
    batch.cols[0].noNulls = true;
    batch.cols[1].isNull[0] = true;
    expr.evaluate(batch);
    Assert.assertEquals(2, batch.size);
    Assert.assertEquals(3, batch.selected[1]);

    // right repeats
    batch = makeStringBatchForColColCompare();
    batch.cols[1].isRepeating = true;
    batch.cols[0].noNulls = true;
    expr.evaluate(batch);
    Assert.assertEquals(3, batch.size);
    Assert.assertEquals(3, batch.selected[2]);

    // right repeats and is null
    batch = makeStringBatchForColColCompare();
    batch.cols[1].isRepeating = true;
    batch.cols[0].noNulls = true;
    batch.cols[1].isNull[0] = true;
    expr.evaluate(batch);
    Assert.assertEquals(0, batch.size);

    // left and right repeat
    batch = makeStringBatchForColColCompare();
    batch.cols[0].isRepeating = true;
    batch.cols[1].isRepeating = true;
    batch.cols[0].noNulls = true;
    expr.evaluate(batch);
    Assert.assertEquals(4, batch.size);

    // left and right repeat and right is null
    batch = makeStringBatchForColColCompare();
    batch.cols[0].isRepeating = true;
    batch.cols[1].isRepeating = true;
    batch.cols[0].noNulls = true;
    batch.cols[1].isNull[0] = true;
    expr.evaluate(batch);
    Assert.assertEquals(0, batch.size);
  }

  @Test
  public void testStringColCompareStringColProjection() throws HiveException {
    VectorizedRowBatch batch;
    VectorExpression expr;
    long [] outVector;

    /* input data
     *
     * col0       col1
     * ===============
     * blue       red
     * green      green
     * red        blue
     * NULL       red            col0 data is empty string if we un-set NULL property
     */

    // nulls possible on left, right
    batch = makeStringBatchForColColCompare();
    expr = new StringGroupColLessStringGroupColumn(0, 1, 3);
    expr.evaluate(batch);
    Assert.assertEquals(4, batch.size);
    outVector = ((LongColumnVector) batch.cols[3]).vector;
    Assert.assertFalse(batch.cols[3].isNull[0]);
    Assert.assertEquals(1, outVector[0]);
    Assert.assertFalse(batch.cols[3].isNull[1]);
    Assert.assertEquals(0, outVector[1]);
    Assert.assertFalse(batch.cols[3].isNull[2]);
    Assert.assertEquals(0, outVector[2]);
    Assert.assertTrue(batch.cols[3].isNull[3]);

    // no nulls possible
    batch = makeStringBatchForColColCompare();
    batch.cols[0].noNulls = true;
    batch.cols[1].noNulls = true;
    expr.evaluate(batch);
    Assert.assertEquals(4, batch.size);
    outVector = ((LongColumnVector) batch.cols[3]).vector;
    Assert.assertTrue(batch.cols[3].noNulls);
    Assert.assertFalse(batch.cols[3].isNull[0]);
    Assert.assertEquals(1, outVector[0]);
    Assert.assertFalse(batch.cols[3].isNull[1]);
    Assert.assertEquals(0, outVector[1]);
    Assert.assertFalse(batch.cols[3].isNull[2]);
    Assert.assertEquals(0, outVector[2]);
    Assert.assertFalse(batch.cols[3].isNull[3]);
    Assert.assertEquals(1, outVector[3]);

    // nulls on left, no nulls on right
    batch = makeStringBatchForColColCompare();
    batch.cols[1].noNulls = true;
    expr.evaluate(batch);
    Assert.assertEquals(4, batch.size);
    outVector = ((LongColumnVector) batch.cols[3]).vector;
    Assert.assertFalse(batch.cols[3].isNull[0]);
    Assert.assertEquals(1, outVector[0]);
    Assert.assertFalse(batch.cols[3].isNull[1]);
    Assert.assertEquals(0, outVector[1]);
    Assert.assertFalse(batch.cols[3].isNull[2]);
    Assert.assertEquals(0, outVector[2]);
    Assert.assertTrue(batch.cols[3].isNull[3]);

    // nulls on right, no nulls on left
    batch = makeStringBatchForColColCompare();
    batch.cols[0].noNulls = true;
    batch.cols[1].isNull[3] = true;
    expr.evaluate(batch);
    Assert.assertEquals(4, batch.size);
    outVector = ((LongColumnVector) batch.cols[3]).vector;
    Assert.assertFalse(batch.cols[3].isNull[0]);
    Assert.assertEquals(1, outVector[0]);
    Assert.assertFalse(batch.cols[3].isNull[1]);
    Assert.assertEquals(0, outVector[1]);
    Assert.assertFalse(batch.cols[3].isNull[2]);
    Assert.assertEquals(0, outVector[2]);
    Assert.assertTrue(batch.cols[3].isNull[3]);

    // Now vary isRepeating
    // nulls possible on left, right

    // left repeats
    batch = makeStringBatchForColColCompare();
    batch.cols[0].isRepeating = true;
    expr.evaluate(batch);
    Assert.assertEquals(4, batch.size);
    outVector = ((LongColumnVector) batch.cols[3]).vector;
    Assert.assertFalse(batch.cols[3].isNull[0]);
    Assert.assertEquals(1, outVector[0]);
    Assert.assertFalse(batch.cols[3].isNull[1]);
    Assert.assertEquals(1, outVector[1]);
    Assert.assertFalse(batch.cols[3].isNull[2]);
    Assert.assertEquals(0, outVector[2]);
    Assert.assertFalse(batch.cols[3].isNull[3]);
    Assert.assertEquals(1, outVector[3]);


    // right repeats
    batch = makeStringBatchForColColCompare();
    batch.cols[1].isRepeating = true;
    expr.evaluate(batch);
    Assert.assertEquals(4, batch.size);
    outVector = ((LongColumnVector) batch.cols[3]).vector;
    Assert.assertFalse(batch.cols[3].noNulls);
    Assert.assertFalse(batch.cols[3].isNull[0]);
    Assert.assertEquals(1, outVector[0]);
    Assert.assertFalse(batch.cols[3].isNull[1]);
    Assert.assertEquals(1, outVector[1]);
    Assert.assertFalse(batch.cols[3].isNull[2]);
    Assert.assertEquals(0, outVector[2]);
    Assert.assertTrue(batch.cols[3].isNull[3]);

    // left and right repeat
    batch = makeStringBatchForColColCompare();
    batch.cols[0].isRepeating = true;
    batch.cols[1].isRepeating = true;
    expr.evaluate(batch);
    Assert.assertEquals(4, batch.size);
    outVector = ((LongColumnVector) batch.cols[3]).vector;
    Assert.assertTrue(batch.cols[3].isRepeating);
    Assert.assertFalse(batch.cols[3].isNull[0]);
    Assert.assertEquals(1, outVector[0]);

    // Now vary isRepeating
    // nulls possible only on left

    // left repeats
    batch = makeStringBatchForColColCompare();
    batch.cols[0].isRepeating = true;
    batch.cols[1].noNulls = true;
    expr.evaluate(batch);
    outVector = ((LongColumnVector) batch.cols[3]).vector;
    Assert.assertEquals(4, batch.size);
    Assert.assertFalse(batch.cols[3].isNull[0]);
    Assert.assertEquals(1, outVector[0]);
    Assert.assertFalse(batch.cols[3].isNull[1]);
    Assert.assertEquals(1, outVector[1]);
    Assert.assertFalse(batch.cols[3].isNull[2]);
    Assert.assertEquals(0, outVector[2]);
    Assert.assertFalse(batch.cols[3].isNull[3]);
    Assert.assertEquals(1, outVector[3]);

    // left repeats and is null
    batch = makeStringBatchForColColCompare();
    batch.cols[0].isRepeating = true;
    batch.cols[1].noNulls = true;
    batch.cols[0].isNull[0] = true;
    expr.evaluate(batch);
    Assert.assertEquals(4, batch.size);
    Assert.assertFalse(batch.cols[3].noNulls);
    Assert.assertTrue(batch.cols[3].isRepeating);
    Assert.assertTrue(batch.cols[3].isNull[0]);

    // right repeats
    batch = makeStringBatchForColColCompare();
    batch.cols[1].isRepeating = true;
    batch.cols[1].noNulls = true;
    expr.evaluate(batch);
    outVector = ((LongColumnVector) batch.cols[3]).vector;
    Assert.assertEquals(4, batch.size);
    Assert.assertFalse(batch.cols[3].noNulls);
    Assert.assertFalse(batch.cols[3].isNull[0]);
    Assert.assertEquals(1, outVector[0]);
    Assert.assertFalse(batch.cols[3].isNull[1]);
    Assert.assertEquals(1, outVector[1]);
    Assert.assertFalse(batch.cols[3].isNull[2]);
    Assert.assertEquals(0, outVector[2]);
    Assert.assertTrue(batch.cols[3].isNull[3]);

    // left and right repeat
    batch = makeStringBatchForColColCompare();
    batch.cols[0].isRepeating = true;
    batch.cols[1].isRepeating = true;
    batch.cols[1].noNulls = true;
    expr.evaluate(batch);
    Assert.assertEquals(4, batch.size);
    outVector = ((LongColumnVector) batch.cols[3]).vector;
    Assert.assertTrue(batch.cols[3].isRepeating);
    Assert.assertFalse(batch.cols[3].isNull[0]);
    Assert.assertEquals(1, outVector[0]);



    // Now vary isRepeating
    // nulls possible only on right

    // left repeats
    batch = makeStringBatchForColColCompare();
    batch.cols[0].isRepeating = true;
    batch.cols[0].noNulls = true;
    batch.cols[1].isNull[0] = true;
    expr.evaluate(batch);
    outVector = ((LongColumnVector) batch.cols[3]).vector;
    Assert.assertEquals(4, batch.size);
    Assert.assertFalse(batch.cols[3].noNulls);
    Assert.assertTrue(batch.cols[3].isNull[0]);
    Assert.assertFalse(batch.cols[3].isNull[1]);
    Assert.assertEquals(1, outVector[1]);
    Assert.assertFalse(batch.cols[3].isNull[2]);
    Assert.assertEquals(0, outVector[2]);
    Assert.assertFalse(batch.cols[3].isNull[3]);
    Assert.assertEquals(1, outVector[3]);

    // right repeats
    batch = makeStringBatchForColColCompare();
    batch.cols[1].isRepeating = true;
    batch.cols[0].noNulls = true;
    expr.evaluate(batch);
    outVector = ((LongColumnVector) batch.cols[3]).vector;
    Assert.assertEquals(4, batch.size);
    Assert.assertFalse(batch.cols[3].noNulls);
    Assert.assertFalse(batch.cols[3].isNull[0]);
    Assert.assertEquals(1, outVector[0]);
    Assert.assertFalse(batch.cols[3].isNull[1]);
    Assert.assertEquals(1, outVector[1]);
    Assert.assertFalse(batch.cols[3].isNull[2]);
    Assert.assertEquals(0, outVector[2]);
    Assert.assertFalse(batch.cols[3].isNull[3]);
    Assert.assertEquals(1, outVector[3]);

    // right repeats and is null
    batch = makeStringBatchForColColCompare();
    batch.cols[1].isRepeating = true;
    batch.cols[0].noNulls = true;
    batch.cols[1].isNull[0] = true;
    expr.evaluate(batch);
    Assert.assertEquals(4, batch.size);
    Assert.assertFalse(batch.cols[3].noNulls);
    Assert.assertTrue(batch.cols[3].isRepeating);
    Assert.assertTrue(batch.cols[3].isNull[0]);

    // left and right repeat
    batch = makeStringBatchForColColCompare();
    batch.cols[0].isRepeating = true;
    batch.cols[1].isRepeating = true;
    batch.cols[0].noNulls = true;
    expr.evaluate(batch);
    Assert.assertEquals(4, batch.size);
    outVector = ((LongColumnVector) batch.cols[3]).vector;
    Assert.assertTrue(batch.cols[3].isRepeating);
    Assert.assertFalse(batch.cols[3].isNull[0]);
    Assert.assertEquals(1, outVector[0]);

    // left and right repeat and right is null
    batch = makeStringBatchForColColCompare();
    batch.cols[0].isRepeating = true;
    batch.cols[1].isRepeating = true;
    batch.cols[0].noNulls = true;
    batch.cols[1].isNull[0] = true;
    expr.evaluate(batch);
    Assert.assertEquals(4, batch.size);
    Assert.assertFalse(batch.cols[3].noNulls);
    Assert.assertTrue(batch.cols[3].isRepeating);
    Assert.assertTrue(batch.cols[3].isNull[0]);
  }

  VectorizedRowBatch makeStringBatch() {
    // create a batch with one string ("Bytes") column
    VectorizedRowBatch batch = new VectorizedRowBatch(3);
    BytesColumnVector v = new BytesColumnVector();
    batch.cols[0] = v;
    batch.cols[1] = new BytesColumnVector();          // to hold output if needed
    batch.cols[2] = new LongColumnVector(batch.size); // to hold boolean output
    /*
     * Add these 3 values:
     *
     * red
     * green
     * NULL
     */
    v.setRef(0, red, 0, red.length);
    v.isNull[0] = false;
    v.setRef(1, green, 0, green.length);
    v.isNull[1] = false;
    v.setRef(2,  emptyString,  0,  emptyString.length);
    v.isNull[2] = true;

    v.noNulls = false;

    batch.size = 3;
    return batch;
  }

  VectorizedRowBatch makeStringBatchMixedCase() {
    // create a batch with two string ("Bytes") columns
    VectorizedRowBatch batch = new VectorizedRowBatch(2, VectorizedRowBatch.DEFAULT_SIZE);
    BytesColumnVector v = new BytesColumnVector(VectorizedRowBatch.DEFAULT_SIZE);
    batch.cols[0] = v;
    BytesColumnVector outV = new BytesColumnVector(VectorizedRowBatch.DEFAULT_SIZE);
    outV.initBuffer();
    batch.cols[1] = outV;
    /*
     * Add these 3 values:
     *
     * mixedUp
     * green
     * NULL
     */
    v.setRef(0, mixedUp, 0, mixedUp.length);
    v.isNull[0] = false;
    v.setRef(1, green, 0, green.length);
    v.isNull[1] = false;
    v.setRef(2,  emptyString,  0,  emptyString.length);
    v.isNull[2] = true;
    v.noNulls = false;

    batch.size = 3;
    return batch;
  }

  VectorizedRowBatch makeStringBatchMixedCharSize() {

    // create a new batch with one char column (for input) and one long column (for output)
    VectorizedRowBatch batch = new VectorizedRowBatch(2, VectorizedRowBatch.DEFAULT_SIZE);
    BytesColumnVector v = new BytesColumnVector(VectorizedRowBatch.DEFAULT_SIZE);
    batch.cols[0] = v;
    LongColumnVector outV = new LongColumnVector(VectorizedRowBatch.DEFAULT_SIZE);
    batch.cols[1] = outV;

    /*
     * Add these 3 values:
     *
     * mixedUp
     * green
     * NULL
     * <4 char string with mult-byte chars>
     */
    v.setRef(0, mixedUp, 0, mixedUp.length);
    v.isNull[0] = false;
    v.setRef(1, green, 0, green.length);
    v.isNull[1] = false;
    v.setRef(2,  emptyString,  0,  emptyString.length);
    v.isNull[2] = true;
    v.noNulls = false;
    v.setRef(3, multiByte, 0, 10);
    v.isNull[3] = false;

    batch.size = 4;
    return batch;
  }

  @Test
  public void testColLower() throws HiveException {
    // has nulls, not repeating
    VectorizedRowBatch batch = makeStringBatchMixedCase();
    StringLower expr = new StringLower(0, 1);
    expr.evaluate(batch);
    BytesColumnVector outCol = (BytesColumnVector) batch.cols[1];
    int cmp = StringExpr.compare(mixedUpLower, 0, mixedUpLower.length, outCol.vector[0],
        outCol.start[0], outCol.length[0]);
    Assert.assertEquals(0, cmp);
    Assert.assertTrue(outCol.isNull[2]);
    int cmp2 = StringExpr.compare(green, 0, green.length, outCol.vector[1],
        outCol.start[1], outCol.length[1]);
    Assert.assertEquals(0, cmp2);

    // no nulls, not repeating
    batch = makeStringBatchMixedCase();
    batch.cols[0].noNulls = true;
    expr.evaluate(batch);
    outCol = (BytesColumnVector) batch.cols[1];
    cmp = StringExpr.compare(mixedUpLower, 0, mixedUpLower.length, outCol.vector[0],
        outCol.start[0], outCol.length[0]);
    Assert.assertEquals(0, cmp);
    Assert.assertTrue(outCol.noNulls);

    // has nulls, is repeating
    batch = makeStringBatchMixedCase();
    batch.cols[0].isRepeating = true;
    expr.evaluate(batch);
    outCol = (BytesColumnVector) batch.cols[1];
    cmp = StringExpr.compare(mixedUpLower, 0, mixedUpLower.length, outCol.vector[0],
        outCol.start[0], outCol.length[0]);
    Assert.assertEquals(0, cmp);
    Assert.assertTrue(outCol.isRepeating);

    // no nulls, is repeating
    batch = makeStringBatchMixedCase();
    batch.cols[0].isRepeating = true;
    batch.cols[0].noNulls = true;
    expr.evaluate(batch);
    outCol = (BytesColumnVector) batch.cols[1];
    cmp = StringExpr.compare(mixedUpLower, 0, mixedUpLower.length, outCol.vector[0],
        outCol.start[0], outCol.length[0]);
    Assert.assertEquals(0, cmp);
    Assert.assertTrue(outCol.isRepeating);
    Assert.assertTrue(outCol.noNulls);
  }

  @Test
  public void testColUpper() throws HiveException {

    // no nulls, not repeating

    /* We don't test all the combinations because (at least currently)
     * the logic is inherited to be the same as testColLower, which checks all the cases).
     */
    VectorizedRowBatch batch = makeStringBatchMixedCase();
    StringUpper expr = new StringUpper(0, 1);
    batch.cols[0].noNulls = true;
    expr.evaluate(batch);
    BytesColumnVector outCol = (BytesColumnVector) batch.cols[1];
    int cmp = StringExpr.compare(mixedUpUpper, 0, mixedUpUpper.length, outCol.vector[0],
        outCol.start[0], outCol.length[0]);
    Assert.assertEquals(0, cmp);
    Assert.assertTrue(outCol.noNulls);
  }

  @Test
  public void testStringLength() throws HiveException {

    // has nulls, not repeating
    VectorizedRowBatch batch = makeStringBatchMixedCharSize();
    StringLength expr = new StringLength(0, 1);
    expr.evaluate(batch);
    LongColumnVector outCol = (LongColumnVector) batch.cols[1];
    Assert.assertEquals(5, outCol.vector[1]); // length of green is 5
    Assert.assertTrue(outCol.isNull[2]);
    Assert.assertEquals(4, outCol.vector[3]); // this one has the mixed-size chars

    // no nulls, not repeating
    batch = makeStringBatchMixedCharSize();
    batch.cols[0].noNulls = true;
    expr.evaluate(batch);
    outCol = (LongColumnVector) batch.cols[1];
    Assert.assertTrue(outCol.noNulls);
    Assert.assertEquals(4, outCol.vector[3]); // this one has the mixed-size chars

    // has nulls, is repeating
    batch = makeStringBatchMixedCharSize();
    batch.cols[0].isRepeating = true;
    expr.evaluate(batch);
    outCol = (LongColumnVector) batch.cols[1];
    Assert.assertTrue(outCol.isRepeating);
    Assert.assertEquals(7, outCol.vector[0]); // length of "mixedUp"

    // no nulls, is repeating
    batch = makeStringBatchMixedCharSize();
    batch.cols[0].isRepeating = true;
    batch.cols[0].noNulls = true;
    expr.evaluate(batch);
    outCol = (LongColumnVector) batch.cols[1];
    Assert.assertEquals(7, outCol.vector[0]); // length of "mixedUp"
    Assert.assertTrue(outCol.isRepeating);
    Assert.assertTrue(outCol.noNulls);
  }

  private VectorizedRowBatch makeStringBatch2In1Out() {
    VectorizedRowBatch batch = new VectorizedRowBatch(3);
    BytesColumnVector v = new BytesColumnVector();
    batch.cols[0] = v;
    BytesColumnVector v2 = new BytesColumnVector();
    batch.cols[1] = v2;
    batch.cols[2] = new BytesColumnVector();

    v.setRef(0, red, 0, red.length);
    v.isNull[0] = false;
    v.setRef(1, green, 0, green.length);
    v.isNull[1] = false;
    v.setRef(2,  emptyString,  0,  emptyString.length);
    v.isNull[2] = true;
    v.noNulls = false;

    v2.setRef(0, red, 0, red.length);
    v2.isNull[0] = false;
    v2.setRef(1, green, 0, green.length);
    v2.isNull[1] = false;
    v2.setRef(2,  emptyString,  0,  emptyString.length);
    v2.isNull[2] = true;
    v2.noNulls = false;

    batch.size = 3;
    return batch;
  }

  private VectorizedRowBatch makeStringBatchForColColCompare() {
    VectorizedRowBatch batch = new VectorizedRowBatch(4);
    BytesColumnVector v = new BytesColumnVector();
    batch.cols[0] = v;
    BytesColumnVector v2 = new BytesColumnVector();
    batch.cols[1] = v2;
    batch.cols[2] = new BytesColumnVector();
    batch.cols[3] = new LongColumnVector();

    v.setRef(0, blue, 0, blue.length);
    v.isNull[0] = false;
    v.setRef(1, green, 0, green.length);
    v.isNull[1] = false;
    v.setRef(2,  red,  0,  red.length);
    v.isNull[2] = false;
    v.setRef(3, emptyString, 0, emptyString.length);
    v.isNull[3] = true;
    v.noNulls = false;

    v2.setRef(0, red, 0, red.length);
    v2.isNull[0] = false;
    v2.setRef(1, green, 0, green.length);
    v2.isNull[1] = false;
    v2.setRef(2,  blue,  0,  blue.length);
    v2.isNull[2] = false;
    v2.setRef(3, red, 0, red.length);
    v2.isNull[3] = false;
    v2.noNulls = false;

    batch.size = 4;
    return batch;
  }

  @Test
  public void testStringLike() throws HiveException {

    // has nulls, not repeating
    VectorizedRowBatch batch;
    Text pattern;
    int initialBatchSize;
    batch = makeStringBatchMixedCharSize();
    pattern = new Text(mixPercentPattern);
    FilterStringColLikeStringScalar expr = new FilterStringColLikeStringScalar(0, mixPercentPattern);
    expr.transientInit(hiveConf);
    expr.evaluate(batch);

    // verify that the beginning entry is the only one that matches
    Assert.assertEquals(1, batch.size);
    Assert.assertEquals(0, batch.selected[0]);

    // no nulls, not repeating
    batch = makeStringBatchMixedCharSize();
    batch.cols[0].noNulls = true;
    expr.evaluate(batch);

    // verify that the beginning entry is the only one that matches
    Assert.assertEquals(1, batch.size);
    Assert.assertEquals(0, batch.selected[0]);

    // has nulls, is repeating
    batch = makeStringBatchMixedCharSize();
    initialBatchSize = batch.size;
    batch.cols[0].isRepeating = true;
    expr.evaluate(batch);

    // all rows qualify
    Assert.assertEquals(initialBatchSize, batch.size);

    // same, but repeating value is null
    batch = makeStringBatchMixedCharSize();
    batch.cols[0].isRepeating = true;
    batch.cols[0].isNull[0] = true;
    expr.evaluate(batch);

    // no rows qualify
    Assert.assertEquals(0, batch.size);

    // no nulls, is repeating
    batch = makeStringBatchMixedCharSize();
    initialBatchSize = batch.size;
    batch.cols[0].isRepeating = true;
    batch.cols[0].noNulls = true;
    expr.evaluate(batch);

    // all rows qualify
    Assert.assertEquals(initialBatchSize, batch.size);
  }

  @Test
  public void testStringLikePatternType() throws HiveException {
    FilterStringColLikeStringScalar expr;
    VectorizedRowBatch vrb = VectorizedRowGroupGenUtil.getVectorizedRowBatch(1, 1, 1);
    vrb.cols[0] = new BytesColumnVector(1);
    BytesColumnVector bcv = (BytesColumnVector) vrb.cols[0];
    vrb.size = 0;

    // BEGIN pattern
    expr = new FilterStringColLikeStringScalar(0, "abc%".getBytes());
    expr.transientInit(hiveConf);
    expr.evaluate(vrb);
    Assert.assertEquals(FilterStringColLikeStringScalar.BeginChecker.class,
        expr.checker.getClass());

    expr = new FilterStringColLikeStringScalar(0, "abc\\%def%".getBytes());
    expr.transientInit(hiveConf);
    expr.evaluate(vrb);
    Assert.assertEquals(FilterStringColLikeStringScalar.BeginChecker.class,
        expr.checker.getClass());

    // END pattern
    expr = new FilterStringColLikeStringScalar(0, "%abc".getBytes(StandardCharsets.UTF_8));
    expr.transientInit(hiveConf);
    expr.evaluate(vrb);
    Assert.assertEquals(FilterStringColLikeStringScalar.EndChecker.class,
        expr.checker.getClass());

    expr = new FilterStringColLikeStringScalar(0, "%abc\\%def".getBytes(StandardCharsets.UTF_8));
    expr.transientInit(hiveConf);
    expr.evaluate(vrb);
    Assert.assertEquals(FilterStringColLikeStringScalar.EndChecker.class,
        expr.checker.getClass());

    // MIDDLE pattern
    expr = new FilterStringColLikeStringScalar(0, "%abc%".getBytes());
    expr.transientInit(hiveConf);
    expr.evaluate(vrb);
    Assert.assertEquals(FilterStringColLikeStringScalar.MiddleChecker.class,
        expr.checker.getClass());

    expr = new FilterStringColLikeStringScalar(0, "%abc\\%def%".getBytes());
    expr.transientInit(hiveConf);
    expr.evaluate(vrb);
    Assert.assertEquals(FilterStringColLikeStringScalar.MiddleChecker.class,
        expr.checker.getClass());

    // CHAIN pattern
    expr = new FilterStringColLikeStringScalar(0, "%abc%de".getBytes());
    expr.transientInit(hiveConf);
    expr.evaluate(vrb);
    Assert.assertEquals(FilterStringColLikeStringScalar.ChainedChecker.class,
        expr.checker.getClass());

    // COMPLEX pattern
    expr = new FilterStringColLikeStringScalar(0, "%abc_%de".getBytes());
    expr.transientInit(hiveConf);
    expr.evaluate(vrb);
    Assert.assertEquals(FilterStringColLikeStringScalar.ComplexChecker.class,
        expr.checker.getClass());

    expr = new FilterStringColLikeStringScalar(0, "abc_".getBytes());
    expr.transientInit(hiveConf);
    expr.evaluate(vrb);
    Assert.assertEquals(FilterStringColLikeStringScalar.ComplexChecker.class,
        expr.checker.getClass());

    expr = new FilterStringColLikeStringScalar(0, "abc\\_def_".getBytes());
    expr.transientInit(hiveConf);
    expr.evaluate(vrb);
    Assert.assertEquals(FilterStringColLikeStringScalar.ComplexChecker.class,
        expr.checker.getClass());

    expr = new FilterStringColLikeStringScalar(0, "_abc".getBytes(StandardCharsets.UTF_8));
    expr.transientInit(hiveConf);
    expr.evaluate(vrb);
    Assert.assertEquals(FilterStringColLikeStringScalar.ComplexChecker.class,
        expr.checker.getClass());

    expr = new FilterStringColLikeStringScalar(0, "_abc\\_def".getBytes(StandardCharsets.UTF_8));
    expr.transientInit(hiveConf);
    expr.evaluate(vrb);
    Assert.assertEquals(FilterStringColLikeStringScalar.ComplexChecker.class,
        expr.checker.getClass());

    expr = new FilterStringColLikeStringScalar(0, "_abc_".getBytes());
    expr.transientInit(hiveConf);
    expr.evaluate(vrb);
    Assert.assertEquals(FilterStringColLikeStringScalar.ComplexChecker.class,
        expr.checker.getClass());

    expr = new FilterStringColLikeStringScalar(0, "_abc\\_def_".getBytes());
    expr.transientInit(hiveConf);
    expr.evaluate(vrb);
    Assert.assertEquals(FilterStringColLikeStringScalar.ComplexChecker.class,
        expr.checker.getClass());

    expr = new FilterStringColLikeStringScalar(0, "_abc_de".getBytes());
    expr.transientInit(hiveConf);
    expr.evaluate(vrb);
    Assert.assertEquals(FilterStringColLikeStringScalar.ComplexChecker.class,
        expr.checker.getClass());


    expr = new FilterStringColLikeStringScalar(0,
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa_b".getBytes());
    expr.transientInit(hiveConf);
    expr.evaluate(vrb);
    Assert.assertEquals(FilterStringColLikeStringScalar.ComplexChecker.class,
        expr.checker.getClass());

    // NONE pattern
    expr = new FilterStringColLikeStringScalar(0, "abc".getBytes());
    expr.transientInit(hiveConf);
    expr.evaluate(vrb);
    Assert.assertEquals(FilterStringColLikeStringScalar.NoneChecker.class,
        expr.checker.getClass());
  }

  @Test
  public void testStringLikeMultiByte() throws HiveException {
    FilterStringColLikeStringScalar expr;
    VectorizedRowBatch batch;

    // verify that a multi byte LIKE expression matches a matching string
    batch = makeStringBatchMixedCharSize();
    expr = new FilterStringColLikeStringScalar(0, ('%' + new String(multiByte) + '%').getBytes(StandardCharsets.UTF_8));
    expr.transientInit(hiveConf);
    expr.evaluate(batch);
    Assert.assertEquals(1, batch.size);

    // verify that a multi byte LIKE expression doesn't match a non-matching string
    batch = makeStringBatchMixedCharSize();
    expr = new FilterStringColLikeStringScalar(0, ('%' + new String(multiByte) + 'x').getBytes(StandardCharsets.UTF_8));
    expr.transientInit(hiveConf);
    expr.evaluate(batch);
    Assert.assertEquals(0, batch.size);
  }

  private String randomizePattern(Random control, String value) {
    switch (control.nextInt(10)) {
    default:
    case 0: {
      return value;
    }
    case 1: {
      return control.nextInt(1000) + value;
    }
    case 2: {
      return value + control.nextInt(1000);
    }
    case 3: {
      return control.nextInt(1000) + value.substring(1);
    }
    case 4: {
      return value.substring(1) + control.nextInt(1000);
    }
    case 5: {
      return control.nextInt(1000) + value.substring(0, value.length() - 1);
    }
    case 6: {
      return "";
    }
    case 7: {
      return value.toLowerCase();
    }
    case 8: {
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < control.nextInt(12); i++) {
        sb.append((char) ('a' + control.nextInt(26)));
      }
      return sb.toString();
    }
    case 9: {
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < control.nextInt(12); i++) {
        sb.append((char) ('A' + control.nextInt(26)));
      }
      return sb.toString();
    }
    }
  }

  private String generateCandidate(Random control, String pattern) {
    StringBuilder sb = new StringBuilder();
    final StringTokenizer tokens = new StringTokenizer(pattern, "%");
    final boolean leftAnchor = pattern.startsWith("%");
    final boolean rightAnchor = pattern.endsWith("%");
    for (int i = 0; tokens.hasMoreTokens(); i++) {
      String chunk = tokens.nextToken();
      if (leftAnchor && i == 0) {
        // first item
        sb.append(randomizePattern(control, chunk));
      } else if (rightAnchor && tokens.hasMoreTokens() == false) {
        // last item
        sb.append(randomizePattern(control, chunk));
      } else {
        // middle item
        sb.append(randomizePattern(control, chunk));
      }
    }
    return sb.toString();
  }

  @Test
  public void testStringLikeRandomized() throws HiveException {
    final String [] patterns = new String[] {
        "ABC%",
        "%ABC",
        "%ABC%",
        "ABC%DEF",
        "ABC%DEF%",
        "%ABC%DEF",
        "%ABC%DEF%",
        "ABC%DEF%EFG",
        "%ABC%DEF%EFG",
        "%ABC%DEF%EFG%H",
    };
    long positive = 0;
    long negative = 0;
    Random control = new Random(1234);
    UDFLike udf = new UDFLike();
    for (String pattern : patterns) {
      VectorExpression expr = new FilterStringColLikeStringScalar(0, pattern.getBytes(StandardCharsets.UTF_8));
      expr.transientInit(hiveConf);
      VectorizedRowBatch batch = VectorizedRowGroupGenUtil.getVectorizedRowBatch(1, 1, 1);
      batch.cols[0] = new BytesColumnVector(1);
      BytesColumnVector bcv = (BytesColumnVector) batch.cols[0];

      Text pText = new Text(pattern);
      for (int i=0; i < 1024; i++) {
        String input = generateCandidate(control,pattern);
        BooleanWritable like = udf.evaluate(new Text(input), pText);
        batch.reset();
        bcv.initBuffer();
        byte[] utf8 = input.getBytes(StandardCharsets.UTF_8);
        bcv.setVal(0, utf8, 0, utf8.length);
        bcv.noNulls = true;
        batch.size = 1;
        expr.evaluate(batch);
        if (like.get()) {
          positive++;
        } else {
          negative++;
        }
        assertEquals(String.format("Checking '%s' against '%s'", input, pattern), like.get(), (batch.size != 0));
      }
    }
    LOG.info(String.format("Randomized testing: ran %d positive tests and %d negative tests",
        positive, negative));
  }

  @Test
  public void testColConcatStringScalar() throws HiveException {

    // has nulls, not repeating
    VectorizedRowBatch batch = makeStringBatch();
    StringGroupColConcatStringScalar expr = new StringGroupColConcatStringScalar(0, red, 1);
    expr.evaluate(batch);
    BytesColumnVector outCol = (BytesColumnVector) batch.cols[1];

    int cmp = StringExpr.compare(redred, 0, redred.length, outCol.vector[0],
        outCol.start[0], outCol.length[0]);
    Assert.assertEquals(0, cmp);
    Assert.assertTrue(outCol.isNull[2]);
    int cmp2 = StringExpr.compare(greenred, 0, greenred.length, outCol.vector[1],
        outCol.start[1], outCol.length[1]);
    Assert.assertEquals(0, cmp2);
    Assert.assertFalse(outCol.noNulls);
    Assert.assertFalse(outCol.isRepeating);

    // no nulls, not repeating
    batch = makeStringBatch();
    batch.cols[0].noNulls = true;
    expr.evaluate(batch);
    outCol = (BytesColumnVector) batch.cols[1];
    cmp = StringExpr.compare(redred, 0, redred.length, outCol.vector[0],
        outCol.start[0], outCol.length[0]);
    Assert.assertEquals(0, cmp);

    cmp2 = StringExpr.compare(greenred, 0, greenred.length, outCol.vector[1],
        outCol.start[1], outCol.length[1]);
    Assert.assertEquals(0, cmp2);

    int cmp3 = StringExpr.compare(red, 0, red.length, outCol.vector[2],
        outCol.start[2], outCol.length[2]);
    Assert.assertEquals(0, cmp3);

    Assert.assertTrue(outCol.noNulls);
    Assert.assertFalse(outCol.isRepeating);

    // has nulls, is repeating
    batch = makeStringBatch();
    batch.cols[0].isRepeating = true;
    expr.evaluate(batch);
    outCol = (BytesColumnVector) batch.cols[1];
    cmp = StringExpr.compare(redred, 0, redred.length, outCol.vector[0],
        outCol.start[0], outCol.length[0]);
    Assert.assertEquals(0, cmp);
    Assert.assertTrue(outCol.isRepeating);

    // no nulls, is repeating
    batch = makeStringBatch();
    batch.cols[0].isRepeating = true;
    batch.cols[0].noNulls = true;
    expr.evaluate(batch);
    outCol = (BytesColumnVector) batch.cols[1];
    cmp = StringExpr.compare(redred, 0, redred.length, outCol.vector[0],
        outCol.start[0], outCol.length[0]);
    Assert.assertEquals(0, cmp);
    Assert.assertTrue(outCol.isRepeating);
    Assert.assertTrue(outCol.noNulls);
  }

  @Test
  public void testColConcatCharScalar() throws HiveException {

    // has nulls, not repeating
    VectorizedRowBatch batch = makeStringBatch();
    StringGroupColConcatStringScalar expr =
        new StringGroupColConcatStringScalar(
            0, new HiveChar(new String(red), 6).getStrippedValue().getBytes(), 1);
    expr.evaluate(batch);
    BytesColumnVector outCol = (BytesColumnVector) batch.cols[1];

    int cmp = StringExpr.compare(redred, 0, redred.length, outCol.vector[0],
        outCol.start[0], outCol.length[0]);
    Assert.assertEquals(0, cmp);
    Assert.assertTrue(outCol.isNull[2]);
    int cmp2 = StringExpr.compare(greenred, 0, greenred.length, outCol.vector[1],
        outCol.start[1], outCol.length[1]);
    Assert.assertEquals(0, cmp2);
    Assert.assertFalse(outCol.noNulls);
    Assert.assertFalse(outCol.isRepeating);

    // no nulls, not repeating
    batch = makeStringBatch();
    batch.cols[0].noNulls = true;
    expr.evaluate(batch);
    outCol = (BytesColumnVector) batch.cols[1];
    cmp = StringExpr.compare(redred, 0, redred.length, outCol.vector[0],
        outCol.start[0], outCol.length[0]);
    Assert.assertEquals(0, cmp);

    cmp2 = StringExpr.compare(greenred, 0, greenred.length, outCol.vector[1],
        outCol.start[1], outCol.length[1]);
    Assert.assertEquals(0, cmp2);

    int cmp3 = StringExpr.compare(red, 0, red.length, outCol.vector[2],
        outCol.start[2], outCol.length[2]);
    Assert.assertEquals(0, cmp3);

    Assert.assertTrue(outCol.noNulls);
    Assert.assertFalse(outCol.isRepeating);

    // has nulls, is repeating
    batch = makeStringBatch();
    batch.cols[0].isRepeating = true;
    expr.evaluate(batch);
    outCol = (BytesColumnVector) batch.cols[1];
    cmp = StringExpr.compare(redred, 0, redred.length, outCol.vector[0],
        outCol.start[0], outCol.length[0]);
    Assert.assertEquals(0, cmp);
    Assert.assertTrue(outCol.isRepeating);

    // no nulls, is repeating
    batch = makeStringBatch();
    batch.cols[0].isRepeating = true;
    batch.cols[0].noNulls = true;
    expr.evaluate(batch);
    outCol = (BytesColumnVector) batch.cols[1];
    cmp = StringExpr.compare(redred, 0, redred.length, outCol.vector[0],
        outCol.start[0], outCol.length[0]);
    Assert.assertEquals(0, cmp);
    Assert.assertTrue(outCol.isRepeating);
    Assert.assertTrue(outCol.noNulls);
  }

  @Test
  public void testColConcatVarCharScalar() throws HiveException {

    // has nulls, not repeating
    VectorizedRowBatch batch = makeStringBatch();
    StringGroupColConcatStringScalar expr =
        new StringGroupColConcatStringScalar(
            0, new HiveVarchar(new String(red), 14).getValue().getBytes(), 1);
    expr.evaluate(batch);
    BytesColumnVector outCol = (BytesColumnVector) batch.cols[1];

    int cmp = StringExpr.compare(redred, 0, redred.length, outCol.vector[0],
        outCol.start[0], outCol.length[0]);
    Assert.assertEquals(0, cmp);
    Assert.assertTrue(outCol.isNull[2]);
    int cmp2 = StringExpr.compare(greenred, 0, greenred.length, outCol.vector[1],
        outCol.start[1], outCol.length[1]);
    Assert.assertEquals(0, cmp2);
    Assert.assertFalse(outCol.noNulls);
    Assert.assertFalse(outCol.isRepeating);

    // no nulls, not repeating
    batch = makeStringBatch();
    batch.cols[0].noNulls = true;
    expr.evaluate(batch);
    outCol = (BytesColumnVector) batch.cols[1];
    cmp = StringExpr.compare(redred, 0, redred.length, outCol.vector[0],
        outCol.start[0], outCol.length[0]);
    Assert.assertEquals(0, cmp);

    cmp2 = StringExpr.compare(greenred, 0, greenred.length, outCol.vector[1],
        outCol.start[1], outCol.length[1]);
    Assert.assertEquals(0, cmp2);

    int cmp3 = StringExpr.compare(red, 0, red.length, outCol.vector[2],
        outCol.start[2], outCol.length[2]);
    Assert.assertEquals(0, cmp3);

    Assert.assertTrue(outCol.noNulls);
    Assert.assertFalse(outCol.isRepeating);

    // has nulls, is repeating
    batch = makeStringBatch();
    batch.cols[0].isRepeating = true;
    expr.evaluate(batch);
    outCol = (BytesColumnVector) batch.cols[1];
    cmp = StringExpr.compare(redred, 0, redred.length, outCol.vector[0],
        outCol.start[0], outCol.length[0]);
    Assert.assertEquals(0, cmp);
    Assert.assertTrue(outCol.isRepeating);

    // no nulls, is repeating
    batch = makeStringBatch();
    batch.cols[0].isRepeating = true;
    batch.cols[0].noNulls = true;
    expr.evaluate(batch);
    outCol = (BytesColumnVector) batch.cols[1];
    cmp = StringExpr.compare(redred, 0, redred.length, outCol.vector[0],
        outCol.start[0], outCol.length[0]);
    Assert.assertEquals(0, cmp);
    Assert.assertTrue(outCol.isRepeating);
    Assert.assertTrue(outCol.noNulls);
  }

  @Test
  public void testStringScalarConcatCol() throws HiveException {

    // has nulls, not repeating
    VectorizedRowBatch batch = makeStringBatch();
    StringScalarConcatStringGroupCol expr = new StringScalarConcatStringGroupCol(red, 0, 1);
    expr.evaluate(batch);
    BytesColumnVector outCol = (BytesColumnVector) batch.cols[1];

    int cmp = StringExpr.compare(redred, 0, redred.length, outCol.vector[0],
        outCol.start[0], outCol.length[0]);
    Assert.assertEquals(0, cmp);
    Assert.assertTrue(outCol.isNull[2]);
    int cmp2 = StringExpr.compare(redgreen, 0, redgreen.length, outCol.vector[1],
        outCol.start[1], outCol.length[1]);
    Assert.assertEquals(0, cmp2);
    Assert.assertFalse(outCol.noNulls);
    Assert.assertFalse(outCol.isRepeating);

    // no nulls, not repeating
    batch = makeStringBatch();
    batch.cols[0].noNulls = true;
    expr.evaluate(batch);
    outCol = (BytesColumnVector) batch.cols[1];
    cmp = StringExpr.compare(redred, 0, redred.length, outCol.vector[0],
        outCol.start[0], outCol.length[0]);
    Assert.assertEquals(0, cmp);

    cmp2 = StringExpr.compare(redgreen, 0, redgreen.length, outCol.vector[1],
        outCol.start[1], outCol.length[1]);
    Assert.assertEquals(0, cmp2);

    int cmp3 = StringExpr.compare(red, 0, red.length, outCol.vector[2],
        outCol.start[2], outCol.length[2]);
    Assert.assertEquals(0, cmp3);

    Assert.assertTrue(outCol.noNulls);
    Assert.assertFalse(outCol.isRepeating);

    // has nulls, is repeating
    batch = makeStringBatch();
    batch.cols[0].isRepeating = true;
    expr.evaluate(batch);
    outCol = (BytesColumnVector) batch.cols[1];
    cmp = StringExpr.compare(redred, 0, redred.length, outCol.vector[0],
        outCol.start[0], outCol.length[0]);
    Assert.assertEquals(0, cmp);
    Assert.assertTrue(outCol.isRepeating);

    // no nulls, is repeating
    batch = makeStringBatch();
    batch.cols[0].isRepeating = true;
    batch.cols[0].noNulls = true;
    expr.evaluate(batch);
    outCol = (BytesColumnVector) batch.cols[1];
    cmp = StringExpr.compare(redred, 0, redred.length, outCol.vector[0],
        outCol.start[0], outCol.length[0]);
    Assert.assertEquals(0, cmp);
    Assert.assertTrue(outCol.isRepeating);
    Assert.assertTrue(outCol.noNulls);
  }

  @Test
  public void testCharScalarConcatCol() throws HiveException {

    // has nulls, not repeating
    VectorizedRowBatch batch = makeStringBatch();
    StringScalarConcatStringGroupCol expr =
        new StringScalarConcatStringGroupCol(
            new HiveChar(new String(red), 6).getStrippedValue().getBytes(), 0, 1);
    expr.evaluate(batch);
    BytesColumnVector outCol = (BytesColumnVector) batch.cols[1];

    int cmp = StringExpr.compare(redred, 0, redred.length, outCol.vector[0],
        outCol.start[0], outCol.length[0]);
    Assert.assertEquals(0, cmp);
    Assert.assertTrue(outCol.isNull[2]);
    int cmp2 = StringExpr.compare(redgreen, 0, redgreen.length, outCol.vector[1],
        outCol.start[1], outCol.length[1]);
    Assert.assertEquals(0, cmp2);
    Assert.assertFalse(outCol.noNulls);
    Assert.assertFalse(outCol.isRepeating);

    // no nulls, not repeating
    batch = makeStringBatch();
    batch.cols[0].noNulls = true;
    expr.evaluate(batch);
    outCol = (BytesColumnVector) batch.cols[1];
    cmp = StringExpr.compare(redred, 0, redred.length, outCol.vector[0],
        outCol.start[0], outCol.length[0]);
    Assert.assertEquals(0, cmp);

    cmp2 = StringExpr.compare(redgreen, 0, redgreen.length, outCol.vector[1],
        outCol.start[1], outCol.length[1]);
    Assert.assertEquals(0, cmp2);

    int cmp3 = StringExpr.compare(red, 0, red.length, outCol.vector[2],
        outCol.start[2], outCol.length[2]);
    Assert.assertEquals(0, cmp3);

    Assert.assertTrue(outCol.noNulls);
    Assert.assertFalse(outCol.isRepeating);

    // has nulls, is repeating
    batch = makeStringBatch();
    batch.cols[0].isRepeating = true;
    expr.evaluate(batch);
    outCol = (BytesColumnVector) batch.cols[1];
    cmp = StringExpr.compare(redred, 0, redred.length, outCol.vector[0],
        outCol.start[0], outCol.length[0]);
    Assert.assertEquals(0, cmp);
    Assert.assertTrue(outCol.isRepeating);

    // no nulls, is repeating
    batch = makeStringBatch();
    batch.cols[0].isRepeating = true;
    batch.cols[0].noNulls = true;
    expr.evaluate(batch);
    outCol = (BytesColumnVector) batch.cols[1];
    cmp = StringExpr.compare(redred, 0, redred.length, outCol.vector[0],
        outCol.start[0], outCol.length[0]);
    Assert.assertEquals(0, cmp);
    Assert.assertTrue(outCol.isRepeating);
    Assert.assertTrue(outCol.noNulls);
  }

  @Test
  public void testVarCharScalarConcatCol() throws HiveException {

    // has nulls, not repeating
    VectorizedRowBatch batch = makeStringBatch();
    StringScalarConcatStringGroupCol expr =
        new StringScalarConcatStringGroupCol(
            new HiveVarchar(new String(red), 14).getValue().getBytes(), 0, 1);
    expr.evaluate(batch);
    BytesColumnVector outCol = (BytesColumnVector) batch.cols[1];

    int cmp = StringExpr.compare(redred, 0, redred.length, outCol.vector[0],
        outCol.start[0], outCol.length[0]);
    Assert.assertEquals(0, cmp);
    Assert.assertTrue(outCol.isNull[2]);
    int cmp2 = StringExpr.compare(redgreen, 0, redgreen.length, outCol.vector[1],
        outCol.start[1], outCol.length[1]);
    Assert.assertEquals(0, cmp2);
    Assert.assertFalse(outCol.noNulls);
    Assert.assertFalse(outCol.isRepeating);

    // no nulls, not repeating
    batch = makeStringBatch();
    batch.cols[0].noNulls = true;
    expr.evaluate(batch);
    outCol = (BytesColumnVector) batch.cols[1];
    cmp = StringExpr.compare(redred, 0, redred.length, outCol.vector[0],
        outCol.start[0], outCol.length[0]);
    Assert.assertEquals(0, cmp);

    cmp2 = StringExpr.compare(redgreen, 0, redgreen.length, outCol.vector[1],
        outCol.start[1], outCol.length[1]);
    Assert.assertEquals(0, cmp2);

    int cmp3 = StringExpr.compare(red, 0, red.length, outCol.vector[2],
        outCol.start[2], outCol.length[2]);
    Assert.assertEquals(0, cmp3);

    Assert.assertTrue(outCol.noNulls);
    Assert.assertFalse(outCol.isRepeating);

    // has nulls, is repeating
    batch = makeStringBatch();
    batch.cols[0].isRepeating = true;
    expr.evaluate(batch);
    outCol = (BytesColumnVector) batch.cols[1];
    cmp = StringExpr.compare(redred, 0, redred.length, outCol.vector[0],
        outCol.start[0], outCol.length[0]);
    Assert.assertEquals(0, cmp);
    Assert.assertTrue(outCol.isRepeating);

    // no nulls, is repeating
    batch = makeStringBatch();
    batch.cols[0].isRepeating = true;
    batch.cols[0].noNulls = true;
    expr.evaluate(batch);
    outCol = (BytesColumnVector) batch.cols[1];
    cmp = StringExpr.compare(redred, 0, redred.length, outCol.vector[0],
        outCol.start[0], outCol.length[0]);
    Assert.assertEquals(0, cmp);
    Assert.assertTrue(outCol.isRepeating);
    Assert.assertTrue(outCol.noNulls);
  }

  @Test
  public void testColConcatCol() throws HiveException {

    // has nulls, not repeating
    VectorizedRowBatch batch = makeStringBatch2In1Out();
    StringGroupConcatColCol expr = new StringGroupConcatColCol(0, 1, 2);
    expr.evaluate(batch);
    BytesColumnVector outCol = (BytesColumnVector) batch.cols[2];

    int cmp = StringExpr.compare(redred, 0, redred.length, outCol.vector[0],
        outCol.start[0], outCol.length[0]);
    Assert.assertEquals(0, cmp);
    Assert.assertTrue(outCol.isNull[2]);
    int cmp2 = StringExpr.compare(greengreen, 0, greengreen.length, outCol.vector[1],
        outCol.start[1], outCol.length[1]);
    Assert.assertEquals(0, cmp2);
    Assert.assertFalse(outCol.noNulls);
    Assert.assertFalse(outCol.isRepeating);

    // no nulls, not repeating
    batch = makeStringBatch2In1Out();
    batch.cols[0].noNulls = true;
    batch.cols[1].noNulls = true;
    expr.evaluate(batch);
    outCol = (BytesColumnVector) batch.cols[2];

    cmp = StringExpr.compare(redred, 0, redred.length, outCol.vector[0],
        outCol.start[0], outCol.length[0]);
    Assert.assertEquals(0, cmp);

    cmp2 = StringExpr.compare(greengreen, 0, greengreen.length, outCol.vector[1],
        outCol.start[1], outCol.length[1]);
    Assert.assertEquals(0, cmp2);

    int cmp3 = StringExpr.compare(emptyString, 0, emptyString.length,
        outCol.vector[2], outCol.start[2], outCol.length[2]);
    Assert.assertEquals(0, cmp3);

    Assert.assertTrue(outCol.noNulls);
    Assert.assertFalse(outCol.isRepeating);

    // has nulls, is repeating

    batch = makeStringBatch2In1Out();
    batch.cols[0].isRepeating = true;                  // only left input repeating
    batch.cols[0].isNull[0] = true;
    expr.evaluate(batch);
    outCol = (BytesColumnVector) batch.cols[2];

    Assert.assertEquals(3, batch.size);
    Assert.assertEquals(true, outCol.isRepeating);
    Assert.assertEquals(true, outCol.isNull[0]);

       // same, but repeating input is not null

    batch = makeStringBatch2In1Out();
    batch.cols[0].isRepeating = true;
    expr.evaluate(batch);
    outCol = (BytesColumnVector) batch.cols[2];
    Assert.assertEquals(false, outCol.isRepeating);  //TEST FAILED
    cmp = StringExpr.compare(redred, 0, redred.length, outCol.vector[0],
        outCol.start[0], outCol.length[0]);
    Assert.assertEquals(0, cmp);
    Assert.assertEquals(true, outCol.isNull[2]);

    batch = makeStringBatch2In1Out();
    batch.cols[1].isRepeating = true;                  // only right input repeating
    batch.cols[1].isNull[0] = true;
    expr.evaluate(batch);
    outCol = (BytesColumnVector) batch.cols[2];

    Assert.assertEquals(3, batch.size);
    Assert.assertEquals(true, outCol.isRepeating);
    Assert.assertEquals(true, outCol.isNull[0]);

    batch = makeStringBatch2In1Out();
    batch.cols[0].isRepeating = true;                  // both inputs repeat
    batch.cols[0].isNull[0] = true;
    batch.cols[1].isRepeating = true;
    batch.cols[1].isNull[0] = true;
    expr.evaluate(batch);
    outCol = (BytesColumnVector) batch.cols[2];

    Assert.assertEquals(3, batch.size);
    Assert.assertEquals(true, outCol.isRepeating);
    Assert.assertEquals(true, outCol.isNull[0]);

    // no nulls, is repeating
    batch = makeStringBatch2In1Out();
    batch.cols[1].isRepeating = true;             // only right input repeating and has no nulls
    batch.cols[1].noNulls = true;
    expr.evaluate(batch);
    outCol = (BytesColumnVector) batch.cols[2];

    Assert.assertEquals(3, batch.size);
    Assert.assertEquals(false, outCol.isRepeating);
    Assert.assertEquals(false, outCol.isNull[0]);
    Assert.assertEquals(false, outCol.noNulls);
    Assert.assertEquals(true, outCol.isNull[2]);
    cmp = StringExpr.compare(greenred, 0, greenred.length, outCol.vector[1],
        outCol.start[1], outCol.length[1]);
    Assert.assertEquals(0, cmp);

         // try again with left input also having no nulls
    batch.cols[0].noNulls = true;
    expr.evaluate(batch);
    Assert.assertEquals(false, outCol.isRepeating);
    cmp = StringExpr.compare(red, 0, red.length, outCol.vector[2],
        outCol.start[2], outCol.length[2]);
    Assert.assertEquals(0, cmp);

    batch = makeStringBatch2In1Out();
    batch.cols[0].isRepeating = true;             // only left input repeating and has no nulls
    batch.cols[0].noNulls = true;
    expr.evaluate(batch);
    outCol = (BytesColumnVector) batch.cols[2];

    Assert.assertEquals(3, batch.size);
    Assert.assertEquals(false, outCol.isRepeating);
    Assert.assertEquals(false, outCol.isNull[0]);
    Assert.assertEquals(false, outCol.noNulls);
    Assert.assertEquals(true, outCol.isNull[2]);
    cmp = StringExpr.compare(redgreen, 0, redgreen.length, outCol.vector[1],
        outCol.start[1], outCol.length[1]);
    Assert.assertEquals(0, cmp);

    batch = makeStringBatch2In1Out();
    batch.cols[0].isRepeating = true;                  // both inputs repeat
    batch.cols[0].noNulls = true;
    batch.cols[1].isRepeating = true;
    batch.cols[1].noNulls = true;
    expr.evaluate(batch);
    outCol = (BytesColumnVector) batch.cols[2];

    Assert.assertEquals(3, batch.size);
    Assert.assertEquals(true, outCol.isRepeating);
    Assert.assertEquals(false, outCol.isNull[0]);
    cmp = StringExpr.compare(redred, 0, redred.length, outCol.vector[0],
        outCol.start[0], outCol.length[0]);
    Assert.assertEquals(0, cmp);
  }

  @Test
  public void testSubstrStart() throws HiveException {
    // Testing no nulls and no repeating
    VectorizedRowBatch batch = new VectorizedRowBatch(2);
    BytesColumnVector v = new BytesColumnVector();
    batch.cols[0] = v;
    BytesColumnVector outV = new BytesColumnVector();
    batch.cols[1] = outV;
    byte[] data1 = "abcd string".getBytes(StandardCharsets.UTF_8);
    byte[] data2 = "efgh string".getBytes(StandardCharsets.UTF_8);
    byte[] data3 = "efgh".getBytes(StandardCharsets.UTF_8);
    batch.size = 3;
    v.noNulls = true;
    v.setRef(0, data1, 0, data1.length);
    v.isNull[0] = false;
    v.setRef(1, data2, 0, data2.length);
    v.isNull[1] = false;
    v.setRef(2, data3, 0, data3.length);
    v.isNull[2] = false;

    StringSubstrColStart expr = new StringSubstrColStart(0, 6, 1);
    expr.evaluate(batch);
    BytesColumnVector outCol = (BytesColumnVector) batch.cols[1];
    Assert.assertEquals(3, batch.size);
    Assert.assertTrue(outCol.noNulls);
    Assert.assertFalse(outCol.isRepeating);
    byte[] expected = "string".getBytes(StandardCharsets.UTF_8);
    Assert.assertEquals(0,
    StringExpr.compare(
            expected, 0, expected.length, outCol.vector[0], outCol.start[0], outCol.length[0]
        )
    );

    Assert.assertEquals(0,
    StringExpr.compare(
            expected, 0, expected.length, outCol.vector[1], outCol.start[1], outCol.length[1]
        )
    );

    // This yields empty because starting idx is out of bounds.
    Assert.assertEquals(0,
    StringExpr.compare(
            emptyString, 0, emptyString.length, outCol.vector[2], outCol.start[2], outCol.length[2]
        )
    );

    outCol.noNulls = false;
    outCol.isRepeating = true;

    // Testing negative substring index.
    // Start index -6 should yield the last 6 characters of the string

    expr = new StringSubstrColStart(0, -6, 1);
    expr.evaluate(batch);
    outCol = (BytesColumnVector) batch.cols[1];
    Assert.assertEquals(3, batch.size);
    Assert.assertFalse(outCol.isRepeating);
    Assert.assertEquals(0,
    StringExpr.compare(
            expected, 0, expected.length, outCol.vector[0], outCol.start[0], outCol.length[0]
        )
    );

    Assert.assertEquals(0,
    StringExpr.compare(
            expected, 0, expected.length, outCol.vector[1], outCol.start[1], outCol.length[1]
        )
    );

    Assert.assertEquals(0,
    StringExpr.compare(
            emptyString, 0, emptyString.length, outCol.vector[2], outCol.start[2], outCol.length[2]
        )
    );

    outCol.noNulls = false;
    outCol.isRepeating = true;

    // Testing substring starting from index 1

    expr = new StringSubstrColStart(0, 1, 1);
    expr.evaluate(batch);
    Assert.assertEquals(3, batch.size);
    Assert.assertFalse(outCol.isRepeating);

    Assert.assertEquals(0,
    StringExpr.compare(
            data1, 0, data1.length, outCol.vector[0], outCol.start[0], outCol.length[0]
        )
    );

    Assert.assertEquals(0,
    StringExpr.compare(
            data2, 0, data2.length, outCol.vector[1], outCol.start[1], outCol.length[1]
        )
    );

    Assert.assertEquals(0,
    StringExpr.compare(
            data3, 0, data3.length, outCol.vector[2], outCol.start[2], outCol.length[2]
        )
    );

    outV.noNulls = false;
    outV.isRepeating = true;

    // Testing with nulls

    expr = new StringSubstrColStart(0, 6, 1);
    v.noNulls = false;
    v.isNull[0] = true;
    expr.evaluate(batch);
    Assert.assertEquals(3, batch.size);
    Assert.assertFalse(outV.noNulls);
    Assert.assertTrue(outV.isNull[0]);

    Assert.assertEquals(0,
    StringExpr.compare(
            expected, 0, expected.length, outCol.vector[1], outCol.start[1], outCol.length[1]
        )
    );

    Assert.assertEquals(0,
    StringExpr.compare(
            emptyString, 0, emptyString.length, outCol.vector[2], outCol.start[2], outCol.length[2]
        )
    );

    outCol.noNulls = false;
    outCol.isRepeating = false;

    // Testing with repeating and no nulls

    outV = new BytesColumnVector();
    v = new BytesColumnVector();
    v.isRepeating = true;
    v.noNulls = true;
    v.setRef(0, data1, 0, data1.length);
    batch = new VectorizedRowBatch(2);
    batch.cols[0] = v;
    batch.cols[1] = outV;
    expr.evaluate(batch);
    outCol = (BytesColumnVector) batch.cols[1];
    expected = "string".getBytes(StandardCharsets.UTF_8);
    Assert.assertTrue(outV.isRepeating);
    Assert.assertTrue(outV.noNulls);
    Assert.assertEquals(0,
    StringExpr.compare(
            expected, 0, expected.length, outCol.vector[0], outCol.start[0], outCol.length[0]
        )
    );

    // Testing multiByte string substring

    v = new BytesColumnVector();
    v.isRepeating = false;
    v.noNulls = true;
    v.setRef(0, multiByte, 0, 10);
    batch.cols[0] = v;
    batch.cols[1] = outV;
    outV.isRepeating = true;
    outV.noNulls = false;
    expr = new StringSubstrColStart(0, 3, 1);
    batch.size = 1;
    expr.evaluate(batch);
    outCol = (BytesColumnVector) batch.cols[1];
    Assert.assertFalse(outV.isRepeating);
    Assert.assertEquals(0,
    StringExpr.compare(
            // 3nd char starts from index 3 and total length should be 7 bytes as max is 10
            multiByte, 3, 10 - 3, outCol.vector[0], outCol.start[0], outCol.length[0]
        )
    );


    // Testing multiByte string with reference starting mid array

    v = new BytesColumnVector();
    v.isRepeating = false;
    v.noNulls = true;

    // string is 2 chars long (a 3 byte and a 4 byte char)
    v.setRef(0, multiByte, 3, 7);
    batch.cols[0] = v;
    batch.cols[1] = outV;
    outV.isRepeating = true;
    outV.noNulls = false;
    outCol = (BytesColumnVector) batch.cols[1];
    expr = new StringSubstrColStart(0, 2, 1);
    expr.evaluate(batch);
    Assert.assertFalse(outV.isRepeating);
    Assert.assertEquals(0,
    StringExpr.compare(
            // the result is the last 1 character, which occupies 4 bytes
            multiByte, 6, 4, outCol.vector[0], outCol.start[0], outCol.length[0]
        )
    );
  }

  @Test
  public void testSubstrStartLen() throws HiveException {
    // Testing no nulls and no repeating

    VectorizedRowBatch batch = new VectorizedRowBatch(2);
    BytesColumnVector v = new BytesColumnVector();
    batch.cols[0] = v;
    BytesColumnVector outV = new BytesColumnVector();
    batch.cols[1] = outV;
    byte[] data1 = "abcd string".getBytes(StandardCharsets.UTF_8);
    byte[] data2 = "efgh string".getBytes(StandardCharsets.UTF_8);
    byte[] data3 = "efgh".getBytes(StandardCharsets.UTF_8);
    batch.size = 3;
    v.noNulls = true;
    v.setRef(0, data1, 0, data1.length);
    v.isNull[0] = false;
    v.setRef(1, data2, 0, data2.length);
    v.isNull[1] = false;
    v.setRef(2, data3, 0, data3.length);
    v.isNull[2] = false;

    outV.isRepeating = true;
    outV.noNulls = false;

    StringSubstrColStartLen expr = new StringSubstrColStartLen(0, 6, 6, 1);
    expr.evaluate(batch);
    BytesColumnVector outCol = (BytesColumnVector) batch.cols[1];
    Assert.assertEquals(3, batch.size);
    Assert.assertFalse(outCol.isRepeating);
    byte[] expected = "string".getBytes(StandardCharsets.UTF_8);
    Assert.assertEquals(0,
    StringExpr.compare(
            expected, 0, expected.length, outCol.vector[0], outCol.start[0], outCol.length[0]
        )
    );

    Assert.assertEquals(0,
    StringExpr.compare(
            expected, 0, expected.length, outCol.vector[1], outCol.start[1], outCol.length[1]
        )
    );

    Assert.assertEquals(0,
    StringExpr.compare(
            emptyString, 0, emptyString.length, outCol.vector[2], outCol.start[2], outCol.length[2]
        )
    );

    // Testing negative substring index
    outV.isRepeating = true;
    outV.noNulls = false;

    expr = new StringSubstrColStartLen(0, -6, 6, 1);
    expr.evaluate(batch);
    outCol = (BytesColumnVector) batch.cols[1];
    Assert.assertFalse(outCol.isRepeating);
    Assert.assertEquals(3, batch.size);

    Assert.assertEquals(0,
    StringExpr.compare(
            expected, 0, expected.length, outCol.vector[0], outCol.start[0], outCol.length[0]
        )
    );

    Assert.assertEquals(0,
        StringExpr.compare(
            expected, 0, expected.length, outCol.vector[1], outCol.start[1], outCol.length[1]
        )
    );

    // This yields empty because starting index is out of bounds
    Assert.assertEquals(0,
    StringExpr.compare(
            emptyString, 0, emptyString.length, outCol.vector[2], outCol.start[2], outCol.length[2]
        )
    );

    //Testing substring index starting with 1 and zero length

    outV.isRepeating = true;
    outV.noNulls = false;

    expr = new StringSubstrColStartLen(0, 1, 0, 1);
    outCol = (BytesColumnVector) batch.cols[1];
    expr.evaluate(batch);
    Assert.assertEquals(3, batch.size);
    Assert.assertFalse(outCol.isRepeating);
    Assert.assertEquals(0,
        StringExpr.compare(
            data1, 1, 0, outCol.vector[0], outCol.start[0], outCol.length[0]
        )
    );

    Assert.assertEquals(0,
        StringExpr.compare(
            data2, 1, 0, outCol.vector[1], outCol.start[1], outCol.length[1]
        )
    );

    Assert.assertEquals(0,
        StringExpr.compare(
            data3, 1, 0, outCol.vector[2], outCol.start[2], outCol.length[2]
        )
    );


    //Testing substring index starting with 0 and length equal to array length

    outV.isRepeating = true;
    outV.noNulls = false;

    expr = new StringSubstrColStartLen(0, 0, 11, 1);
    outCol = (BytesColumnVector) batch.cols[1];
    expr.evaluate(batch);
    Assert.assertEquals(3, batch.size);
    Assert.assertFalse(outCol.isRepeating);
    Assert.assertEquals(0,
    StringExpr.compare(
            data1, 0, data1.length, outCol.vector[0], outCol.start[0], outCol.length[0]
        )
    );

    Assert.assertEquals(0,
    StringExpr.compare(
            data2, 0, data2.length, outCol.vector[1], outCol.start[1], outCol.length[1]
        )
    );

    Assert.assertEquals(0,
    StringExpr.compare(
            data3, 0, data3.length, outCol.vector[2], outCol.start[2], outCol.length[2]
        )
    );


    // Testing setting length larger than array length, which should cap to the length itself

    outV.isRepeating = true;
    outV.noNulls = false;

    expr = new StringSubstrColStartLen(0, 6, 10, 1);
    expr.evaluate(batch);
    outCol = (BytesColumnVector) batch.cols[1];
    Assert.assertEquals(3, batch.size);
    Assert.assertFalse(outCol.isRepeating);
    Assert.assertEquals(0,
    StringExpr.compare(
            expected, 0, expected.length, outCol.vector[0], outCol.start[0], outCol.length[0]
        )
    );

    Assert.assertEquals(0,
    StringExpr.compare(
            expected, 0, expected.length, outCol.vector[1], outCol.start[1], outCol.length[1]
        )
    );

    Assert.assertEquals(0,
    StringExpr.compare(
            emptyString, 0, emptyString.length, outCol.vector[2], outCol.start[2], outCol.length[2]
        )
    );

    outV.isRepeating = true;
    outV.noNulls = true;

    // Testing with nulls

    v.noNulls = false;
    v.isNull[0] = true;
    expr.evaluate(batch);
    Assert.assertEquals(3, batch.size);
    Assert.assertFalse(outV.noNulls);
    Assert.assertTrue(outV.isNull[0]);
    Assert.assertFalse(outCol.isRepeating);
    Assert.assertEquals(0,
    StringExpr.compare(
            expected, 0, expected.length, outCol.vector[1], outCol.start[1], outCol.length[1]
        )
    );

    Assert.assertEquals(0,
        StringExpr.compare(
            emptyString, 0, emptyString.length, outCol.vector[2], outCol.start[2], outCol.length[2]
        )
    );


    // Testing with repeating and no nulls
    outV = new BytesColumnVector();
    v = new BytesColumnVector();
    outV.isRepeating = false;
    outV.noNulls = true;
    v.isRepeating = true;
    v.noNulls = false;
    v.setRef(0, data1, 0, data1.length);
    batch = new VectorizedRowBatch(2);
    batch.cols[0] = v;
    batch.cols[1] = outV;
    expr.evaluate(batch);
    outCol = (BytesColumnVector) batch.cols[1];
    Assert.assertTrue(outCol.noNulls);
    Assert.assertTrue(outCol.isRepeating);

    Assert.assertEquals(0,
    StringExpr.compare(
            expected, 0, expected.length, outCol.vector[0], outCol.start[0], outCol.length[0]
        )
    );

    // Testing with multiByte String
    v = new BytesColumnVector();
    v.isRepeating = false;
    v.noNulls = true;
    batch.size = 1;
    v.setRef(0, multiByte, 0, 10);
    batch.cols[0] = v;
    batch.cols[1] = outV;
    outV.isRepeating = true;
    outV.noNulls = false;
    expr = new StringSubstrColStartLen(0, 3, 2, 1);
    expr.evaluate(batch);
    Assert.assertEquals(1, batch.size);
    Assert.assertFalse(outV.isRepeating);
    Assert.assertEquals(0,
    StringExpr.compare(
            // 3rd char starts at index 3, and with length 2 it is covering the rest of the array.
            multiByte, 3, 10 - 3, outCol.vector[0], outCol.start[0], outCol.length[0]
        )
    );

    // Testing multiByte string with reference set to mid array
    v = new BytesColumnVector();
    v.isRepeating = false;
    v.noNulls = true;
    outV = new BytesColumnVector();
    batch.size = 1;
    v.setRef(0, multiByte, 3, 7);
    batch.cols[0] = v;
    batch.cols[1] = outV;
    outV.isRepeating = true;
    outV.noNulls = false;
    expr = new StringSubstrColStartLen(0, 2, 2, 1);
    expr.evaluate(batch);
    outCol = (BytesColumnVector) batch.cols[1];
    Assert.assertEquals(1, batch.size);
    Assert.assertFalse(outV.isRepeating);
    Assert.assertEquals(0,
    StringExpr.compare(
            // 2nd substring index refers to the 6th index (last char in the array)
            multiByte, 6, 10 - 6, outCol.vector[0], outCol.start[0], outCol.length[0]
        )
    );
  }

  @Test
  public void testVectorLTrim() throws HiveException {
    VectorizedRowBatch b = makeTrimBatch();
    VectorExpression expr = new StringLTrimCol(0, 1);
    expr.evaluate(b);
    BytesColumnVector outV = (BytesColumnVector) b.cols[1];
    Assert.assertEquals(0,
        StringExpr.compare(emptyString, 0, 0, outV.vector[0], 0, 0));
    Assert.assertEquals(0,
        StringExpr.compare(blanksLeft, 2, 3, outV.vector[1], outV.start[1], outV.length[1]));
    Assert.assertEquals(0,
        StringExpr.compare(blanksRight, 0, 5, outV.vector[2], outV.start[2], outV.length[2]));
    Assert.assertEquals(0,
        StringExpr.compare(blanksBoth, 2, 5, outV.vector[3], outV.start[3], outV.length[3]));
    Assert.assertEquals(0,
        StringExpr.compare(red, 0, 3, outV.vector[4], outV.start[4], outV.length[4]));
    Assert.assertEquals(0,
        StringExpr.compare(blankString, 0, 0, outV.vector[5], outV.start[5], outV.length[5]));
  }

  @Test
  public void testVectorRTrim() throws HiveException {
    VectorizedRowBatch b = makeTrimBatch();
    VectorExpression expr = new StringRTrimCol(0, 1);
    expr.evaluate(b);
    BytesColumnVector outV = (BytesColumnVector) b.cols[1];
    Assert.assertEquals(0,
        StringExpr.compare(emptyString, 0, 0, outV.vector[0], 0, 0));
    Assert.assertEquals(0,
        StringExpr.compare(blanksLeft, 0, 5, outV.vector[1], outV.start[1], outV.length[1]));
    Assert.assertEquals(0,
        StringExpr.compare(blanksRight, 0, 3, outV.vector[2], outV.start[2], outV.length[2]));
    Assert.assertEquals(0,
        StringExpr.compare(blanksBoth, 0, 5, outV.vector[3], outV.start[3], outV.length[3]));
    Assert.assertEquals(0,
        StringExpr.compare(red, 0, 3, outV.vector[4], outV.start[4], outV.length[4]));
    Assert.assertEquals(0,
        StringExpr.compare(blankString, 0, 0, outV.vector[5], outV.start[5], outV.length[5]));
  }

  @Test
  public void testVectorTrim() throws HiveException {
    VectorizedRowBatch b = makeTrimBatch();
    VectorExpression expr = new StringTrimCol(0, 1);
    expr.evaluate(b);
    BytesColumnVector outV = (BytesColumnVector) b.cols[1];
    Assert.assertEquals(0,
        StringExpr.compare(emptyString, 0, 0, outV.vector[0], 0, 0));
    Assert.assertEquals(0,
        StringExpr.compare(blanksLeft, 2, 3, outV.vector[1], outV.start[1], outV.length[1]));
    Assert.assertEquals(0,
        StringExpr.compare(blanksRight, 0, 3, outV.vector[2], outV.start[2], outV.length[2]));
    Assert.assertEquals(0,
        StringExpr.compare(blanksBoth, 2, 3, outV.vector[3], outV.start[3], outV.length[3]));
    Assert.assertEquals(0,
        StringExpr.compare(red, 0, 3, outV.vector[4], outV.start[4], outV.length[4]));
    Assert.assertEquals(0,
        StringExpr.compare(blankString, 0, 0, outV.vector[5], outV.start[5], outV.length[5]));
  }

  // Make a batch to test the trim functions.
  private VectorizedRowBatch makeTrimBatch() {
    VectorizedRowBatch b = new VectorizedRowBatch(2);
    BytesColumnVector inV = new BytesColumnVector();
    BytesColumnVector outV = new BytesColumnVector();
    b.cols[0] = inV;
    b.cols[1] = outV;
    inV.setRef(0, emptyString, 0, 0);
    inV.setRef(1, blanksLeft, 0, blanksLeft.length);
    inV.setRef(2, blanksRight, 0, blanksRight.length);
    inV.setRef(3, blanksBoth, 0, blanksBoth.length);
    inV.setRef(4, red, 0, red.length);
    inV.setRef(5, blankString, 0, blankString.length);
    b.size = 5;
    return b;
  }

  // Test boolean-valued (non-filter) IN expression for strings
  @Test
  public void testStringInExpr() throws HiveException {

    // test basic operation
    VectorizedRowBatch b = makeStringBatch();
    b.size = 2;
    b.cols[0].noNulls = true;
    byte[][] inVals = new byte[2][];
    inVals[0] = red;
    inVals[1] = blue;
    StringColumnInList expr = new StringColumnInList(0, 2);
    expr.setInListValues(inVals);
    expr.evaluate(b);
    LongColumnVector outV = (LongColumnVector) b.cols[2];
    Assert.assertEquals(1, outV.vector[0]);
    Assert.assertEquals(0, outV.vector[1]);

    // test null input
    b = makeStringBatch();
    b.size = 2;
    b.cols[0].noNulls = false;
    b.cols[0].isNull[0] = true;
    expr.evaluate(b);
    outV = (LongColumnVector) b.cols[2];
    Assert.assertEquals(true, !outV.noNulls && outV.isNull[0] && !outV.isNull[1]);
    Assert.assertEquals(0, outV.vector[1]);

    // test repeating logic
    b = makeStringBatch();
    b.size = 2;
    b.cols[0].noNulls = true;
    b.cols[0].isRepeating = true;
    expr.evaluate(b);
    outV = (LongColumnVector) b.cols[2];
    Assert.assertEquals(1, outV.vector[0]);
    Assert.assertEquals(true, outV.isRepeating);
  }

  /**
   * Test vectorized regex expression.
   */
  @Test
  public void testRegex() throws HiveException {
    VectorizedRowBatch b = makeStringBatch();
    FilterStringColRegExpStringScalar expr = new FilterStringColRegExpStringScalar(0, "a.*".getBytes());
    expr.transientInit(hiveConf);
    b.size = 5;
    b.selectedInUse = false;
    BytesColumnVector v = (BytesColumnVector) b.cols[0];
    v.isRepeating = false;
    v.noNulls = false;
    String s1 = "4kMasVoB7lX1wc5i64bNk";
    String s2 = "a27V63IL7jK3o";
    String s3 = "27V63IL7jK3oa";
    String s4 = "27V63IL7jK3o";
    v.isNull[0] = false;
    v.setRef(0, s1.getBytes(), 0, s1.getBytes().length);
    v.isNull[1] = true;
    v.vector[1] = null;
    v.isNull[2] = false;
    v.setRef(2, s2.getBytes(), 0, s2.getBytes().length);
    v.isNull[3] = false;
    v.setRef(3, s3.getBytes(), 0, s3.getBytes().length);
    v.isNull[4] = false;
    v.setRef(4, s4.getBytes(), 0, s4.getBytes().length);

    expr.evaluate(b);
    Assert.assertTrue(b.selectedInUse);
    Assert.assertEquals(3,b.size);
    Assert.assertEquals(0,b.selected[0]);
    Assert.assertEquals(2,b.selected[1]);
    Assert.assertEquals(3,b.selected[2]);
  }
 }
