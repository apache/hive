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

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;

/**
 * This is a superclass for binary string functions that operate directly on the
 * input and set the output. First parameter is a column second is a scalar.
 * It is suitable for direct, in-place operations on
 * strings, such as for fast implementations of two parameter version of  TRIM(), LTRIM(), and RTRIM().
 */
abstract public class StringTrimColScalarBase extends StringUnaryUDFDirect {
  private static final long serialVersionUID = 1L;

  protected static final byte[] EMPTY_BYTES = new byte[0];

  private byte[] trimChars;

  public StringTrimColScalarBase(int inputColumn, byte[] trimChars, int outputColumnNum) {
    super(inputColumn, outputColumnNum);
    this.trimChars = trimChars;
  }

  public StringTrimColScalarBase() {
    super();
  }

  protected boolean shouldTrim(int character) {
    return shouldTrimByte((byte) character, trimChars, 0, trimChars.length);
  }

  static boolean shouldTrimByte(byte character, byte[] trimBytes, int trimStart, int trimLen) {
    final int trimEnd = trimStart + trimLen;
    for (int i = trimStart; i < trimEnd; ++i) {
      if (trimBytes[i] == character) {
        return true;
      }
    }
    return false;
  }

  static void trimBoth(BytesColumnVector outV, byte[] bytes, int startIndex, int length,
      byte[] trimBytes, int trimStart, int trimLen, int batchIndex) {
    final int end = startIndex + length;
    int leftIndex = startIndex;
    while (leftIndex < end && shouldTrimByte(bytes[leftIndex], trimBytes, trimStart, trimLen)) {
      leftIndex++;
    }
    if (leftIndex == end) {
      outV.setVal(batchIndex, EMPTY_BYTES, 0, 0);
      return;
    }

    int rightIndex = end - 1;
    final int rightLimit = leftIndex + 1;
    while (rightIndex >= rightLimit && shouldTrimByte(bytes[rightIndex], trimBytes, trimStart, trimLen)) {
      rightIndex--;
    }
    final int resultLength = rightIndex - leftIndex + 1;
    if (resultLength <= 0) {
      throw new RuntimeException("Not expected");
    }
    outV.setVal(batchIndex, bytes, leftIndex, resultLength);
  }

  static void trimLeft(BytesColumnVector outV, byte[] bytes, int startIndex, int length,
      byte[] trimBytes, int trimStart, int trimLen, int batchIndex) {
    final int end = startIndex + length;
    int index = startIndex;
    while (index < end && shouldTrimByte(bytes[index], trimBytes, trimStart, trimLen)) {
      index++;
    }

    final int resultLength = end - index;
    if (resultLength == 0) {
      outV.setVal(batchIndex, EMPTY_BYTES, 0, 0);
      return;
    }
    outV.setVal(batchIndex, bytes, index, resultLength);
  }

  static void trimRight(BytesColumnVector outV, byte[] bytes, int startIndex, int length,
      byte[] trimBytes, int trimStart, int trimLen, int batchIndex) {
    final int start = startIndex;
    int index = startIndex + length - 1;
    while (index >= start && shouldTrimByte(bytes[index], trimBytes, trimStart, trimLen)) {
      index--;
    }

    final int resultLength = index - start + 1;
    if (resultLength == 0) {
      outV.setVal(batchIndex, EMPTY_BYTES, 0, 0);
      return;
    }
    outV.setVal(batchIndex, bytes, start, resultLength);
  }

  @Override
  public VectorExpressionDescriptor.Descriptor getDescriptor() {
    VectorExpressionDescriptor.Builder b = new VectorExpressionDescriptor.Builder();
    b.setMode(VectorExpressionDescriptor.Mode.PROJECTION)
            .setNumArguments(2)
            .setArgumentTypes(
                    VectorExpressionDescriptor.ArgumentType.STRING_FAMILY,
                    VectorExpressionDescriptor.ArgumentType.STRING_FAMILY)
            .setInputExpressionTypes(
                    VectorExpressionDescriptor.InputExpressionType.COLUMN,
                    VectorExpressionDescriptor.InputExpressionType.SCALAR);
    return b.build();
  }
}