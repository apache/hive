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

public class StringTrim extends StringUnaryUDFDirect {
  private static final long serialVersionUID = 1L;

  private static final byte[] EMPTY_BYTES = new byte[0];

  public StringTrim(int inputColumn, int outputColumnNum) {
    super(inputColumn, outputColumnNum);
  }

  public StringTrim() {
    super();
  }

  /**
   * TRIM element i of the vector, eliminating blanks from the left
   * and right sides of the string, and place the result in outV.
   * Operate on the data in place, and set the output by reference
   * to improve performance. Ignore null handling. That will be handled separately.
   */
  protected void func(BytesColumnVector outV, byte[][] vector, int[] start, int[] length,
      int batchIndex) {

    byte[] bytes = vector[batchIndex];
    final int startIndex = start[batchIndex];
    final int end = startIndex + length[batchIndex];
    int leftIndex = startIndex;
    while(leftIndex < end && bytes[leftIndex] == 0x20) {
      leftIndex++;
    }
    if (leftIndex == end) {
      outV.setVal(batchIndex, EMPTY_BYTES, 0, 0);
      return;
    }

    // Have at least 1 non-blank; Skip trailing blank characters.
    int rightIndex = end - 1;
    final int rightLimit = leftIndex + 1;
    while(rightIndex >= rightLimit && bytes[rightIndex] == 0x20) {
      rightIndex--;
    }
    final int resultLength = rightIndex - leftIndex + 1;
    if (resultLength <= 0) {
      throw new RuntimeException("Not expected");
    }
    outV.setVal(batchIndex, bytes, leftIndex, resultLength);
  }
}
