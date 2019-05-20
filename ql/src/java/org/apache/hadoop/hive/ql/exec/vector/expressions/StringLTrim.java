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

public class StringLTrim extends StringUnaryUDFDirect {
  private static final long serialVersionUID = 1L;

  private static final byte[] EMPTY_BYTES = new byte[0];

  public StringLTrim(int inputColumn, int outputColumnNum) {
    super(inputColumn, outputColumnNum);
  }

  public StringLTrim() {
    super();
  }

  /**
   * LTRIM element i of the vector, and place the result in outV.
   * Operate on the data in place, and set the output by reference
   * to improve performance. Ignore null handling. That will be handled separately.
   */
  protected void func(BytesColumnVector outV, byte[][] vector, int[] start, int[] length,
      int batchIndex) {
    byte[] bytes = vector[batchIndex];
    final int startIndex = start[batchIndex];

    // Skip past blank characters.
    final int end = startIndex + length[batchIndex];
    int index = startIndex;
    while(index < end && bytes[index] == 0x20) {
      index++;
    }

    final int resultLength = end - index;
    if (resultLength == 0) {
      outV.setVal(batchIndex, EMPTY_BYTES, 0, 0);
      return;
    }
    outV.setVal(batchIndex, bytes, index, resultLength);
  }
}
