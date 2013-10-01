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

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;

public class StringTrim extends StringUnaryUDFDirect {
  private static final long serialVersionUID = 1L;

  public StringTrim(int inputColumn, int outputColumn) {
    super(inputColumn, outputColumn);
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
  protected void func(BytesColumnVector outV, byte[][] vector, int[] start, int[] length, int i) {
    int l = start[i];
    int r = start[i] + length[i] - 1;

    // skip blank character on left
    while(l <= r && vector[i][l] == 0x20) {
      l++;
    }

    // skip blank characters on right
    while(l <= r && vector[i][r] == 0x20) {
      r--;
    }

    outV.setVal(i, vector[i], l, (r - l) + 1);
  }
}
