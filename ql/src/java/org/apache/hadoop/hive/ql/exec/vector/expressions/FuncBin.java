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

/**
 * Vectorized implementation of Bin(long) function that returns string.
 */
public class FuncBin extends FuncLongToString {
  private static final long serialVersionUID = 1L;

  public FuncBin(int inputCol, int outputColumnNum) {
    super(inputCol, outputColumnNum);
  }

  public FuncBin() {
    super();
  }

  @Override
  void prepareResult(int i, long[] vector, BytesColumnVector outV) {
    long num = vector[i];
    // Extract the bits of num into bytes[] from right to left
    int len = 0;
    do {
      len++;
      bytes[bytes.length - len] = (byte) ('0' + (num & 1));
      num >>>= 1;
    } while (num != 0);
    outV.setVal(i, bytes, bytes.length - len, len);
  }
}
