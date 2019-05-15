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

public class CastStringGroupToChar extends StringUnaryUDFDirect implements TruncStringOutput {

  private static final long serialVersionUID = 1L;
  private int maxLength; // Must be manually set with setMaxLength.

  public CastStringGroupToChar(int inputColumn, int outputColumnNum) {
    super(inputColumn, outputColumnNum);
  }

  public CastStringGroupToChar() {
    super();
  }

  /**
   * Do right trim and truncate for CHAR.
   */
  protected void func(BytesColumnVector outV, byte[][] vector, int[] start, int[] length, int i) {
    StringExpr.rightTrimAndTruncate(outV, i, vector[i], start[i], length[i], maxLength);
  }

  @Override
  public int getMaxLength() {
    return maxLength;
  }

  @Override
  public void setMaxLength(int maxLength) {
    this.maxLength = maxLength;
  }

  public String vectorExpressionParameters() {
    return getColumnParamString(0, inputColumn) + ", maxLength " + maxLength;
  }
}
