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

import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;

/**
 * Type cast decimal to boolean
 */
public class CastDecimalToBoolean extends FuncDecimalToLong {
  private static final long serialVersionUID = 1L;

  public CastDecimalToBoolean() {
    super();
  }

  public CastDecimalToBoolean(int inputColumn, int outputColumnNum) {
    super(inputColumn, outputColumnNum);
  }

  @Override
  /**
   * If the input is 0 (i.e. the signum of the decimal is 0), return 0 for false.
   * Otherwise, return 1 for true.
   */
  protected void func(LongColumnVector outV, DecimalColumnVector inV,  int i) {
    outV.vector[i] = inV.vector[i].signum() == 0 ? 0 : 1;
  }
}
