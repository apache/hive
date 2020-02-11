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

import org.apache.hadoop.hive.ql.udf.generic.RoundUtils;

// Vectorized implementation of BROUND(Col, N) function
public class BRoundWithNumDigitsDoubleToDouble extends RoundWithNumDigitsDoubleToDouble {
  private static final long serialVersionUID = 18493485928L;

  public BRoundWithNumDigitsDoubleToDouble(int colNum, long scalarVal, int outputColumnNum) {
    super(colNum, scalarVal, outputColumnNum);
  }

  public BRoundWithNumDigitsDoubleToDouble() {
    super();
  }

  // Round to the specified number of decimal places using half-even round function.
  @Override
  public double func(double d) {
    return RoundUtils.bround(d, getDecimalPlaces().get());
  }

}
