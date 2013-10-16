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

import org.apache.hadoop.hive.ql.udf.UDFRound;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;

// Vectorized implementation of ROUND(Col, N) function
public class RoundWithNumDigitsDoubleToDouble extends MathFuncDoubleToDouble
    implements ISetLongArg {
  private static final long serialVersionUID = 1L;

  private IntWritable decimalPlaces;
  private transient UDFRound roundFunc;
  private transient DoubleWritable dw;

  public RoundWithNumDigitsDoubleToDouble(int colNum, int outputColumn) {
    super(colNum, outputColumn);
    this.decimalPlaces = new IntWritable();
    roundFunc = new UDFRound();
    dw = new DoubleWritable();
  }

  public RoundWithNumDigitsDoubleToDouble() {
    super();
    dw = new DoubleWritable();
    roundFunc = new UDFRound();
  }

  // Round to the specified number of decimal places using the standard Hive round function.
  @Override
  public double func(double d) {
    dw.set(d);
    return roundFunc.evaluate(dw, decimalPlaces).get();
  }

  void setDecimalPlaces(IntWritable decimalPlaces) {
    this.decimalPlaces = decimalPlaces;
  }

  IntWritable getDecimalPlaces() {
    return this.decimalPlaces;
  }

  void setRoundFunc(UDFRound roundFunc) {
    this.roundFunc = roundFunc;
  }

  UDFRound getRoundFunc() {
    return this.roundFunc;
  }

  @Override
  public void setArg(long l) {
    this.decimalPlaces.set((int) l);
  }
}
