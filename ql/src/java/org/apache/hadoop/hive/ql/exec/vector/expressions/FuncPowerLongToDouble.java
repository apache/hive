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

import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;

/**
 * Vectorized implementation for Pow(a, power) and Power(a, power)
 */
public class FuncPowerLongToDouble extends MathFuncLongToDouble {
  private static final long serialVersionUID = 1L;

  private final double power;

  public FuncPowerLongToDouble(int colNum, double scalarVal, int outputColumnNum) {
    super(colNum, outputColumnNum);
    this.power = scalarVal;
  }

  public FuncPowerLongToDouble() {
    super();

    // Dummy final assignments.
    power = -1;
  }

  @Override
  public double func(long l) {
    return Math.pow((double) l, power);
  }

  public double getPower() {
    return power;
  }

  @Override
  protected void cleanup(DoubleColumnVector outputColVector, int[] sel,
      boolean selectedInUse, int n) {
    // do nothing
  }

  @Override
  public VectorExpressionDescriptor.Descriptor getDescriptor() {
    return (new VectorExpressionDescriptor.Builder())
        .setMode(
            VectorExpressionDescriptor.Mode.PROJECTION)
        .setNumArguments(2)
        .setArgumentTypes(
            VectorExpressionDescriptor.ArgumentType.INT_FAMILY,
            VectorExpressionDescriptor.ArgumentType.FLOAT_FAMILY)
        .setInputExpressionTypes(
            VectorExpressionDescriptor.InputExpressionType.COLUMN,
            VectorExpressionDescriptor.InputExpressionType.SCALAR).build();
  }
}
