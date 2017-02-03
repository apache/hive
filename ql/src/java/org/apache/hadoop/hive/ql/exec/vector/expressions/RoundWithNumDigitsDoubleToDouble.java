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

import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.udf.generic.RoundUtils;
import org.apache.hadoop.io.IntWritable;

// Vectorized implementation of ROUND(Col, N) function
public class RoundWithNumDigitsDoubleToDouble extends MathFuncDoubleToDouble
    implements ISetLongArg {
  private static final long serialVersionUID = 1L;

  private IntWritable decimalPlaces;

  public RoundWithNumDigitsDoubleToDouble(int colNum, long scalarVal, int outputColumn) {
    super(colNum, outputColumn);
    this.decimalPlaces = new IntWritable();
    decimalPlaces.set((int) scalarVal);
  }

  public RoundWithNumDigitsDoubleToDouble() {
    super();
  }

  // Round to the specified number of decimal places using the standard Hive round function.
  @Override
  public double func(double d) {
    return RoundUtils.round(d, decimalPlaces.get());
  }

  void setDecimalPlaces(IntWritable decimalPlaces) {
    this.decimalPlaces = decimalPlaces;
  }

  public IntWritable getDecimalPlaces() {
    return this.decimalPlaces;
  }

  @Override
  public void setArg(long l) {
    this.decimalPlaces.set((int) l);
  }

  @Override
  public String vectorExpressionParameters() {
    return "col " + colNum + ", decimalPlaces " + decimalPlaces.get();
  }

  @Override
  public VectorExpressionDescriptor.Descriptor getDescriptor() {
    VectorExpressionDescriptor.Builder b = new VectorExpressionDescriptor.Builder();
    b.setMode(VectorExpressionDescriptor.Mode.PROJECTION)
        .setNumArguments(2)
        .setArgumentTypes(
            VectorExpressionDescriptor.ArgumentType.FLOAT_FAMILY,
            VectorExpressionDescriptor.ArgumentType.INT_FAMILY)
        .setInputExpressionTypes(
            VectorExpressionDescriptor.InputExpressionType.COLUMN,
            VectorExpressionDescriptor.InputExpressionType.SCALAR);
    return b.build();
  }
}
