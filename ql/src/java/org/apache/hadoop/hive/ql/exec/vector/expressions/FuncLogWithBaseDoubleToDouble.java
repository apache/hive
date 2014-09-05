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


public class FuncLogWithBaseDoubleToDouble extends MathFuncDoubleToDouble
    implements ISetDoubleArg {
  private static final long serialVersionUID = 1L;

  private double base;

  public FuncLogWithBaseDoubleToDouble(double scalarVal, int colNum, int outputColumn) {
    super(colNum, outputColumn);
    this.base = scalarVal;
  }

  public FuncLogWithBaseDoubleToDouble() {
    super();
  }

  @Override
  protected double func(double d) {
    return Math.log(d) / Math.log(base);
  }

  public double getBase() {
    return base;
  }

  public void setBase(double base) {
    this.base = base;
  }

  // used to set the second argument to function (a constant base)
  @Override
  public void setArg(double d) {
    this.base = d;
  }

  @Override
  public VectorExpressionDescriptor.Descriptor getDescriptor() {
    VectorExpressionDescriptor.Builder b = new VectorExpressionDescriptor.Builder();
    b.setMode(VectorExpressionDescriptor.Mode.PROJECTION)
        .setNumArguments(2)
        .setArgumentTypes(
            VectorExpressionDescriptor.ArgumentType.FLOAT_FAMILY,
            VectorExpressionDescriptor.ArgumentType.FLOAT_FAMILY)
        .setInputExpressionTypes(
            VectorExpressionDescriptor.InputExpressionType.SCALAR,
            VectorExpressionDescriptor.InputExpressionType.COLUMN);
    return b.build();
  }
}
