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

import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

public class PosModDoubleToDouble extends MathFuncDoubleToDouble {
  private static final long serialVersionUID = 1L;

  private final double divisor;
  private boolean isOutputTypeFloat;

  public PosModDoubleToDouble(int inputCol, double scalarVal, int outputColumnNum) {
    super(inputCol, outputColumnNum);
    this.divisor = scalarVal;
  }

  public PosModDoubleToDouble() {
    super();

    // Dummy final assignments.
    divisor = 0;
  }

  /**
   * Set type of the output column and also set the flag which determines if cast to float
   * is needed while calculating PosMod expression
   */
  @Override
  public void setOutputTypeInfo(TypeInfo outputTypeInfo) {
    this.outputTypeInfo = outputTypeInfo;
    isOutputTypeFloat = outputTypeInfo != null && serdeConstants.FLOAT_TYPE_NAME
        .equals(outputTypeInfo.getTypeName());
  }

  @Override
  protected double func(double v) {
    // if the outputType is a float cast the arguments to float to replicate the overflow behavior
    // in non-vectorized UDF GenericUDFPosMod
    if (isOutputTypeFloat) {
      float castedV = (float) v;
      float castedDivisor = (float) divisor;
      return ((castedV % castedDivisor) + castedDivisor) % castedDivisor;
    }
    // return positive modulo
    return ((v % divisor) + divisor) % divisor;
  }

  @Override
  public String vectorExpressionParameters() {
    return "col " + inputColumnNum[0] + ", divisor " + divisor;
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
