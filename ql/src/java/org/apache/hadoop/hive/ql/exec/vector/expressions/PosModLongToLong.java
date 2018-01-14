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

public class PosModLongToLong extends MathFuncLongToLong {
  private static final long serialVersionUID = 1L;

  private final long divisor;
  private String outputCastType = serdeConstants.BIGINT_TYPE_NAME;

  public PosModLongToLong(int inputCol, long scalarVal, int outputColumnNum) {
    super(inputCol, outputColumnNum);
    this.divisor = scalarVal;
  }

  public PosModLongToLong() {
    super();

    // Dummy final assignments.
    divisor = 0;
  }

  @Override
  protected long func(long v) {
    // pmod calculation can overflow based on the type of arguments
    // casting the arguments according to outputTypeInfo so that the
    // results match with GenericUDFPosMod implementation
    switch (outputCastType) {
    case serdeConstants.TINYINT_TYPE_NAME:
      byte castedByte = (byte) v;
      byte castedDivisorByte = (byte) divisor;
      return ((castedByte % castedDivisorByte) + castedDivisorByte) % castedDivisorByte;

    case serdeConstants.SMALLINT_TYPE_NAME:
      short castedShort = (short) v;
      short castedDivisorShort = (short) divisor;
      return ((castedShort % castedDivisorShort) + castedDivisorShort) % castedDivisorShort;

    case serdeConstants.INT_TYPE_NAME:
      int castedInt = (int) v;
      int castedDivisorInt = (int) divisor;
      return ((castedInt % castedDivisorInt) + castedDivisorInt) % castedDivisorInt;
    default:
      // default is using long types
      return ((v % divisor) + divisor) % divisor;
    }
  }

  @Override
  public void setOutputTypeInfo(TypeInfo outputTypeInfo) {
    this.outputTypeInfo = outputTypeInfo;
    //default outputTypeInfo is long
    if (outputTypeInfo != null) {
      outputCastType = outputTypeInfo.getTypeName();
    }
  }

  @Override
  public String vectorExpressionParameters() {
    return "col " + colNum + ", divisor " + divisor;
  }

  @Override
  public VectorExpressionDescriptor.Descriptor getDescriptor() {
    return (new VectorExpressionDescriptor.Builder())
        .setMode(
            VectorExpressionDescriptor.Mode.PROJECTION)
        .setNumArguments(2)
        .setArgumentTypes(
            VectorExpressionDescriptor.ArgumentType.INT_FAMILY,
            VectorExpressionDescriptor.ArgumentType.INT_FAMILY)
        .setInputExpressionTypes(
            VectorExpressionDescriptor.InputExpressionType.COLUMN,
            VectorExpressionDescriptor.InputExpressionType.SCALAR).build();
  }
}
