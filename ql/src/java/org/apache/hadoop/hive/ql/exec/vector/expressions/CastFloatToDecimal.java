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
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;

/**
 * Cast input float to a decimal. Get target value scale from output column vector.
 */
public class CastFloatToDecimal extends FuncDoubleToDecimal {

  private static final long serialVersionUID = 1L;

  public CastFloatToDecimal() {
    super();
  }

  public CastFloatToDecimal(int inputColumn, int outputColumnNum) {
    super(inputColumn, outputColumnNum);
  }

  @Override
  protected void func(DecimalColumnVector outV, DoubleColumnVector inV, int i) {
    HiveDecimalWritable decWritable = outV.vector[i];

    // TEMPORARY: In order to avoid a new version of storage-api, do the conversion here...
    byte[] floatBytes = Float.toString((float) inV.vector[i]).getBytes();
    decWritable.setFromBytes(floatBytes, 0, floatBytes.length);
    if (!decWritable.mutateEnforcePrecisionScale(outV.precision, outV.scale)) {
      outV.isNull[i] = true;
      outV.noNulls = false;
    }
  }

  @Override
  public VectorExpressionDescriptor.Descriptor getDescriptor() {
    VectorExpressionDescriptor.Builder b = new VectorExpressionDescriptor.Builder();
    b.setMode(VectorExpressionDescriptor.Mode.PROJECTION)
        .setNumArguments(1)
        .setArgumentTypes(
            VectorExpressionDescriptor.ArgumentType.FLOAT)
        .setInputExpressionTypes(
            VectorExpressionDescriptor.InputExpressionType.COLUMN);
    return b.build();
  }
}
