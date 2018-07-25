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

import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.IfExprLongColumnLongScalar;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;

/**
 * Compute IF(expr1, expr2, expr3) for 3 input column expressions.
 * The first is always a boolean (LongColumnVector).
 * The second is a column or non-constant expression result.
 * The third is a constant value.
 */
public class IfExprDecimal64ColumnDecimal64Scalar extends IfExprLongColumnLongScalar {

  private static final long serialVersionUID = 1L;

  public IfExprDecimal64ColumnDecimal64Scalar(int arg1Column, int arg2Column, long arg3Scalar,
      int outputColumnNum) {
    super(arg1Column, arg2Column, arg3Scalar, outputColumnNum);
  }

  public IfExprDecimal64ColumnDecimal64Scalar() {
    super();
  }

  @Override
  public String vectorExpressionParameters() {
    DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) inputTypeInfos[2];
    HiveDecimalWritable writable = new HiveDecimalWritable();
    writable.deserialize64(arg3Scalar, decimalTypeInfo.scale());
    return getColumnParamString(0, arg1Column) + ", " + getColumnParamString(1, arg2Column) +
        ", decimal64Val " + arg3Scalar +
        ", decimalVal " + writable.toString();
  }

  @Override
  public VectorExpressionDescriptor.Descriptor getDescriptor() {
    return (new VectorExpressionDescriptor.Builder())
        .setMode(
            VectorExpressionDescriptor.Mode.PROJECTION)
        .setNumArguments(3)
        .setArgumentTypes(
            VectorExpressionDescriptor.ArgumentType.getType("long"),
            VectorExpressionDescriptor.ArgumentType.DECIMAL_64,
            VectorExpressionDescriptor.ArgumentType.DECIMAL_64)
        .setInputExpressionTypes(
            VectorExpressionDescriptor.InputExpressionType.COLUMN,
            VectorExpressionDescriptor.InputExpressionType.COLUMN,
            VectorExpressionDescriptor.InputExpressionType.SCALAR).build();
  }
}
