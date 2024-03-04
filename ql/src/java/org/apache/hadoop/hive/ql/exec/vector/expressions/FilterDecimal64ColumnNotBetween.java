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

import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterLongColumnNotBetween;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;

public class FilterDecimal64ColumnNotBetween extends FilterLongColumnNotBetween {

  private static final long serialVersionUID = 1L;

  public FilterDecimal64ColumnNotBetween(int colNum, long leftValue, long rightValue) {
    super(colNum, leftValue, rightValue);
  }

  public FilterDecimal64ColumnNotBetween() {
    super();
  }

  @Override
  public String vectorExpressionParameters() {
    DecimalTypeInfo decimalTypeInfo1 = (DecimalTypeInfo) inputTypeInfos[1];
    HiveDecimalWritable writable1 = new HiveDecimalWritable();
    writable1.deserialize64(leftValue, decimalTypeInfo1.scale());

    DecimalTypeInfo decimalTypeInfo2 = (DecimalTypeInfo) inputTypeInfos[2];
    HiveDecimalWritable writable2 = new HiveDecimalWritable();
    writable2.deserialize64(rightValue, decimalTypeInfo2.scale());
    return
        getColumnParamString(0, colNum) +
        ", decimal64LeftVal " + leftValue + ", decimalLeftVal " + writable1.toString() +
        ", decimal64RightVal " + rightValue + ", decimalRightVal " + writable2.toString();
  }

  @Override
  public VectorExpressionDescriptor.Descriptor getDescriptor() {
    return (new VectorExpressionDescriptor.Builder())
        .setMode(
            VectorExpressionDescriptor.Mode.FILTER)
        .setNumArguments(3)
        .setArgumentTypes(
            VectorExpressionDescriptor.ArgumentType.DECIMAL_64,
            VectorExpressionDescriptor.ArgumentType.DECIMAL_64,
            VectorExpressionDescriptor.ArgumentType.DECIMAL_64)
        .setInputExpressionTypes(
            VectorExpressionDescriptor.InputExpressionType.COLUMN,
            VectorExpressionDescriptor.InputExpressionType.SCALAR,
            VectorExpressionDescriptor.InputExpressionType.SCALAR).build();
  }
}
