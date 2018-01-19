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

import java.sql.Timestamp;

import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;

/**
 * Compute IF(expr1, expr2, expr3) for 3 input column expressions.
 * The first is always a boolean (LongColumnVector).
 * The second is a column or non-constant expression result.
 * The third is a constant value.
 */
public class IfExprTimestampScalarColumn extends IfExprTimestampScalarColumnBase {

  private static final long serialVersionUID = 1L;

  public IfExprTimestampScalarColumn(int arg1Column, Timestamp arg2Scalar, int arg3Column,
      int outputColumnNum) {
    super(arg1Column, arg2Scalar, arg3Column, outputColumnNum);
  }

  public IfExprTimestampScalarColumn() {
    super();
  }

  @Override
  public VectorExpressionDescriptor.Descriptor getDescriptor() {
    return (new VectorExpressionDescriptor.Builder())
        .setMode(
            VectorExpressionDescriptor.Mode.PROJECTION)
        .setNumArguments(3)
        .setArgumentTypes(
            VectorExpressionDescriptor.ArgumentType.getType("int_family"),
            VectorExpressionDescriptor.ArgumentType.getType("timestamp"),
            VectorExpressionDescriptor.ArgumentType.getType("timestamp"))
        .setInputExpressionTypes(
            VectorExpressionDescriptor.InputExpressionType.COLUMN,
            VectorExpressionDescriptor.InputExpressionType.SCALAR,
            VectorExpressionDescriptor.InputExpressionType.COLUMN).build();
  }
}
