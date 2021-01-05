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

/**
 * This is a superclass for binary string functions that operate directly on the
 * input and set the output. First parametes is a column second is a scalar.
 * It is suitable for direct, in-place operations on
 * strings, such as for fast implementations of two parameter version of  TRIM(), LTRIM(), and RTRIM().
 */
abstract public class StringBinaryColScalarUDFDirect extends StringUnaryUDFDirect {
  private static final long serialVersionUID = 1L;

  public StringBinaryColScalarUDFDirect(int inputColumn, int outputColumnNum) {
    super(inputColumn, outputColumnNum);
  }

  public StringBinaryColScalarUDFDirect() {
    super();
  }

  @Override
  public VectorExpressionDescriptor.Descriptor getDescriptor() {
    VectorExpressionDescriptor.Builder b = new VectorExpressionDescriptor.Builder();
    b.setMode(VectorExpressionDescriptor.Mode.PROJECTION)
            .setNumArguments(2)
            .setArgumentTypes(
                    VectorExpressionDescriptor.ArgumentType.STRING_FAMILY,
                    VectorExpressionDescriptor.ArgumentType.STRING_FAMILY)
            .setInputExpressionTypes(
                    VectorExpressionDescriptor.InputExpressionType.COLUMN,
                    VectorExpressionDescriptor.InputExpressionType.SCALAR);
    return b.build();
  }
}