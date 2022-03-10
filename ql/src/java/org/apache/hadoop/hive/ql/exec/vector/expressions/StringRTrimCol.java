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

import java.nio.charset.StandardCharsets;

import static org.apache.hadoop.hive.ql.udf.generic.GenericUDFBaseTrim.DEFAULT_TRIM_CHARS;

public class StringRTrimCol extends StringRTrimColScalar {
  private static final long serialVersionUID = 1L;

  public StringRTrimCol(int inputColumn, int outputColumnNum) {
    super(inputColumn, DEFAULT_TRIM_CHARS.getBytes(StandardCharsets.UTF_8), outputColumnNum);
  }

  public StringRTrimCol() {
    super();
  }

  @Override
  public VectorExpressionDescriptor.Descriptor getDescriptor() {
    VectorExpressionDescriptor.Builder b = new VectorExpressionDescriptor.Builder();
    b.setMode(VectorExpressionDescriptor.Mode.PROJECTION)
            .setNumArguments(1)
            .setArgumentTypes(
                    VectorExpressionDescriptor.ArgumentType.STRING_FAMILY)
            .setInputExpressionTypes(
                    VectorExpressionDescriptor.InputExpressionType.COLUMN);
    return b.build();
  }
}
