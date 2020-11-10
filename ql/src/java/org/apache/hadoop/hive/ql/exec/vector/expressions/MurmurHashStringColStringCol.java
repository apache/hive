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

import java.nio.ByteBuffer;

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor.Builder;
import org.apache.hadoop.hive.ql.exec.vector.expressions.MurmurHashExpression;
import org.apache.hive.common.util.Murmur3;

/**
 * Vectorized expression for hash(Column[string], Column[string]).
 */
public class MurmurHashStringColStringCol extends MurmurHashExpression {
  private static final long serialVersionUID = 1L;

  public MurmurHashStringColStringCol() {
    super();
  }

  public MurmurHashStringColStringCol(int colNumFirst, int colNumSecond, int outputColumnNum) {
    super(colNumFirst, colNumSecond, outputColumnNum);
  }

  @Override
  protected void hash(ColumnVector inputColVector1, ColumnVector inputColVector2,
      LongColumnVector outputColVector, int i, ByteBuffer byteBuffer) {
    BytesColumnVector inV1 = (BytesColumnVector) inputColVector1;
    BytesColumnVector inV2 = (BytesColumnVector) inputColVector2;

    // hash of value from 1. column
    int hash = inV1.isNull[i] ? 0
      : Murmur3.hash32(inV1.vector[i], inV1.start[i], inV1.length[i], Murmur3.DEFAULT_SEED);
    // hash of value from 2. column
    int hash2 = inV2.isNull[i] ? 0
      : Murmur3.hash32(inV2.vector[i], inV2.start[i], inV2.length[i], Murmur3.DEFAULT_SEED);

    outputColVector.vector[i] = 31 * hash + hash2;
  }

  @Override
  protected void setArguments(Builder builder) {
    builder.setArgumentTypes(VectorExpressionDescriptor.ArgumentType.STRING_FAMILY,
        VectorExpressionDescriptor.ArgumentType.STRING_FAMILY);
  }
}
