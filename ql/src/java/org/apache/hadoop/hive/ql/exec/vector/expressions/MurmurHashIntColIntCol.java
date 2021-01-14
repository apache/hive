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

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor.Builder;
import org.apache.hadoop.hive.ql.exec.vector.expressions.MurmurHashExpression;
import org.apache.hive.common.util.Murmur3;

/**
 * Vectorized expression for hash(Column[int], Column[int]).
 */
public class MurmurHashIntColIntCol extends MurmurHashExpression {
  private static final long serialVersionUID = 1L;

  public MurmurHashIntColIntCol() {
    super();
  }

  public MurmurHashIntColIntCol(int colNumFirst, int colNumSecond, int outputColumnNum) {
    super(colNumFirst, colNumSecond, outputColumnNum);
  }

  @Override
  protected void hash(ColumnVector inputColVector1, ColumnVector inputColVector2,
      LongColumnVector outputColVector, int i, ByteBuffer byteBuffer) {
    LongColumnVector inV1 = (LongColumnVector) inputColVector1;
    LongColumnVector inV2 = (LongColumnVector) inputColVector2;

    // hash of value from 1. column
    int hash = 0;
    if (!inV1.isNull[i]) {
      byteBuffer.clear();
      byteBuffer.putLong(inV1.vector[i]);
      hash = Murmur3.hash32(byteBuffer.array(), 8);
    }

    int hash2 = 0;
    // hash of value from 2. column
    if (!inV2.isNull[i]) {
      byteBuffer.clear();
      byteBuffer.putLong(inV2.vector[i]);
      hash2 = Murmur3.hash32(byteBuffer.array(), 8);
    }

    outputColVector.vector[i] = 31 * hash + hash2;
  }

  @Override
  protected void setArguments(Builder builder) {
    builder.setArgumentTypes(VectorExpressionDescriptor.ArgumentType.INT_FAMILY,
        VectorExpressionDescriptor.ArgumentType.INT_FAMILY);
  }
}
