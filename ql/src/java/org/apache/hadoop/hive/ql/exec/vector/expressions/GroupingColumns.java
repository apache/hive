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

import java.util.Arrays;

import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;

public class GroupingColumns extends MathFuncLongToLong {
  private static final long serialVersionUID = 1L;

  private final long[] masks;

  public GroupingColumns(int inputColumnNum, int[] indices, int outputColumnNum) {
    super(inputColumnNum, outputColumnNum);
    final int size = indices.length;
    masks = new long[size];
    for (int i = 0; i < size; i++) {
      masks[i] = 1L << indices[i];
    }
  }

  public GroupingColumns() {
    super();

    // Dummy final assignments.
    masks = null;
  }

  @Override
  protected long func(long v) {

    final int size = masks.length;
    final int adjust = size - 1;
    long result = 0;
    for (int i = 0; i < size; i++) {
      if ((v & masks[i]) != 0) {
        result += 1L << (adjust - i);
      }
    }
    return result;
  }

  @Override
  public String vectorExpressionParameters() {
    return "col " + inputColumnNum[0] + ", masks " + Arrays.toString(masks);
  }

  @Override
  public VectorExpressionDescriptor.Descriptor getDescriptor() {
    return null;  // Not applicable.
  }
}
