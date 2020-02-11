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

public class GroupingColumn extends MathFuncLongToLong {
  private static final long serialVersionUID = 1L;

  private final long mask;

  public GroupingColumn(int inputColumnNum, int index, int outputColumnNum) {
    super(inputColumnNum, outputColumnNum);
    this.mask = 1L << index;
  }

  public GroupingColumn() {
    super();

    // Dummy final assignments.
    mask = 0;
  }

  @Override
  protected long func(long v) {
    return (v & mask) == 0 ? 0 : 1;
  }

  @Override
  public String vectorExpressionParameters() {
    return "col " + colNum + ", mask " + mask;
  }

  @Override
  public VectorExpressionDescriptor.Descriptor getDescriptor() {
    return null;  // Not applicable.
  }
}
