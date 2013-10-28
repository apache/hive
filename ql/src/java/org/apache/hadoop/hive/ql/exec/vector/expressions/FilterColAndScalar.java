/**
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
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

public class FilterColAndScalar extends VectorExpression {

  private static final long serialVersionUID = 1L;
  private long value;
  private int colNum;

  public FilterColAndScalar() {
    super();
  }

  public FilterColAndScalar(int colNum, long scalarVal) {
    this();
    this.colNum = colNum;
    this.value = scalarVal;
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) {
    if (value == 0) {
      batch.size = 0;
      return;
    }
    super.evaluateChildren(batch);
  }

  @Override
  public int getOutputColumn() {
    return -1;
  }

  @Override
  public String getOutputType() {
    return "boolean";
  }

  public int getColNum() {
    return colNum;
  }

  public void setColNum(int colNum) {
    this.colNum = colNum;
  }

  public double getValue() {
    return value;
  }

  public void setValue(long value) {
    this.value = value;
  }

  @Override
  public VectorExpressionDescriptor.Descriptor getDescriptor() {
    return (new VectorExpressionDescriptor.Builder())
        .setMode(
            VectorExpressionDescriptor.Mode.FILTER)
        .setNumArguments(2)
        .setArgumentTypes(
            VectorExpressionDescriptor.ArgumentType.LONG,
            VectorExpressionDescriptor.ArgumentType.LONG)
        .setInputExpressionTypes(
            VectorExpressionDescriptor.InputExpressionType.COLUMN,
            VectorExpressionDescriptor.InputExpressionType.SCALAR).build();
  }
}
