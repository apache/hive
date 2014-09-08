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

/**
 * An expression representing a column, only children are evaluated.
 */
public class IdentityExpression extends VectorExpression {

  private static final long serialVersionUID = 1L;

  private int colNum = -1;
  private String type = null;

  public IdentityExpression() {
  }

  public IdentityExpression(int colNum, String type) {
    this.colNum = colNum;
    this.type = type;
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) {
    if (childExpressions != null) {
      this.evaluateChildren(batch);
    }
  }

  @Override
  public int getOutputColumn() {
    return colNum;
  }

  @Override
  public String getOutputType() {
    return type;
  }

  public int getColNum() {
    return getOutputColumn();
  }

  public String getType() {
    return getOutputType();
  }

  public void setColNum(int colNum) {
    this.colNum = colNum;
  }

  public void setType(String type) {
    this.type = type;
  }

  @Override
  public VectorExpressionDescriptor.Descriptor getDescriptor() {
    return (new VectorExpressionDescriptor.Builder()).build();
  }
}
