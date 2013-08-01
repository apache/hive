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

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

/**
 * Constant is represented as a vector with repeating values.
 */
public class ConstantVectorExpression extends VectorExpression {

  private static enum Type {
    LONG,
    DOUBLE,
    BYTES
  }

  private final Type type;
  private final int outputColumn;
  private long longValue = 0;
  private double doubleValue = 0;
  private byte[] bytesValue = null;
  private int bytesValueLength = 0;
  private final String typeString;

  ConstantVectorExpression(int outputColumn, String typeString) {
    this.outputColumn = outputColumn;
    this.typeString = typeString;
    if ("string".equalsIgnoreCase(typeString)) {
      this.type = Type.BYTES;
    } else if ("double".equalsIgnoreCase(typeString)) {
      this.type = Type.DOUBLE;
    } else {
      this.type = Type.LONG;
    }
  }

  public ConstantVectorExpression(int outputColumn, long value) {
    this(outputColumn, "long");
    this.longValue = value;
  }

  public ConstantVectorExpression(int outputColumn, double value) {
    this(outputColumn, "double");
    this.doubleValue = value;
  }

  public ConstantVectorExpression(int outputColumn, byte[] value) {
    this(outputColumn, "string");
    this.bytesValue = value;
    this.bytesValueLength = this.bytesValue.length;
  }

  private void evaluateLong(VectorizedRowBatch vrg) {
    LongColumnVector cv = (LongColumnVector) vrg.cols[outputColumn];
    cv.isRepeating = true;
    cv.noNulls = true;
    cv.vector[0] = longValue;
  }

  private void evaluateDouble(VectorizedRowBatch vrg) {
    DoubleColumnVector cv = (DoubleColumnVector) vrg.cols[outputColumn];
    cv.isRepeating = true;
    cv.noNulls = true;
    cv.vector[0] = doubleValue;
  }

  private void evaluateBytes(VectorizedRowBatch vrg) {
    BytesColumnVector cv = (BytesColumnVector) vrg.cols[outputColumn];
    cv.isRepeating = true;
    cv.noNulls = true;
    cv.setRef(0, bytesValue, 0, bytesValueLength);
  }

  @Override
  public void evaluate(VectorizedRowBatch vrg) {
    switch (type) {
    case LONG:
      evaluateLong(vrg);
      break;
    case DOUBLE:
      evaluateDouble(vrg);
      break;
    case BYTES:
      evaluateBytes(vrg);
      break;
    }
  }

  @Override
  public int getOutputColumn() {
    return outputColumn;
  }

  @Override
  public String getOutputType() {
    return typeString;
  }
}
