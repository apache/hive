/* * Licensed to the Apache Software Foundation (ASF) under one
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
/**
 * This class performs constant OR column expression where the constant is null
 */
package org.apache.hadoop.hive.ql.exec.vector.expressions;

import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.metadata.HiveException;

public class ScalarNullOrCol extends ColOrCol {
  private static final long serialVersionUID = 1L;

  public ScalarNullOrCol(ConstantVectorExpression expression, int colNum, int outputColumnNum) {
    super(colNum, -1, outputColumnNum);
  }

  public ScalarNullOrCol() {
    super();
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) throws HiveException {
    if (childExpressions != null) {
      super.evaluateChildren(batch);
    }

    LongColumnVector inputColVector = (LongColumnVector) batch.cols[inputColumnNum[0]];

    super.doEvaluate(batch, new LongColumnVector(inputColVector.vector.length).fillWithNulls(),
        inputColVector);
  }

  @Override
  public String vectorExpressionParameters() {
    return getColumnParamString(0, inputColumnNum[0]) + ", " + getLongValueParamString(1, 0);
  }

  @Override
  public VectorExpressionDescriptor.Descriptor getDescriptor() {
    return (new VectorExpressionDescriptor.Builder())
        .setMode(
            VectorExpressionDescriptor.Mode.PROJECTION)
        .setNumArguments(2)
        .setArgumentTypes(
            VectorExpressionDescriptor.ArgumentType.getType("long"),
            VectorExpressionDescriptor.ArgumentType.getType("long"))
        .setInputExpressionTypes(
            VectorExpressionDescriptor.InputExpressionType.NULLSCALAR,
            VectorExpressionDescriptor.InputExpressionType.COLUMN).build();
  }
}
