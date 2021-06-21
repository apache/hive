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
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor.Descriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.metadata.HiveException;

/**
 * Base class for vectorized implementation of GenericUDFMurmurHash. See subclasses for int/string
 * parameter variations.
 */
public abstract class MurmurHashExpression extends VectorExpression {
  private static final long serialVersionUID = 1L;

  public MurmurHashExpression() {
    super();
  }

  public MurmurHashExpression(int colNumFirst, int colNumSecond, int outputColumnNum) {
    super(colNumFirst, colNumSecond, outputColumnNum);
  }

  @Override
  public String vectorExpressionParameters() {
    return getColumnParamString(0, inputColumnNum[0]) + ", " + getColumnParamString(1, inputColumnNum[1]);
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) throws HiveException {
    if (childExpressions != null) {
      this.evaluateChildren(batch);
    }

    ColumnVector inputColVector1 = batch.cols[inputColumnNum[0]];
    ColumnVector inputColVector2 = batch.cols[inputColumnNum[1]];
    LongColumnVector outputColVector = (LongColumnVector) batch.cols[outputColumnNum];

    int[] sel = batch.selected;
    boolean[] inputIsNull1 = inputColVector1.isNull;
    boolean[] inputIsNull2 = inputColVector2.isNull;

    boolean[] outputIsNull = outputColVector.isNull;
    int n = batch.size;

    // return immediately if batch is empty
    if (n == 0) {
      return;
    }

    // We do not need to do a column reset since we are carefully changing the output.
    outputColVector.isRepeating = false;

    ByteBuffer buffer = ByteBuffer.allocate(8);

    if (inputColVector1.isRepeating && inputColVector2.isRepeating) {
      outputIsNull[0] = false; // output of hash will never be null

      // if any of the first element is not null, calculate hash
      if ((inputColVector1.noNulls || !inputIsNull1[0])
          || (inputColVector2.noNulls || !inputIsNull2[0])) {
        hash(inputColVector1, inputColVector2, outputColVector, 0, buffer);
      } else { // otherwise, hash is 0
        outputColVector.vector[0] = 0;
      }
      outputColVector.isRepeating = true;
      return;
    }

    if (batch.selectedInUse) {
      for (int j = 0; j != n; j++) {
        hash(inputColVector1, inputColVector2, outputColVector, sel[j], buffer);
      }
    } else {
      for (int i = 0; i != n; i++) {
        hash(inputColVector1, inputColVector2, outputColVector, i, buffer);
      }
    }
  }

  /**
   * The actual hash method for an item in the input vectors. This implementation is supposed to be
   * in line with non-vectorized one (ObjectInspectorUtils.getBucketHashCode) for multiple fields.
   * The method is abstract and is supposed to be implemented in subclasses.
   */
  protected abstract void hash(ColumnVector inputColVector1, ColumnVector inputColVector2,
      LongColumnVector outputColVector, int i, ByteBuffer byteBuffer);

  @Override
  public Descriptor getDescriptor() {
    VectorExpressionDescriptor.Builder b = new VectorExpressionDescriptor.Builder();
    b.setMode(VectorExpressionDescriptor.Mode.PROJECTION).setNumArguments(2)
        .setInputExpressionTypes(VectorExpressionDescriptor.InputExpressionType.COLUMN,
            VectorExpressionDescriptor.InputExpressionType.COLUMN);

    setArguments(b);
    return b.build();
  }

  protected abstract void setArguments(Builder builder);
}
