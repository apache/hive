/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.exec.vector.expressions;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

public class IfExprNullColumn extends VectorExpression {

  private static final long serialVersionUID = 1L;

  private final int arg1Column;
  private final int arg2Column;
  private final int outputColumn;

  public IfExprNullColumn(int arg1Column, int arg2Column, int outputColumn) {
    this.arg1Column = arg1Column;
    this.arg2Column = arg2Column;
    this.outputColumn = outputColumn;
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) {

    if (childExpressions != null) {
      super.evaluateChildren(batch);
    }

    final LongColumnVector arg1ColVector = (LongColumnVector) batch.cols[arg1Column];
    final ColumnVector arg2ColVector = batch.cols[arg2Column];
    final ColumnVector outputColVector = batch.cols[outputColumn];

    final int[] sel = batch.selected;
    final int n = batch.size;
    final boolean[] null1 = arg1ColVector.isNull;
    final long[] vector1 = arg1ColVector.vector;
    final boolean[] isNull = outputColVector.isNull;

    if (n == 0) {
      return;
    }

    arg2ColVector.flatten(batch.selectedInUse, sel, n);

    if (arg1ColVector.isRepeating) {
      if (!null1[0] && vector1[0] == 1) {
        outputColVector.noNulls = false;
        isNull[0] = true;
      } else {
        outputColVector.setElement(0, 0, arg2ColVector);
      }
      return;
    }
    if (batch.selectedInUse) {
      for (int j = 0; j < n; j++) {
        int i = sel[j];
        if (!null1[0] && vector1[i] == 1) {
          outputColVector.noNulls = false;
          isNull[i] = true;
        } else {
          outputColVector.setElement(i, i, arg2ColVector);
        }
      }
    } else {
      for (int i = 0; i < n; i++) {
        if (!null1[0] && vector1[i] == 1) {
          outputColVector.noNulls = false;
          isNull[i] = true;
        } else {
          outputColVector.setElement(i, i, arg2ColVector);
        }
      }
    }

    arg2ColVector.unFlatten();
  }

  @Override
  public int getOutputColumn() {
    return outputColumn;
  }

  @Override
  public String vectorExpressionParameters() {
    return "col " + arg1Column + ", null, col "+ arg2Column;
  }

  @Override
  public VectorExpressionDescriptor.Descriptor getDescriptor() {
    throw new UnsupportedOperationException("Undefined descriptor");
  }
}
