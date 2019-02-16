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

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.metadata.HiveException;

/**
 * Vectorized instruction to get an element from a list with a scalar index and put
 * the result in an output column.
 */
public class ListIndexColScalar extends VectorExpression {
  private static final long serialVersionUID = 1L;

  private int listColumnNum;
  private int index;

  public ListIndexColScalar() {
    super();
  }

  public ListIndexColScalar(int listColumn, int index, int outputColumnNum) {
    super(outputColumnNum);
    this.listColumnNum = listColumn;
    this.index = index;
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) throws HiveException {

    // return immediately if batch is empty
    final int n = batch.size;
    if (n == 0) {
      return;
    }

    if (childExpressions != null) {
      super.evaluateChildren(batch);
    }

    ColumnVector outV = batch.cols[outputColumnNum];
    ListColumnVector listV = (ListColumnVector) batch.cols[listColumnNum];
    ColumnVector childV = listV.child;
    int[] sel = batch.selected;
    boolean[] listIsNull = listV.isNull;
    boolean[] outputIsNull = outV.isNull;

    if (index < 0) {
      outV.isNull[0] = true;
      outV.noNulls = false;
      outV.isRepeating = true;
      return;
    }

    // We do not need to do a column reset since we are carefully changing the output.
    outV.isRepeating = false;

    /*
     * Do careful maintenance of the outputColVector.noNulls flag.
     */

    if (listV.isRepeating) {
      if (listV.noNulls || !listIsNull[0]) {
        final long repeatedLongListLength = listV.lengths[0];
        if (index >= repeatedLongListLength) {
          outV.isNull[0] = true;
          outV.noNulls = false;
        } else {
          outV.isNull[0] = false;
          outV.setElement(0, (int) (listV.offsets[0] + index), childV);
        }
      } else {
        outV.isNull[0] = true;
        outV.noNulls = false;
      }
      outV.isRepeating = true;
      return;
    }

    /*
     * Individual row processing for LIST vector with scalar constant INDEX value.
     */
    if (listV.noNulls) {
      if (batch.selectedInUse) {

        // CONSIDER: For large n, fill n or all of isNull array and use the tighter ELSE loop.

        if (!outV.noNulls) {
          for (int j = 0; j < n; j++) {
            final int i = sel[j];
            final long longListLength = listV.lengths[i];
            if (index >= longListLength) {
              outV.isNull[i] = true;
              outV.noNulls = false;
            } else {
              outV.isNull[i] = false;
              outV.setElement(i, (int) (listV.offsets[i] + index), childV);
            }
          }
        } else {
          for (int j = 0; j < n; j++) {
            final int i = sel[j];
            final long longListLength = listV.lengths[i];
            if (index >= longListLength) {
              outV.isNull[i] = true;
              outV.noNulls = false;
            } else {
              outV.setElement(i, (int) (listV.offsets[i] + index), childV);
            }
          }
        }
      } else {
        if (!outV.noNulls) {

          // Assume it is almost always a performance win to fill all of isNull so we can
          // safely reset noNulls.
          Arrays.fill(outV.isNull, false);
          outV.noNulls = true;
        }
        for (int i = 0; i < n; i++) {
          final long longListLength = listV.lengths[i];
          if (index >= longListLength) {
            outV.isNull[i] = true;
            outV.noNulls = false;
          } else {
            outV.setElement(i, (int) (listV.offsets[i] + index), childV);
          }
        }
      }
    } else /* there are NULLs in the LIST */ {

      if (batch.selectedInUse) {
        for (int j=0; j != n; j++) {
          int i = sel[j];
          if (!listIsNull[i]) {
            final long longListLength = listV.lengths[i];
            if (index >= longListLength) {
              outV.isNull[i] = true;
              outV.noNulls = false;
            } else {
              outV.isNull[i] = false;
              outV.setElement(i, (int) (listV.offsets[i] + index), childV);
            }
          } else {
            outputIsNull[i] = true;
            outV.noNulls = false;
          }
        }
      } else {
        for (int i = 0; i != n; i++) {
          if (!listIsNull[i]) {
            final long longListLength = listV.lengths[i];
            if (index >= longListLength) {
              outV.isNull[i] = true;
              outV.noNulls = false;
            } else {
              outV.isNull[i] = false;
              outV.setElement(i, (int) (listV.offsets[i] + index), childV);
            }
          } else {
            outputIsNull[i] = true;
            outV.noNulls = false;
          }
        }
      }
    }
  }

  @Override
  public String vectorExpressionParameters() {
    return getColumnParamString(0, listColumnNum) + ", " + getColumnParamString(1, index);
  }

  @Override
  public VectorExpressionDescriptor.Descriptor getDescriptor() {
    return (new VectorExpressionDescriptor.Builder())
        .setMode(
            VectorExpressionDescriptor.Mode.PROJECTION)
        .setNumArguments(2)
        .setArgumentTypes(
            VectorExpressionDescriptor.ArgumentType.LIST,
            VectorExpressionDescriptor.ArgumentType.INT_FAMILY)
        .setInputExpressionTypes(
            VectorExpressionDescriptor.InputExpressionType.COLUMN,
            VectorExpressionDescriptor.InputExpressionType.SCALAR).build();
  }
}
