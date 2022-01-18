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
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.metadata.HiveException;

/**
 * Vectorized instruction to get the field of Struct type with field name and put
 * the result in an output column.
 */
public class VectorUDFStructField extends VectorExpression {

  private int fieldIndex;

  public VectorUDFStructField() {
    super();
  }

  public VectorUDFStructField(int structColumnNum, int fieldIndex, int outputColumnNum) {
    super(structColumnNum, outputColumnNum);
    this.fieldIndex = fieldIndex;
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
    int[] sel = batch.selected;
    StructColumnVector structColumnVector = (StructColumnVector) batch.cols[inputColumnNum[0]];
    ColumnVector fieldColumnVector = structColumnVector.fields[fieldIndex];

    boolean[] inputIsNull = structColumnVector.isNull;
    boolean[] outputIsNull = outV.isNull;

    outV.reset();

    if (structColumnVector.isRepeating) {
      if (structColumnVector.noNulls || !structColumnVector.isNull[0]) {
        outputIsNull[0] = false;
        outV.setElement(0, 0, fieldColumnVector);
      } else {
        outputIsNull[0] = true;
        outV.noNulls = false;
      }
      outV.isRepeating = true;
      return;
    }
    if (structColumnVector.noNulls) {
      if (batch.selectedInUse) {

        // CONSIDER: For large n, fill n or all of isNull array and use the tighter ELSE loop.

        if (!outV.noNulls) {
          for(int j = 0; j != n; j++) {
           final int i = sel[j];
           outputIsNull[i] = false;
           outV.setElement(i, i, fieldColumnVector);
         }
        } else {
          for(int j = 0; j != n; j++) {
            final int i = sel[j];
            outV.setElement(i, i, fieldColumnVector);
          }
        }
      } else {
        if (!outV.noNulls) {

          // Assume it is almost always a performance win to fill all of isNull so we can
          // safely reset noNulls.
          Arrays.fill(outputIsNull, false);
          outV.noNulls = true;
        }
        for(int i = 0; i != n; i++) {
          outV.setElement(i, i, fieldColumnVector);
        }
      }
    } else  /* there are NULLs in the structColumnVector */ {

      /*
       * Do careful maintenance of the outputColVector.noNulls flag.
       */

      if (batch.selectedInUse) {
        for(int j=0; j != n; j++) {
          int i = sel[j];
          if (!inputIsNull[i]) {
            outputIsNull[i] = false;
            outV.setElement(i, i, fieldColumnVector);
          } else {
            outputIsNull[i] = true;
            outV.noNulls = false;
          }
        }
      } else {
        for(int i = 0; i != n; i++) {
          if (!inputIsNull[i]) {
            outputIsNull[i] = false;
            outV.setElement(i, i, fieldColumnVector);
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
    return getColumnParamString(0, inputColumnNum[0]) + ", " + getColumnParamString(1, fieldIndex);
  }

  @Override
  public VectorExpressionDescriptor.Descriptor getDescriptor() {
    return (new VectorExpressionDescriptor.Builder())
        .setMode(
            VectorExpressionDescriptor.Mode.PROJECTION)
        .setNumArguments(2)
        .setArgumentTypes(
            VectorExpressionDescriptor.ArgumentType.STRUCT,
            VectorExpressionDescriptor.ArgumentType.INT_FAMILY)
        .setInputExpressionTypes(
            VectorExpressionDescriptor.InputExpressionType.COLUMN,
            VectorExpressionDescriptor.InputExpressionType.SCALAR).build();
  }
}
