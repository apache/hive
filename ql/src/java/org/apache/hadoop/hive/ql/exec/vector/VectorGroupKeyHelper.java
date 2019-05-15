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

package org.apache.hadoop.hive.ql.exec.vector;

import java.io.IOException;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector.Type;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.DataOutputBuffer;

/**
 * Class for copying the group key from an input batch to an output batch.
 */
public class VectorGroupKeyHelper extends VectorColumnSetInfo {

  private int[] inputColumnNums;

  public VectorGroupKeyHelper(int keyCount) {
    super(keyCount);
   }

  void init(VectorExpression[] keyExpressions) throws HiveException {

    // NOTE: To support pruning the grouping set id dummy key by VectorGroupbyOpeator MERGE_PARTIAL
    // case, we use the keyCount passed to the constructor and not keyExpressions.length.

    // Inspect the output type of each key expression.  And, remember the output columns.
    inputColumnNums = new int[keyCount];
    for(int i = 0; i < keyCount; ++i) {
      VectorExpression keyExpression = keyExpressions[i];

      TypeInfo typeInfo = keyExpression.getOutputTypeInfo();
      addKey(typeInfo);

      // The output of the key expression is the input column.
      final int inputColumnNum = keyExpression.getOutputColumnNum();

      inputColumnNums[i] = inputColumnNum;
    }
    finishAdding();
  }

  /*
   * This helper method copies the group keys from one vectorized row batch to another,
   * but does not increment the outputBatch.size (i.e. the next output position).
   * 
   * It was designed for VectorGroupByOperator's sorted reduce group batch processing mode
   * to copy the group keys at startGroup.
   */
  public void copyGroupKey(VectorizedRowBatch inputBatch, VectorizedRowBatch outputBatch,
          DataOutputBuffer buffer) throws HiveException {

    for(int i = 0; i< longIndices.length; ++i) {
      final int outputColumnNum = longIndices[i];
      final int inputColumnNum = inputColumnNums[outputColumnNum];
      LongColumnVector inputColumnVector = (LongColumnVector) inputBatch.cols[inputColumnNum];
      LongColumnVector outputColumnVector = (LongColumnVector) outputBatch.cols[outputColumnNum];

      // This vectorized code pattern says: 
      //    If the input batch has no nulls at all (noNulls is true) OR
      //    the input row is NOT NULL, copy the value.
      //
      //    Otherwise, we have a NULL input value.  The standard way to mark a NULL in the
      //    output batch is: turn off noNulls indicating there is at least one NULL in the batch
      //    and mark that row as NULL.
      //
      //    When a vectorized row batch is reset, noNulls is set to true and the isNull array
      //    is zeroed.
      //
      // We grab the key at index 0.  We don't care about selected or repeating since all keys
      // in the input batch are suppose to be the same.
      //
      if (inputColumnVector.noNulls || !inputColumnVector.isNull[0]) {
        outputColumnVector.vector[outputBatch.size] = inputColumnVector.vector[0];
      } else {
        outputColumnVector.noNulls = false;
        outputColumnVector.isNull[outputBatch.size] = true;
      }
    }
    for(int i=0;i<doubleIndices.length; ++i) {
      final int outputColumnNum = doubleIndices[i];
      final int inputColumnNum = inputColumnNums[outputColumnNum];
      DoubleColumnVector inputColumnVector = (DoubleColumnVector) inputBatch.cols[inputColumnNum];
      DoubleColumnVector outputColumnVector = (DoubleColumnVector) outputBatch.cols[outputColumnNum];
      if (inputColumnVector.noNulls || !inputColumnVector.isNull[0]) {
        outputColumnVector.vector[outputBatch.size] = inputColumnVector.vector[0];
      } else {
        outputColumnVector.noNulls = false;
        outputColumnVector.isNull[outputBatch.size] = true;
      }
    }
    for(int i=0;i<stringIndices.length; ++i) {
      final int outputColumnNum = stringIndices[i];
      final int inputColumnNum = inputColumnNums[outputColumnNum];
      BytesColumnVector inputColumnVector = (BytesColumnVector) inputBatch.cols[inputColumnNum];
      BytesColumnVector outputColumnVector = (BytesColumnVector) outputBatch.cols[outputColumnNum];
      if (inputColumnVector.noNulls || !inputColumnVector.isNull[0]) {
        // Copy bytes into scratch buffer.
        int start = buffer.getLength();
        int length = inputColumnVector.length[0];
        try {
          buffer.write(inputColumnVector.vector[0], inputColumnVector.start[0], length);
        } catch (IOException ioe) {
          throw new IllegalStateException("bad write", ioe);
        }
        outputColumnVector.setRef(outputBatch.size, buffer.getData(), start, length);
      } else {
        outputColumnVector.noNulls = false;
        outputColumnVector.isNull[outputBatch.size] = true;
      }
    }
    for(int i=0;i<decimalIndices.length; ++i) {
      final int outputColumnNum = decimalIndices[i];
      final int inputColumnNum = inputColumnNums[outputColumnNum];
      DecimalColumnVector inputColumnVector = (DecimalColumnVector) inputBatch.cols[inputColumnNum];
      DecimalColumnVector outputColumnVector = (DecimalColumnVector) outputBatch.cols[outputColumnNum];
      if (inputColumnVector.noNulls || !inputColumnVector.isNull[0]) {

        // Since we store references to HiveDecimalWritable instances, we must use the update method instead
        // of plain assignment.
        outputColumnVector.set(outputBatch.size, inputColumnVector.vector[0]);
      } else {
        outputColumnVector.noNulls = false;
        outputColumnVector.isNull[outputBatch.size] = true;
      }
    }
    for(int i=0;i<timestampIndices.length; ++i) {
      final int outputColumnNum = timestampIndices[i];
      final int inputColumnNum = inputColumnNums[outputColumnNum];
      TimestampColumnVector inputColumnVector = (TimestampColumnVector) inputBatch.cols[inputColumnNum];
      TimestampColumnVector outputColumnVector = (TimestampColumnVector) outputBatch.cols[outputColumnNum];
      if (inputColumnVector.noNulls || !inputColumnVector.isNull[0]) {
        outputColumnVector.isNull[outputBatch.size] = false;
        outputColumnVector.setElement(outputBatch.size, 0, inputColumnVector);
      } else {
        outputColumnVector.noNulls = false;
        outputColumnVector.isNull[outputBatch.size] = true;
      }
    }
    for(int i=0;i<intervalDayTimeIndices.length; ++i) {
      final int outputColumnNum = intervalDayTimeIndices[i];
      final int inputColumnNum = inputColumnNums[outputColumnNum];
      IntervalDayTimeColumnVector inputColumnVector = (IntervalDayTimeColumnVector) inputBatch.cols[inputColumnNum];
      IntervalDayTimeColumnVector outputColumnVector = (IntervalDayTimeColumnVector) outputBatch.cols[outputColumnNum];
      if (inputColumnVector.noNulls || !inputColumnVector.isNull[0]) {
        outputColumnVector.isNull[outputBatch.size] = false;
        outputColumnVector.setElement(outputBatch.size, 0, inputColumnVector);
      } else {
        outputColumnVector.noNulls = false;
        outputColumnVector.isNull[outputBatch.size] = true;
      }
    }
  }
}
