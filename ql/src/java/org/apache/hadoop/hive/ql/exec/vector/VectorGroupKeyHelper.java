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

package org.apache.hadoop.hive.ql.exec.vector;

import java.io.IOException;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.io.DataOutputBuffer;

/**
 * Class for copying the group key from an input batch to an output batch.
 */
public class VectorGroupKeyHelper extends VectorColumnSetInfo {

  public VectorGroupKeyHelper(int keyCount) {
    super(keyCount);
   }

  void init(VectorExpression[] keyExpressions) throws HiveException {
    // Inspect the output type of each key expression.
    for(int i=0; i < keyExpressions.length; ++i) {
      addKey(keyExpressions[i].getOutputType());
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
      int keyIndex = longIndices[i];
      LongColumnVector inputColumnVector = (LongColumnVector) inputBatch.cols[keyIndex];
      LongColumnVector outputColumnVector = (LongColumnVector) outputBatch.cols[keyIndex];

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
      int keyIndex = doubleIndices[i];
      DoubleColumnVector inputColumnVector = (DoubleColumnVector) inputBatch.cols[keyIndex];
      DoubleColumnVector outputColumnVector = (DoubleColumnVector) outputBatch.cols[keyIndex];
      if (inputColumnVector.noNulls || !inputColumnVector.isNull[0]) {
        outputColumnVector.vector[outputBatch.size] = inputColumnVector.vector[0];
      } else {
        outputColumnVector.noNulls = false;
        outputColumnVector.isNull[outputBatch.size] = true;
      }
    }
    for(int i=0;i<stringIndices.length; ++i) {
      int keyIndex = stringIndices[i];
      BytesColumnVector inputColumnVector = (BytesColumnVector) inputBatch.cols[keyIndex];
      BytesColumnVector outputColumnVector = (BytesColumnVector) outputBatch.cols[keyIndex];
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
      int keyIndex = decimalIndices[i];
      DecimalColumnVector inputColumnVector = (DecimalColumnVector) inputBatch.cols[keyIndex];
      DecimalColumnVector outputColumnVector = (DecimalColumnVector) outputBatch.cols[keyIndex];
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
      int keyIndex = timestampIndices[i];
      TimestampColumnVector inputColumnVector = (TimestampColumnVector) inputBatch.cols[keyIndex];
      TimestampColumnVector outputColumnVector = (TimestampColumnVector) outputBatch.cols[keyIndex];
      if (inputColumnVector.noNulls || !inputColumnVector.isNull[0]) {

        outputColumnVector.setElement(outputBatch.size, 0, inputColumnVector);
      } else {
        outputColumnVector.noNulls = false;
        outputColumnVector.isNull[outputBatch.size] = true;
      }
    }
    for(int i=0;i<intervalDayTimeIndices.length; ++i) {
      int keyIndex = intervalDayTimeIndices[i];
      IntervalDayTimeColumnVector inputColumnVector = (IntervalDayTimeColumnVector) inputBatch.cols[keyIndex];
      IntervalDayTimeColumnVector outputColumnVector = (IntervalDayTimeColumnVector) outputBatch.cols[keyIndex];
      if (inputColumnVector.noNulls || !inputColumnVector.isNull[0]) {

        outputColumnVector.setElement(outputBatch.size, 0, inputColumnVector);
      } else {
        outputColumnVector.noNulls = false;
        outputColumnVector.isNull[outputBatch.size] = true;
      }
    }
  }
}
