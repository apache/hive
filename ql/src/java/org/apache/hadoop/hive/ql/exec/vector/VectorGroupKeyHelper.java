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
import java.util.Arrays;

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

  public void copyGroupKey(VectorizedRowBatch inputBatch, VectorizedRowBatch outputBatch,
          DataOutputBuffer buffer) throws HiveException {
    // Grab the key at index 0.  We don't care about selected or repeating since all keys in the input batch are the same.
    for(int i = 0; i< longIndices.length; ++i) {
      int keyIndex = longIndices[i];
      LongColumnVector inputColumnVector = (LongColumnVector) inputBatch.cols[keyIndex];
      LongColumnVector outputColumnVector = (LongColumnVector) outputBatch.cols[keyIndex];
      if (inputColumnVector.noNulls || !inputColumnVector.isNull[0]) {
        outputColumnVector.vector[outputBatch.size] = inputColumnVector.vector[0];
      } else if (inputColumnVector.noNulls ){
        outputColumnVector.noNulls = false;
        outputColumnVector.isNull[outputBatch.size] = true;
      } else {
        outputColumnVector.isNull[outputBatch.size] = true;
      }
    }
    for(int i=0;i<doubleIndices.length; ++i) {
      int keyIndex = doubleIndices[i];
      DoubleColumnVector inputColumnVector = (DoubleColumnVector) inputBatch.cols[keyIndex];
      DoubleColumnVector outputColumnVector = (DoubleColumnVector) outputBatch.cols[keyIndex];
      if (inputColumnVector.noNulls || !inputColumnVector.isNull[0]) {
        outputColumnVector.vector[outputBatch.size] = inputColumnVector.vector[0];
      } else if (inputColumnVector.noNulls ){
        outputColumnVector.noNulls = false;
        outputColumnVector.isNull[outputBatch.size] = true;
      } else {
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
      } else if (inputColumnVector.noNulls ){
        outputColumnVector.noNulls = false;
        outputColumnVector.isNull[outputBatch.size] = true;
      } else {
        outputColumnVector.isNull[outputBatch.size] = true;
      }
    }
    for(int i=0;i<decimalIndices.length; ++i) {
      int keyIndex = decimalIndices[i];
      DecimalColumnVector inputColumnVector = (DecimalColumnVector) inputBatch.cols[keyIndex];
      DecimalColumnVector outputColumnVector = (DecimalColumnVector) outputBatch.cols[keyIndex];
      if (inputColumnVector.noNulls || !inputColumnVector.isNull[0]) {
        outputColumnVector.vector[outputBatch.size] = inputColumnVector.vector[0];
      } else if (inputColumnVector.noNulls ){
        outputColumnVector.noNulls = false;
        outputColumnVector.isNull[outputBatch.size] = true;
      } else {
        outputColumnVector.isNull[outputBatch.size] = true;
      }
    }
  }
}
