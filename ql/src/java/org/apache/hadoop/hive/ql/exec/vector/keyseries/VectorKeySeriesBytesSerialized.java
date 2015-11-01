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

package org.apache.hadoop.hive.ql.exec.vector.keyseries;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.expressions.StringExpr;
import org.apache.hadoop.hive.serde2.fast.SerializeWrite;
import com.google.common.base.Preconditions;

/**
 * A key series of a single column of byte array keys where the keys get serialized.
 */
public class VectorKeySeriesBytesSerialized<T extends SerializeWrite>
    extends VectorKeySeriesSerializedImpl<T> implements VectorKeySeriesSerialized {

  private final int columnNum;

  private int outputStartPosition;

  public VectorKeySeriesBytesSerialized(int columnNum, T serializeWrite) {
    super(serializeWrite);
    this.columnNum = columnNum;
  }

  @Override
  public void processBatch(VectorizedRowBatch batch) throws IOException {

    currentBatchSize = batch.size;
    Preconditions.checkState(currentBatchSize > 0);

    BytesColumnVector bytesColVector = (BytesColumnVector) batch.cols[columnNum];

    byte[][] vectorBytesArrays = bytesColVector.vector;
    int[] vectorStarts = bytesColVector.start;
    int[] vectorLengths = bytesColVector.length;

    // The serialize routine uses this to build serializedKeyLengths.
    outputStartPosition = 0;
    output.reset();

    if (bytesColVector.isRepeating){
      duplicateCounts[0] = currentBatchSize;
      if (bytesColVector.noNulls || !bytesColVector.isNull[0]) {
        seriesIsAllNull[0] = false;
        serialize(0, vectorBytesArrays[0], vectorStarts[0], vectorLengths[0]);
        nonNullKeyCount = 1;
      } else {
        seriesIsAllNull[0] = true;
        nonNullKeyCount = 0;
      }
      seriesCount = 1;
      Preconditions.checkState(seriesCount <= currentBatchSize);
    } else {
      seriesCount = 0;
      nonNullKeyCount = 0;
      if (batch.selectedInUse) {
        int[] selected = batch.selected;
        if (bytesColVector.noNulls) {

          duplicateCounts[0] = 1;
          int index;
          index = selected[0];
          byte[] prevKeyBytes = vectorBytesArrays[index];
          int prevKeyStart = vectorStarts[index];
          int prevKeyLength = vectorLengths[index];
          serialize(0, prevKeyBytes, prevKeyStart, prevKeyLength);

          int currentKeyStart;
          int currentKeyLength;
          byte[] currentKeyBytes;
          for (int logical = 1; logical < currentBatchSize; logical++) {
            index = selected[logical];
            currentKeyBytes = vectorBytesArrays[index];
            currentKeyStart = vectorStarts[index];
            currentKeyLength = vectorLengths[index];
            if (StringExpr.equal(prevKeyBytes, prevKeyStart, prevKeyLength,
                                 currentKeyBytes, currentKeyStart, currentKeyLength)) {
              duplicateCounts[seriesCount]++;
            } else {
              duplicateCounts[++seriesCount] = 1;
              serialize(seriesCount, currentKeyBytes, currentKeyStart, currentKeyLength);
              prevKeyBytes = currentKeyBytes;
              prevKeyStart = currentKeyStart;
              prevKeyLength = currentKeyLength;
            }
          }
          Arrays.fill(seriesIsAllNull, 0, ++seriesCount, false);
          nonNullKeyCount = seriesCount;
          Preconditions.checkState(seriesCount <= currentBatchSize);
        } else {
          boolean[] isNull = bytesColVector.isNull;

          boolean prevKeyIsNull;
          byte[] prevKeyBytes = null;
          int prevKeyStart = 0;
          int prevKeyLength = 0;
          duplicateCounts[0] = 1;
          int index = selected[0];
          if (isNull[index]) {
            seriesIsAllNull[0] = true;
            prevKeyIsNull = true;
          } else {
            seriesIsAllNull[0] = false;
            prevKeyIsNull = false;
            prevKeyBytes = vectorBytesArrays[index];
            prevKeyStart = vectorStarts[index];
            prevKeyLength = vectorLengths[index];
            serialize(0, prevKeyBytes, prevKeyStart, prevKeyLength);
            nonNullKeyCount = 1;
          }

          int currentKeyStart;
          int currentKeyLength;
          byte[] currentKeyBytes;
          for (int logical = 1; logical < currentBatchSize; logical++) {
            index = selected[logical];
            if (isNull[index]) {
              if (prevKeyIsNull) {
                duplicateCounts[seriesCount]++;
              } else {
                duplicateCounts[++seriesCount] = 1;
                seriesIsAllNull[seriesCount] = true;
                prevKeyIsNull = true;
              }
            } else {
              currentKeyBytes = vectorBytesArrays[index];
              currentKeyStart = vectorStarts[index];
              currentKeyLength = vectorLengths[index];
              if (!prevKeyIsNull &&
                  StringExpr.equal(prevKeyBytes, prevKeyStart, prevKeyLength,
                                   currentKeyBytes, currentKeyStart, currentKeyLength)) {
                duplicateCounts[seriesCount]++;
              } else {
                duplicateCounts[++seriesCount] = 1;
                seriesIsAllNull[seriesCount] = false;
                serialize(nonNullKeyCount++, currentKeyBytes, currentKeyStart, currentKeyLength);
                prevKeyIsNull = false;
                prevKeyBytes = currentKeyBytes;
                prevKeyStart = currentKeyStart;
                prevKeyLength = currentKeyLength;
              }
            }
          }
          seriesCount++;
          Preconditions.checkState(seriesCount <= currentBatchSize);
        }
      } else {

        // NOT selectedInUse

        if (bytesColVector.noNulls) {

          duplicateCounts[0] = 1;
          byte[] prevKeyBytes = vectorBytesArrays[0];
          int prevKeyStart = vectorStarts[0];
          int prevKeyLength = vectorLengths[0];
          serialize(0, prevKeyBytes, prevKeyStart, prevKeyLength);

          int currentKeyStart;
          int currentKeyLength;
          byte[] currentKeyBytes;
          for (int index = 1; index < currentBatchSize; index++) {
            currentKeyBytes = vectorBytesArrays[index];
            currentKeyStart = vectorStarts[index];
            currentKeyLength = vectorLengths[index];
            if (StringExpr.equal(prevKeyBytes, prevKeyStart, prevKeyLength,
                                 currentKeyBytes, currentKeyStart, currentKeyLength)) {
              duplicateCounts[seriesCount]++;
            } else {
              duplicateCounts[++seriesCount] = 1;
              serialize(seriesCount, currentKeyBytes, currentKeyStart, currentKeyLength);
              prevKeyBytes = currentKeyBytes;
              prevKeyStart = currentKeyStart;
              prevKeyLength = currentKeyLength;
            }
          }
          Arrays.fill(seriesIsAllNull, 0, ++seriesCount, false);
          nonNullKeyCount = seriesCount;
          Preconditions.checkState(seriesCount <= currentBatchSize);
        } else {
          boolean[] isNull = bytesColVector.isNull;

          boolean prevKeyIsNull;
          byte[] prevKeyBytes = null;
          int prevKeyStart = 0;
          int prevKeyLength = 0;
          duplicateCounts[0] = 1;
          if (isNull[0]) {
            seriesIsAllNull[0] = true;
            prevKeyIsNull = true;
          } else {
            seriesIsAllNull[0] = false;
            prevKeyIsNull = false;
            prevKeyBytes = vectorBytesArrays[0];
            prevKeyStart = vectorStarts[0];
            prevKeyLength = vectorLengths[0];
            serialize(0, prevKeyBytes, prevKeyStart, prevKeyLength);
            nonNullKeyCount = 1;
          }

          byte[] currentKeyBytes;
          int currentKeyStart;
          int currentKeyLength;
          for (int index = 1; index < currentBatchSize; index++) {
            if (isNull[index]) {
              if (prevKeyIsNull) {
                duplicateCounts[seriesCount]++;
              } else {
                duplicateCounts[++seriesCount] = 1;
                seriesIsAllNull[seriesCount] = true;
                prevKeyIsNull = true;
              }
            } else {
              currentKeyBytes = vectorBytesArrays[index];
              currentKeyStart = vectorStarts[index];
              currentKeyLength = vectorLengths[index];
              if (!prevKeyIsNull &&
                  StringExpr.equal(prevKeyBytes, prevKeyStart, prevKeyLength,
                                   currentKeyBytes, currentKeyStart, currentKeyLength)) {
                duplicateCounts[seriesCount]++;
              } else {
                duplicateCounts[++seriesCount] = 1;
                seriesIsAllNull[seriesCount] = false;
                serialize(nonNullKeyCount++, currentKeyBytes, currentKeyStart, currentKeyLength);
                prevKeyIsNull = false;
                prevKeyBytes = currentKeyBytes;
                prevKeyStart = currentKeyStart;
                prevKeyLength = currentKeyLength;
              }
            }
          }
          seriesCount++;
          Preconditions.checkState(seriesCount <= currentBatchSize);
        }
      }
    }

    // Finally.
    computeSerializedHashCodes();
    positionToFirst();
    Preconditions.checkState(validate());
  }

  private void serialize(int pos, byte[] bytes, int start, int length) throws IOException {
    serializeWrite.setAppend(output);
    serializeWrite.writeString(bytes, start, length);
    int outputNewPosition = output.getLength();
    serializedKeyLengths[pos] = outputNewPosition - outputStartPosition;
    outputStartPosition = outputNewPosition;
  }
}
