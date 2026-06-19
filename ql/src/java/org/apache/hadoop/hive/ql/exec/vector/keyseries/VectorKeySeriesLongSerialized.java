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

package org.apache.hadoop.hive.ql.exec.vector.keyseries;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.serde2.fast.SerializeWrite;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;

import com.google.common.base.Preconditions;

/**
 * A key series of a single column of long keys where the keys get serialized.
 */
public class VectorKeySeriesLongSerialized<T extends SerializeWrite>
    extends VectorKeySeriesSerializedImpl<T> implements VectorKeySeriesSerialized {

  private final int columnNum;
  private PrimitiveCategory primitiveCategory;

  private int currentKeyStart;

  public VectorKeySeriesLongSerialized(int columnNum, PrimitiveTypeInfo primitiveTypeInfo,
      T serializeWrite) {
    super(serializeWrite);
    this.columnNum = columnNum;
    primitiveCategory = primitiveTypeInfo.getPrimitiveCategory();
  }

  @Override
  public void processBatch(VectorizedRowBatch batch) throws IOException {

    currentBatchSize = batch.size;
    Preconditions.checkState(currentBatchSize > 0);

    LongColumnVector longColVector = (LongColumnVector) batch.cols[columnNum];

    long[] vector = longColVector.vector;

    // The serialize routine uses this to build serializedKeyLengths.
    currentKeyStart = 0;
    output.reset();

    if (longColVector.isRepeating){
      duplicateCounts[0] = currentBatchSize;
      if (longColVector.noNulls || !longColVector.isNull[0]) {
        seriesIsAllNull[0] = false;
        serialize(0, vector[0]);
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
        if (longColVector.noNulls) {

          duplicateCounts[0] = 1;
          long prevKey = vector[selected[0]];
          serialize(0, prevKey);

          long currentKey;
          for (int logical = 1; logical < currentBatchSize; logical++) {
            currentKey = vector[selected[logical]];
            if (prevKey == currentKey) {
              duplicateCounts[seriesCount]++;
            } else {
              duplicateCounts[++seriesCount] = 1;
              serialize(seriesCount, currentKey);
              prevKey = currentKey;
            }
          }
          Arrays.fill(seriesIsAllNull, 0, ++seriesCount, false);
          nonNullKeyCount = seriesCount;
          Preconditions.checkState(seriesCount <= currentBatchSize);
        } else {
          boolean[] isNull = longColVector.isNull;

          boolean prevKeyIsNull;
          long prevKey = 0;
          duplicateCounts[0] = 1;
          int index = selected[0];
          if (isNull[index]) {
            seriesIsAllNull[0] = true;
            prevKeyIsNull = true;
            nonNullKeyCount = 0;
          } else {
            seriesIsAllNull[0] = false;
            prevKeyIsNull = false;
            prevKey = vector[index];
            serialize(0, prevKey);
            nonNullKeyCount = 1;
          }

          long currentKey;
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
              currentKey = vector[index];
              if (!prevKeyIsNull && prevKey == currentKey) {
                duplicateCounts[seriesCount]++;
              } else {
                duplicateCounts[++seriesCount] = 1;
                seriesIsAllNull[seriesCount] = false;
                serialize(nonNullKeyCount++, currentKey);
                prevKeyIsNull = false;
                prevKey = currentKey;
              }
            }
          }
          seriesCount++;
          Preconditions.checkState(seriesCount <= currentBatchSize);
        }
      } else {

        // NOT selectedInUse

        if (longColVector.noNulls) {

          duplicateCounts[0] = 1;
          long prevKey = vector[0];
          serialize(0, prevKey);
          long currentKey;
          for (int index = 1; index < currentBatchSize; index++) {
            currentKey = vector[index];
            if (prevKey == currentKey) {
              duplicateCounts[seriesCount]++;
            } else {
              duplicateCounts[++seriesCount] = 1;
              serialize(seriesCount, currentKey);
              prevKey = currentKey;
            }
          }
          Arrays.fill(seriesIsAllNull, 0, ++seriesCount, false);
          nonNullKeyCount = seriesCount;
          Preconditions.checkState(seriesCount <= currentBatchSize);
        } else {
          boolean[] isNull = longColVector.isNull;

          boolean prevKeyIsNull;
          long prevKey = 0;
          duplicateCounts[0] = 1;
          if (isNull[0]) {
            seriesIsAllNull[0] = true;
            prevKeyIsNull = true;
            nonNullKeyCount = 0;
          } else {
            seriesIsAllNull[0] = false;
            prevKeyIsNull = false;
            prevKey = vector[0];
            serialize(0, prevKey);
            nonNullKeyCount = 1;
          }

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
              long currentKey = vector[index];
              if (!prevKeyIsNull && prevKey == currentKey) {
                duplicateCounts[seriesCount]++;
              } else {
                duplicateCounts[++seriesCount] = 1;
                seriesIsAllNull[seriesCount] = false;
                serialize(nonNullKeyCount++, currentKey);
                prevKeyIsNull = false;
                prevKey = currentKey;
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

  private void serialize(int pos, long value) throws IOException {
    serializeWrite.setAppend(output);

    switch (primitiveCategory) {
    case BOOLEAN:
      serializeWrite.writeBoolean(value != 0);
      break;
    case BYTE:
      serializeWrite.writeByte((byte) value);
      break;
    case SHORT:
      serializeWrite.writeShort((short) value);
      break;
    case INT:
      serializeWrite.writeInt((int) value);
      break;
    case DATE:
      serializeWrite.writeDate((int) value);
      break;
    case LONG:
      serializeWrite.writeLong(value);
      break;
    default:
      throw new RuntimeException("Unexpected primitive category " + primitiveCategory.name());
    }
    int outputNewPosition = output.getLength();
    serializedKeyLengths[pos] = outputNewPosition - currentKeyStart;
    currentKeyStart = outputNewPosition;
  }
}
