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

import org.apache.hadoop.hive.ql.exec.vector.VectorSerializeRow;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.expressions.StringExpr;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.fast.SerializeWrite;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * A key series of a multiple columns of keys where the keys get serialized.
 * (Or, it can be 1 column).
 */
public class VectorKeySeriesMultiSerialized<T extends SerializeWrite>
    extends VectorKeySeriesSerializedImpl<T> implements VectorKeySeriesSerialized {

  private static final Logger LOG = LoggerFactory.getLogger(
      VectorKeySeriesMultiSerialized.class.getName());

  private VectorSerializeRow<T> keySerializeRow;

  private boolean[] hasAnyNulls;

  public VectorKeySeriesMultiSerialized(T serializeWrite) {
    super(serializeWrite);
  }

  public void init(TypeInfo[] typeInfos, int[] columnNums) throws HiveException {
    keySerializeRow = new VectorSerializeRow<T>(serializeWrite);
    keySerializeRow.init(typeInfos, columnNums);
    hasAnyNulls = new boolean[VectorizedRowBatch.DEFAULT_SIZE];
  }

  @Override
  public void processBatch(VectorizedRowBatch batch) throws IOException {

    currentBatchSize = batch.size;
    Preconditions.checkState(currentBatchSize > 0);

    // LOG.info("VectorKeySeriesMultiSerialized processBatch size " + currentBatchSize + " numCols " + batch.numCols + " selectedInUse " + batch.selectedInUse);

    int prevKeyStart = 0;
    int prevKeyLength;
    int currentKeyStart = 0;
    output.reset();

    seriesCount = 0;
    boolean prevKeyIsNull;
    duplicateCounts[0] = 1;
    if (batch.selectedInUse) {
      int[] selected = batch.selected;
      int index = selected[0];
      keySerializeRow.setOutputAppend(output);
      keySerializeRow.serializeWrite(batch, index);
      if (keySerializeRow.getIsAllNulls()) {
        seriesIsAllNull[0] = prevKeyIsNull = true;
        prevKeyLength = 0;
        output.setWritePosition(0);
        nonNullKeyCount = 0;
      } else {
        seriesIsAllNull[0] = prevKeyIsNull = false;
        serializedKeyLengths[0] = currentKeyStart = prevKeyLength = output.getLength();
        hasAnyNulls[0] = keySerializeRow.getHasAnyNulls();
        nonNullKeyCount = 1;
      }
  
      int keyLength;
      for (int logical = 1; logical < currentBatchSize; logical++) {
        index = selected[logical];
        keySerializeRow.setOutputAppend(output);
        keySerializeRow.serializeWrite(batch, index);
        if (keySerializeRow.getIsAllNulls()) {
          if (prevKeyIsNull) {
            duplicateCounts[seriesCount]++;
          } else {
            duplicateCounts[++seriesCount] = 1;
            seriesIsAllNull[seriesCount] = prevKeyIsNull = true;
          }
          output.setWritePosition(currentKeyStart);
        } else {
          keyLength = output.getLength() - currentKeyStart;
          if (!prevKeyIsNull &&
              StringExpr.equal(
                  output.getData(), prevKeyStart, prevKeyLength,
                  output.getData(), currentKeyStart, keyLength)) {
            duplicateCounts[seriesCount]++;
            output.setWritePosition(currentKeyStart);
          } else {
            duplicateCounts[++seriesCount] = 1;
            seriesIsAllNull[seriesCount] = prevKeyIsNull = false;
            prevKeyStart = currentKeyStart;
            serializedKeyLengths[nonNullKeyCount] = prevKeyLength =  keyLength;
            currentKeyStart += keyLength;
            hasAnyNulls[nonNullKeyCount] = keySerializeRow.getHasAnyNulls();
            nonNullKeyCount++;
          }
        }
      }
      seriesCount++;
      Preconditions.checkState(seriesCount <= currentBatchSize);
    } else {
      keySerializeRow.setOutputAppend(output);
      keySerializeRow.serializeWrite(batch, 0);
      if (keySerializeRow.getIsAllNulls()) {
        seriesIsAllNull[0] = prevKeyIsNull = true;
        prevKeyLength = 0;
        output.setWritePosition(0);
        nonNullKeyCount = 0;
      } else {
        seriesIsAllNull[0] = prevKeyIsNull = false;
        serializedKeyLengths[0] = currentKeyStart = prevKeyLength = output.getLength();
        hasAnyNulls[0] = keySerializeRow.getHasAnyNulls();
        nonNullKeyCount = 1;
      }

      int keyLength;
      for (int index = 1; index < currentBatchSize; index++) {
        keySerializeRow.setOutputAppend(output);
        keySerializeRow.serializeWrite(batch, index);
        if (keySerializeRow.getIsAllNulls()) {
          if (prevKeyIsNull) {
            duplicateCounts[seriesCount]++;
          } else {
            duplicateCounts[++seriesCount] = 1;
            seriesIsAllNull[seriesCount] = prevKeyIsNull = true;
          }
          output.setWritePosition(currentKeyStart);
        } else {
          keyLength = output.getLength() - currentKeyStart;
          if (!prevKeyIsNull &&
              StringExpr.equal(
                  output.getData(), prevKeyStart, prevKeyLength,
                  output.getData(), currentKeyStart, keyLength)) {
            duplicateCounts[seriesCount]++;
            output.setWritePosition(currentKeyStart);
          } else {
            duplicateCounts[++seriesCount] = 1;
            seriesIsAllNull[seriesCount] = prevKeyIsNull = false;
            prevKeyStart = currentKeyStart;
            serializedKeyLengths[nonNullKeyCount] = prevKeyLength =  keyLength;
            currentKeyStart += keyLength;
            hasAnyNulls[nonNullKeyCount] = keySerializeRow.getHasAnyNulls();
            nonNullKeyCount++;
          }
        }
      }
      seriesCount++;
      Preconditions.checkState(seriesCount <= currentBatchSize);
    }

    // Finally.
    computeSerializedHashCodes();
    positionToFirst();
    Preconditions.checkState(validate());
  }

  @Override
  public void setNextNonNullKey(int nonNullKeyPosition) {
    super.setNextNonNullKey(nonNullKeyPosition);

    currentHasAnyNulls = hasAnyNulls[nonNullKeyPosition];
  }
}
