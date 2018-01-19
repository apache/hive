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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import com.google.common.base.Preconditions;

/**
 * Implementation of when a one key series or a serialized key series is being presented.
 *
 */
public abstract class VectorKeySeriesSingleImpl extends VectorKeySeriesImpl
    implements VectorKeySeries {

  private static final Log LOG = LogFactory.getLog(VectorKeySeriesSingleImpl.class.getName());

  protected int currentBatchSize;

  // The number of keys (with sequential duplicates collapsed, both NULL and non-NULL) in the batch.
  protected int seriesCount;

  // The current position in the key series.
  protected int seriesPosition;

  // The number of duplicates for each series key (NULL or non-NULL).
  protected final int[] duplicateCounts;

  // Whether a series key is NULL.
  protected final boolean[] seriesIsAllNull;

  // The number of non-NULL keys.  They have associated hash codes and key data. 
  protected int nonNullKeyCount;

  // The current non-NULL key position.
  protected int nonNullKeyPosition;

  // The hash code for each non-NULL key.
  protected final int[] hashCodes;

  VectorKeySeriesSingleImpl() {
    super();

    seriesCount = 0;
    seriesPosition = 0;

    duplicateCounts = new int[VectorizedRowBatch.DEFAULT_SIZE];
    seriesIsAllNull = new boolean[VectorizedRowBatch.DEFAULT_SIZE];

    nonNullKeyCount = 0;
    nonNullKeyPosition = -1;

    hashCodes = new int[VectorizedRowBatch.DEFAULT_SIZE];
  }

  public boolean validate() {
    Preconditions.checkState(seriesCount > 0);
    Preconditions.checkState(seriesCount <= currentBatchSize);
    Preconditions.checkState(nonNullKeyCount >= 0);
    Preconditions.checkState(nonNullKeyCount <= seriesCount);

    validateDuplicateCount();
    return true;
  }

  private void validateDuplicateCount() {
    int sum = 0;
    int duplicateCount;
    for (int i = 0; i < seriesCount; i++) {
      duplicateCount = duplicateCounts[i];
      Preconditions.checkState(duplicateCount > 0);
      Preconditions.checkState(duplicateCount <= currentBatchSize);
      sum += duplicateCount;
    }
    Preconditions.checkState(sum == currentBatchSize);
  }

  @Override
  public void positionToFirst() {
    seriesPosition = 0;

    currentLogical = 0;
    currentDuplicateCount = duplicateCounts[0];
    currentIsAllNull = seriesIsAllNull[0];

    if (!currentIsAllNull) {
      nonNullKeyPosition = 0;
      currentHashCode = hashCodes[0];
      setNextNonNullKey(0);
    } else {
      nonNullKeyPosition = -1;
    }
    Preconditions.checkState(currentDuplicateCount > 0);
  }

  // Consumes whole key.
  @Override
  public boolean next() {

    currentLogical += currentDuplicateCount;
    if (currentLogical >= currentBatchSize) {
      return false;
    }

    Preconditions.checkState(seriesPosition + 1 < seriesCount);

    seriesPosition++;
    currentDuplicateCount = duplicateCounts[seriesPosition];
    currentIsAllNull = seriesIsAllNull[seriesPosition];

    if (!currentIsAllNull) {
      Preconditions.checkState(nonNullKeyPosition + 1 < nonNullKeyCount);
      nonNullKeyPosition++;
      currentHashCode = hashCodes[nonNullKeyPosition];
      setNextNonNullKey(nonNullKeyPosition);
    }
    Preconditions.checkState(currentDuplicateCount > 0);
    return true;
  }

  // For use by VectorKeySeriesMulti so that the minimum equal key can be advanced.
  public void advance(int duplicateCount) {

    currentLogical += currentDuplicateCount;

    currentDuplicateCount -= duplicateCount;
    if (currentDuplicateCount == 0) {
      seriesPosition++;
      currentIsAllNull = seriesIsAllNull[seriesPosition];
      currentDuplicateCount = duplicateCounts[seriesPosition];

      if (!currentIsAllNull) {
        nonNullKeyPosition++;
        currentHashCode = hashCodes[nonNullKeyPosition];
        setNextNonNullKey(nonNullKeyPosition);
      }
    }
  }

  protected abstract void setNextNonNullKey(int nonNullKeyPosition);
}