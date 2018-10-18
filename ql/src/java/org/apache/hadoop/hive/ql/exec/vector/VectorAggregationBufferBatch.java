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

import java.util.Arrays;

import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.VectorAggregateExpression;
import org.apache.hadoop.hive.ql.util.JavaDataModel;

/**
 * This maps a batch to the aggregation buffers sets to use for each row (key)
 *
 */
public class VectorAggregationBufferBatch {

  /**
   * Batch sized array of aggregation buffer sets.
   * The array is preallocated and is reused for each batch, but the individual entries
   * will reference different aggregation buffer set from batch to batch.
   * the array is not reset between batches, content past this.index will be stale.
   */
  private final VectorAggregationBufferRow[] aggregationBuffers;

  /**
   * Same as aggregationBuffers but only distinct buffers
   */
  private final VectorAggregationBufferRow[] distinctAggregationBuffers;

  /**
   * versioning number gets incremented on each batch. This allows us to cache the selection
   * mapping info in the aggregation buffer set themselves while still being able to
   * detect stale info.
   */
  private int version;

  /**
   * Get the number of distinct aggregation buffer sets (ie. keys) used in current batch.
   */
  private int distinctCount;

  /**
   * Memory consumed by a set of aggregation buffers
   */
  private long aggregatorsFixedSize;

  /**
   * Array of indexes for aggregators that have variable size
   */
  private int[] variableSizeAggregators;;

  /**
   * returns True if any of the aggregators has a variable size
   * @return
   */
  public boolean getHasVariableSize() {
    return variableSizeAggregators.length > 0;
  }

  /**
   * Returns the fixed size consumed by the aggregation buffers
   * @return
   */
  public long getAggregatorsFixedSize() {
    return aggregatorsFixedSize;
  }

  /**
   * the array of aggregation buffers for the current batch.
   * content past the {@link #getDistinctBufferSetCount()} index
   * is stale from previous batches.
   * @return
   */
  public VectorAggregationBufferRow[] getAggregationBuffers() {
    return aggregationBuffers;
  }

  /**
   * number of distinct aggregation buffer sets (ie. keys) in the current batch.
   * @return
   */
  public int getDistinctBufferSetCount () {
    return distinctCount;
  }


  public VectorAggregationBufferBatch() {
    aggregationBuffers = new VectorAggregationBufferRow[VectorizedRowBatch.DEFAULT_SIZE];
    distinctAggregationBuffers = new VectorAggregationBufferRow[VectorizedRowBatch.DEFAULT_SIZE];
  }

  /**
   * resets the internal aggregation buffers sets index and increments the versioning
   * used to optimize the selection vector population.
   */
  public void startBatch() {
    version++;
    distinctCount = 0;
  }

  /**
   * assigns the given aggregation buffer set to a given batch row (by row number).
   * populates the selection vector appropriately. This is where the versioning numbers
   * play a role in determining if the index cached on the aggregation buffer set is stale.
   */
  public void mapAggregationBufferSet(VectorAggregationBufferRow bufferSet, int row) {
    if (version != bufferSet.getVersion()) {
      bufferSet.setVersionAndIndex(version, distinctCount);
      distinctAggregationBuffers[distinctCount] = bufferSet;
      ++distinctCount;
    }
    aggregationBuffers[row] = bufferSet;
  }

  public void compileAggregationBatchInfo(VectorAggregateExpression[] aggregators) {
    JavaDataModel model = JavaDataModel.get();
    int[] variableSizeAggregators = new int[aggregators.length];
    int indexVariableSizes = 0;

    aggregatorsFixedSize = JavaDataModel.alignUp(
        model.object() +
        model.primitive1()*2 +
        model.ref(),
        model.memoryAlign());

    aggregatorsFixedSize += model.lengthForObjectArrayOfSize(aggregators.length);
    for(int i=0;i<aggregators.length;++i) {
      VectorAggregateExpression aggregator = aggregators[i];
      aggregatorsFixedSize += aggregator.getAggregationBufferFixedSize();
      if (aggregator.hasVariableSize()) {
        variableSizeAggregators[indexVariableSizes] = i;
        ++indexVariableSizes;
      }
    }
    this.variableSizeAggregators = Arrays.copyOfRange(
        variableSizeAggregators, 0, indexVariableSizes);
  }

  public int getVariableSize(int batchSize) {
    int variableSize = 0;
    for (int i=0; i< variableSizeAggregators.length; ++i) {
      for(int r=0; r<distinctCount; ++r) {
         VectorAggregationBufferRow buf = distinctAggregationBuffers[r];
         variableSize += buf.getAggregationBuffer(variableSizeAggregators[i]).getVariableSize();
      }
    }
    return (variableSize * batchSize)/distinctCount;
  }

}
