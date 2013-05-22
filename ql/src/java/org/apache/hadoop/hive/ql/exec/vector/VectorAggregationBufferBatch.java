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
  private VectorAggregationBufferRow[] aggregationBuffers;
  
  /**
   * the selection vector that maps row within a batch to the 
   * specific aggregation buffer set to use. 
   */
  private int[] selection;
  
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

  /**
   * gets the selection vector to use for the current batch. This maps the batch rows by position 
   * (row number) to an index in the {@link #getAggregationBuffers()} array.
   * @return
   */
  public int[] getSelectionVector() {
    return selection;
  }
  
  public VectorAggregationBufferBatch() {
    aggregationBuffers = new VectorAggregationBufferRow[VectorizedRowBatch.DEFAULT_SIZE];
    selection = new int [VectorizedRowBatch.DEFAULT_SIZE];
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
      ++distinctCount;
    }
    aggregationBuffers[row] = bufferSet;
  }

}
