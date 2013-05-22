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

import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.VectorAggregateExpression;

/**
 * Represents a set of aggregation buffers to be used for a specific key for UDAF GROUP BY.
 *
 */
public class VectorAggregationBufferRow {
  private VectorAggregateExpression.AggregationBuffer[] aggregationBuffers;
  private int version;
  private int index;
  
  public VectorAggregationBufferRow(
      VectorAggregateExpression.AggregationBuffer[] aggregationBuffers) {
    this.aggregationBuffers = aggregationBuffers;
  }
  
  /**
   * returns the aggregation buffer for an aggregation expression, by index.
   */
  public VectorAggregateExpression.AggregationBuffer getAggregationBuffer(int bufferIndex) {
    return aggregationBuffers[bufferIndex];
  }

  /**
   * returns the array of aggregation buffers (the entire set).
   */
  public VectorAggregateExpression.AggregationBuffer[] getAggregationBuffers() {
    return aggregationBuffers;
  }

  /** 
   * Versioning used to detect staleness of the index cached for benefit of
   * {@link org.apache.hadoop.hive.ql.exec.vector.VectorAggregationBufferBatch VectorAggregationBufferBatch}.
   */
  public int getVersion() {
    return version;
  }
  
  /**
   * cached index used by VectorAggregationBufferBatch.
   * @return
   */
  public int getIndex() {
    return index;
  }

  /**
   * accessor for VectorAggregationBufferBatch to set its caching info on this set.
   */
  public void setVersionAndIndex(int version, int index) {
    this.index  = index;
    this.version = version;
  }
  
}
