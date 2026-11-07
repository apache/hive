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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.ql.plan;

public class TezEdgeProperty {

  public enum EdgeType {//todo: HIVE-15549
    SIMPLE_EDGE,//SORT_PARTITION_EDGE
    BROADCAST_EDGE,
    CONTAINS,//used for union (all?)
    CUSTOM_EDGE,//CO_PARTITION_EDGE
    CUSTOM_SIMPLE_EDGE,//PARTITION_EDGE
    ONE_TO_ONE_EDGE,
    XPROD_EDGE
  }

  private EdgeType edgeType;
  private int numBuckets;
  private Integer bufferSize;

  public TezEdgeProperty(EdgeType edgeType, int buckets) {
    this.edgeType = edgeType;
    this.numBuckets = buckets;
  }

  public TezEdgeProperty(EdgeType edgeType) {
    this(edgeType, -1);
  }

  public EdgeType getEdgeType() {
    return edgeType;
  }

  public int getNumBuckets() {
    return numBuckets;
  }

  public void setBufferSize(Integer bufferSize) {
    this.bufferSize = bufferSize;
  }

  public Integer getBufferSize() {
    return bufferSize;
  }

  public void setEdgeType(EdgeType type) {
    this.edgeType = type;
  }

}
