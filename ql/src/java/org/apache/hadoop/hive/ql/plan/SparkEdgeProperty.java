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

package org.apache.hadoop.hive.ql.plan;


@Explain(displayName = "Edge Property")
public class SparkEdgeProperty {
  public static long SHUFFLE_NONE = 0; // No shuffle is needed. For union only.
  public static long SHUFFLE_GROUP = 1; // Shuffle, keys are coming together
  public static long SHUFFLE_SORT = 2;  // Shuffle, keys are sorted

  private long edgeType;
  
  private int numPartitions;

  public SparkEdgeProperty(long edgeType, int numPartitions) {
    this.edgeType = edgeType;
    this.numPartitions = numPartitions;
  }

  public SparkEdgeProperty(long edgeType) {
    this.edgeType = edgeType;
  }

  public boolean isShuffleNone() {
    return edgeType == SHUFFLE_NONE;
  }
  
  public void setShuffleNone() {
    edgeType = SHUFFLE_NONE;
  }

  public boolean isShuffleGroup() {
    return (edgeType & SHUFFLE_GROUP) != 0;
  }
  
  public void setShuffleGroup() {
    edgeType |= SHUFFLE_GROUP;
  }
  
  public boolean isShuffleSort() {
    return (edgeType & SHUFFLE_SORT) != 0;
  }

  public void setShuffleSort() {
    edgeType |= SHUFFLE_SORT;
  }
  
  public long getEdgeType() {
    return edgeType;
  }

  @Explain(displayName = "Shuffle Type")
  public String getShuffleType() {
    if (isShuffleNone()) {
      return "NONE";
    }
    
    StringBuilder sb = new StringBuilder();
    if (isShuffleGroup()) {
      sb.append("GROUP");
    }

    if (sb.length() != 0) {
      sb.append(" ");
    }

    if (isShuffleSort()) {
      sb.append("SORT");
    }

    return sb.toString();
  }

  public int getNumPartitions() {
    return numPartitions;
  }

  public void setNumPartitions(int numPartitions) {
    this.numPartitions = numPartitions;
  }
}

