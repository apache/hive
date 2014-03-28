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

package org.apache.hadoop.hive.ql.exec.tez;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.Map.Entry;

import org.apache.hadoop.io.Writable;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;

class CustomEdgeConfiguration implements Writable {
  boolean vertexInited = false;
  int numBuckets = -1;
  Multimap<Integer, Integer> bucketToTaskMap = null;
  
  public CustomEdgeConfiguration() {
  }
  
  public CustomEdgeConfiguration(int numBuckets, Multimap<Integer, Integer> routingTable) {
    this.bucketToTaskMap = routingTable;
    this.numBuckets = numBuckets;
    if (routingTable != null) {
      vertexInited = true;
    }
  }
  
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeBoolean(vertexInited);
    out.writeInt(numBuckets);
    if (bucketToTaskMap == null) {
      return;
    }
    
    out.writeInt(bucketToTaskMap.size());
    for (Entry<Integer, Collection<Integer>> entry : bucketToTaskMap.asMap().entrySet()) {
      int bucketNum = entry.getKey();
      for (Integer taskId : entry.getValue()) {
        out.writeInt(bucketNum);
        out.writeInt(taskId);
      }
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.vertexInited = in.readBoolean();
    this.numBuckets = in.readInt();
    if (this.vertexInited == false) {
      return;
    }

    int count = in.readInt();
    bucketToTaskMap = LinkedListMultimap.create();
    for (int i = 0; i < count; i++) {
      bucketToTaskMap.put(in.readInt(), in.readInt());
    }

    if (count != bucketToTaskMap.size()) {
      throw new IOException("Was not a clean translation. Some records are missing");
    }
  }

  public Multimap<Integer, Integer> getRoutingTable() {
    return bucketToTaskMap;
  }

  public int getNumBuckets() {
    return numBuckets;
  }
}