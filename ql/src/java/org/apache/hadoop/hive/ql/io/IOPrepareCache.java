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

package org.apache.hadoop.hive.ql.io;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;

/**
 * IOPrepareCache is used to cache pre-query io-related objects.
 * It should be cleared every time a new query issued.
 *  
 */
public class IOPrepareCache {
  
  private static ThreadLocal<IOPrepareCache> threadLocalIOPrepareCache = new ThreadLocal<IOPrepareCache>();
  
  public static IOPrepareCache get() {
    IOPrepareCache cache = IOPrepareCache.threadLocalIOPrepareCache.get();
    if (cache == null) {
      threadLocalIOPrepareCache.set(new IOPrepareCache());
      cache = IOPrepareCache.threadLocalIOPrepareCache.get();
    }

    return cache;
  }
  
  public void clear() {
    if(partitionDescMap != null) {
      partitionDescMap.clear();      
    }
  }
  
  private Map<Map<Path, PartitionDesc>, Map<Path, PartitionDesc>> partitionDescMap;
  
  public Map<Map<Path, PartitionDesc>, Map<Path, PartitionDesc>> allocatePartitionDescMap() {
    if (partitionDescMap == null) {
      partitionDescMap = new HashMap<>();
    }
    return partitionDescMap;
  }

  public Map<Map<Path, PartitionDesc>, Map<Path, PartitionDesc>> getPartitionDescMap() {
    return partitionDescMap;
  }

  public void setPartitionDescMap(
      Map<Map<Path, PartitionDesc>, Map<Path, PartitionDesc>> partitionDescMap) {
    this.partitionDescMap = partitionDescMap;
  } 

}
