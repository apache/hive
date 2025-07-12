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

/**
 * IOPrepareCache is used to cache pre-query io-related objects.
 * It should be cleared every time a new query issued.
 *  
 */
public class IOPrepareCache {
  
  private static final ThreadLocal<IOPrepareCache> threadLocalIOPrepareCache =
      new ThreadLocal<>();
  
  public static IOPrepareCache get() {
    IOPrepareCache cache = IOPrepareCache.threadLocalIOPrepareCache.get();
    if (cache == null) {
      threadLocalIOPrepareCache.set(new IOPrepareCache());
      cache = IOPrepareCache.threadLocalIOPrepareCache.get();
    }
    return cache;
  }
  
  private Map<Path, Path> partitionDescMap;
  
  public Map<Path, Path> allocatePartitionDescMap() {
    if (partitionDescMap == null) {
      partitionDescMap = new HashMap<>();
    }
    return partitionDescMap;
  }

  public Map<Path, Path> getPartitionDescMap() {
    return partitionDescMap;
  }

  public void clear() {
    if (partitionDescMap != null) {
      partitionDescMap.clear();
    }
  }
}
