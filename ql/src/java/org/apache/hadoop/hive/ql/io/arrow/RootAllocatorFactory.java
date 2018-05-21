/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.io.arrow;

import org.apache.arrow.memory.RootAllocator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_ARROW_ROOT_ALLOCATOR_LIMIT;

/**
 * Thread-safe singleton factory for RootAllocator
 */
public enum RootAllocatorFactory {
  INSTANCE;

  private RootAllocator rootAllocator;

  RootAllocatorFactory() {
  }

  public synchronized RootAllocator getRootAllocator(Configuration conf) {
    if (rootAllocator == null) {
      final long limit = HiveConf.getLongVar(conf, HIVE_ARROW_ROOT_ALLOCATOR_LIMIT);
      rootAllocator = new RootAllocator(limit);
    }
    return rootAllocator;
  }

  //arrowAllocatorLimit is ignored if an allocator was previously created
  public synchronized RootAllocator getOrCreateRootAllocator(long arrowAllocatorLimit) {
    if (rootAllocator == null) {
      rootAllocator = new RootAllocator(arrowAllocatorLimit);
    }
    return rootAllocator;
  }
}
