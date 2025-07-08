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
package org.apache.hadoop.hive.ql.txn.compactor;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.hadoop.hive.ql.metadata.Table;

import java.util.concurrent.Callable;

public class HiveTableCache {

  private Cache<String, Table> cache;

  public HiveTableCache(boolean isCacheEnabled) {
    if (isCacheEnabled) {
      cache = Caffeine.newBuilder().softValues().build();
    }
  }

  public Table computeIfAbsent(String key, Callable<Table> callable) throws Exception {
    if (cache != null) {
      return cache.get(key, k -> {
        try {
          return callable.call();
        } catch (Exception e) {
          throw new CompactionException(e);
        }
      });
    }
    return callable.call();
  }

  public void invalidate() {
    if (cache != null) {
      cache.invalidateAll();
    }
  }
}
