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
import org.apache.thrift.TBase;

import java.util.concurrent.Callable;

public class MetadataCache {

  private Cache<String, TBase> metaCache;

  public MetadataCache(boolean isCacheEnabled) {
    if (isCacheEnabled) {
      metaCache = Caffeine.newBuilder().softValues().build();
    }
  }

  public <T extends TBase<T,?>> T computeIfAbsent(String key, Callable<T> callable) throws Exception {
    if (metaCache != null) {
      return (T) metaCache.get(key, k -> {
        try {
          return callable.call();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      });
    }
    return callable.call();
  }

  public void invalidate() {
    if (metaCache != null) {
      metaCache.invalidateAll();
    }
  }
}
