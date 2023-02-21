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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.thrift.TBase;

import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

public class CacheContainer {

  private Optional<Cache<String, TBase>> metaCache = Optional.empty();

  public <T extends TBase<T,?>> T computeIfAbsent(String key, Callable<T> callable) throws Exception {
    if (metaCache.isPresent()) {
      try {
        return (T) metaCache.get().get(key, callable);
      } catch (ExecutionException e) {
        throw (Exception) e.getCause();
      }
    }
    return callable.call();
  }

  public Optional<Cache<String, TBase>> initializeCache(boolean tableCacheOn) {
    if (tableCacheOn) {
      metaCache = Optional.of(CacheBuilder.newBuilder().softValues().build());
    }
    return metaCache;
  }

  public void invalidateMetaCache() {
    metaCache.ifPresent(Cache::invalidateAll);
  }
}
