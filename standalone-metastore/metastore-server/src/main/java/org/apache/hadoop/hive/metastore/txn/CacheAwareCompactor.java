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
package org.apache.hadoop.hive.metastore.txn;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.UncheckedExecutionException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public interface CacheAwareCompactor {

  static void trySetCache(Object obj, CompactorMetadataCache cache) {
    if (CacheAwareCompactor.class.isAssignableFrom(obj.getClass())) {
      ((CacheAwareCompactor) obj).setCache(cache);
    }
  }

  void setCache(CompactorMetadataCache cache);

  class CompactorMetadataCache {

    private final Cache<String, Table> tableCache;

    @VisibleForTesting
    public CompactorMetadataCache(long timeout, TimeUnit unit) {
      this.tableCache = CacheBuilder.newBuilder().expireAfterAccess(timeout, unit).softValues().build();
    }

    public static CompactorMetadataCache createIfEnabled(Configuration conf) {
      long timeout = MetastoreConf.getTimeVar(conf,
        MetastoreConf.ConfVars.COMPACTOR_METADATA_CACHE_TIMEOUT, TimeUnit.SECONDS);
      if (timeout == 0) {
        return null;
      }
      return new CompactorMetadataCache(timeout, TimeUnit.SECONDS);
    }

    public Table resolveTable(CompactionInfo ci, Callable<Table> loader) {
      try {
        return tableCache.get(ci.getFullTableName(), loader);
      } catch (ExecutionException e) {
        throw new UncheckedExecutionException(e);
      }
    }

    public void invalidateAll() {
      tableCache.invalidateAll();
    }
  }
}
