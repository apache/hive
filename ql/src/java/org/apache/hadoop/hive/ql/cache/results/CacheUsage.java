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

package org.apache.hadoop.hive.ql.cache.results;

import org.apache.hadoop.hive.ql.cache.results.QueryResultsCache.CacheEntry;
import org.apache.hadoop.hive.ql.cache.results.QueryResultsCache.QueryInfo;

/**
 * Helper class during semantic analysis that indicates if the query can use the cache,
 * or if the results from the query can be added to the results cache.
 */
public class CacheUsage {

  public enum CacheStatus {
    CACHE_NOT_USED,
    QUERY_USING_CACHE,
    CAN_CACHE_QUERY_RESULTS,
  };

  private CacheUsage.CacheStatus status;
  private CacheEntry cacheEntry;
  private QueryInfo queryInfo;

  public CacheUsage(CacheStatus status, CacheEntry cacheEntry) {
    this.status = status;
    this.cacheEntry = cacheEntry;
  }

  public CacheUsage(CacheStatus status, QueryInfo queryInfo) {
    this.status = status;
    this.queryInfo = queryInfo;
  }

  public CacheUsage.CacheStatus getStatus() {
    return status;
  }

  public void setStatus(CacheUsage.CacheStatus status) {
    this.status = status;
  }

  public CacheEntry getCacheEntry() {
    return cacheEntry;
  }

  public void setCacheEntry(CacheEntry cacheEntry) {
    this.cacheEntry = cacheEntry;
  }

  public QueryInfo getQueryInfo() {
    return queryInfo;
  }

  public void setQueryInfo(QueryInfo queryInfo) {
    this.queryInfo = queryInfo;
  }
}
