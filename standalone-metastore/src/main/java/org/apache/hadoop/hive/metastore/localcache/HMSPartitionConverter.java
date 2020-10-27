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

package org.apache.hadoop.hive.metastore.localcache;

import org.apache.hadoop.hive.metastore.api.GetPartitionsByNamesRequest;
import org.apache.hadoop.hive.metastore.api.GetPartitionsByNamesResult;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.GetTableResult;
import org.apache.hadoop.hive.metastore.api.MetaException;

import java.util.List;

/**
 * Interface to convert a HMS fetched result into an engine specific converted
 * Result.
 */
public interface HMSPartitionConverter {

  /**
   * Create the converted result given a result and the cache value which stores objects
   * needed for the conversion.
   */
  public GetPartitionsByNamesResult createPartitionsByNamesResult(
      GetPartitionsByNamesResult result,
      PartitionCacheHelper.CacheValue cacheValue) throws MetaException;

  /**
   * Create an initial empty cache object.
   */
  public PartitionCacheHelper.CacheValue createConverterPartitionCacheValue(boolean isConcurrent);

  /**
   * Add the given names requested with its result to the given cacheValue.
   */
  public void addToPartitionCache(GetPartitionsByNamesRequest missingNamesRqst,
      PartitionCacheHelper.CacheValue cachedValue, GetPartitionsByNamesResult missingNamesResult
      ) throws MetaException;
}
