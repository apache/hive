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

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hive.metastore.HMSConverter;
import org.apache.hadoop.hive.metastore.api.GetPartitionsByNamesRequest;
import org.apache.hadoop.hive.metastore.api.GetPartitionsByNamesResult;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class to handle HMS cache storage for partitions.
 */
public class PartitionCacheHelper {

  public enum Level {
    HS2,
    QUERY
  }

  private static final Logger LOG = LoggerFactory.getLogger(PartitionCacheHelper.class);

  // The HMS request
  private final GetPartitionsByNamesRequest rqst;

  // The string containing the type of cache (either HS2 or Query), used for logging.
  private final Level level;

  // The list of Partition objects made by the request that were found in the CacheValue.
  private final List<Partition> partitionsInCache;

  // The list of partition names that were not in the CacheValue and need to be fetched
  // from the HMS server.
  private final List<String> partitionNamesMissing;

  // Top level converter
  private final HMSConverter hmsConverter;

  // The converter object that converts an HMS object into an engine specific object.
  private final HMSPartitionConverter partitionConverter;

  private final Table table;

  public PartitionCacheHelper(GetPartitionsByNamesRequest rqst,
      HMSConverter hmsConverter, Table table, Level level) throws MetaException {
    this(null, rqst, hmsConverter, table, level);
  }

  /**
   * Constructor
   * @param cachedValue contains the cache holding all partitions previously fetched.
   * @param rqst the current request being made to HMS
   * @param hmsConverter a converter that converts the result into an engine specific result.
   * @param level enum used for debugging denoting the type of cache (i.e. "Query" or "HS2")
   */
  public PartitionCacheHelper(CacheValue cachedValue, GetPartitionsByNamesRequest rqst,
      HMSConverter hmsConverter, Table table, Level level) throws MetaException {
    this.rqst = rqst;
    this.level = level;
    Pair<List<Partition>, List<String>> foundAndMissingPartitionPair =
        analyzePartitionsInCache(rqst, cachedValue);
    partitionsInCache = foundAndMissingPartitionPair.getLeft();
    partitionNamesMissing = foundAndMissingPartitionPair.getRight();
    this.hmsConverter = hmsConverter;
    this.partitionConverter =
        (hmsConverter == null) ? null : hmsConverter.getPartitionConverter(rqst, table);
    this.table = table;
  }

  public boolean cacheContainsAllPartitions() {
    return partitionNamesMissing.isEmpty();
  }

  public GetPartitionsByNamesRequest getMissingNamesRequest() {
    GetPartitionsByNamesRequest newRqst = new GetPartitionsByNamesRequest(rqst);
    newRqst.setNames(partitionNamesMissing);
    return newRqst;
  }

  /**
   * Add new result values into cache.  This method mutates the cacheValue variable that
   * is passed in.
   */
  public void addToCache(GetPartitionsByNamesRequest missingNamesRequest,
      GetPartitionsByNamesResult missingNamesResult, CacheValue cacheValue) throws MetaException {
    Preconditions.checkState(missingNamesRequest.getNames().size() ==
        missingNamesResult.getPartitions().size());
    for (int i = 0; i < missingNamesRequest.getNames().size(); ++i) {
      String name = missingNamesRequest.getNames().get(i);
      Partition partition = missingNamesResult.getPartitions().get(i);
      cacheValue.partitionsMap.put(name, partition);
    }
    if (partitionConverter != null) {
      this.partitionConverter.addToPartitionCache(missingNamesRequest, cacheValue,
          missingNamesResult);
    }
  }

  /**
   * Analyze the partitions in the cache, breaking the request partition names into ones
   * that are found in the cache and ones that are not.
   */
  private Pair<List<Partition>, List<String>> analyzePartitionsInCache(
      GetPartitionsByNamesRequest rqst, CacheValue cachedValue) throws MetaException {
    List<Partition> partitionsFound = new ArrayList<>();
    List<String> partitionNamesMissing = new ArrayList<>();
    List<String> partitionNamesFound = new ArrayList<>();
    if (cachedValue == null) {
      // no cached partitions exist, so all requested partitions are missing
      partitionNamesMissing.addAll(rqst.getNames());
      return Pair.of(partitionsFound, partitionNamesMissing);
    }

    for (String partitionName : rqst.getNames()) {
      Partition cachedPartition = cachedValue.partitionsMap.get(partitionName);
      if (cachedPartition == null) {
        partitionNamesMissing.add(partitionName);
      } else {
        partitionsFound.add(cachedPartition);
        partitionNamesFound.add(partitionName);
      }
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("{} level HMS cache: method=analyzePartitionsInCache, dbName={}, tblName={}, " +
          "fileMetadata={}, partitionNames hit={}", level, rqst.getDb_name(),
          rqst.getTbl_name(), rqst.isGetFileMetadata(), partitionNamesFound);
      LOG.debug("{} level HMS cache miss: method=analyzePartitionsInCache, dbName={}, " +
          "tblName={}, fileMetadata={}, partitionNames missing={}" + level, rqst.getDb_name(),
          rqst.getTbl_name(), rqst.isGetFileMetadata(), partitionNamesMissing);
    }

    return Pair.of(partitionsFound, partitionNamesMissing);
  }

  /**
   * Compute the requested result.
   */
  public GetPartitionsByNamesResult fetchRequestedResultFromCache(CacheValue cacheValue
      ) throws MetaException {
    List<Partition> resultPartitions = new ArrayList<>();
    // iterate through request names and retrieve item out of cache.
    for (String partitionName : rqst.getNames()) {
      resultPartitions.add(cacheValue.partitionsMap.get(partitionName));
    }
    GetPartitionsByNamesResult result = new GetPartitionsByNamesResult(resultPartitions);
    // If the converter exists, return the converted result.
    if (partitionConverter != null) {
      result = partitionConverter.createPartitionsByNamesResult(result, cacheValue);
    }

    return result;
  }

  /**
   * Convert a result when there is no cache.
   */
  public GetPartitionsByNamesResult fetchRequestedResult(
      GetPartitionsByNamesResult result) throws MetaException {
    if (partitionConverter == null) {
      return result;
    }

    // Create a temporary cache value which will hold all the conversions for the
    // partitions for this request.
    CacheValue cacheValue = createCacheValue(rqst, hmsConverter, table, false);
    partitionConverter.addToPartitionCache(rqst, cacheValue, result);
    return partitionConverter.createPartitionsByNamesResult(result, cacheValue);
  }

  /**
   * Class that contains all informaiton needed to handle a request to retrieve the partitions
   * for a table.
   */
  public static class CacheValue {
    // Store a map that allows easy retrieval for the stored partitions.
    public ConcurrentHashMap<String, Partition> partitionsMap = new ConcurrentHashMap<>();
  }

  /**
   * create an initial empty cache value. If there is a partition converter, the
   * derived partition converter class needs to instantiate the object.
   */
  public static CacheValue createCacheValue(GetPartitionsByNamesRequest rqst,
      HMSConverter converter, Table table, boolean isConcurrent) {
    if (converter == null) {
      return new CacheValue();
    }

    HMSPartitionConverter partitionConverter = converter.getPartitionConverter(rqst, table);
    return (partitionConverter == null)
        ? new CacheValue()
        : partitionConverter.createConverterPartitionCacheValue(isConcurrent);
  }
}
